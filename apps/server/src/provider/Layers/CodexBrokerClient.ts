import * as NodeFSP from "node:fs/promises";
import * as NodeHttp from "node:http";
import * as NodeHttps from "node:https";

import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

const MAX_RESPONSE_BYTES = 64 * 1024;
const DEFAULT_REQUEST_TIMEOUT = Duration.seconds(60);

export interface CodexBrokerConfig {
  readonly url: URL;
  readonly clientKey: string;
  readonly caCertPath?: string;
}

export interface CodexBrokerLease {
  readonly status: "ok";
  readonly accountId: string;
  readonly accountLabel: string;
  readonly accessToken: string;
  readonly chatgptAccountId: string;
  readonly expiresAt: string;
  readonly shortRemainingPercent: number | null;
  readonly weeklyRemainingPercent: number | null;
  readonly shortResetsAt: string | null;
  readonly weeklyResetsAt: string | null;
}

export interface CodexBrokerWait {
  readonly status: "wait";
  readonly code: string;
  readonly nextRetryAt: string | null;
  readonly retryAfterSeconds: number;
}

export type CodexBrokerFailureKind = "quota" | "auth" | "rate_limit";

export interface CodexBrokerRouteInput {
  readonly sessionId: string;
  readonly turnId: string;
  readonly preferredAccountId?: string;
  readonly failedAccountId?: string;
  readonly failureKind?: CodexBrokerFailureKind;
}

export class CodexBrokerConfigurationError extends Schema.TaggedErrorClass<CodexBrokerConfigurationError>()(
  "CodexBrokerConfigurationError",
  { message: Schema.String },
) {}

export class CodexBrokerRequestError extends Schema.TaggedErrorClass<CodexBrokerRequestError>()(
  "CodexBrokerRequestError",
  { message: Schema.String },
) {}

export type CodexBrokerError = CodexBrokerConfigurationError | CodexBrokerRequestError;

export interface CodexBrokerClient {
  readonly health: Effect.Effect<void, CodexBrokerRequestError>;
  readonly route: (
    input: CodexBrokerRouteInput,
    options?: { readonly timeout?: Duration.Duration },
  ) => Effect.Effect<CodexBrokerLease | CodexBrokerWait, CodexBrokerRequestError>;
}

const NullableString = Schema.Union([Schema.String, Schema.Null]);
const WireLeaseBase = {
  status: Schema.Literal("ok"),
  account_id: Schema.String,
  account_label: Schema.String,
  access_token: Schema.String,
  chatgpt_account_id: Schema.String,
  expires_at: Schema.String,
  short_remaining_percent: Schema.Union([Schema.Number, Schema.Null]),
  weekly_remaining_percent: Schema.Union([Schema.Number, Schema.Null]),
};
const WireLease = Schema.Union([
  Schema.Struct({
    ...WireLeaseBase,
    short_resets_at: NullableString,
    weekly_resets_at: NullableString,
  }),
  Schema.Struct(WireLeaseBase),
]);
const WireWait = Schema.Struct({
  status: Schema.Literal("wait"),
  code: Schema.String,
  next_retry_at: NullableString,
  retry_after_seconds: Schema.Number,
});
const decodeLease = Schema.decodeUnknownSync(WireLease, { onExcessProperty: "error" });
const decodeWait = Schema.decodeUnknownSync(WireWait, { onExcessProperty: "error" });

export function parseCodexBrokerConfig(
  environment: NodeJS.ProcessEnv,
): Effect.Effect<CodexBrokerConfig | undefined, CodexBrokerConfigurationError> {
  const url = environment.CODEX_BROKER_URL?.trim();
  const clientKey = environment.CODEX_BROKER_CLIENT_KEY?.trim();
  const caCertPath = environment.CODEX_BROKER_CA_CERT?.trim();

  if (!url && !clientKey) return Effect.succeed(undefined);
  if (!url || !clientKey) {
    return Effect.fail(
      new CodexBrokerConfigurationError({
        message: "CODEX_BROKER_URL and CODEX_BROKER_CLIENT_KEY must be configured together.",
      }),
    );
  }

  return Effect.try({
    try: () => {
      let parsedUrl: URL;
      try {
        parsedUrl = new URL(url);
      } catch {
        throw new Error("invalid URL");
      }
      if (parsedUrl.protocol !== "https:") {
        throw new Error("CODEX_BROKER_URL must use HTTPS.");
      }
      return {
        url: parsedUrl,
        clientKey,
        ...(caCertPath ? { caCertPath } : {}),
      };
    },
    catch: (cause) =>
      new CodexBrokerConfigurationError({
        message:
          cause instanceof Error && cause.message === "CODEX_BROKER_URL must use HTTPS."
            ? cause.message
            : "CODEX_BROKER_URL must be a valid HTTPS URL.",
      }),
  });
}

function sanitizedRequestError(message: string): CodexBrokerRequestError {
  return new CodexBrokerRequestError({ message });
}

function requestBuffer(input: {
  readonly config: CodexBrokerConfig;
  readonly path: string;
  readonly method?: "POST";
  readonly body?: string;
  readonly timeout: Duration.Duration;
}): Effect.Effect<
  { readonly statusCode: number | undefined; readonly body: Buffer },
  CodexBrokerRequestError
> {
  return Effect.tryPromise({
    try: async (signal) => {
      const ca = input.config.caCertPath
        ? await NodeFSP.readFile(input.config.caCertPath)
        : undefined;
      return await new Promise<{ readonly statusCode: number | undefined; readonly body: Buffer }>(
        (resolve, reject) => {
          let url: URL;
          try {
            url = new URL(input.path, input.config.url);
          } catch {
            reject(new Error("invalid request URL"));
            return;
          }
          const req = NodeHttps.request(
            url,
            {
              ...(input.method ? { method: input.method } : {}),
              ca,
              signal,
              headers: {
                authorization: `Bearer ${input.config.clientKey}`,
                ...(input.body
                  ? {
                      "content-type": "application/json",
                      "content-length": Buffer.byteLength(input.body),
                    }
                  : {}),
              },
            },
            (response: NodeHttp.IncomingMessage) => {
              const chunks: Buffer[] = [];
              let size = 0;
              response.on("data", (chunk: Buffer) => {
                size += chunk.length;
                if (size > MAX_RESPONSE_BYTES) {
                  response.destroy(new Error("response too large"));
                  return;
                }
                chunks.push(chunk);
              });
              response.on("error", reject);
              response.on("end", () =>
                resolve({ statusCode: response.statusCode, body: Buffer.concat(chunks) }),
              );
            },
          );
          req.setTimeout(Duration.toMillis(input.timeout), () =>
            req.destroy(new Error("request timed out")),
          );
          req.on("error", reject);
          req.end(input.body);
        },
      );
    },
    catch: () => sanitizedRequestError("Codex Broker request failed."),
  });
}

export function makeCodexBrokerClient(config: CodexBrokerConfig): CodexBrokerClient {
  const health = requestBuffer({
    config,
    path: "/api/v1/health",
    timeout: DEFAULT_REQUEST_TIMEOUT,
  }).pipe(
    Effect.flatMap(({ statusCode }) =>
      statusCode === 200
        ? Effect.void
        : Effect.fail(
            sanitizedRequestError(`Codex Broker health check failed (${statusCode ?? "unknown"}).`),
          ),
    ),
  );

  const route: CodexBrokerClient["route"] = (input, options) => {
    const hasFailureAccount = input.failedAccountId !== undefined;
    if (hasFailureAccount !== (input.failureKind !== undefined)) {
      return Effect.fail(
        sanitizedRequestError("Codex Broker failure account and kind must be provided together."),
      );
    }

    const body = JSON.stringify({
      session_id: input.sessionId,
      turn_id: input.turnId,
      ...(input.preferredAccountId ? { preferred_account_id: input.preferredAccountId } : {}),
      ...(input.failedAccountId ? { failed_account_id: input.failedAccountId } : {}),
      ...(input.failureKind ? { failure_kind: input.failureKind } : {}),
    });

    return requestBuffer({
      config,
      path: "/api/v1/route",
      method: "POST",
      body,
      timeout: options?.timeout ?? DEFAULT_REQUEST_TIMEOUT,
    }).pipe(
      Effect.flatMap(({ statusCode, body: responseBody }) =>
        Effect.try({
          try: (): CodexBrokerLease | CodexBrokerWait => {
            let parsed: unknown;
            try {
              parsed = JSON.parse(responseBody.toString("utf8"));
            } catch {
              throw new Error("invalid JSON");
            }
            if (statusCode === 200) {
              const lease = decodeLease(parsed);
              return {
                status: "ok",
                accountId: lease.account_id,
                accountLabel: lease.account_label,
                accessToken: lease.access_token,
                chatgptAccountId: lease.chatgpt_account_id,
                expiresAt: lease.expires_at,
                shortRemainingPercent: lease.short_remaining_percent,
                weeklyRemainingPercent: lease.weekly_remaining_percent,
                shortResetsAt: "short_resets_at" in lease ? lease.short_resets_at : null,
                weeklyResetsAt: "weekly_resets_at" in lease ? lease.weekly_resets_at : null,
              };
            }
            if (statusCode === 429) {
              const wait = decodeWait(parsed);
              return {
                status: "wait",
                code: wait.code,
                nextRetryAt: wait.next_retry_at,
                retryAfterSeconds: wait.retry_after_seconds,
              };
            }
            throw new Error("invalid response status");
          },
          catch: () =>
            sanitizedRequestError(`Invalid Codex Broker response (${statusCode ?? "unknown"}).`),
        }),
      ),
    );
  };

  return { health, route };
}
