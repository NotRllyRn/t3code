import * as NodeCrypto from "node:crypto";

import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import type * as CodexClient from "effect-codex-app-server/client";
import * as CodexErrors from "effect-codex-app-server/errors";

import {
  type CodexBrokerClient,
  type CodexBrokerFailureKind,
  type CodexBrokerLease,
  type CodexBrokerRouteInput,
  makeCodexBrokerClient,
  type CodexBrokerConfig,
} from "./CodexBrokerClient.ts";

const REFRESH_TIMEOUT = Duration.seconds(8);

export class CodexBrokerPoolExhaustedError extends Schema.TaggedErrorClass<CodexBrokerPoolExhaustedError>()(
  "CodexBrokerPoolExhaustedError",
  {
    message: Schema.String,
    retryAfterSeconds: Schema.Number,
    nextRetryAt: Schema.Union([Schema.String, Schema.Null]),
  },
) {}

export type CodexBrokerAuthError =
  | CodexBrokerPoolExhaustedError
  | import("./CodexBrokerClient.ts").CodexBrokerRequestError;

export interface CodexBrokerAuthContext {
  readonly lease: Effect.Effect<CodexBrokerLease>;
  readonly applyLogin: (
    client: CodexClient.CodexAppServerClient["Service"],
  ) => Effect.Effect<void, CodexErrors.CodexAppServerError>;
  readonly registerRefreshHandler: (
    client: CodexClient.CodexAppServerClient["Service"],
  ) => Effect.Effect<void>;
}

export const authenticateCodexAppServer = Effect.fn("CodexBrokerAuth.authenticateCodexAppServer")(
  function* (
    client: CodexClient.CodexAppServerClient["Service"],
    auth: CodexBrokerAuthContext | undefined,
  ) {
    if (!auth) return;
    yield* auth.registerRefreshHandler(client);
    yield* auth.applyLogin(client);
  },
);

export interface CodexBrokerIntegration {
  readonly instanceId: string;
  readonly client: CodexBrokerClient;
  readonly acquireEphemeralAuth: (
    operation: string,
  ) => Effect.Effect<CodexBrokerAuthContext, CodexBrokerAuthError>;
}

function brokerId(prefix: string, ...parts: ReadonlyArray<string>): string {
  const raw = [prefix, ...parts].join(":");
  if (raw.length <= 200) return raw;
  return `${prefix}:${NodeCrypto.createHash("sha256").update(raw).digest("hex")}`;
}

function poolExhausted(wait: {
  readonly retryAfterSeconds: number;
  readonly nextRetryAt: string | null;
}): CodexBrokerPoolExhaustedError {
  return new CodexBrokerPoolExhaustedError({
    message: "Codex Broker has no account available yet.",
    retryAfterSeconds: wait.retryAfterSeconds,
    nextRetryAt: wait.nextRetryAt,
  });
}

const routeLease = Effect.fn("CodexBrokerAuth.routeLease")(function* (
  client: CodexBrokerClient,
  input: CodexBrokerRouteInput,
  timeout?: Duration.Duration,
) {
  const result = yield* client.route(input, timeout ? { timeout } : undefined);
  if (result.status === "wait") return yield* poolExhausted(result);
  return result;
});

function makeAuthContext(input: {
  readonly client: CodexBrokerClient;
  readonly routeInput: CodexBrokerRouteInput;
  readonly initialLease: CodexBrokerLease;
}): Effect.Effect<CodexBrokerAuthContext> {
  return Effect.gen(function* () {
    const leaseRef = yield* Ref.make(input.initialLease);

    const applyLogin: CodexBrokerAuthContext["applyLogin"] = Effect.fn(
      "CodexBrokerAuth.applyLogin",
    )(function* (client) {
      const lease = yield* Ref.get(leaseRef);
      yield* client.request("account/login/start", {
        type: "chatgptAuthTokens",
        accessToken: lease.accessToken,
        chatgptAccountId: lease.chatgptAccountId,
      });
    });

    const registerRefreshHandler: CodexBrokerAuthContext["registerRefreshHandler"] = (client) =>
      client.handleServerRequest("account/chatgptAuthTokens/refresh", () =>
        Effect.gen(function* () {
          const current = yield* Ref.get(leaseRef);
          const replacement = yield* routeLease(
            input.client,
            {
              ...input.routeInput,
              preferredAccountId: current.accountId,
              failedAccountId: current.accountId,
              failureKind: "auth" satisfies CodexBrokerFailureKind,
            },
            REFRESH_TIMEOUT,
          );
          if (replacement.accountId !== current.accountId) {
            return yield* CodexErrors.CodexAppServerRequestError.internalError(
              "Codex Broker selected a replacement account; retry at a safe turn boundary.",
            );
          }
          yield* Ref.set(leaseRef, replacement);
          return {
            accessToken: replacement.accessToken,
            chatgptAccountId: replacement.chatgptAccountId,
          };
        }).pipe(
          Effect.timeout(REFRESH_TIMEOUT),
          Effect.mapError((error) =>
            error._tag === "CodexAppServerRequestError"
              ? error
              : CodexErrors.CodexAppServerRequestError.internalError(
                  "Codex Broker could not refresh external authentication.",
                ),
          ),
        ),
      );

    return {
      lease: Ref.get(leaseRef),
      applyLogin,
      registerRefreshHandler,
    };
  });
}

export function makeCodexBrokerIntegration(
  config: CodexBrokerConfig,
  instanceId: string,
  client: CodexBrokerClient = makeCodexBrokerClient(config),
): CodexBrokerIntegration {
  return {
    instanceId,
    client,
    acquireEphemeralAuth: Effect.fn("CodexBrokerAuth.acquireEphemeralAuth")(function* (operation) {
      const nonce = NodeCrypto.randomUUID();
      const routeInput = {
        sessionId: brokerId("t3-util", instanceId, operation, nonce),
        turnId: nonce,
      } satisfies CodexBrokerRouteInput;
      const initialLease = yield* routeLease(client, routeInput);
      return yield* makeAuthContext({ client, routeInput, initialLease });
    }),
  };
}
