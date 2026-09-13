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
  CodexBrokerRequestError,
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

export type { CodexBrokerFailureKind };

export interface CodexBrokerLeaseSelection {
  readonly lease: CodexBrokerLease;
  readonly accountChanged: boolean;
}

export interface CodexBrokerSession extends CodexBrokerAuthContext {
  readonly brokerSessionId: string;
  readonly beginLogicalTurn: (
    brokerTurnId: string,
  ) => Effect.Effect<CodexBrokerLeaseSelection, CodexBrokerAuthError>;
  readonly reportTerminalFailure: (
    brokerTurnId: string,
    kind: CodexBrokerFailureKind,
  ) => Effect.Effect<CodexBrokerLeaseSelection, CodexBrokerAuthError>;
  readonly completeLogicalTurn: (brokerTurnId: string) => Effect.Effect<void>;
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
  readonly newInteractiveSession: (
    threadId: string,
  ) => Effect.Effect<CodexBrokerSession, CodexBrokerAuthError>;
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

const routeLeaseWaiting = Effect.fn("CodexBrokerAuth.routeLeaseWaiting")(function* (
  client: CodexBrokerClient,
  input: CodexBrokerRouteInput,
) {
  let nextInput = input;
  while (true) {
    const result = yield* client.route(nextInput);
    if (result.status === "ok") return result;
    yield* Effect.sleep(Duration.seconds(Math.max(0, result.retryAfterSeconds)));
    nextInput = {
      sessionId: input.sessionId,
      turnId: input.turnId,
      ...(input.preferredAccountId ? { preferredAccountId: input.preferredAccountId } : {}),
    };
  }
});

function makeAuthContext(input: {
  readonly client: CodexBrokerClient;
  readonly sessionId: string;
  readonly initialTurnId: string;
  readonly initialLease: CodexBrokerLease;
}): Effect.Effect<CodexBrokerSession> {
  return Effect.gen(function* () {
    const leaseRef = yield* Ref.make(input.initialLease);
    const activeTurnIdRef = yield* Ref.make<string | undefined>(input.initialTurnId);
    const pendingReplacementRef = yield* Ref.make<CodexBrokerLease | undefined>(undefined);

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
          const activeTurnId = yield* Ref.get(activeTurnIdRef);
          const replacement = yield* routeLease(
            input.client,
            {
              sessionId: input.sessionId,
              turnId: activeTurnId ?? input.initialTurnId,
              preferredAccountId: current.accountId,
              failedAccountId: current.accountId,
              failureKind: "auth",
            },
            REFRESH_TIMEOUT,
          );
          if (replacement.accountId !== current.accountId) {
            yield* Ref.set(pendingReplacementRef, replacement);
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

    const select = Effect.fn("CodexBrokerAuth.select")(function* (
      brokerTurnId: string,
      failureKind?: CodexBrokerFailureKind,
    ) {
      const current = yield* Ref.get(leaseRef);
      const pending = failureKind
        ? yield* Ref.getAndSet(pendingReplacementRef, undefined)
        : undefined;
      const replacement =
        pending ??
        (yield* routeLeaseWaiting(input.client, {
          sessionId: input.sessionId,
          turnId: brokerTurnId,
          preferredAccountId: current.accountId,
          ...(failureKind ? { failedAccountId: current.accountId, failureKind } : {}),
        }));
      if (
        failureKind !== undefined &&
        failureKind !== "auth" &&
        replacement.accountId === current.accountId
      ) {
        return yield* new CodexBrokerRequestError({
          message: "Codex Broker returned the failed account again.",
        });
      }
      yield* Ref.set(activeTurnIdRef, brokerTurnId);
      yield* Ref.set(leaseRef, replacement);
      return {
        lease: replacement,
        accountChanged: replacement.accountId !== current.accountId,
      };
    });

    return {
      brokerSessionId: input.sessionId,
      lease: Ref.get(leaseRef),
      applyLogin,
      registerRefreshHandler,
      beginLogicalTurn: (brokerTurnId) => select(brokerTurnId),
      reportTerminalFailure: (brokerTurnId, kind) => select(brokerTurnId, kind),
      completeLogicalTurn: (brokerTurnId) =>
        Ref.update(activeTurnIdRef, (active) => (active === brokerTurnId ? undefined : active)),
    };
  });
}

export function makeCodexBrokerIntegration(
  config: CodexBrokerConfig,
  instanceId: string,
  client: CodexBrokerClient = makeCodexBrokerClient(config),
): CodexBrokerIntegration {
  const acquire = Effect.fn("CodexBrokerAuth.acquire")(function* (
    sessionId: string,
    turnId: string,
  ) {
    const initialLease = yield* routeLease(client, { sessionId, turnId });
    return yield* makeAuthContext({ client, sessionId, initialTurnId: turnId, initialLease });
  });

  return {
    instanceId,
    client,
    acquireEphemeralAuth: Effect.fn("CodexBrokerAuth.acquireEphemeralAuth")(function* (operation) {
      const nonce = NodeCrypto.randomUUID();
      return yield* acquire(brokerId("t3-util", instanceId, operation, nonce), nonce);
    }),
    newInteractiveSession: Effect.fn("CodexBrokerAuth.newInteractiveSession")(function* (threadId) {
      const bootstrapTurnId = NodeCrypto.randomUUID();
      return yield* acquire(brokerId("t3", instanceId, threadId), bootstrapTurnId);
    }),
  };
}
