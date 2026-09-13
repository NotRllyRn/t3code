import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import type * as CodexClient from "effect-codex-app-server/client";

import { makeCodexBrokerIntegration } from "./CodexBrokerAuth.ts";
import type {
  CodexBrokerClient,
  CodexBrokerLease,
  CodexBrokerRouteInput,
} from "./CodexBrokerClient.ts";

const lease = (accountId: string, token: string): CodexBrokerLease => ({
  status: "ok",
  accountId,
  accountLabel: "Account",
  accessToken: token,
  chatgptAccountId: `chatgpt-${accountId}`,
  expiresAt: "2099-01-01T00:00:00Z",
  shortRemainingPercent: 90,
  weeklyRemainingPercent: 80,
  shortResetsAt: null,
  weeklyResetsAt: null,
});

it.effect("maps broker identities to external auth and refreshes only the same account", () =>
  Effect.gen(function* () {
    const routed: CodexBrokerRouteInput[] = [];
    const leases = [lease("broker-a", "token-1"), lease("broker-a", "token-2")];
    const broker: CodexBrokerClient = {
      health: Effect.void,
      route: (input) => {
        routed.push(input);
        const next = leases.shift();
        return next ? Effect.succeed(next) : Effect.die("missing test lease");
      },
    };
    const integration = makeCodexBrokerIntegration(
      { url: new URL("https://broker.test"), clientKey: "secret" },
      "instance",
      broker,
    );
    const auth = yield* integration.acquireEphemeralAuth("probe");

    const logins: unknown[] = [];
    let refresh: (() => Effect.Effect<unknown, unknown>) | undefined;
    const appClient = {
      request: (_method: string, payload: unknown) => {
        logins.push(payload);
        return Effect.succeed({});
      },
      handleServerRequest: (_method: string, handler: () => Effect.Effect<unknown, unknown>) =>
        Effect.sync(() => {
          refresh = handler;
        }),
    } as unknown as CodexClient.CodexAppServerClient["Service"];

    yield* auth.registerRefreshHandler(appClient);
    yield* auth.applyLogin(appClient);
    assert.deepStrictEqual(logins, [
      {
        type: "chatgptAuthTokens",
        accessToken: "token-1",
        chatgptAccountId: "chatgpt-broker-a",
      },
    ]);
    assert.isDefined(refresh);
    assert.deepStrictEqual(yield* refresh(), {
      accessToken: "token-2",
      chatgptAccountId: "chatgpt-broker-a",
    });
    assert.strictEqual(routed[1]?.failedAccountId, "broker-a");
    assert.strictEqual(routed[1]?.failureKind, "auth");
  }),
);

it.effect("rejects a mid-request account switch", () =>
  Effect.gen(function* () {
    const leases = [lease("broker-a", "token-1"), lease("broker-b", "token-2")];
    const broker: CodexBrokerClient = {
      health: Effect.void,
      route: () => {
        const next = leases.shift();
        return next ? Effect.succeed(next) : Effect.die("missing test lease");
      },
    };
    const auth = yield* makeCodexBrokerIntegration(
      { url: new URL("https://broker.test"), clientKey: "secret" },
      "instance",
      broker,
    ).acquireEphemeralAuth("probe");

    let refresh: (() => Effect.Effect<unknown, unknown>) | undefined;
    const appClient = {
      handleServerRequest: (_method: string, handler: () => Effect.Effect<unknown, unknown>) =>
        Effect.sync(() => {
          refresh = handler;
        }),
    } as unknown as CodexClient.CodexAppServerClient["Service"];
    yield* auth.registerRefreshHandler(appClient);
    assert.isDefined(refresh);
    assert.strictEqual((yield* Effect.exit(refresh()))._tag, "Failure");
  }),
);
