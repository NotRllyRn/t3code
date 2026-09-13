import * as NodeChildProcess from "node:child_process";
import * as NodeFSP from "node:fs/promises";
import * as NodeHttps from "node:https";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeUtil from "node:util";

import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";

import { makeCodexBrokerClient, parseCodexBrokerConfig } from "./CodexBrokerClient.ts";

const execFile = NodeUtil.promisify(NodeChildProcess.execFile);

it.effect("requires complete HTTPS configuration", () =>
  Effect.gen(function* () {
    assert.isUndefined(yield* parseCodexBrokerConfig({}));
    assert(
      Option.isNone(
        yield* Effect.option(parseCodexBrokerConfig({ CODEX_BROKER_URL: "https://broker.test" })),
      ),
    );
    assert(
      Option.isNone(
        yield* Effect.option(
          parseCodexBrokerConfig({
            CODEX_BROKER_URL: "http://broker.test",
            CODEX_BROKER_CLIENT_KEY: "secret",
          }),
        ),
      ),
    );
  }),
);

it.effect("uses bearer auth and a custom CA, validates responses, and sanitizes failures", () =>
  Effect.acquireUseRelease(
    Effect.promise(() => NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "t3-broker-test-"))),
    (directory) =>
      Effect.gen(function* () {
        const key = NodePath.join(directory, "server.key");
        const cert = NodePath.join(directory, "server.crt");
        yield* Effect.promise(() =>
          execFile("openssl", [
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-sha256",
            "-days",
            "1",
            "-subj",
            "/CN=127.0.0.1",
            "-addext",
            "subjectAltName=IP:127.0.0.1",
            "-keyout",
            key,
            "-out",
            cert,
          ]),
        );

        const server = NodeHttps.createServer(
          {
            key: yield* Effect.promise(() => NodeFSP.readFile(key)),
            cert: yield* Effect.promise(() => NodeFSP.readFile(cert)),
          },
          (request, response) => {
            assert.strictEqual(request.headers.authorization, "Bearer client-secret");
            if (request.url === "/api/v1/health") {
              response.writeHead(200).end();
              return;
            }

            let body = "";
            request.setEncoding("utf8");
            request.on("data", (chunk: string) => {
              body += chunk;
            });
            request.on("end", () => {
              let turnId = "";
              try {
                turnId = (JSON.parse(body) as { turn_id: string }).turn_id;
              } catch {
                response.writeHead(400).end();
                return;
              }
              if (turnId === "wait") {
                response.writeHead(429).end(
                  JSON.stringify({
                    status: "wait",
                    code: "POOL_EXHAUSTED",
                    next_retry_at: null,
                    retry_after_seconds: 12,
                  }),
                );
                return;
              }
              if (turnId === "large") {
                response
                  .writeHead(200)
                  .end(JSON.stringify({ padding: "access-secret".repeat(7_000) }));
                return;
              }
              response.writeHead(200).end(
                JSON.stringify({
                  status: "ok",
                  account_id: "public-account",
                  account_label: "Personal",
                  access_token: "access-secret",
                  chatgpt_account_id: "chatgpt-account",
                  expires_at: "2099-01-01T00:00:00Z",
                  short_remaining_percent: 80,
                  weekly_remaining_percent: null,
                }),
              );
            });
          },
        );

        yield* Effect.acquireUseRelease(
          Effect.callback<number, Error>((resume) => {
            server.once("error", (error) => resume(Effect.fail(error)));
            server.listen(0, "127.0.0.1", () => {
              const address = server.address();
              if (!address || typeof address === "string") {
                resume(Effect.fail(new Error("HTTPS test server did not bind to a TCP port")));
                return;
              }
              resume(Effect.succeed(address.port));
            });
          }),
          (port) =>
            Effect.gen(function* () {
              const client = makeCodexBrokerClient({
                url: new URL(`https://127.0.0.1:${port}`),
                clientKey: "client-secret",
                caCertPath: cert,
              });
              yield* client.health;
              assert.deepStrictEqual(
                yield* client.route({ sessionId: "session", turnId: "turn" }),
                {
                  status: "ok",
                  accountId: "public-account",
                  accountLabel: "Personal",
                  accessToken: "access-secret",
                  chatgptAccountId: "chatgpt-account",
                  expiresAt: "2099-01-01T00:00:00Z",
                  shortRemainingPercent: 80,
                  weeklyRemainingPercent: null,
                  shortResetsAt: null,
                  weeklyResetsAt: null,
                },
              );
              assert.strictEqual(
                (yield* client.route({ sessionId: "session", turnId: "wait" })).status,
                "wait",
              );

              const failure = yield* Effect.exit(
                client.route({ sessionId: "session", turnId: "large" }),
              );
              const rendered = String(failure);
              assert(!rendered.includes("client-secret"));
              assert(!rendered.includes("access-secret"));
            }),
          () =>
            Effect.callback<void>((resume) => {
              server.close(() => resume(Effect.void));
            }),
        );
      }),
    (directory) => Effect.promise(() => NodeFSP.rm(directory, { recursive: true, force: true })),
  ),
);
