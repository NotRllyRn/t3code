// @effect-diagnostics nodeBuiltinImport:off
import * as NodeCrypto from "node:crypto";

import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import { ChildProcess, ChildProcessSpawner } from "effect/process";
import * as EffectCodexSchema from "effect-codex-app-server/schema";

import {
  type CodexSettings,
  DEFAULT_TEXT_GENERATION_REASONING_EFFORT,
  type ServerProviderModel,
  TextGenerationError,
} from "@t3tools/contracts";
import { resolveSpawnCommand } from "@t3tools/shared/shell";

import { resolveAttachmentPath } from "../attachmentStore.ts";
import * as ServerConfig from "../config.ts";
import { expandHomePath } from "../pathExpansion.ts";
import { codexExecLaunchArgs, resolveCodexLaunchArgs } from "../provider/codexLaunchArgs.ts";
import * as TextGenerationOperations from "./TextGenerationOperations.ts";
import { normalizeCliError, toJsonSchemaObject } from "./TextGenerationUtils.ts";
import { codexModelFamily, getModelSelectionStringOptionValue } from "@t3tools/shared/model";
import { getCodexServiceTierOptionValue } from "../codexModelOptions.ts";
import {
  authenticateCodexAppServer,
  classifyCodexBrokerFailure,
  type CodexBrokerIntegration,
  type CodexBrokerSession,
} from "../provider/CodexBrokerAuth.ts";
import { withCodexAppServerClient } from "../provider/CodexProvider.ts";

const CODEX_TIMEOUT_MS = 180_000;
const encodeJsonString = Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown));
const isTextGenerationError = Schema.is(TextGenerationError);
/**
 * Build a Codex text-generation closure bound to a specific `CodexSettings`
 * payload. See `makeCodexAdapter` for the overall per-instance rationale.
 */
export const makeCodexTextGeneration = Effect.fn("makeCodexTextGeneration")(function* (
  codexConfig: CodexSettings,
  environment?: NodeJS.ProcessEnv,
  getModels: Effect.Effect<ReadonlyArray<ServerProviderModel>> = Effect.succeed([]),
  resolveRuntime?: Effect.Effect<
    import("../provider/CodexManagedRuntime.ts").CodexEffectiveRuntime,
    import("@t3tools/contracts").ProviderSetupError,
    Scope.Scope
  >,
  brokerIntegration?: CodexBrokerIntegration,
) {
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const commandSpawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const serverConfig = yield* Effect.service(ServerConfig.ServerConfig);
  const resolvedEnvironment = environment ?? process.env;

  const resolveModel = Effect.fn("CodexTextGeneration.resolveModel")(function* (
    requestedModel: string,
  ) {
    const models = yield* getModels;
    return (
      models.find((candidate) => candidate.slug === requestedModel)?.slug ??
      models.find(
        (candidate) => !candidate.isCustom && codexModelFamily(candidate.slug) === requestedModel,
      )?.slug ??
      requestedModel
    );
  });

  const readStreamAsString = <E>(
    operation: string,
    stream: Stream.Stream<Uint8Array, E>,
  ): Effect.Effect<string, TextGenerationError> =>
    stream.pipe(
      Stream.decodeText(),
      Stream.runFold(
        () => "",
        (acc, chunk) => acc + chunk,
      ),
      Effect.mapError((cause) =>
        normalizeCliError("codex", operation, cause, "Failed to collect process output"),
      ),
    );

  const removeTempFileDir = (filePath: string): Effect.Effect<void, never> =>
    fileSystem
      .remove(path.dirname(filePath), { recursive: true })
      .pipe(Effect.catch(() => Effect.void));

  // Deliberately unscoped: text generation runs from background fibers whose
  // ambient scope may already be closed (a closed scope reaps the temp
  // directory the moment it is created). Each allocation removes its own
  // directory on failure; success-path cleanup is explicit in runCodexJson.
  const writeTempFile = (
    operation: string,
    prefix: string,
    content: string,
  ): Effect.Effect<string, TextGenerationError> =>
    fileSystem
      .makeTempFile({
        prefix: `t3code-${prefix}-${process.pid}-`,
      })
      .pipe(
        Effect.tap((filePath) =>
          fileSystem
            .writeFileString(filePath, content)
            .pipe(Effect.onError(() => removeTempFileDir(filePath))),
        ),
        Effect.mapError(
          (cause) =>
            new TextGenerationError({
              operation,
              detail: `Failed to write temp file`,
              cause,
            }),
        ),
      );

  const encodeJsonForOperation = (
    operation: TextGenerationOperations.Operation,
    value: unknown,
  ): Effect.Effect<string, TextGenerationError> =>
    encodeJsonString(value).pipe(
      Effect.mapError(
        (cause) =>
          new TextGenerationError({
            operation,
            detail: "Failed to encode structured output schema.",
            cause,
          }),
      ),
    );

  const materializeImageAttachments = Effect.fn("materializeImageAttachments")(function* (
    attachments: TextGenerationOperations.Request<Schema.Top>["attachments"],
  ) {
    if (!attachments || attachments.length === 0) {
      return [];
    }

    const imagePaths: string[] = [];
    for (const attachment of attachments) {
      if (attachment.type !== "image") {
        continue;
      }

      const resolvedPath = resolveAttachmentPath({
        attachmentsDir: serverConfig.attachmentsDir,
        attachment,
      });
      if (!resolvedPath || !path.isAbsolute(resolvedPath)) {
        continue;
      }
      const fileInfo = yield* fileSystem.stat(resolvedPath).pipe(Effect.orElseSucceed(() => null));
      if (!fileInfo || fileInfo.type !== "File") {
        continue;
      }
      imagePaths.push(resolvedPath);
    }
    return imagePaths;
  });

  const runBrokeredCodexJson = Effect.fn("runBrokeredCodexJson")(function* <S extends Schema.Top>(
    request: TextGenerationOperations.Request<S>,
    imagePaths: ReadonlyArray<string>,
  ): Effect.fn.Return<S["Type"], TextGenerationError, S["DecodingServices"]> {
    if (!brokerIntegration) {
      return yield* new TextGenerationError({
        operation: request.operation,
        detail: "Codex Broker is not configured.",
      });
    }
    const brokerTurnId = NodeCrypto.randomUUID();
    const brokerSession = yield* brokerIntegration
      .newEphemeralSession(request.operation, brokerTurnId)
      .pipe(
        Effect.mapError(
          (cause) =>
            new TextGenerationError({
              operation: request.operation,
              detail: `Codex Broker request failed: ${cause.message}`,
              cause,
            }),
        ),
      );
    const model = yield* resolveModel(request.modelSelection.model);
    const reasoningEffort =
      getModelSelectionStringOptionValue(request.modelSelection, "reasoningEffort") ??
      DEFAULT_TEXT_GENERATION_REASONING_EFFORT;
    const serviceTier = getCodexServiceTierOptionValue(request.modelSelection);
    const outputSchema = toJsonSchemaObject(request.outputSchema);
    const decodeOutput = Schema.decodeEffect(Schema.fromJsonString(request.outputSchema));

    const runAttempt = Effect.fn("runBrokeredCodexJson.attempt")(function* (
      session: CodexBrokerSession,
    ) {
      const completed = yield* Deferred.make<EffectCodexSchema.V2TurnCompletedNotification>();
      let providerThreadId: string | undefined;
      let providerTurnId: string | undefined;
      let output: string | undefined;
      let failureKind: ReturnType<typeof classifyCodexBrokerFailure>;
      const { client } = yield* withCodexAppServerClient({
        binaryPath: codexConfig.binaryPath || "codex",
        homePath: codexConfig.homePath,
        launchArgs: resolveCodexLaunchArgs(codexConfig.launchArgs, resolvedEnvironment),
        cwd: request.cwd,
        environment: resolvedEnvironment,
      });
      yield* authenticateCodexAppServer(client, session);
      yield* client.handleServerNotification("item/completed", (payload) =>
        Effect.sync(() => {
          if (
            (providerThreadId === undefined || payload.threadId === providerThreadId) &&
            (providerTurnId === undefined || payload.turnId === providerTurnId) &&
            payload.item.type === "agentMessage"
          ) {
            output = payload.item.text;
          }
        }),
      );
      yield* client.handleServerNotification("error", (payload) =>
        Effect.sync(() => {
          if (
            (providerThreadId === undefined || payload.threadId === providerThreadId) &&
            (providerTurnId === undefined || payload.turnId === providerTurnId) &&
            !payload.willRetry
          ) {
            failureKind = classifyCodexBrokerFailure(payload);
          }
        }),
      );
      yield* client.handleServerNotification("turn/completed", (payload) =>
        (providerThreadId === undefined || payload.threadId === providerThreadId) &&
        (providerTurnId === undefined || payload.turn.id === providerTurnId)
          ? Deferred.succeed(completed, payload).pipe(Effect.asVoid)
          : Effect.void,
      );

      const thread = yield* client.request("thread/start", {
        cwd: request.cwd,
        ephemeral: true,
        approvalPolicy: "never",
        sandbox: "read-only",
        model,
        ...(serviceTier ? { serviceTier } : {}),
      });
      providerThreadId = thread.thread.id;
      const turn = yield* client.request("turn/start", {
        threadId: providerThreadId,
        input: [
          { type: "text", text: request.prompt },
          ...imagePaths.map((path) => ({ type: "localImage" as const, path })),
        ],
        model,
        effort: reasoningEffort,
        ...(serviceTier ? { serviceTier } : {}),
        approvalPolicy: "never",
        sandboxPolicy: { type: "readOnly" },
        outputSchema: outputSchema as Schema.Json,
      });
      providerTurnId = turn.turn.id;
      const result = yield* Deferred.await(completed);
      if (result.turn.status !== "completed") {
        if (failureKind) return { failureKind } as const;
        return yield* new TextGenerationError({
          operation: request.operation,
          detail: "Codex App Server text generation failed.",
        });
      }
      if (output === undefined) {
        return yield* new TextGenerationError({
          operation: request.operation,
          detail: "Codex App Server returned no final message.",
        });
      }
      return { output } as const;
    });

    return yield* Effect.gen(function* () {
      for (let attempt = 0; attempt < 32; attempt += 1) {
        const result = yield* runAttempt(brokerSession).pipe(
          Effect.scoped,
          Effect.mapError((cause) =>
            isTextGenerationError(cause)
              ? cause
              : new TextGenerationError({
                  operation: request.operation,
                  detail: "Codex App Server text generation failed.",
                  cause,
                }),
          ),
        );
        if ("output" in result) {
          return yield* decodeOutput(result.output).pipe(
            Effect.mapError(
              (cause) =>
                new TextGenerationError({
                  operation: request.operation,
                  detail: "Codex returned invalid structured output.",
                  cause,
                }),
            ),
          );
        }
        yield* brokerSession.reportTerminalFailure(brokerTurnId, result.failureKind).pipe(
          Effect.mapError(
            (cause) =>
              new TextGenerationError({
                operation: request.operation,
                detail: `Codex Broker recovery failed: ${cause.message}`,
                cause,
              }),
          ),
        );
      }
      return yield* new TextGenerationError({
        operation: request.operation,
        detail: "Codex Broker recovery attempt limit reached.",
      });
    }).pipe(
      Effect.timeoutOption(CODEX_TIMEOUT_MS),
      Effect.flatMap(
        Option.match({
          onNone: () =>
            Effect.fail(
              new TextGenerationError({
                operation: request.operation,
                detail: "Codex request timed out.",
              }),
            ),
          onSome: Effect.succeed,
        }),
      ),
    );
  });

  const runCodexJson = Effect.fn("runCodexJson")(function* <S extends Schema.Top>({
    operation,
    cwd,
    prompt,
    outputSchema: outputSchemaJson,
    modelSelection,
    attachments,
  }: TextGenerationOperations.Request<S>): Effect.fn.Return<
    S["Type"],
    TextGenerationError,
    S["DecodingServices"]
  > {
    const imagePaths = yield* materializeImageAttachments(attachments);
    if (brokerIntegration) {
      return yield* runBrokeredCodexJson(
        { operation, cwd, prompt, outputSchema: outputSchemaJson, modelSelection, attachments },
        imagePaths,
      );
    }
    const schemaJson = yield* encodeJsonForOperation(
      operation,
      toJsonSchemaObject(outputSchemaJson),
    );
    const schemaPath = yield* writeTempFile(operation, "codex-schema", schemaJson);
    const outputPath = yield* writeTempFile(operation, "codex-output", "").pipe(
      Effect.onError(() => removeTempFileDir(schemaPath)),
    );

    const runCodexCommand = Effect.fn("runCodexJson.runCodexCommand")(function* () {
      const resolved = resolveRuntime
        ? yield* resolveRuntime.pipe(
            Effect.mapError(
              (cause) => new TextGenerationError({ operation, detail: cause.detail }),
            ),
          )
        : undefined;
      const effectiveConfig = resolved?.config ?? codexConfig;
      const effectiveEnvironment = resolved?.environment ?? resolvedEnvironment;
      const models = yield* getModels;
      const requestedModel = modelSelection.model;
      const model =
        models.find((candidate) => candidate.slug === requestedModel)?.slug ??
        models.find(
          (candidate) => !candidate.isCustom && codexModelFamily(candidate.slug) === requestedModel,
        )?.slug ??
        requestedModel;
      const launchArgs = resolveCodexLaunchArgs(effectiveConfig.launchArgs, effectiveEnvironment);
      const reasoningEffort =
        getModelSelectionStringOptionValue(modelSelection, "reasoningEffort") ??
        DEFAULT_TEXT_GENERATION_REASONING_EFFORT;
      const serviceTier = resolved ? undefined : getCodexServiceTierOptionValue(modelSelection);
      const spawnCommand = yield* resolveSpawnCommand(
        effectiveConfig.binaryPath || "codex",
        [
          "exec",
          ...codexExecLaunchArgs(launchArgs),
          "--ephemeral",
          "--skip-git-repo-check",
          "-s",
          "read-only",
          "--model",
          model,
          "--config",
          `model_reasoning_effort="${reasoningEffort}"`,
          ...(serviceTier ? ["--config", `service_tier="${serviceTier}"`] : []),
          "--output-schema",
          schemaPath,
          "--output-last-message",
          outputPath,
          ...imagePaths.flatMap((imagePath) => ["--image", imagePath]),
          "-",
        ],
        { env: effectiveEnvironment },
      );
      const command = ChildProcess.make(spawnCommand.command, spawnCommand.args, {
        env: {
          ...effectiveEnvironment,
          ...(effectiveConfig.homePath
            ? { CODEX_HOME: expandHomePath(effectiveConfig.homePath) }
            : {}),
        },
        cwd,
        shell: spawnCommand.shell,
        stdin: {
          stream: Stream.encodeText(Stream.make(prompt)),
        },
      });

      const child = yield* commandSpawner
        .spawn(command)
        .pipe(
          Effect.mapError((cause) =>
            normalizeCliError("codex", operation, cause, "Failed to spawn Codex CLI process"),
          ),
        );

      const [stdout, stderr, exitCode] = yield* Effect.all(
        [
          readStreamAsString(operation, child.stdout),
          readStreamAsString(operation, child.stderr),
          child.exitCode.pipe(
            Effect.mapError((cause) =>
              normalizeCliError("codex", operation, cause, "Failed to read Codex CLI exit code"),
            ),
          ),
        ],
        { concurrency: "unbounded" },
      );

      if (exitCode !== 0) {
        const stderrDetail = stderr.trim();
        const stdoutDetail = stdout.trim();
        const detail = stderrDetail.length > 0 ? stderrDetail : stdoutDetail;
        return yield* new TextGenerationError({
          operation,
          detail:
            detail.length > 0
              ? `Codex CLI command failed: ${detail}`
              : `Codex CLI command failed with code ${exitCode}.`,
        });
      }
    });

    const cleanup = Effect.all([removeTempFileDir(schemaPath), removeTempFileDir(outputPath)], {
      concurrency: "unbounded",
    }).pipe(Effect.asVoid);

    return yield* Effect.gen(function* () {
      yield* runCodexCommand().pipe(
        Effect.scoped,
        Effect.timeoutOption(CODEX_TIMEOUT_MS),
        Effect.flatMap(
          Option.match({
            onNone: () =>
              Effect.fail(
                new TextGenerationError({ operation, detail: "Codex CLI request timed out." }),
              ),
            onSome: () => Effect.void,
          }),
        ),
      );

      const decodeOutput = Schema.decodeEffect(Schema.fromJsonString(outputSchemaJson));

      return yield* fileSystem.readFileString(outputPath).pipe(
        Effect.mapError(
          (cause) =>
            new TextGenerationError({
              operation,
              detail: "Failed to read Codex output file.",
              cause,
            }),
        ),
        Effect.flatMap(decodeOutput),
        Effect.catchTags({
          SchemaError: (cause) =>
            Effect.fail(
              new TextGenerationError({
                operation,
                detail: "Codex returned invalid structured output.",
                cause,
              }),
            ),
        }),
      );
    }).pipe(Effect.ensuring(cleanup));
  });

  return TextGenerationOperations.fromRunner("CodexTextGeneration", runCodexJson);
});
