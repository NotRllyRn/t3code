import * as NodeCrypto from "node:crypto";

import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";
import type * as CodexClient from "effect-codex-app-server/client";
import * as EffectCodexSchema from "effect-codex-app-server/schema";

import {
  type CodexSettings,
  DEFAULT_TEXT_GENERATION_REASONING_EFFORT,
  type ModelSelection,
  TextGenerationError,
} from "@t3tools/contracts";
import { sanitizeBranchFragment, sanitizeFeatureBranchName } from "@t3tools/shared/git";
import { resolveSpawnCommand } from "@t3tools/shared/shell";

import { resolveAttachmentPath } from "../attachmentStore.ts";
import * as ServerConfig from "../config.ts";
import { expandHomePath } from "../pathExpansion.ts";
import { codexExecLaunchArgs, resolveCodexLaunchArgs } from "../provider/Layers/codexLaunchArgs.ts";
import * as TextGeneration from "./TextGeneration.ts";
import {
  buildBranchNamePrompt,
  buildCommitMessagePrompt,
  buildPrContentPrompt,
  buildThreadTitlePrompt,
} from "./TextGenerationPrompts.ts";
import {
  normalizeCliError,
  sanitizeCommitSubject,
  sanitizePrTitle,
  sanitizeThreadTitle,
  toJsonSchemaObject,
} from "./TextGenerationUtils.ts";
import { getModelSelectionStringOptionValue } from "@t3tools/shared/model";
import { getCodexServiceTierOptionValue } from "../codexModelOptions.ts";
import {
  authenticateCodexAppServer,
  classifyCodexBrokerFailure,
  type CodexBrokerIntegration,
} from "../provider/Layers/CodexBrokerAuth.ts";
import { withCodexAppServerClient } from "../provider/Layers/CodexProvider.ts";

const CODEX_TIMEOUT_MS = 180_000;
const encodeJsonString = Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown));

interface CodexTextGenerationOptions {
  readonly withAppServerClient?: (input: {
    readonly binaryPath: string;
    readonly homePath?: string | undefined;
    readonly launchArgs?: string | undefined;
    readonly cwd: string;
    readonly environment?: NodeJS.ProcessEnv | undefined;
  }) => Effect.Effect<
    { readonly client: CodexClient.CodexAppServerClient["Service"] },
    unknown,
    Scope.Scope
  >;
}

/**
 * Build a Codex text-generation closure bound to a specific `CodexSettings`
 * payload. See `makeCodexAdapter` for the overall per-instance rationale.
 */
export const makeCodexTextGeneration = Effect.fn("makeCodexTextGeneration")(function* (
  codexConfig: CodexSettings,
  environment?: NodeJS.ProcessEnv,
  brokerIntegration?: CodexBrokerIntegration,
  options?: CodexTextGenerationOptions,
) {
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const commandSpawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const serverConfig = yield* Effect.service(ServerConfig.ServerConfig);
  const resolvedEnvironment = environment ?? process.env;

  type MaterializedImageAttachments = {
    readonly imagePaths: ReadonlyArray<string>;
  };

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

  const writeTempFile = (
    operation: string,
    prefix: string,
    content: string,
  ): Effect.Effect<string, TextGenerationError, Scope.Scope> =>
    fileSystem
      .makeTempFileScoped({
        prefix: `t3code-${prefix}-${process.pid}-`,
      })
      .pipe(
        Effect.tap((filePath) => fileSystem.writeFileString(filePath, content)),
        Effect.mapError(
          (cause) =>
            new TextGenerationError({
              operation,
              detail: `Failed to write temp file`,
              cause,
            }),
        ),
      );

  const safeUnlink = (filePath: string): Effect.Effect<void, never> =>
    fileSystem.remove(filePath).pipe(Effect.catch(() => Effect.void));

  const encodeJsonForOperation = (
    operation:
      | "generateCommitMessage"
      | "generatePrContent"
      | "generateBranchName"
      | "generateThreadTitle",
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
    _operation:
      | "generateCommitMessage"
      | "generatePrContent"
      | "generateBranchName"
      | "generateThreadTitle",
    attachments: TextGeneration.BranchNameGenerationInput["attachments"],
  ): Effect.fn.Return<MaterializedImageAttachments, TextGenerationError> {
    if (!attachments || attachments.length === 0) {
      return { imagePaths: [] };
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
    return { imagePaths };
  });

  const runBrokeredCodexJson = Effect.fn("runBrokeredCodexJson")(function* <S extends Schema.Top>({
    operation,
    cwd,
    prompt,
    outputSchemaJson,
    imagePaths,
    modelSelection,
  }: {
    operation:
      | "generateCommitMessage"
      | "generatePrContent"
      | "generateBranchName"
      | "generateThreadTitle";
    cwd: string;
    prompt: string;
    outputSchemaJson: S;
    imagePaths: ReadonlyArray<string>;
    modelSelection: ModelSelection;
  }): Effect.fn.Return<S["Type"], TextGenerationError, S["DecodingServices"] | Scope.Scope> {
    if (!brokerIntegration) {
      return yield* new TextGenerationError({
        operation,
        detail: "Codex Broker is not configured.",
      });
    }
    const brokerTurnId = NodeCrypto.randomUUID();
    const brokerSession = yield* brokerIntegration
      .newEphemeralSession(operation, brokerTurnId)
      .pipe(
        Effect.mapError(
          (cause) =>
            new TextGenerationError({
              operation,
              detail: `Codex Broker request failed: ${cause.message}`,
              cause,
            }),
        ),
      );
    const reasoningEffort =
      getModelSelectionStringOptionValue(modelSelection, "reasoningEffort") ??
      DEFAULT_TEXT_GENERATION_REASONING_EFFORT;
    const serviceTier = getCodexServiceTierOptionValue(modelSelection);
    const outputSchema = toJsonSchemaObject(outputSchemaJson);

    const runAttempt = Effect.fn("runBrokeredCodexJson.attempt")(function* () {
      const completed = yield* Deferred.make<EffectCodexSchema.V2TurnCompletedNotification>();
      let providerThreadId: string | undefined;
      let providerTurnId: string | undefined;
      let output: string | undefined;
      let failureKind: ReturnType<typeof classifyCodexBrokerFailure>;
      const openAppServer = options?.withAppServerClient ?? withCodexAppServerClient;
      const { client } = yield* openAppServer({
        binaryPath: codexConfig.binaryPath || "codex",
        homePath: codexConfig.homePath,
        launchArgs: resolveCodexLaunchArgs(codexConfig.launchArgs, resolvedEnvironment),
        cwd,
        environment: resolvedEnvironment,
      });
      yield* authenticateCodexAppServer(client, brokerSession);
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
        cwd,
        ephemeral: true,
        approvalPolicy: "never",
        sandbox: "read-only",
        model: modelSelection.model,
        ...(serviceTier ? { serviceTier } : {}),
      });
      providerThreadId = thread.thread.id;
      const turn = yield* client.request("turn/start", {
        threadId: providerThreadId,
        input: [
          { type: "text", text: prompt },
          ...imagePaths.map((path) => ({ type: "localImage" as const, path })),
        ],
        model: modelSelection.model,
        effort: reasoningEffort,
        ...(serviceTier ? { serviceTier } : {}),
        approvalPolicy: "never",
        sandboxPolicy: { type: "readOnly" },
        outputSchema,
      });
      providerTurnId = turn.turn.id;
      const result = yield* Deferred.await(completed);
      if (result.threadId !== providerThreadId || result.turn.id !== providerTurnId) {
        return yield* new TextGenerationError({
          operation,
          detail: "Codex App Server returned a mismatched turn.",
        });
      }
      if (result.turn.status !== "completed") {
        if (failureKind) return { failureKind } as const;
        return yield* new TextGenerationError({
          operation,
          detail: "Codex App Server text generation failed.",
        });
      }
      if (output === undefined) {
        return yield* new TextGenerationError({
          operation,
          detail: "Codex App Server returned no final message.",
        });
      }
      return { output } as const;
    });

    const generate = Effect.gen(function* () {
      for (let attempt = 0; attempt < 32; attempt += 1) {
        const result = yield* runAttempt().pipe(
          Effect.scoped,
          Effect.mapError((cause) =>
            cause instanceof TextGenerationError
              ? cause
              : new TextGenerationError({
                  operation,
                  detail: "Codex App Server text generation failed.",
                  cause,
                }),
          ),
        );
        if ("output" in result) {
          return yield* Schema.decodeEffect(Schema.fromJsonString(outputSchemaJson))(
            result.output,
          ).pipe(
            Effect.mapError(
              (cause) =>
                new TextGenerationError({
                  operation,
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
                operation,
                detail: `Codex Broker recovery failed: ${cause.message}`,
                cause,
              }),
          ),
        );
      }
      return yield* new TextGenerationError({
        operation,
        detail: "Codex Broker recovery attempt limit reached.",
      });
    });

    return yield* generate.pipe(
      Effect.timeoutOption(CODEX_TIMEOUT_MS),
      Effect.flatMap(
        Option.match({
          onNone: () =>
            Effect.fail(new TextGenerationError({ operation, detail: "Codex request timed out." })),
          onSome: Effect.succeed,
        }),
      ),
    );
  });

  const runCodexJson = Effect.fn("runCodexJson")(function* <S extends Schema.Top>({
    operation,
    cwd,
    prompt,
    outputSchemaJson,
    imagePaths = [],
    cleanupPaths = [],
    modelSelection,
  }: {
    operation:
      | "generateCommitMessage"
      | "generatePrContent"
      | "generateBranchName"
      | "generateThreadTitle";
    cwd: string;
    prompt: string;
    outputSchemaJson: S;
    imagePaths?: ReadonlyArray<string>;
    cleanupPaths?: ReadonlyArray<string>;
    modelSelection: ModelSelection;
  }): Effect.fn.Return<S["Type"], TextGenerationError, S["DecodingServices"]> {
    if (brokerIntegration) {
      return yield* runBrokeredCodexJson({
        operation,
        cwd,
        prompt,
        outputSchemaJson,
        imagePaths,
        modelSelection,
      });
    }

    const schemaJson = yield* encodeJsonForOperation(
      operation,
      toJsonSchemaObject(outputSchemaJson),
    );
    const schemaPath = yield* writeTempFile(operation, "codex-schema", schemaJson);
    const outputPath = yield* writeTempFile(operation, "codex-output", "");

    const runCodexCommand = Effect.fn("runCodexJson.runCodexCommand")(function* () {
      const launchArgs = resolveCodexLaunchArgs(codexConfig.launchArgs, resolvedEnvironment);
      const reasoningEffort =
        getModelSelectionStringOptionValue(modelSelection, "reasoningEffort") ??
        DEFAULT_TEXT_GENERATION_REASONING_EFFORT;
      const serviceTier = getCodexServiceTierOptionValue(modelSelection);
      const spawnCommand = yield* resolveSpawnCommand(
        codexConfig.binaryPath || "codex",
        [
          "exec",
          ...codexExecLaunchArgs(launchArgs),
          "--ephemeral",
          "--skip-git-repo-check",
          "-s",
          "read-only",
          "--model",
          modelSelection.model,
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
        { env: resolvedEnvironment },
      );
      const command = ChildProcess.make(spawnCommand.command, spawnCommand.args, {
        env: {
          ...resolvedEnvironment,
          ...(codexConfig.homePath ? { CODEX_HOME: expandHomePath(codexConfig.homePath) } : {}),
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

    const cleanup = Effect.all(
      [schemaPath, outputPath, ...cleanupPaths].map((filePath) => safeUnlink(filePath)),
      {
        concurrency: "unbounded",
      },
    ).pipe(Effect.asVoid);

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

  const generateCommitMessage: TextGeneration.TextGeneration["Service"]["generateCommitMessage"] =
    Effect.fn("CodexTextGeneration.generateCommitMessage")(function* (input) {
      const { prompt, outputSchema } = buildCommitMessagePrompt({
        branch: input.branch,
        stagedSummary: input.stagedSummary,
        stagedPatch: input.stagedPatch,
        includeBranch: input.includeBranch === true,
        policy: input.policy,
      });

      const generated = yield* runCodexJson({
        operation: "generateCommitMessage",
        cwd: input.cwd,
        prompt,
        outputSchemaJson: outputSchema,
        modelSelection: input.modelSelection,
      });

      return {
        subject: sanitizeCommitSubject(generated.subject),
        body: generated.body.trim(),
        ...("branch" in generated && typeof generated.branch === "string"
          ? { branch: sanitizeFeatureBranchName(generated.branch) }
          : {}),
      };
    });

  const generatePrContent: TextGeneration.TextGeneration["Service"]["generatePrContent"] =
    Effect.fn("CodexTextGeneration.generatePrContent")(function* (input) {
      const { prompt, outputSchema } = buildPrContentPrompt({
        baseBranch: input.baseBranch,
        headBranch: input.headBranch,
        commitSummary: input.commitSummary,
        diffSummary: input.diffSummary,
        diffPatch: input.diffPatch,
        policy: input.policy,
        changeRequestTemplate: input.changeRequestTemplate,
      });

      const generated = yield* runCodexJson({
        operation: "generatePrContent",
        cwd: input.cwd,
        prompt,
        outputSchemaJson: outputSchema,
        modelSelection: input.modelSelection,
      });

      return {
        title: sanitizePrTitle(generated.title),
        body: generated.body.trim(),
      };
    });

  const generateBranchName: TextGeneration.TextGeneration["Service"]["generateBranchName"] =
    Effect.fn("CodexTextGeneration.generateBranchName")(function* (input) {
      const { imagePaths } = yield* materializeImageAttachments(
        "generateBranchName",
        input.attachments,
      );
      const { prompt, outputSchema } = buildBranchNamePrompt({
        message: input.message,
        attachments: input.attachments,
      });

      const generated = yield* runCodexJson({
        operation: "generateBranchName",
        cwd: input.cwd,
        prompt,
        outputSchemaJson: outputSchema,
        imagePaths,
        modelSelection: input.modelSelection,
      });

      return {
        branch: sanitizeBranchFragment(generated.branch),
      };
    });

  const generateThreadTitle: TextGeneration.TextGeneration["Service"]["generateThreadTitle"] =
    Effect.fn("CodexTextGeneration.generateThreadTitle")(function* (input) {
      const { imagePaths } = yield* materializeImageAttachments(
        "generateThreadTitle",
        input.attachments,
      );
      const { prompt, outputSchema } = buildThreadTitlePrompt({
        message: input.message,
        previousTitle: input.previousTitle,
        attachments: input.attachments,
      });

      const generated = yield* runCodexJson({
        operation: "generateThreadTitle",
        cwd: input.cwd,
        prompt,
        outputSchemaJson: outputSchema,
        imagePaths,
        modelSelection: input.modelSelection,
      });

      return {
        title: sanitizeThreadTitle(generated.title),
      } satisfies TextGeneration.ThreadTitleGenerationResult;
    });

  return {
    generateCommitMessage,
    generatePrContent,
    generateBranchName,
    generateThreadTitle,
  } satisfies TextGeneration.TextGeneration["Service"];
});
