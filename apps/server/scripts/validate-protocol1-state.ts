import * as NodeSqlite from "node:sqlite";
import * as NodePath from "node:path";
import { OrchestrationEvent, ORCHESTRATION_PROTOCOL_VERSION } from "@t3tools/contracts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import { ServerConfig } from "../src/config.ts";
import { makeSqlitePersistenceLive } from "../src/persistence/Layers/Sqlite.ts";
import { OrchestrationEventStoreLive } from "../src/persistence/Layers/OrchestrationEventStore.ts";
import { OrchestrationProjectionPipelineLive } from "../src/orchestration/Layers/ProjectionPipeline.ts";
import { OrchestrationProjectionPipeline } from "../src/orchestration/Services/ProjectionPipeline.ts";

if (ORCHESTRATION_PROTOCOL_VERSION !== 1) throw new Error("This exporter requires protocol 1");
const filename = NodePath.resolve(process.argv[2] ?? "");
if (!process.argv[2])
  throw new Error("Usage: node apps/server/scripts/validate-protocol1-state.ts <export.sqlite>");
const db = new NodeSqlite.DatabaseSync(filename, { readOnly: true });
if (!db.prepare("SELECT 1 FROM sqlite_master WHERE name='protocol1_export_manifest'").get())
  throw new Error("Refusing to open a database without the exporter marker");
const validEvent = Schema.is(OrchestrationEvent);
let count = 0;
for (const row of db.prepare("SELECT * FROM orchestration_events ORDER BY sequence").iterate()) {
  const event = {
    sequence: row.sequence,
    eventId: row.event_id,
    type: row.event_type,
    aggregateKind: row.aggregate_kind,
    aggregateId: row.stream_id,
    occurredAt: row.occurred_at,
    commandId: row.command_id,
    causationEventId: row.causation_event_id,
    correlationId: row.correlation_id,
    payload: JSON.parse(String(row.payload_json)),
    metadata: JSON.parse(String(row.metadata_json)),
  };
  if (!validEvent(event))
    throw new Error(`Incompatible event at sequence ${row.sequence} (${row.event_type})`);
  count++;
}
db.close();
const layer = OrchestrationProjectionPipelineLive.pipe(
  Layer.provideMerge(OrchestrationEventStoreLive),
  Layer.provideMerge(
    ServerConfig.layerTest(NodePath.dirname(filename), { prefix: "protocol1-validation-" }),
  ),
  Layer.provideMerge(makeSqlitePersistenceLive(filename)),
  Layer.provideMerge(NodeServices.layer),
);
await Effect.runPromise(
  Effect.gen(function* () {
    const pipeline = yield* OrchestrationProjectionPipeline;
    yield* pipeline.bootstrap;
  }).pipe(Effect.provide(layer)),
);
const projected = new NodeSqlite.DatabaseSync(filename, { readOnly: true });
for (const [v2, v1, id] of [
  ["orchestration_v2_projection_threads", "projection_threads", "thread_id"],
  ["orchestration_v2_projection_messages", "projection_thread_messages", "message_id"],
] as const) {
  const missing = projected
    .prepare(
      `SELECT count(*) AS n FROM ${v2} newer LEFT JOIN ${v1} older ON newer.${id}=older.${id} WHERE older.${id} IS NULL`,
    )
    .get();
  if (Number(missing?.n)) throw new Error(`Missing ${missing?.n} records in ${v1}`);
}
const mismatched = projected
  .prepare(
    `SELECT count(*) AS n FROM orchestration_v2_projection_messages newer JOIN projection_thread_messages older USING(message_id) WHERE json_extract(newer.payload_json,'$.text') != older.text`,
  )
  .get();
if (Number(mismatched?.n)) throw new Error("Message text changed during export");
if (projected.prepare("PRAGMA integrity_check").get()?.integrity_check !== "ok")
  throw new Error("Integrity check failed");
if (projected.prepare("PRAGMA foreign_key_check").all().length)
  throw new Error("Foreign key check failed");
console.log(
  JSON.stringify({
    validatedEvents: count,
    projects: projected.prepare("SELECT count(*) AS n FROM projection_projects").get()?.n,
    threads: projected.prepare("SELECT count(*) AS n FROM projection_threads").get()?.n,
    messages: projected.prepare("SELECT count(*) AS n FROM projection_thread_messages").get()?.n,
  }),
);
projected.close();
