import * as NodeFS from "node:fs";
const contract = NodeFS.readFileSync(
  new URL("../packages/contracts/src/environment.ts", import.meta.url),
  "utf8",
);
if (!/export const ORCHESTRATION_PROTOCOL_VERSION = 1;/.test(contract)) {
  throw new Error(
    "codex-broker is pinned to orchestration protocol 1. See docs/operations/protocol1.md before merging upstream.",
  );
}
