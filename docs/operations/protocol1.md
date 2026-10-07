# Protocol 1 with Codex Broker

This fork's `codex-broker` branch is pinned to upstream `6c8fed35dded`:
the macOS 0.0.45 build with that hash uses orchestration protocol 1. The mobile
1.4.1 release/backport source (`6af29c143c`) also declares protocol 1.
Version 0.0.45 alone is insufficient: later upstream revisions changed the
orchestrator and wire contract to protocol 2 without changing that version.

Do not merge upstream main wholesale or use `t3 update`/`npx t3@latest` on this
installation. Backport compatible fixes to this base. The server bundle build
runs `scripts/check-protocol1.ts`; retain that check. Changing the advertised
version alone cannot make V2 commands, snapshots, or events compatible.

## Account recovery

Codex Broker selects accounts at the adapter boundary. A terminal quota/auth/rate
limit can arrive in an `error` notification or only in `turn/completed`.
The adapter reports the failure to Broker, re-authenticates or replaces its
runtime, and resumes the **same native Codex thread** with the original model,
reasoning effort, and interaction mode. Partial transcript and filesystem work
remain in that thread. Continuation instructs Codex to avoid repeating completed
work; it cannot restore a model's private inference state or guarantee exactly-once
external tool execution.

A recoverable failure must not reach orchestration as a completed app turn. All
native attempts retain the first turn's logical ID, with native IDs in provider
refs. The recovery scope outlives the runtime being replaced. Bound retries and
report unrecoverable/all-accounts-unavailable failures explicitly.

## Database export

`scripts/migrate-protocol1.py` uses SQLite's backup API to take a consistent,
read-only snapshot, including committed WAL contents. It refuses existing outputs
and unknown migration histories. It creates `state.sqlite` with the V1 log plus
translated V2 conversations, messages, tool activity, run status and checkpoints.
Native Codex thread references are written as V1 resume cursors. Existing auth
sessions/pairing links and V1 state are retained. Original V2 events and projections
remain in archive tables for audit; the original `statev2.sqlite` is untouched.
The migration history is capped at the pinned base's migration 54.

Unfinished runs become interrupted, and runtimes become stopped. They are not
launched automatically by the migration. Reopen the conversation and send a
continuation after reconnecting. A V2 thread that never successfully started has
no native continuation reference; it keeps its app messages and starts a native
thread on the next request. V2 lineage/attempt metadata remain archived, since
protocol-1 clients have no equivalent controls.

Validate with the **pinned checkout's dependencies** before switching service:

```bash
python3 scripts/migrate-protocol1.py --source /path/statev2.sqlite --destination /path/new-state.sqlite
node apps/server/scripts/validate-protocol1-state.ts /path/new-state.sqlite
```

The validator decodes every event with the V1 contract, runs the real projection
bootstrap without starting providers, compares conversation/message IDs and text,
and checks SQLite integrity and foreign keys. It writes projections only to the
export. Run it on a copied/exported database, never the running database.

## This server

The production service is `t3code-codex-broker.service`, port 3773, with data under
`/var/lib/t3code-codex-broker/userdata`. The tested deployment checkout is
`/root/code/t3code-protocol1`, tracking `origin/codex-broker`. A systemd override
points to its built server. `/root/code/t3code` remains the original checkout.

The first switch is handled by `scripts/deploy-protocol1.sh`. It refuses to run a
second export after protocol-1 deployment, stops the exact service, backs up both
SQLite files plus the unit, exports and validates, installs the new database and
service override, restarts, and checks the advertised protocol. Its failure trap
restores the old database/override and restarts the previous server. Backups and
validation logs remain under `/var/lib/t3code-codex-broker/backups/protocol1-*`.
Keep the source V2 file: do not overwrite it with later protocol-1 state. Reverting
later requires reconciling history created after this switch; restoring the old
V2 snapshot alone would omit it.

After this one-time migration, update this fork without repeating the exporter:

```bash
export PATH=/root/.nvm/versions/node/v24.16.0/bin:$PATH
cd /root/code/t3code-protocol1
git pull --ff-only origin codex-broker
vp install --frozen-lockfile
vp run --filter t3 build
systemctl restart t3code-codex-broker
systemctl is-active t3code-codex-broker
```

Verify `/.well-known/t3/environment` reports `orchestrationProtocolVersion: 1`.
Reconnect both installed clients, open an older and newer conversation, and run a
short Codex request. Existing pairing records are preserved; a real client
connection and account exhaustion need client/live-pool testing in addition to
adapter fixtures. If an old pairing is rejected, generate a fresh pairing with
the pinned server CLI rather than upgrading the installed client/server ad hoc.
