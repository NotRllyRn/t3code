#!/usr/bin/env bash
set -euo pipefail
umask 077
checkout=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
base=${T3_PROTOCOL1_BASE_DIR:-/var/lib/t3code-codex-broker}
unit=${T3_PROTOCOL1_UNIT:-t3code-codex-broker}
node_bin=${T3_PROTOCOL1_NODE:-/root/.nvm/versions/node/v24.16.0/bin/node}
data=$base/userdata
backup=$base/backups/protocol1-$(date -u +%Y%m%dT%H%M%SZ)
override=/etc/systemd/system/$unit.service.d/protocol1.conf
export PATH="$(dirname "$node_bin"):$PATH"
"$node_bin" "$checkout/scripts/check-protocol1.ts"
test -f "$checkout/apps/server/dist/client/index.html"
test -f "$data/statev2.sqlite"
# A stale V2 file must never replace post-switch V1 history.
if python3 - "$data/state.sqlite" <<'PY'
import sqlite3,sys
c=sqlite3.connect(f'file:{sys.argv[1]}?mode=ro',uri=True)
raise SystemExit(0 if c.execute("SELECT 1 FROM sqlite_master WHERE name='protocol1_export_manifest'").fetchone() else 1)
PY
then
  echo 'Already deployed. Use the update/restart commands in docs/operations/protocol1.md.' >&2
  exit 1
fi
mkdir -p "$backup"
old_override=0
if test -f "$override"; then cp -a "$override" "$backup/previous-override.conf"; old_override=1; fi
systemctl cat "$unit" > "$backup/service-before.txt"
cp -a /etc/systemd/system/"$unit".service "$backup/service-before.service"
installed=0
rollback() {
  local status=$?
  trap - ERR
  systemctl stop "$unit" || true
  if (( installed )); then
    mv "$data/state.sqlite" "$backup/failed-state.sqlite" || true
    for sidecar in -wal -shm; do
      if test -e "$data/state.sqlite$sidecar"; then mv "$data/state.sqlite$sidecar" "$backup/failed-state.sqlite$sidecar"; fi
    done
    cp "$backup/state.sqlite" "$data/state.sqlite"
  fi
  if (( old_override )); then cp "$backup/previous-override.conf" "$override"; else rm -f "$override"; fi
  systemctl daemon-reload
  systemctl start "$unit" || true
  echo "Deployment failed; previous service restored. Inspect $backup" >&2
  exit "$status"
}
trap rollback ERR
systemctl stop "$unit"
python3 - "$data" "$backup" <<'PY'
from pathlib import Path
import sqlite3,sys
for name in ['state.sqlite','statev2.sqlite']:
 src=sqlite3.connect(f'file:{Path(sys.argv[1])/name}?mode=ro',uri=True)
 dst=sqlite3.connect(str(Path(sys.argv[2])/name));src.backup(dst);dst.close();src.close()
PY
python3 "$checkout/scripts/migrate-protocol1.py" --source "$backup/statev2.sqlite" --destination "$backup/new-state.sqlite" > "$backup/export.json"
"$node_bin" "$checkout/apps/server/scripts/validate-protocol1-state.ts" "$backup/new-state.sqlite" > "$backup/validation.log" 2>&1
# Save the exact original file and its sidecars after the reader has stopped.
installed=1
for suffix in '' -wal -shm; do
  if test -e "$data/state.sqlite$suffix"; then mv "$data/state.sqlite$suffix" "$backup/original-state.sqlite$suffix"; fi
done
cp "$backup/new-state.sqlite" "$data/state.sqlite"
mkdir -p "$(dirname "$override")"
cat > "$override" <<EOF
[Service]
WorkingDirectory=$checkout
ExecStart=
ExecStart=$node_bin $checkout/apps/server/dist/bin.mjs serve --mode web --host 0.0.0.0 --port 3773 --base-dir $base
EOF
systemctl daemon-reload
systemctl start "$unit"
python3 - <<'PY'
import json,time,urllib.request
for attempt in range(30):
 try:
  with urllib.request.urlopen('http://127.0.0.1:3773/.well-known/t3/environment',timeout=2) as r:
   assert json.load(r)['orchestrationProtocolVersion']==1
  with urllib.request.urlopen('http://127.0.0.1:3773/',timeout=2) as r: assert r.status==200
  break
 except Exception:
  if attempt==29: raise
  time.sleep(1)
PY
trap - ERR
printf 'Protocol 1 is active. Backups: %s\n' "$backup"
cat "$backup/export.json"
cat "$backup/validation.log"
