#!/usr/bin/env python3
"""Export a V2 SQLite snapshot to the pinned protocol-1 server. Never edits the source."""
import argparse
import datetime
import json
import os
from pathlib import Path
import sqlite3
import tempfile


def migrate(source, destination):
    source, destination = Path(source).resolve(), Path(destination).resolve()
    if source == destination or destination.exists():
        raise ValueError('Destination must be a new file, distinct from the source')
    destination.parent.mkdir(parents=True, exist_ok=True)
    fd, scratch = tempfile.mkstemp(prefix='.protocol1-', dir=destination.parent)
    os.close(fd)
    src = sqlite3.connect(f'file:{source}?mode=ro', uri=True)
    db = sqlite3.connect(scratch)
    db.row_factory = sqlite3.Row
    try:
        src.backup(db)
        if db.execute('PRAGMA integrity_check').fetchone()[0] != 'ok':
            raise ValueError('Source integrity check failed')
        tables = {r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        if 'orchestration_v2_projection_threads' not in tables:
            raise ValueError('Source is not an orchestration V2 database')
        history = dict(db.execute('SELECT migration_id,name FROM effect_sql_migrations'))
        if history.get(54) != 'ProjectionThreadsAutoSettleDisabledAt' or history.get(55) != 'OrchestrationV2' or max(history) > 58:
            raise ValueError('Unrecognized migration history; review the exporter before proceeding')
        now = datetime.datetime.now(datetime.timezone.utc).isoformat(timespec='milliseconds').replace('+00:00', 'Z')
        counts = {'threads': 0, 'messages': 0, 'runs': 0, 'activities': 0, 'resumable_threads': 0}
        with db:
            # Keep every original V2 event and projection for audit/rollback, while
            # exposing only valid V1 events to the old reader. Do not reinterpret V2 payloads.
            db.execute('CREATE TABLE protocol2_event_archive AS SELECT * FROM orchestration_events WHERE application_event_version=2')
            db.execute('DELETE FROM orchestration_events WHERE application_event_version=2')
            db.execute('DELETE FROM orchestration_command_receipts WHERE command_type != ?', ('legacy',))
            db.execute('DELETE FROM effect_sql_migrations WHERE migration_id > 54')
            # Legacy cursors can be ahead of the last V1 event after V2 ran.
            max_v1 = db.execute('SELECT coalesce(max(sequence),0) FROM orchestration_events').fetchone()[0]
            db.execute('UPDATE projection_state SET last_applied_sequence=min(last_applied_sequence,?)', (max_v1,))
            versions = {(r[0], r[1]): r[2] for r in db.execute('SELECT aggregate_kind,stream_id,max(stream_version) FROM orchestration_events GROUP BY 1,2')}
            event_index = 0
            def emit(kind, tid, payload, at=None, aggregate='thread'):
                nonlocal event_index
                event_index += 1
                key = (aggregate, tid)
                versions[key] = versions.get(key, -1) + 1
                db.execute('''INSERT INTO orchestration_events
                    (event_id,aggregate_kind,stream_id,stream_version,event_type,occurred_at,command_id,
                     causation_event_id,correlation_id,actor_kind,payload_json,metadata_json,application_event_version)
                    VALUES (?,?,?,?,?,?,NULL,NULL,NULL,'server',?,'{}',1)''',
                    (f'protocol1-export:{event_index}', aggregate, tid, versions[key], kind, at or now, json.dumps(payload)))
            old_threads = {r['thread_id']: dict(r) for r in db.execute('SELECT * FROM projection_threads')}
            threads = {r['thread_id']: json.loads(r['payload_json']) for r in db.execute('SELECT * FROM orchestration_v2_projection_threads')}
            for tid, t in threads.items():
                if tid not in old_threads:
                    emit('thread.created', tid, dict(threadId=tid, projectId=t['projectId'], title=t['title'],
                        modelSelection=t['modelSelection'], runtimeMode=t['runtimeMode'], interactionMode=t['interactionMode'],
                        branch=t.get('branch'), worktreePath=t.get('worktreePath'), createdAt=t['createdAt'], updatedAt=t['updatedAt']), t['createdAt'])
                    counts['threads'] += 1
                emit('thread.meta-updated', tid, dict(threadId=tid, updatedAt=t['updatedAt'], **{k:t.get(k) for k in
                     ['title','modelSelection','branch','worktreePath','linkedPullRequest','branchPullRequest','activeOrderKey']}), t['updatedAt'])
                for name, field in [('runtime-mode-set','runtimeMode'), ('interaction-mode-set','interactionMode')]:
                    emit('thread.'+name, tid, {'threadId':tid, field:t[field], 'updatedAt':t['updatedAt']}, t['updatedAt'])
                emit('thread.session-set', tid, {'threadId':tid, 'session':dict(threadId=tid,status='stopped',providerName=None,
                     runtimeMode=t['runtimeMode'],activeTurnId=None,lastError=None,updatedAt=now)})
            # Projects are shared between V1 and V2. Re-baseline new projects into
            # the V1 log as well, so a full projection rebuild retains them.
            for r in db.execute('SELECT * FROM projection_projects').fetchall():
                p=dict(r)
                if not db.execute("SELECT 1 FROM orchestration_events WHERE event_type='project.created' AND stream_id=?",(p['project_id'],)).fetchone():
                    emit('project.created',p['project_id'],dict(projectId=p['project_id'],title=p['title'],workspaceRoot=p['workspace_root'],
                         defaultModelSelection=json.loads(p['default_model_selection_json']) if p['default_model_selection_json'] else None,
                         scripts=json.loads(p['scripts_json']),createdAt=p['created_at'],updatedAt=p['updated_at']),p['created_at'],'project')
            old_messages = {r['message_id']:dict(r) for r in db.execute('SELECT * FROM projection_thread_messages')}
            messages = [json.loads(r[0]) for r in db.execute('SELECT payload_json FROM orchestration_v2_projection_messages ORDER BY created_at,message_id')]
            by_run = {}
            def message(m):
                old=old_messages.get(m['id'])
                turn=m.get('runId') or (old['turn_id'] if old else None)
                emit('thread.message-sent',m['threadId'],dict(threadId=m['threadId'],messageId=m['id'],role=m['role'],text=m['text'],
                     attachments=m.get('attachments',[]),turnId=turn,streaming=False,createdAt=m['createdAt'],updatedAt=m['updatedAt'],
                     **({'context':json.loads(old['context_json'])} if old and old.get('context_json') else {})),m['updatedAt'])
                counts['messages']+=1
            for m in messages:
                if m.get('runId'):
                    by_run.setdefault(m['runId'],[]).append(m)
                else:
                    old=old_messages.get(m['id'])
                    if not old or old['text']!=m['text'] or old['is_streaming']:
                        message(m)
            checkpoints={}
            for r in db.execute('SELECT payload_json FROM orchestration_v2_projection_checkpoints ORDER BY captured_at'):
                cp=json.loads(r[0])
                if cp.get('runId') and cp.get('appRunOrdinal') is not None: checkpoints[cp['runId']]=cp
            for r in db.execute('SELECT payload_json FROM orchestration_v2_projection_runs ORDER BY requested_at,ordinal').fetchall():
                run=json.loads(r[0]); tid=run['threadId']; t=threads[tid]; rid=run['id']
                def session(status,active,at):
                    emit('thread.session-set',tid,{'threadId':tid,'session':dict(threadId=tid,status=status,providerName='codex',
                        providerInstanceId=run['providerInstanceId'],runtimeMode=t['runtimeMode'],activeTurnId=active,lastError=None,updatedAt=at)},at)
                session('running',rid,run.get('startedAt') or run['requestedAt'])
                for m in by_run.pop(rid,[]): message(m)
                ended=run.get('completedAt') or now
                session({'completed':'ready','failed':'error','interrupted':'interrupted'}.get(run['status'],'interrupted'),None,ended)
                cp=checkpoints.get(rid)
                if cp:
                    assistant=next((m['id'] for m in reversed(messages) if m.get('runId')==rid and m['role']=='assistant'),None)
                    emit('thread.turn-diff-completed',tid,dict(threadId=tid,turnId=rid,checkpointTurnCount=cp['appRunOrdinal'],
                        checkpointRef=cp['ref'],status=cp['status'],files=cp['files'],assistantMessageId=assistant,completedAt=cp['capturedAt']),cp['capturedAt'])
                counts['runs']+=1
            if by_run: raise ValueError('Messages refer to missing runs')
            for r in db.execute('SELECT * FROM orchestration_v2_projection_turn_items ORDER BY ordinal').fetchall():
                item=json.loads(r['payload_json'])
                # Imported legacy items already have their V1 activities. New
                # tools/notices retain their entire V2 payload as activity detail.
                if not item.get('runId') or item['type'] in ['assistant_message','user_message','checkpoint']: continue
                emit('thread.activity-appended',r['thread_id'],{'threadId':r['thread_id'],'activity':dict(id='protocol1-item:'+item['id'],
                    tone='error' if item['type']=='error' else 'tool',kind='protocol2.'+item['type'],
                    summary=item.get('title') or item['type'].replace('_',' '),payload=item,turnId=item['runId'],createdAt=item['updatedAt'])},item['updatedAt'])
                counts['activities']+=1
            for tid,t in threads.items():
                at=t['updatedAt'];common={'threadId':tid,'updatedAt':at}
                emit('thread.archived' if t.get('archivedAt') else 'thread.unarchived',tid,
                    {**common,**({'archivedAt':t['archivedAt']} if t.get('archivedAt') else {})},at)
                emit('thread.pinned' if t.get('pinnedAt') else 'thread.unpinned',tid,
                    {**common,**({'pinnedAt':t['pinnedAt'],**({'pinOrderKey':t['pinOrderKey']} if t.get('pinOrderKey') else {})} if t.get('pinnedAt') else {})},at)
                emit('thread.auto-settle-set',tid,{**common,'autoSettleDisabledAt':t.get('autoSettleDisabledAt')},at)
                if t.get('snoozedUntil'):
                    emit('thread.snoozed',tid,{**common,'snoozedUntil':t['snoozedUntil'],'snoozedAt':t['snoozedAt']},at)
                else: emit('thread.unsnoozed',tid,{**common,'reason':'user'},at)
                if t.get('settledOverride')=='settled' and t.get('settledAt'):
                    emit('thread.settled',tid,{**common,'settledAt':t['settledAt']},at)
                elif t.get('settledOverride')=='unsettled':
                    emit('thread.unsettled',tid,{**common,'reason':'user'},at)
                if t.get('deletedAt'): emit('thread.deleted',tid,{'threadId':tid,'deletedAt':t['deletedAt']},t['deletedAt'])
                provider_thread=db.execute('SELECT payload_json FROM orchestration_v2_projection_provider_threads WHERE provider_thread_id=?',(t.get('activeProviderThreadId'),)).fetchone()
                if not provider_thread: continue
                pt=json.loads(provider_thread[0]);native=pt.get('nativeThreadRef') or {}
                if pt['driver']!='codex':
                    raise ValueError('Unsupported native provider thread; review continuation before downgrade')
                if not native.get('nativeId'): continue  # Provider thread never successfully started.
                instance=pt['providerInstanceId']
                # V1 adapter resumes from the native Codex id, never the V2 app id.
                cursor=json.dumps({'threadId':native['nativeId']})
                project=db.execute('SELECT workspace_root FROM projection_projects WHERE project_id=?',(t['projectId'],)).fetchone()
                runtime=json.dumps({'cwd':t.get('worktreePath') or project[0],'model':t['modelSelection']['model'],
                    'activeTurnId':None,'lastError':None,'modelSelection':t['modelSelection'],
                    'continueAfterServerUpdate':None,'continueAfterServerUpdatePrepared':None})
                db.execute('''INSERT OR REPLACE INTO provider_session_runtime
                    (thread_id,provider_name,adapter_key,runtime_mode,status,last_seen_at,resume_cursor_json,runtime_payload_json,provider_instance_id)
                    VALUES (?,?,?,?,'stopped',?,?,?,?)''',(tid,'codex',instance,t['runtimeMode'],now,cursor,runtime,instance))
                counts['resumable_threads']+=1
            # No runtime or unfinished workflow is transferred as runnable work.
            db.execute("UPDATE provider_session_runtime SET status='stopped'")
            db.execute('CREATE TABLE protocol1_export_manifest (source TEXT,exported_at TEXT,counts_json TEXT)')
            db.execute('INSERT INTO protocol1_export_manifest VALUES (?,?,?)',(str(source),now,json.dumps(counts)))
        if db.execute('PRAGMA integrity_check').fetchone()[0]!='ok': raise ValueError('Export integrity check failed')
        if db.execute('PRAGMA foreign_key_check').fetchall(): raise ValueError('Export foreign-key check failed')
        db.close();src.close()
        os.link(scratch,destination)  # no replace, including races
        os.unlink(scratch)
        return counts
    finally:
        db.close();src.close()
        if os.path.exists(scratch): os.unlink(scratch)

if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source',required=True)
    parser.add_argument('--destination',required=True)
    args=parser.parse_args()
    print(json.dumps(migrate(args.source,args.destination),sort_keys=True))
