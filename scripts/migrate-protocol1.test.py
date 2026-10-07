import importlib.util
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest

spec = importlib.util.spec_from_file_location('exporter', Path(__file__).with_name('migrate-protocol1.py'))
exporter = importlib.util.module_from_spec(spec)
spec.loader.exec_module(exporter)

class ExportTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.source = Path(self.tmp.name)/'statev2.sqlite'
        self.target = Path(self.tmp.name)/'state.sqlite'
        db = sqlite3.connect(self.source)
        self.addCleanup(db.close)
        self.db = db
        db.executescript('''
          CREATE TABLE effect_sql_migrations(migration_id INTEGER PRIMARY KEY,name TEXT);
          INSERT INTO effect_sql_migrations VALUES(54,'ProjectionThreadsAutoSettleDisabledAt'),(55,'OrchestrationV2');
          CREATE TABLE orchestration_events(sequence INTEGER PRIMARY KEY AUTOINCREMENT,event_id TEXT UNIQUE,
            aggregate_kind TEXT,stream_id TEXT,stream_version INTEGER,event_type TEXT,occurred_at TEXT,command_id TEXT,
            causation_event_id TEXT,correlation_id TEXT,actor_kind TEXT,payload_json TEXT,metadata_json TEXT,application_event_version INTEGER);
          CREATE TABLE orchestration_command_receipts(command_type TEXT);
          CREATE TABLE projection_state(last_applied_sequence INTEGER);
          CREATE TABLE projection_threads(thread_id TEXT);
          CREATE TABLE projection_projects(project_id TEXT,title TEXT,workspace_root TEXT,default_model_selection_json TEXT,scripts_json TEXT,created_at TEXT,updated_at TEXT);
          CREATE TABLE projection_thread_messages(message_id TEXT,turn_id TEXT,text TEXT,is_streaming INTEGER,context_json TEXT);
          CREATE TABLE provider_session_runtime(thread_id TEXT PRIMARY KEY,provider_name TEXT,adapter_key TEXT,runtime_mode TEXT,status TEXT,last_seen_at TEXT,resume_cursor_json TEXT,runtime_payload_json TEXT,provider_instance_id TEXT);
          CREATE TABLE orchestration_v2_projection_threads(thread_id TEXT,payload_json TEXT);
          CREATE TABLE orchestration_v2_projection_messages(message_id TEXT,created_at TEXT,payload_json TEXT);
          CREATE TABLE orchestration_v2_projection_runs(requested_at TEXT,ordinal INTEGER,payload_json TEXT);
          CREATE TABLE orchestration_v2_projection_checkpoints(captured_at TEXT,payload_json TEXT);
          CREATE TABLE orchestration_v2_projection_turn_items(thread_id TEXT,ordinal INTEGER,payload_json TEXT);
          CREATE TABLE orchestration_v2_projection_provider_threads(provider_thread_id TEXT,payload_json TEXT);
        ''')
        at='2026-10-06T20:00:00.000Z'
        model={'instanceId':'codex','model':'gpt-5.6-sol','options':[]}
        thread=dict(id='new-thread',projectId='project',title='New conversation',modelSelection=model,runtimeMode='full-access',
            interactionMode='default',createdAt=at,updatedAt=at,activeProviderThreadId='provider-thread')
        db.execute('INSERT INTO projection_projects VALUES(?,?,?,?,?,?,?)',('project','Project','/workspace',None,'[]',at,at))
        db.execute('INSERT INTO orchestration_v2_projection_threads VALUES(?,?)',('new-thread',json.dumps(thread)))
        message=dict(id='new-message',threadId='new-thread',role='user',text='Preserve my request',createdAt=at,updatedAt=at,attachments=[])
        db.execute('INSERT INTO orchestration_v2_projection_messages VALUES(?,?,?)',('new-message',at,json.dumps(message)))
        pt=dict(driver='codex',providerInstanceId='codex',nativeThreadRef={'nativeId':'native-codex-thread'})
        db.execute('INSERT INTO orchestration_v2_projection_provider_threads VALUES(?,?)',('provider-thread',json.dumps(pt)))
        db.commit()

    def test_snapshot_preserves_messages_and_native_resume_without_editing_source(self):
        original=self.source.read_bytes()
        result=exporter.migrate(self.source,self.target)
        self.assertEqual(self.source.read_bytes(),original)
        self.assertEqual(result['threads'],1)
        self.assertEqual(result['messages'],1)
        with sqlite3.connect(self.target) as db:
            texts=[json.loads(r[0])['text'] for r in db.execute("SELECT payload_json FROM orchestration_events WHERE event_type='thread.message-sent'")]
            self.assertEqual(texts,['Preserve my request'])
            runtime=db.execute('SELECT status,resume_cursor_json FROM provider_session_runtime').fetchone()
            self.assertEqual(runtime,('stopped','{"threadId": "native-codex-thread"}'))
            self.assertEqual(db.execute('SELECT max(migration_id) FROM effect_sql_migrations').fetchone()[0],54)

    def test_never_overwrites_an_existing_destination(self):
        self.target.write_text('existing database')
        with self.assertRaises(ValueError): exporter.migrate(self.source,self.target)
        self.assertEqual(self.target.read_text(),'existing database')

    def test_refuses_unreviewed_schema_upgrade(self):
        self.db.execute("INSERT INTO effect_sql_migrations VALUES(59,'Future')")
        self.db.commit()
        with self.assertRaises(ValueError): exporter.migrate(self.source,self.target)
        self.assertFalse(self.target.exists())

    def test_refuses_source_as_destination(self):
        with self.assertRaises(ValueError): exporter.migrate(self.source,self.source)

if __name__=='__main__': unittest.main()
