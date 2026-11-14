"""Durable commands and result acknowledgements shared by the three services."""
import json
import time
import uuid
from contextlib import nullcontext
from datetime import datetime
from zoneinfo import ZoneInfo

from core.scrape_queue import QueueStore
from core.series_catalog import migrate as migrate_series

BROWSER_TIMEZONE = ZoneInfo('America/Los_Angeles')


class RuntimeStore(QueueStore):
    def __init__(self, path):
        super().__init__(path)
        with self.connect(write=True) as conn:
            columns = {row['name'] for row in conn.execute('PRAGMA table_info(scrape_tasks)')}
            if 'execution_id' not in columns:
                conn.execute('ALTER TABLE scrape_tasks ADD COLUMN execution_id TEXT')
            if 'paused' not in columns:
                conn.execute('ALTER TABLE scrape_tasks ADD COLUMN paused INTEGER NOT NULL DEFAULT 0')
            event_columns = {row['name'] for row in conn.execute('PRAGMA table_info(scrape_events)')}
            if 'execution_id' not in event_columns:
                conn.execute('ALTER TABLE scrape_events ADD COLUMN execution_id TEXT')
            conn.execute('''
                CREATE TABLE IF NOT EXISTS javdb_listing_items (
                    task_id TEXT NOT NULL, code TEXT NOT NULL, page INTEGER NOT NULL,
                    title TEXT NOT NULL, actors_json TEXT NOT NULL, url TEXT NOT NULL,
                    tags_json TEXT, PRIMARY KEY(task_id, code)
                )
            ''')
            if 'tags_json' not in {row['name'] for row in conn.execute('PRAGMA table_info(javdb_listing_items)')}:
                conn.execute('ALTER TABLE javdb_listing_items ADD COLUMN tags_json TEXT')
            conn.executescript('''
                CREATE INDEX IF NOT EXISTS javdb_listing_code ON javdb_listing_items(code);
                CREATE TABLE IF NOT EXISTS service_state (
                    name TEXT PRIMARY KEY, desired TEXT NOT NULL DEFAULT 'stop',
                    paused INTEGER NOT NULL DEFAULT 0, pid INTEGER, heartbeat REAL DEFAULT 0,
                    state TEXT DEFAULT 'stopped', message TEXT DEFAULT '', current_id TEXT,
                    restart_token TEXT DEFAULT '', generation TEXT DEFAULT ''
                );
                INSERT OR IGNORE INTO service_state(name) VALUES ('consumer'),('browser');
                CREATE TABLE IF NOT EXISTS dependency_commands (
                    id TEXT PRIMARY KEY, task_id TEXT NOT NULL, execution_id TEXT NOT NULL,
                    step TEXT NOT NULL, service TEXT NOT NULL, kind TEXT NOT NULL,
                    payload_json TEXT NOT NULL, priority INTEGER NOT NULL DEFAULT 0,
                    status TEXT NOT NULL DEFAULT 'PENDING', attempt INTEGER NOT NULL DEFAULT 0,
                    owner TEXT, created_at REAL NOT NULL, started_at REAL,
                    next_run_at REAL NOT NULL DEFAULT 0, cancel_requested INTEGER DEFAULT 0,
                    UNIQUE(execution_id,step)
                );
                CREATE INDEX IF NOT EXISTS dependency_ready ON dependency_commands(service,status,next_run_at,priority);
                CREATE TABLE IF NOT EXISTS dependency_results (
                    id INTEGER PRIMARY KEY, command_id TEXT NOT NULL, attempt INTEGER NOT NULL,
                    execution_id TEXT NOT NULL, outcome TEXT NOT NULL, result_json TEXT,
                    error TEXT, created_at REAL NOT NULL, acknowledged_at REAL,
                    UNIQUE(command_id,attempt)
                );
                CREATE TABLE IF NOT EXISTS workflow_targets (target TEXT PRIMARY KEY, task_id TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS browser_access (
                    id INTEGER PRIMARY KEY, accessed_at REAL NOT NULL, kind TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS browser_access_time ON browser_access(accessed_at);
                CREATE TABLE IF NOT EXISTS browser_quota (
                    effective_at INTEGER PRIMARY KEY, profile_json TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS admin_history (
                    id INTEGER PRIMARY KEY, created_at REAL NOT NULL, action TEXT NOT NULL,
                    target TEXT NOT NULL, details_json TEXT NOT NULL
                );
                INSERT OR IGNORE INTO scrape_settings(key,value_json) VALUES
                    ('browser', '{"interval_min":32.5,"interval_max":107.5,"backoff_base":60,"backoff_max":900,"next_allowed_at":0,"failures":0}');
                INSERT OR IGNORE INTO scrape_settings(key,value_json) VALUES
                    ('javdb_listing_schedule', '{"enabled":false,"time":"03:00"}');
                UPDATE scrape_tasks SET status='SUSPENDED',owner=NULL,error='LTX 功能已关闭'
                    WHERE kind IN ('ltx','ltx_preview') AND status IN ('PENDING','RUNNING','WAITING');
            ''')
            migrate_series(conn)

    def start_browser_access_tracking(self):
        with self.connect(write=True) as conn:
            conn.execute("INSERT OR IGNORE INTO scrape_settings(key,value_json) VALUES ('browser_access_since',?)",
                         (json.dumps(time.time()),))

    def reserve_browser_access(self, kind, interval):
        with self.connect(write=True) as conn:
            now = time.time()
            settings = json.loads(conn.execute(
                "SELECT value_json FROM scrape_settings WHERE key='browser'"
            ).fetchone()[0])
            if settings['next_allowed_at'] > now:
                return settings['next_allowed_at']
            until = self.browser_quota_until(connection=conn, now=now)
            if until:
                return until
            conn.execute("INSERT INTO browser_access(accessed_at,kind) VALUES (?,?)", (now, kind))
            conn.execute("""UPDATE scrape_settings SET value_json=json_set(value_json,
                '$.next_allowed_at', ?, '$.cooldown_reason', '等待统一访问间隔') WHERE key='browser'""",
                         (now + interval,))

    def browser_quota_until(self, *, connection=None, now=None):
        now = time.time() if now is None else now
        hour_start = int(now // 3600) * 3600
        with (self.connect() if connection is None else nullcontext(connection)) as conn:
            row = conn.execute("""SELECT profile_json FROM browser_quota WHERE effective_at<=?
                ORDER BY effective_at DESC LIMIT 1""", (hour_start,)).fetchone()
            if row is None:
                return None
            allowed = json.loads(row[0])['limits'][datetime.fromtimestamp(now, BROWSER_TIMEZONE).hour]
            used = conn.execute("SELECT COUNT(*) FROM browser_access WHERE accessed_at>=? AND accessed_at<?",
                                (hour_start, hour_start + 3600)).fetchone()[0]
            return hour_start + 3600 if used >= allowed else None

    def submit(self, kind, code='', payload=None, forced=False, dry_run=False, task_id=None, priority=0, *, connection=None):
        if kind in ('ltx', 'ltx_preview'):
            raise ValueError('LTX 功能已关闭')
        task_id = task_id or uuid.uuid4().hex
        with (self.connect(write=True) if connection is None else nullcontext(connection)) as conn:
            if kind == 'actor' and payload and payload.get('actor_id'):
                active = conn.execute("""SELECT id FROM scrape_tasks WHERE kind='actor'
                    AND json_extract(payload_json, '$.actor_id')=? AND deleted=0 AND cancel_requested=0
                    AND status IN ('PENDING','RUNNING','WAITING','PREPARING')
                    ORDER BY queued_at,id LIMIT 1""", (payload['actor_id'],)).fetchone()
                if active:
                    return active['id']
            conn.execute("""INSERT OR IGNORE INTO scrape_tasks
                (id,kind,code,payload_json,forced,dry_run,priority,created_at,queued_at)
                VALUES (?,?,?,?,?,?,?,?,?)""", (task_id, kind, code, json.dumps(payload or {}, ensure_ascii=False),
                int(forced), int(dry_run), priority, time.time(), time.time()))
        return task_id

    def submit_actor(self, name, actor_id, *, source_code=None, connection=None):
        payload = {'actor_id': actor_id}
        if source_code:
            payload['source_code'] = source_code
        return self.submit('actor', name, payload, priority=-1000, connection=connection)

    def service(self, name, **values):
        if name not in ('consumer', 'browser'):
            raise ValueError('未知服务')
        allowed = {'desired', 'paused', 'pid', 'heartbeat', 'state', 'message', 'current_id', 'restart_token', 'generation'}
        if set(values) - allowed:
            raise ValueError('无效服务字段')
        with self.connect(write=bool(values)) as conn:
            if values:
                conn.execute('UPDATE service_state SET ' + ','.join(key + '=?' for key in values) + ' WHERE name=?',
                             [*values.values(), name])
            row = dict(conn.execute('SELECT * FROM service_state WHERE name=?', (name,)).fetchone())
        row['alive'] = bool(row['heartbeat'] and time.time() - row['heartbeat'] < 20)
        return row

    def audit(self, action, target, details=None):
        with self.connect(write=True) as conn:
            conn.execute('INSERT INTO admin_history(created_at,action,target,details_json) VALUES (?,?,?,?)',
                         (time.time(), action, target, json.dumps(details or {}, ensure_ascii=False)))

    def claim_workflow(self, owner):
        with self.connect(write=True) as conn:
            row = conn.execute("""SELECT * FROM scrape_tasks WHERE status='PENDING'
                AND kind NOT IN ('ltx','ltx_preview') AND deleted=0 AND cancel_requested=0 AND paused=0 AND next_run_at<=?
                ORDER BY forced DESC,priority DESC,queued_at,id LIMIT 1""", (time.time(),)).fetchone()
            if row is None:
                return None
            execution = row['execution_id'] or uuid.uuid4().hex
            conn.execute("UPDATE scrape_tasks SET status='RUNNING',owner=?,execution_id=?,started_at=COALESCE(started_at,?),attempts=attempts+CASE WHEN execution_id IS NULL THEN 1 ELSE 0 END WHERE id=?",
                         (owner, execution, time.time(), row['id']))
            if row['execution_id'] is None:
                conn.execute('UPDATE scrape_events SET execution_id=? WHERE task_id=? AND execution_id IS NULL', (execution, row['id']))
            task = dict(row)
            task.update(status='RUNNING', execution_id=execution)
            return task

    def save_phase(self, task_id, checkpoint):
        with self.connect(write=True) as conn:
            conn.execute("UPDATE scrape_tasks SET checkpoint_json=?,status='PENDING',owner=NULL WHERE id=? AND status='RUNNING'",
                         (json.dumps(checkpoint, ensure_ascii=False), task_id))

    def dispatch(self, task, step, service, kind, payload, checkpoint):
        command_id = uuid.uuid4().hex
        with self.connect(write=True) as conn:
            current = conn.execute('SELECT * FROM scrape_tasks WHERE id=?', (task['id'],)).fetchone()
            if current['cancel_requested'] or current['deleted']:
                raise ValueError('任务已取消')
            conn.execute('''INSERT OR IGNORE INTO dependency_commands
                (id,task_id,execution_id,step,service,kind,payload_json,priority,created_at)
                VALUES (?,?,?,?,?,?,?,?,?)''', (command_id, task['id'], task['execution_id'], step, service, kind,
                json.dumps(payload, ensure_ascii=False), 100000 + task['priority'] if task['forced'] else task['priority'], time.time()))
            command = conn.execute('SELECT id FROM dependency_commands WHERE execution_id=? AND step=?',
                                   (task['execution_id'], step)).fetchone()
            checkpoint['waiting_command'] = command['id']
            conn.execute("UPDATE scrape_tasks SET status='WAITING',owner=NULL,checkpoint_json=? WHERE id=?",
                         (json.dumps(checkpoint, ensure_ascii=False), task['id']))
        return command['id']

    def claim_command(self, service, owner, control_only=False):
        with self.connect(write=True) as conn:
            row = conn.execute('''SELECT c.* FROM dependency_commands c JOIN scrape_tasks t ON t.id=c.task_id
                WHERE c.service=? AND c.status='PENDING' AND c.next_run_at<=? AND c.cancel_requested=0
                AND t.execution_id=c.execution_id AND t.status='WAITING' AND t.cancel_requested=0 AND t.deleted=0 AND t.paused=0
                AND (?=0 OR c.kind IN ('login_status','open_login'))
                ORDER BY c.priority DESC,c.created_at,c.id LIMIT 1''', (service, time.time(), int(control_only))).fetchone()
            if row is None:
                return None
            conn.execute("UPDATE dependency_commands SET status='RUNNING',owner=?,attempt=attempt+1,started_at=? WHERE id=?",
                         (owner, time.time(), row['id']))
            result = dict(row)
            result.update(status='RUNNING', owner=owner, attempt=row['attempt'] + 1)
            return result

    def command(self, command_id):
        with self.connect() as conn:
            row = conn.execute('SELECT * FROM dependency_commands WHERE id=?', (command_id,)).fetchone()
            return dict(row) if row else None

    def publish(self, command, outcome, result=None, error=None, retry_at=None):
        with self.connect(write=True) as conn:
            row = conn.execute('SELECT * FROM dependency_commands WHERE id=?', (command['id'],)).fetchone()
            if row['status'] != 'RUNNING' or row['attempt'] != command['attempt'] or row['owner'] != command['owner']:
                return False
            conn.execute('''INSERT OR IGNORE INTO dependency_results
                (command_id,attempt,execution_id,outcome,result_json,error,created_at) VALUES (?,?,?,?,?,?,?)''',
                (command['id'], command['attempt'], command['execution_id'], outcome, json.dumps(result, ensure_ascii=False), error, time.time()))
            conn.execute("UPDATE dependency_commands SET status=?,owner=NULL,next_run_at=? WHERE id=?",
                         ('PENDING' if retry_at is not None else outcome, retry_at or 0, command['id']))
            if retry_at is not None:
                conn.execute('UPDATE dependency_results SET acknowledged_at=? WHERE command_id=? AND attempt=?',
                             (time.time(), command['id'], command['attempt']))
        return True

    def consume_result(self):
        """Acknowledge a result atomically with recording the workflow continuation."""
        with self.connect(write=True) as conn:
            row = conn.execute('''SELECT r.*,c.task_id,c.kind FROM dependency_results r
                JOIN dependency_commands c ON c.id=r.command_id WHERE r.acknowledged_at IS NULL ORDER BY r.id LIMIT 1''').fetchone()
            if row is None:
                return False
            task = conn.execute('SELECT * FROM scrape_tasks WHERE id=?', (row['task_id'],)).fetchone()
            if task and task['execution_id'] == row['execution_id'] and task['status'] == 'WAITING':
                state = json.loads(task['checkpoint_json'])
                if state.get('waiting_command') == row['command_id']:
                    state['response'] = {'command_id': row['command_id'], 'attempt': row['attempt'], 'outcome': row['outcome'], 'result': json.loads(row['result_json'] or 'null'), 'error': row['error']}
                    state.pop('waiting_command', None)
                    conn.execute("UPDATE scrape_tasks SET status='PENDING',checkpoint_json=? WHERE id=?",
                                 (json.dumps(state, ensure_ascii=False), task['id']))
            conn.execute('UPDATE dependency_results SET acknowledged_at=? WHERE id=?', (time.time(), row['id']))
        return True

    def recover_service(self, service):
        """Called under the service's exclusive process lock, never from Web."""
        with self.connect(write=True) as conn:
            if service == 'consumer':
                for row in conn.execute("SELECT * FROM scrape_tasks WHERE status='RUNNING' AND kind NOT IN ('ltx','ltx_preview')").fetchall():
                    self._history(conn, row, 'INTERRUPTED', None, 'Consumer 退出后恢复工作流阶段')
                conn.execute("UPDATE scrape_tasks SET status='PENDING',owner=NULL WHERE status='RUNNING' AND kind NOT IN ('ltx','ltx_preview')")
            rows = conn.execute("SELECT * FROM dependency_commands WHERE service=? AND status='RUNNING'", (service,)).fetchall()
            for row in rows:
                conn.execute('''INSERT OR IGNORE INTO dependency_results
                    (command_id,attempt,execution_id,outcome,error,created_at) VALUES (?,?,?,'INTERRUPTED',?,?)''',
                    (row['id'], row['attempt'], row['execution_id'], '服务退出，恢复执行', time.time()))
                # An interruption is history, not a terminal workflow response.
                conn.execute('UPDATE dependency_results SET acknowledged_at=? WHERE command_id=? AND attempt=?',
                             (time.time(), row['id'], row['attempt']))
                conn.execute("UPDATE dependency_commands SET status='PENDING',owner=NULL WHERE id=?", (row['id'],))

    def cancel_task(self, task_id):
        with self.connect(write=True) as conn:
            row = conn.execute('SELECT * FROM scrape_tasks WHERE id=?', (task_id,)).fetchone()
            if not row:
                raise ValueError('任务不存在')
            if row['status'] not in ('DONE', 'FAILED', 'SKIPPED', 'CANCELLED', 'SUSPENDED'):
                self._history(conn, row, 'CANCELLED', None, '管理员取消任务')
            conn.execute("UPDATE scrape_tasks SET cancel_requested=1,status='CANCELLED' WHERE id=? AND status NOT IN ('DONE','FAILED','SKIPPED','CANCELLED','SUSPENDED')", (task_id,))
            conn.execute("UPDATE dependency_commands SET cancel_requested=1,status=CASE WHEN status='PENDING' THEN 'CANCELLED' ELSE status END WHERE task_id=?", (task_id,))
        self.audit('cancel', task_id)

    def retry_task(self, task_id):
        with self.connect(write=True) as conn:
            row = conn.execute('SELECT * FROM scrape_tasks WHERE id=?', (task_id,)).fetchone()
            if not row or row['kind'] in ('ltx', 'ltx_preview'):
                raise ValueError('任务不可重试')
            checkpoint = json.loads(row['checkpoint_json'])
            if row['status'] == 'DONE' and row['kind'] == 'pipeline_rescrape':
                codes = [item['code'] for item in checkpoint.get('items', []) if not item['ok']]
                if not codes:
                    raise ValueError('没有失败的影片')
                new_id = uuid.uuid4().hex
                journals = {key: checkpoint.get(key, {}) for key in ('rename_moves', 'rename_identity', 'rename_previous')}
                conn.execute("""INSERT INTO scrape_tasks(id,kind,code,payload_json,checkpoint_json,created_at,queued_at)
                    VALUES (?,'pipeline_rescrape',?,?,?,?,?)""", (new_id, row['code'], json.dumps({**json.loads(row['payload_json']), 'codes': codes}),
                    json.dumps(journals), time.time(), time.time()))
                task_id = new_id
            else:
                if row['status'] not in ('FAILED', 'SKIPPED', 'CANCELLED'):
                    raise ValueError('仅可重试已结束的失败、跳过或取消任务')
                checkpoint.pop('response', None)
                checkpoint.pop('waiting_command', None)
                if checkpoint.get('phase') in ('direct', 'actor_asset'):
                    checkpoint['phase'] = 'start'
                conn.execute("""UPDATE scrape_tasks SET status='PENDING',execution_id=?,checkpoint_json=?,cancel_requested=0,paused=0,
                    deleted=0,error=NULL,next_run_at=0,started_at=NULL,attempts=attempts+1,queued_at=? WHERE id=?""",
                    (uuid.uuid4().hex, json.dumps(checkpoint, ensure_ascii=False), time.time(), task_id))
        self.audit('retry', task_id)
        return task_id

    def event(self, task_id, event, execution_id=None):
        with self.connect(write=True) as conn:
            task = conn.execute('SELECT execution_id FROM scrape_tasks WHERE id=?', (task_id,)).fetchone()
            execution = execution_id if execution_id is not None else task['execution_id'] if task else None
            conn.execute('INSERT INTO scrape_events(task_id,event_json,execution_id) VALUES (?,?,?)',
                         (task_id, json.dumps(dict(event, execution_id=execution), ensure_ascii=False), execution))

    def events(self, task_id, after=0):
        with self.connect() as conn:
            return [dict(row) for row in conn.execute("""SELECT e.* FROM scrape_events e JOIN scrape_tasks t ON t.id=e.task_id
                WHERE e.task_id=? AND e.id>? AND e.execution_id IS t.execution_id ORDER BY e.id""", (task_id, after))]

    def active_tasks(self):
        with self.connect() as conn:
            return [dict(row) for row in conn.execute("SELECT * FROM scrape_tasks WHERE status IN ('PENDING','RUNNING','WAITING','PREPARING') AND cancel_requested=0 AND deleted=0")]

    def acquire_target(self, task_id, target):
        with self.connect(write=True) as conn:
            conn.execute("DELETE FROM workflow_targets WHERE task_id IN (SELECT id FROM scrape_tasks WHERE status IN ('DONE','FAILED','SKIPPED','CANCELLED','SUSPENDED'))")
            conn.execute('INSERT OR IGNORE INTO workflow_targets(target,task_id) VALUES (?,?)', (target, task_id))
            return conn.execute('SELECT task_id FROM workflow_targets WHERE target=?', (target,)).fetchone()[0] == task_id

    def release_targets(self, task_id):
        with self.connect(write=True) as conn:
            conn.execute('DELETE FROM workflow_targets WHERE task_id=?', (task_id,))
