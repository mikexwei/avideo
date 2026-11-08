"""Durable scrape tasks, execution history, and worker control state."""
import json
import re
import sqlite3
import time
import uuid
from contextlib import contextmanager


TERMINAL = {'DONE', 'FAILED', 'SKIPPED', 'CANCELLED'}


def base_code(code):
    return re.sub(r'-(?:UC|U|C)$', '', code.strip().upper())


class QueueConflict(ValueError):
    def __init__(self, rows):
        self.rows = rows
        super().__init__('部分番号已有执行记录，请确认是否重新刮削')


class QueueStore:
    def __init__(self, path):
        self.path = str(path)
        with self.connect() as conn:
            conn.executescript('''
                CREATE TABLE IF NOT EXISTS scrape_tasks (
                    id TEXT PRIMARY KEY, kind TEXT NOT NULL, code TEXT NOT NULL DEFAULT '',
                    status TEXT NOT NULL DEFAULT 'PENDING', priority INTEGER NOT NULL DEFAULT 0,
                    forced INTEGER NOT NULL DEFAULT 0, dry_run INTEGER NOT NULL DEFAULT 0,
                    payload_json TEXT NOT NULL DEFAULT '{}', result_json TEXT,
                    result_status TEXT, error TEXT, attempts INTEGER NOT NULL DEFAULT 0,
                    retries INTEGER NOT NULL DEFAULT 0, cancel_requested INTEGER NOT NULL DEFAULT 0,
                    deleted INTEGER NOT NULL DEFAULT 0, owner TEXT,
                    created_at REAL NOT NULL, queued_at REAL NOT NULL,
                    started_at REAL, finished_at REAL, next_run_at REAL NOT NULL DEFAULT 0,
                    checkpoint_json TEXT NOT NULL DEFAULT '{}'
                );
                CREATE UNIQUE INDEX IF NOT EXISTS idx_scrape_ltx_code ON scrape_tasks(code) WHERE kind='ltx';
                CREATE INDEX IF NOT EXISTS idx_scrape_pending ON scrape_tasks(status, next_run_at, priority);
                CREATE TABLE IF NOT EXISTS scrape_history (
                    id INTEGER PRIMARY KEY, task_id TEXT NOT NULL, code TEXT NOT NULL,
                    status TEXT NOT NULL, result_json TEXT, error TEXT, forced INTEGER NOT NULL,
                    started_at REAL, finished_at REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS scrape_events (
                    id INTEGER PRIMARY KEY, task_id TEXT NOT NULL, event_json TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_scrape_events_task ON scrape_events(task_id,id);
                CREATE TABLE IF NOT EXISTS scrape_settings (key TEXT PRIMARY KEY, value_json TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS scrape_worker (
                    id INTEGER PRIMARY KEY CHECK(id=1), pid INTEGER, heartbeat REAL,
                    state TEXT NOT NULL DEFAULT 'stopped', message TEXT NOT NULL DEFAULT '',
                    task_id TEXT, code TEXT, next_at TEXT
                );
                INSERT OR IGNORE INTO scrape_worker(id) VALUES (1);
                INSERT OR IGNORE INTO scrape_settings VALUES ('control', '{"desired":"run","paused":false}');
                INSERT OR IGNORE INTO scrape_settings VALUES ('quota', '{"daily_limit":200,"peak_hour":17,"spread":4}');
            ''')
            if conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='ltx_scrape_results'").fetchone():
                for row in conn.execute("SELECT *,CAST(strftime('%s',scraped_at) AS REAL) AS scraped_epoch FROM ltx_scrape_results").fetchall():
                    now = time.time()
                    task_id = uuid.uuid4().hex
                    status = 'FAILED' if row['status'] == 'ERROR' else 'DONE'
                    result = row['links_json'] or '[]'
                    added = conn.execute('''INSERT OR IGNORE INTO scrape_tasks
                        (id,kind,code,status,result_json,result_status,error,attempts,created_at,queued_at,finished_at)
                        VALUES (?,'ltx',?,?,?,?,?,1,?,?,?)''',
                        (task_id, base_code(row['code']), status, result, row['status'], row['error'],
                         row['scraped_epoch'] or now, row['scraped_epoch'] or now, row['scraped_epoch'] or now)).rowcount
                    if added:
                        conn.execute('''INSERT INTO scrape_history(task_id,code,status,result_json,error,forced,finished_at)
                            VALUES (?,?,?,?,?,0,?)''', (task_id, base_code(row['code']), status, result, row['error'], row['scraped_epoch'] or now))

    @contextmanager
    def connect(self, write=False):
        conn = sqlite3.connect(self.path, timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute('PRAGMA busy_timeout=30000')
        try:
            if write:
                conn.execute('BEGIN IMMEDIATE')
            yield conn
            conn.commit()
        except BaseException:
            conn.rollback()
            raise
        finally:
            conn.close()

    def settings(self, key):
        with self.connect() as conn:
            return json.loads(conn.execute('SELECT value_json FROM scrape_settings WHERE key=?', (key,)).fetchone()[0])

    def set_settings(self, key, values):
        with self.connect(write=True) as conn:
            current = json.loads(conn.execute('SELECT value_json FROM scrape_settings WHERE key=?', (key,)).fetchone()[0])
            current.update(values)
            conn.execute('UPDATE scrape_settings SET value_json=? WHERE key=?', (json.dumps(current), key))
        return current

    def get(self, task_id):
        with self.connect() as conn:
            row = conn.execute('SELECT * FROM scrape_tasks WHERE id=?', (task_id,)).fetchone()
            return dict(row) if row else None

    def submit(self, kind, code='', payload=None, forced=False, dry_run=False, task_id=None):
        task_id = task_id or uuid.uuid4().hex
        now = time.time()
        with self.connect(write=True) as conn:
            conn.execute('''INSERT OR IGNORE INTO scrape_tasks
                (id,kind,code,payload_json,forced,dry_run,created_at,queued_at)
                VALUES (?,?,?,?,?,?,?,?)''',
                (task_id, kind, code, json.dumps(payload or {}, ensure_ascii=False), int(forced), int(dry_run), now, now))
        return task_id

    def enqueue(self, codes, confirm=False, forced=False, priority=0):
        codes = list(dict.fromkeys(base_code(code) for code in codes))
        now = time.time()
        with self.connect(write=True) as conn:
            last = conn.execute('SELECT MAX(queued_at) FROM scrape_tasks').fetchone()[0]
            queued_at = max(now, (last or 0) + 0.000001)
            existing = {}
            for code in codes:
                row = conn.execute("SELECT * FROM scrape_tasks WHERE kind='ltx' AND code=?", (code,)).fetchone()
                if row:
                    existing[code] = row
            conflicts = [dict(row) for row in existing.values() if row['status'] in TERMINAL]
            if conflicts and not confirm:
                raise QueueConflict([{'code': row['code'], 'status': row['status'],
                                      'result_status': row['result_status'], 'finished_at': row['finished_at']}
                                     for row in conflicts])
            added = 0
            ids = []
            for position, code in enumerate(codes):
                row = existing.get(code)
                if row and row['status'] == 'RUNNING':
                    if forced:
                        conn.execute('UPDATE scrape_tasks SET forced=1 WHERE id=?', (row['id'],))
                        added += 1
                        ids.append(row['id'])
                    continue
                if row and row['status'] == 'PENDING' and not forced:
                    continue
                task_id = row['id'] if row else uuid.uuid4().hex
                if row:
                    conn.execute('''UPDATE scrape_tasks SET status='PENDING',forced=?,priority=?,queued_at=?,
                        next_run_at=0,cancel_requested=0,deleted=0,owner=NULL,error=NULL,retries=0 WHERE id=?''',
                        (int(forced), priority, queued_at + position * 0.000001, task_id))
                else:
                    conn.execute('''INSERT INTO scrape_tasks(id,kind,code,forced,priority,created_at,queued_at)
                        VALUES (?,'ltx',?,?,?,?,?)''', (task_id, code, int(forced), priority, now, queued_at + position * 0.000001))
                added += 1
                ids.append(task_id)
        return {'added': added, 'ids': ids, 'duplicates': len(codes) - added}

    def list_tasks(self, kind='ltx'):
        with self.connect() as conn:
            return [dict(row) for row in conn.execute('''SELECT * FROM scrape_tasks
                WHERE kind=? AND deleted=0 ORDER BY forced DESC,priority DESC,queued_at,id''', (kind,))]

    def history(self, task_id):
        with self.connect() as conn:
            return [dict(row) for row in conn.execute('SELECT * FROM scrape_history WHERE task_id=? ORDER BY id DESC', (task_id,))]

    def bulk(self, ids, action, priority=0):
        changed = 0
        with self.connect(write=True) as conn:
            for task_id in dict.fromkeys(ids):
                row = conn.execute("SELECT * FROM scrape_tasks WHERE id=? AND kind='ltx'", (task_id,)).fetchone()
                if not row:
                    continue
                if action in ('cancel', 'delete'):
                    conn.execute('''UPDATE scrape_tasks SET cancel_requested=1,deleted=?,
                        status=CASE WHEN status='RUNNING' THEN status ELSE 'CANCELLED' END WHERE id=?''',
                        (int(action == 'delete'), task_id))
                elif action == 'priority':
                    conn.execute('UPDATE scrape_tasks SET priority=? WHERE id=?', (priority, task_id))
                elif action == 'immediate' and row['status'] == 'RUNNING':
                    conn.execute('UPDATE scrape_tasks SET forced=1 WHERE id=?', (task_id,))
                elif action in ('requeue', 'immediate') and row['status'] != 'RUNNING':
                    conn.execute('''UPDATE scrape_tasks SET status='PENDING',forced=?,cancel_requested=0,deleted=0,
                        retries=0,next_run_at=0,queued_at=?,owner=NULL,error=NULL WHERE id=?''',
                        (int(action == 'immediate'), time.time(), task_id))
                else:
                    continue
                changed += 1
        return changed

    def eligibility(self, code, forced=False):
        if not forced and 'VR' in code.split('-')[0].upper():
            return 'VR 番号已排除'
        variants = [code, code + '-C', code + '-U', code + '-UC']
        with self.connect() as conn:
            ids = [row[0] for row in conn.execute('SELECT id FROM videos WHERE deleted=0 AND UPPER(code) IN (?,?,?,?)', variants)]
            if not ids:
                return '影片不存在、已删除或番号已更改'
            if not forced:
                marks = ','.join('?' * len(ids))
                row = conn.execute(f'''SELECT t.name FROM video_tag_link l JOIN tags t ON t.id=l.tag_id
                    WHERE l.video_id IN ({marks}) AND t.name IN ('LTX 无码破解','无码流出','原装无码') LIMIT 1''', ids).fetchone()
                if row:
                    return '影片已有排除标签：' + row[0]
        return None

    def claim(self, owner):
        control = self.settings('control')
        with self.connect(write=True) as conn:
            row = conn.execute('''SELECT * FROM scrape_tasks WHERE status='PENDING' AND deleted=0
                AND cancel_requested=0 AND (forced=1 OR next_run_at<=?) AND (forced=1 OR ?=0)
                ORDER BY forced DESC,CASE WHEN forced=0 AND kind!='ltx' THEN 1 ELSE 0 END DESC,
                priority DESC,queued_at,id LIMIT 1''',
                (time.time(), int(control['paused']))).fetchone()
            if not row:
                return None
            conn.execute("UPDATE scrape_tasks SET status='RUNNING',owner=?,started_at=? WHERE id=?",
                         (owner, time.time(), row['id']))
            result = dict(row)
            result.update(status='RUNNING', owner=owner, started_at=time.time())
            return result

    def begin(self, task_id):
        with self.connect(write=True) as conn:
            conn.execute('UPDATE scrape_tasks SET attempts=attempts+1 WHERE id=?', (task_id,))

    def defer(self, task_id, until=0, error=None, interrupted=False):
        with self.connect(write=True) as conn:
            row = conn.execute('SELECT * FROM scrape_tasks WHERE id=?', (task_id,)).fetchone()
            if interrupted:
                self._history(conn, row, 'CANCELLED' if row['cancel_requested'] else 'INTERRUPTED', None, error)
            conn.execute('''UPDATE scrape_tasks SET status=CASE WHEN cancel_requested=1 THEN 'CANCELLED' ELSE 'PENDING' END,
                owner=NULL,next_run_at=?,error=? WHERE id=?''', (until, error, task_id))
            if interrupted:
                conn.execute('UPDATE scrape_tasks SET queued_at=? WHERE id=?', (time.time(), task_id))

    def finish(self, task_id, status, result=None, error=None):
        encoded = json.dumps(result, ensure_ascii=False) if result is not None else None
        with self.connect(write=True) as conn:
            row = conn.execute('SELECT * FROM scrape_tasks WHERE id=?', (task_id,)).fetchone()
            if not row or row['status'] != 'RUNNING':
                return
            if row['cancel_requested']:
                status = 'CANCELLED'
            self._history(conn, row, status, encoded, error)
            result_status = ('FOUND' if result else 'NOT_FOUND') if row['kind'] == 'ltx' and status == 'DONE' else status
            conn.execute('''UPDATE scrape_tasks SET status=?,result_json=CASE WHEN ?='DONE' THEN ? ELSE result_json END,
                result_status=?,error=?,finished_at=?,owner=NULL WHERE id=?''',
                (status, status, encoded, result_status, error, time.time(), task_id))
            if row['kind'] == 'ltx' and status == 'DONE':
                conn.execute('''CREATE TABLE IF NOT EXISTS ltx_scrape_results (
                    code TEXT PRIMARY KEY COLLATE NOCASE,status TEXT NOT NULL,links_json TEXT NOT NULL DEFAULT '[]',
                    downloaded INTEGER NOT NULL DEFAULT 0,error TEXT,scraped_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT DEFAULT CURRENT_TIMESTAMP)''')
                conn.execute('''INSERT INTO ltx_scrape_results(code,status,links_json) VALUES (?,?,?)
                    ON CONFLICT(code) DO UPDATE SET status=excluded.status,links_json=excluded.links_json,
                    downloaded=CASE WHEN ltx_scrape_results.links_json=excluded.links_json THEN ltx_scrape_results.downloaded ELSE 0 END,
                    error=NULL,scraped_at=CURRENT_TIMESTAMP,updated_at=CURRENT_TIMESTAMP''',
                    (row['code'], result_status, encoded or '[]'))

    def _history(self, conn, row, status, result, error):
        conn.execute('''INSERT INTO scrape_history(task_id,code,status,result_json,error,forced,started_at,finished_at)
            VALUES (?,?,?,?,?,?,?,?)''',
            (row['id'], row['code'], status, result, error, row['forced'], row['started_at'], time.time()))

    def recover(self):
        with self.connect(write=True) as conn:
            for row in conn.execute("SELECT * FROM scrape_tasks WHERE status='RUNNING'").fetchall():
                self._history(conn, row, 'INTERRUPTED', None, 'worker 异常退出，任务恢复等待')
            conn.execute("UPDATE scrape_tasks SET status=CASE WHEN cancel_requested=1 THEN 'CANCELLED' ELSE 'PENDING' END,owner=NULL WHERE status='RUNNING'")

    def event(self, task_id, event):
        with self.connect(write=True) as conn:
            conn.execute('INSERT INTO scrape_events(task_id,event_json) VALUES (?,?)', (task_id, json.dumps(event, ensure_ascii=False)))

    def events(self, task_id, after=0):
        with self.connect() as conn:
            return [dict(row) for row in conn.execute('SELECT * FROM scrape_events WHERE task_id=? AND id>? ORDER BY id', (task_id, after))]

    def checkpoint(self, task_id, value=None):
        with self.connect(write=value is not None) as conn:
            if value is not None:
                conn.execute('UPDATE scrape_tasks SET checkpoint_json=? WHERE id=?', (json.dumps(value, ensure_ascii=False), task_id))
            return json.loads(conn.execute('SELECT checkpoint_json FROM scrape_tasks WHERE id=?', (task_id,)).fetchone()[0])

    def worker_state(self, **values):
        with self.connect(write=bool(values)) as conn:
            if values:
                conn.execute('UPDATE scrape_worker SET ' + ','.join(key + '=?' for key in values) + ' WHERE id=1', list(values.values()))
            row = dict(conn.execute('SELECT * FROM scrape_worker WHERE id=1').fetchone())
        row['alive'] = bool(row['heartbeat'] and time.time() - row['heartbeat'] < 20)
        return row


class StoredJob:
    def __init__(self, store, task_id, execution_id=None):
        self.store, self.job_id = store, task_id
        self.execution_id = execution_id
        self.queue = self

    def put(self, event):
        if self.execution_id is None:
            self.store.event(self.job_id, event)
        else:
            self.store.event(self.job_id, event, execution_id=self.execution_id)

    def checkpoint(self, value=None):
        if self.execution_id is None:
            return self.store.checkpoint(self.job_id, value)
        with self.store.connect(write=value is not None) as conn:
            row = conn.execute('SELECT * FROM scrape_tasks WHERE id=?', (self.job_id,)).fetchone()
            if not row or row['execution_id'] != self.execution_id or row['cancel_requested']:
                from core.task_policy import TargetChanged
                raise TargetChanged('任务已取消或执行版本已改变')
            if value is not None:
                conn.execute('UPDATE scrape_tasks SET checkpoint_json=? WHERE id=?', (json.dumps(value, ensure_ascii=False), self.job_id))
                return value
            return json.loads(row['checkpoint_json'])
