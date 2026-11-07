import fcntl
import json
import os
import subprocess
import sys
import time
from pathlib import Path

from config import DB_PATH
from core.scrape_queue import QueueStore


ROOT = Path(__file__).resolve().parent.parent
_store = None


def store():
    global _store
    if _store is None or _store.path != str(DB_PATH):
        _store = QueueStore(DB_PATH)
    return _store


def ensure_worker():
    queue = store()
    if queue.worker_state()['alive']:
        return
    lock_path = ROOT / 'data' / 'scrape_supervisor.lock'
    with lock_path.open('a+') as guard:
        try:
            fcntl.flock(guard, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return
        env = dict(os.environ, AVIDEO_PROCESS_ROLE='worker', AVIDEO_JAVDB_AUTOSTART='0')
        env['PYTHONPATH'] = str(ROOT)
        with (ROOT / 'data' / 'logs' / 'scrape_worker.log').open('ab') as log:
            subprocess.Popen([sys.executable, '-m', 'core.scrape_worker', '--supervisor'], cwd=ROOT, env=env,
                             stdin=subprocess.DEVNULL, stdout=log, stderr=log, start_new_session=True)


def control_worker(action):
    queue = store()
    if action == 'pause':
        queue.set_settings('control', {'paused': True})
    elif action in ('start', 'resume'):
        queue.set_settings('control', {'desired': 'run', 'paused': False})
        ensure_worker()
    elif action == 'stop':
        queue.set_settings('control', {'desired': 'stop'})
    elif action == 'restart':
        if queue.worker_state()['alive']:
            queue.set_settings('control', {'desired': 'restart'})
        else:
            queue.set_settings('control', {'desired': 'run'})
            ensure_worker()
    else:
        raise ValueError('未知 worker 操作')
    return queue.worker_state()


class WorkerClient:
    def call(self, kind, code='', payload=None, forced=False, dry_run=False):
        queue = store()
        control = queue.settings('control')
        if control['desired'] != 'run' and forced:
            queue.set_settings('control', {'desired': 'run'})
        task_id = queue.submit(kind, code, payload, forced=forced, dry_run=dry_run)
        ensure_worker()
        while True:
            task = queue.get(task_id)
            if task['status'] == 'DONE':
                return json.loads(task['result_json'] or 'null')
            if task['status'] in ('FAILED', 'CANCELLED', 'SKIPPED'):
                if task['result_status'] == 'LOGIN_REQUIRED':
                    raise PermissionError(task['error'])
                raise RuntimeError(task['error'] or task['status'])
            time.sleep(0.5)

    def scrape(self, code):
        return self.call('video', code)

    def scrape_ltx(self, code):
        return self.call('ltx_preview', code, dry_run=True)

    def actor(self, name):
        return self.call('actor', name)

    def check_login(self):
        return self.call('login_status', forced=True)

    def open_login(self):
        return self.call('open_login', forced=True)

    def close(self):
        control_worker('stop')


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('action', choices=['start', 'resume', 'pause', 'stop', 'restart', 'status'])
    action = parser.parse_args().action
    if action == 'status':
        print(json.dumps({'worker': store().worker_state(), 'control': store().settings('control')}, ensure_ascii=False))
    else:
        print(json.dumps(control_worker(action), ensure_ascii=False))
