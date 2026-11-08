"""Independent supervisors and process locks for Consumer and Browser Service."""
import fcntl
import logging
import os
from pathlib import Path
import signal
import subprocess
import sys
import threading
import time
import uuid

from config import DB_PATH
from core.browser_queue import RuntimeStore

ROOT = Path(__file__).resolve().parent.parent


def ensure_service(name):
    store = RuntimeStore(DB_PATH)
    state = store.service(name)
    if state['desired'] != 'run' or state['alive']:
        return
    # The child supervisor takes the durable lock. Concurrent spawns exit harmlessly.
    env = dict(os.environ, AVIDEO_PROCESS_ROLE=name, AVIDEO_JAVDB_AUTOSTART='0', PYTHONPATH=str(ROOT))
    with (ROOT / 'data' / 'logs' / f'{name}.log').open('ab') as log:
        subprocess.Popen([sys.executable, '-m', 'core.service_manager', name, '--supervisor'], cwd=ROOT, env=env,
                         stdin=subprocess.DEVNULL, stdout=log, stderr=log, start_new_session=True)


def control_service(name, action):
    store = RuntimeStore(DB_PATH)
    if action == 'pause':
        state = store.service(name, paused=1)
    elif action in ('start', 'resume'):
        state = store.service(name, desired='run', paused=0)
        ensure_service(name)
    elif action == 'stop':
        state = store.service(name, desired='stop')
    elif action == 'restart':
        state = store.service(name, desired='run', restart_token=uuid.uuid4().hex)
        ensure_service(name)
    else:
        raise ValueError('未知服务操作')
    store.audit(action, name)
    return state


def run_service(name):
    store = RuntimeStore(DB_PATH)
    with (ROOT / 'data' / f'{name}.lock').open('a+') as guard:
        try:
            fcntl.flock(guard, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return
        state = store.service(name)
        store.service(name, generation=state['restart_token'], pid=os.getpid(), heartbeat=time.time(), state='starting', message='')
        if name == 'browser':
            from core.browser_service import BrowserService
            service = BrowserService(store)
        else:
            from core.event_consumer import Consumer
            service = Consumer(store)
        signal.signal(signal.SIGTERM, lambda *_: service.terminate.set())
        signal.signal(signal.SIGINT, lambda *_: service.terminate.set())
        stopped = threading.Event()
        def heartbeat():
            while not stopped.wait(3):
                store.service(name, pid=os.getpid(), heartbeat=time.time())
        heart = threading.Thread(target=heartbeat, daemon=True)
        heart.start()
        try:
            service.run()
        finally:
            stopped.set()
            heart.join(timeout=5)
            store.service(name, heartbeat=0, state='stopped', current_id=None)


def supervise(name):
    store = RuntimeStore(DB_PATH)
    with (ROOT / 'data' / f'{name}_supervisor.lock').open('a+') as guard:
        try:
            fcntl.flock(guard, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return
        while store.service(name)['desired'] == 'run':
            child = subprocess.Popen([sys.executable, '-m', 'core.service_manager', name], cwd=ROOT)
            status = child.wait()
            if status:
                logging.warning('%s exited with %s', name, status)
            time.sleep(1)


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('service', choices=['consumer', 'browser'])
    parser.add_argument('--supervisor', action='store_true')
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    if args.supervisor:
        supervise(args.service)
    else:
        run_service(args.service)
