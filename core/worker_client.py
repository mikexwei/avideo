"""Nonblocking task submission and independent background service controls."""
import json

from config import DB_PATH
from core.browser_queue import RuntimeStore

_store = None


def store():
    global _store
    if _store is None or _store.path != str(DB_PATH):
        _store = RuntimeStore(DB_PATH)
    return _store


def ensure_worker():
    from core.service_manager import ensure_service
    ensure_service('consumer')
    ensure_service('browser')


def control_worker(action):
    from core.service_manager import control_service
    return control_service('consumer', action)


class WorkerClient:
    def call(self, kind, code='', payload=None, forced=False, dry_run=False, priority=0):
        if kind in ('ltx', 'ltx_preview'):
            raise ValueError('LTX 功能已关闭')
        task_id = store().submit(kind, code, payload, forced=forced, dry_run=dry_run, priority=priority)
        ensure_worker()
        return task_id

    def scrape(self, code):
        return self.call('video', code, priority=-1000)

    def actor(self, name, actor_id=None):
        task_id = store().submit_actor(name, actor_id)
        ensure_worker()
        return task_id

    def check_login(self):
        return self.call('login_status')

    def open_login(self):
        return self.call('open_login')

    def close(self):
        from core.service_manager import control_service
        return control_service('browser', 'stop')


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('action', choices=['start', 'resume', 'pause', 'stop', 'restart', 'status'])
    parser.add_argument('--service', choices=['consumer', 'browser'], default='consumer')
    args = parser.parse_args()
    if args.action == 'status':
        result = store().service(args.service)
    else:
        from core.service_manager import control_service
        result = control_service(args.service, args.action)
    print(json.dumps(result, ensure_ascii=False))
