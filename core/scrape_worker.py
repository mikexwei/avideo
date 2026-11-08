"""Compatibility entry point for the former mixed worker.

Process ownership is implemented by service_manager. Browser and workflow code
live in independent modules.
"""
from core.scrape_queue import StoredJob


if __name__ == '__main__':
    import sys
    from core.service_manager import run_service, supervise
    if '--supervisor' in sys.argv:
        supervise('consumer')
    else:
        run_service('consumer')
