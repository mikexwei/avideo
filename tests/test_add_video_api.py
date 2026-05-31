import io
import json
import os
import queue
import sqlite3
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from web.backend.app import app, _Job, _jobs, _jobs_lock


class JobInfraTests(unittest.TestCase):
    def setUp(self):
        app.config['TESTING'] = True
        self.client = app.test_client()

    def test_job_dataclass_has_queue_and_status(self):
        j = _Job(job_id='test-123')
        self.assertEqual(j.job_id, 'test-123')
        self.assertIsInstance(j.queue, queue.Queue)
        self.assertEqual(j.status, 'running')

    def test_jobs_dict_is_module_level(self):
        self.assertIsInstance(_jobs, dict)
        self.assertIsInstance(_jobs_lock, type(threading.Lock()))
