import unittest
from unittest.mock import patch

from web.backend.app import app


class VideoDetailStatusTests(unittest.TestCase):
    def setUp(self):
        app.config['TESTING'] = True
        self.client = app.test_client()

    def test_active_video_status(self):
        with patch('web.backend.app.get_video_by_code', return_value={
            'code': 'ABW-123', 'deleted': 0, 'scrape_status': 'SUCCESS',
        }):
            response = self.client.get('/api/videos/ABW-123')

        data = response.get_json()
        self.assertEqual(response.status_code, 200)
        self.assertTrue(data['record_exists'])
        self.assertTrue(data['exists'])
        self.assertEqual(data['status'], 'ACTIVE')

    def test_deleted_video_status(self):
        with patch('web.backend.app.get_video_by_code', return_value={
            'code': 'ABW-123', 'deleted': 1, 'scrape_status': 'SUCCESS',
        }):
            response = self.client.get('/api/videos/ABW-123')

        data = response.get_json()
        self.assertEqual(response.status_code, 200)
        self.assertTrue(data['record_exists'])
        self.assertFalse(data['exists'])
        self.assertEqual(data['status'], 'DELETED')

    def test_missing_video_status(self):
        with patch('web.backend.app.get_video_by_code', return_value=None):
            response = self.client.get('/api/videos/ABW-123')

        data = response.get_json()
        self.assertEqual(response.status_code, 404)
        self.assertFalse(data['record_exists'])
        self.assertFalse(data['exists'])
        self.assertEqual(data['status'], 'NOT_FOUND')
        self.assertIsNone(data['scrape_status'])


if __name__ == '__main__':
    unittest.main()
