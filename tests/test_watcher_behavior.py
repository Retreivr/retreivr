import os
import sqlite3
import tempfile
import types
import unittest
from datetime import datetime, timezone

from api import main as api_main
from engine import core as engine_core


class WatcherBehaviorTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.tmpdir.name, "db.sqlite")
        engine_core.init_db(self.db_path)
        api_main.app.state.paths = types.SimpleNamespace(db_path=self.db_path)

    async def asyncTearDown(self):
        self.tmpdir.cleanup()

    async def test_adaptive_backoff_math(self):
        original = api_main.get_playlist_videos
        api_main.get_playlist_videos = lambda _yt, _playlist_id: []
        try:
            pl = {"playlist_id": "PL123", "account": "acc"}
            watch = {}
            policy = {
                "min_interval_minutes": 5,
                "max_interval_minutes": 20,
                "idle_backoff_factor": 2,
                "active_reset_minutes": 5,
                "downtime": {
                    "enabled": False,
                    "start": "23:00",
                    "end": "09:00",
                    "timezone": "UTC",
                },
            }
            now = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
            yt_clients = {"acc": object()}

            await api_main._poll_single_playlist({}, now, policy, pl, watch, yt_clients)

            state = api_main._read_watch_state(self.db_path)
            entry = state["PL123"]
            self.assertEqual(entry["current_interval_min"], 10)
            self.assertEqual(entry["consecutive_no_change"], 1)
        finally:
            api_main.get_playlist_videos = original

    async def test_first_run_subscribe_marks_seen_only(self):
        original = api_main.get_playlist_videos
        api_main.get_playlist_videos = lambda _yt, _playlist_id: [
            {"videoId": "A"},
            {"videoId": "B"},
        ]
        try:
            pl = {"playlist_id": "PLSUB", "account": "acc", "mode": "subscribe"}
            watch = {}
            policy = {
                "min_interval_minutes": 5,
                "max_interval_minutes": 20,
                "idle_backoff_factor": 2,
                "active_reset_minutes": 5,
                "downtime": {
                    "enabled": False,
                    "start": "23:00",
                    "end": "09:00",
                    "timezone": "UTC",
                },
            }
            now = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
            yt_clients = {"acc": object()}

            await api_main._poll_single_playlist({}, now, policy, pl, watch, yt_clients)

            with sqlite3.connect(self.db_path) as conn:
                cur = conn.cursor()
                cur.execute(
                    "SELECT video_id, downloaded FROM playlist_videos WHERE playlist_id=?",
                    ("PLSUB",),
                )
                rows = cur.fetchall()
                cur.execute("SELECT COUNT(*) FROM downloads")
                downloads_count = cur.fetchone()[0]
            self.assertEqual({row[0] for row in rows}, {"A", "B"})
            self.assertTrue(all(row[1] == 0 for row in rows))
            self.assertEqual(downloads_count, 0)
        finally:
            api_main.get_playlist_videos = original

