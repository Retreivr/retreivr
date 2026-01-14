import unittest

from engine.core import build_ytdlp_opts


class YtdlpDownloadOptsTests(unittest.TestCase):
    def test_download_opts_no_suppressors(self):
        context = {
            "operation": "download",
            "music_mode": False,
            "final_format": None,
            "audio_only": False,
            "config": {},
            "overrides": {},
        }
        opts = build_ytdlp_opts(context)
        for key in ("download", "skip_download", "extract_flat", "simulate"):
            self.assertNotIn(key, opts)

    def test_download_opts_dropped_keys_warning(self):
        context = {
            "operation": "download",
            "music_mode": False,
            "final_format": None,
            "audio_only": False,
            "config": {},
            "overrides": {
                "skip_download": True,
                "extract_flat": True,
                "socket_timeout": 10,
            },
        }
        with self.assertLogs(level="WARNING") as logs:
            opts = build_ytdlp_opts(context)
        self.assertTrue(
            any("Dropping unsafe yt_dlp_opts for download" in msg for msg in logs.output)
        )
        for key in ("download", "skip_download", "extract_flat"):
            self.assertNotIn(key, opts)
        self.assertEqual(opts.get("socket_timeout"), 10)
