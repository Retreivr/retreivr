import os
import tempfile
import unittest
from unittest.mock import patch

import engine.core as core


class DownloadPathTests(unittest.TestCase):
    def test_native_opts_are_pure_and_download_called(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            opts_seen = {}
            calls = {"download": 0}

            class DummyYDL:
                def __init__(self, opts):
                    opts_seen.update(opts)

                def __enter__(self):
                    return self

                def __exit__(self, exc_type, exc, tb):
                    return False

                def download(self, urls):
                    calls["download"] += 1
                    path = os.path.join(temp_dir, "xJDrOAJQ11Y.webm")
                    with open(path, "wb") as handle:
                        handle.write(b"test")
                    return 0

            with patch.object(core, "YoutubeDL", DummyYDL):
                result = core.download_with_ytdlp_native(
                    "https://www.youtube.com/watch?v=xJDrOAJQ11Y",
                    temp_dir,
                    paths=type("Paths", (), {"ytdlp_temp_dir": temp_dir, "thumbs_dir": None})(),
                    config={},
                )

            self.assertIsNotNone(result)
            self.assertEqual(calls["download"], 1)
            forbidden = {
                "format",
                "extractor_args",
                "merge_output_format",
                "cookiefile",
                "cookiesfrombrowser",
                "http_headers",
                "js_runtimes",
                "retries",
                "fragment_retries",
                "geo_bypass",
                "socket_timeout",
            }
            self.assertTrue(forbidden.isdisjoint(opts_seen))

    def test_native_attempted_first(self):
        with patch.object(core, "download_with_ytdlp_native", return_value="native") as native:
            with patch.object(core, "download_with_ytdlp_hardened", return_value="hardened") as hardened:
                result = core.download_with_ytdlp_auto(
                    "https://www.youtube.com/watch?v=abc",
                    "/tmp",
                    paths=None,
                )
        self.assertEqual(result, "native")
        native.assert_called_once()
        hardened.assert_not_called()

    def test_hardened_runs_on_native_failure(self):
        with patch.object(core, "download_with_ytdlp_native", return_value=None) as native:
            with patch.object(core, "download_with_ytdlp_hardened", return_value="hardened") as hardened:
                result = core.download_with_ytdlp_auto(
                    "https://www.youtube.com/watch?v=abc",
                    "/tmp",
                    paths=None,
                )
        self.assertEqual(result, "hardened")
        native.assert_called_once()
        hardened.assert_called_once()

    def test_hardened_runs_on_native_exception(self):
        with patch.object(core, "download_with_ytdlp_native", side_effect=RuntimeError("boom")) as native:
            with patch.object(core, "download_with_ytdlp_hardened", return_value="hardened") as hardened:
                result = core.download_with_ytdlp_auto(
                    "https://www.youtube.com/watch?v=abc",
                    "/tmp",
                    paths=None,
                )
        self.assertEqual(result, "hardened")
        native.assert_called_once()
        hardened.assert_called_once()
