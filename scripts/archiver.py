#!/usr/bin/env python3
"""
Retreivr playlist archiver with robust retries, metadata embedding, and clean filenames.
- Sequential downloads to avoid throttling; retries across multiple extractor profiles.
- Embedded metadata (title/channel/date/description/tags/URL) and thumbnail as cover art.
- Optional final format copy (webm/mp4/mkv) and filename templating.
- Background copy to destination and SQLite history to avoid duplicate downloads.
- Optional Telegram summary after each run.
"""

import os
import sys


def _require_python_311():
    if sys.version_info[:2] != (3, 11):
        found = sys.version.split()[0]
        raise SystemExit(
            f"ERROR: Retreivr requires Python 3.11.x; found Python {found} "
            f"(executable: {sys.executable})"
        )


if __name__ == "__main__":
    _require_python_311()

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

import argparse
import logging
import signal
import threading

from engine.core import EngineStatus, load_config, run_archive
from engine.paths import CONFIG_DIR, DATA_DIR, DOWNLOADS_DIR, LOG_DIR, TOKENS_DIR, build_engine_paths, ensure_dir, resolve_config_path
from engine.json_utils import safe_json_dumps
from engine.runtime import get_runtime_info

def _setup_logging(log_dir):
    ensure_dir(log_dir)
    logging.basicConfig(
        filename=os.path.join(log_dir, "archiver.log"),
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    console = logging.StreamHandler()
    console.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    console.setLevel(logging.INFO)
    logging.getLogger("").addHandler(console)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default=None)
    parser.add_argument("--single-url", help="Download a single URL and exit (no playlist scan).")
    parser.add_argument("--destination", help="Destination directory for --single-url downloads.")
    parser.add_argument("--format", dest="final_format_override", help="Override final format/container (e.g., mp3, mp4, webm, mkv).")
    parser.add_argument("--js-runtime", help="Force JS runtime (e.g., node:/usr/bin/node or deno:/usr/bin/deno).")
    parser.add_argument("--version", action="store_true", help="Show version info and exit.")
    args = parser.parse_args()

    if args.version:
        print(safe_json_dumps(get_runtime_info(), indent=2))
        return

    paths = build_engine_paths()
    ensure_dir(DATA_DIR)
    ensure_dir(CONFIG_DIR)
    ensure_dir(LOG_DIR)
    ensure_dir(DOWNLOADS_DIR)
    ensure_dir(TOKENS_DIR)
    _setup_logging(LOG_DIR)

    stop_event = threading.Event()

    def _handle_signal(signum, _frame):
        stop_event.set()
        logging.warning("Signal %s received; stopping after current operation", signum)

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    try:
        config_path = resolve_config_path(args.config)
    except ValueError as exc:
        logging.error("Invalid config path: %s", exc)
        return
    if not os.path.exists(config_path):
        logging.error("Config file not found: %s", args.config)
        return

    config = load_config(config_path)

    status = run_archive(
        config,
        paths=paths,
        status=EngineStatus(),
        single_url=args.single_url,
        destination=args.destination,
        final_format_override=args.final_format_override,
        js_runtime_override=args.js_runtime,
        stop_event=stop_event,
        run_source="manual",
    )

    if stop_event.is_set():
        logging.warning("Stopped by signal")
        logging.shutdown()
        sys.exit(130)

    if args.single_url and status.single_download_ok is False:
        logging.shutdown()
        sys.exit(1)

    logging.shutdown()


if __name__ == "__main__":
    main()
