import hashlib
import json
import logging
import os
import re
import shutil
import sqlite3
import subprocess
import tempfile
import threading
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from threading import Thread
from uuid import uuid4
from zoneinfo import ZoneInfo

import requests
from google.oauth2.credentials import Credentials
from google.auth.exceptions import RefreshError
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from yt_dlp import YoutubeDL

from engine.paths import EnginePaths, resolve_dir, TOKENS_DIR

MAX_VIDEO_RETRIES = 6        # Hard cap per video
EXTRACTOR_RETRIES = 2        # Times to retry each extractor before moving on
USE_HARDENED_CLIENTS = True  # Set false to skip android/tv/web hardening attempts.
CLIENT_DELIVERY_TIMEOUT_SECONDS = 600

_GOOGLE_AUTH_RETRY = re.compile(r"Refreshing credentials due to a 401 response\\. Attempt (\\d+)/(\\d+)\\.")
_FORMAT_WEBM = (
    "bestvideo[ext=webm][height<=1080]+bestaudio[ext=webm]/"
    "bestvideo[ext=webm][height<=720]+bestaudio[ext=webm]/"
    "bestvideo[ext=mp4][height<=1080]+bestaudio[ext=m4a]/"
    "bestvideo[ext=mp4][height<=720]+bestaudio[ext=m4a]/"
    "bestvideo*+bestaudio/best"
)
_FORMAT_MUSIC_VIDEO = "bestvideo*+bestaudio/best"
_AUDIO_FORMATS = {"mp3", "m4a", "aac", "opus", "flac"}
_YTDLP_DOWNLOAD_ALLOWLIST = {
    "concurrent_fragment_downloads",
    "cookiefile",
    "cookiesfrombrowser",
    "forceipv4",
    "forceipv6",
    "fragment_retries",
    "geo_verification_proxy",
    "http_headers",
    "max_sleep_interval",
    "nocheckcertificate",
    "noproxy",
    "proxy",
    "ratelimit",
    "retries",
    "sleep_interval",
    "socket_timeout",
    "source_address",
    "throttledratelimit",
    "user_agent",
}


def _install_google_auth_filter():
    def _rewrite(record):
        msg = record.getMessage()
        match = _GOOGLE_AUTH_RETRY.search(msg)
        if match:
            attempt, total = match.groups()
            record.msg = f"Signing into Google OAuth. Attempt {attempt}/{total}."
            record.args = ()
        return True

    for logger_name in ("google.auth.transport.requests", "google.auth.credentials"):
        logger = logging.getLogger(logger_name)
        if getattr(logger, "_yt_archiver_filter", False):
            continue
        logger.addFilter(_rewrite)
        logger.setLevel(logging.WARNING)
        logger._yt_archiver_filter = True


_install_google_auth_filter()


@dataclass
class EngineStatus:
    run_successes: list[str] = field(default_factory=list)
    run_failures: list[str] = field(default_factory=list)
    runtime_warned: bool = False
    single_download_ok: bool | None = None
    current_phase: str | None = None
    last_error_message: str | None = None
    current_playlist_id: str | None = None
    current_video_id: str | None = None
    current_video_title: str | None = None
    progress_current: int | None = None
    progress_total: int | None = None
    progress_percent: int | None = None
    video_progress_percent: int | None = None
    video_downloaded_bytes: int | None = None
    video_total_bytes: int | None = None
    video_speed: float | None = None
    video_eta: int | None = None
    last_completed: str | None = None
    last_completed_at: str | None = None
    last_completed_path: str | None = None
    client_delivery_id: str | None = None
    client_delivery_filename: str | None = None
    client_delivery_expires_at: str | None = None
    client_delivery_mode: str | None = None
    lock: threading.Lock = field(default_factory=threading.Lock, repr=False)


def _status_append(status, field_name, value):
    if status is None:
        return
    lock = getattr(status, "lock", None)
    if lock:
        with lock:
            getattr(status, field_name).append(value)
    else:
        getattr(status, field_name).append(value)


def _status_set(status, field_name, value):
    if status is None:
        return
    lock = getattr(status, "lock", None)
    if lock:
        with lock:
            setattr(status, field_name, value)
    else:
        setattr(status, field_name, value)


def _reset_video_progress(status):
    _status_set(status, "video_progress_percent", None)
    _status_set(status, "video_downloaded_bytes", None)
    _status_set(status, "video_total_bytes", None)
    _status_set(status, "video_speed", None)
    _status_set(status, "video_eta", None)


_CLIENT_DELIVERIES = {}
_CLIENT_DELIVERIES_LOCK = threading.Lock()


def _register_client_delivery(path, filename):
    delivery_id = uuid4().hex
    expires_at = datetime.now(timezone.utc) + timedelta(seconds=CLIENT_DELIVERY_TIMEOUT_SECONDS)
    entry = {
        "path": path,
        "filename": filename,
        "expires_at": expires_at,
        "event": threading.Event(),
        "served": False,
        "delivered": False,
    }
    with _CLIENT_DELIVERIES_LOCK:
        _CLIENT_DELIVERIES[delivery_id] = entry

    def _expire():
        if entry["event"].wait(CLIENT_DELIVERY_TIMEOUT_SECONDS):
            return
        _finalize_client_delivery(delivery_id, timeout=True)
        logging.info("Client delivery temp file cleaned up")

    Thread(target=_expire, daemon=True).start()
    return delivery_id, expires_at, entry["event"]


def _acquire_client_delivery(delivery_id):
    now = datetime.now(timezone.utc)
    with _CLIENT_DELIVERIES_LOCK:
        entry = _CLIENT_DELIVERIES.get(delivery_id)
        if not entry:
            return None
        if entry.get("served"):
            return None
        if entry.get("expires_at") and now >= entry["expires_at"]:
            return None
        entry["served"] = True
        return dict(entry)


def _mark_client_delivery(delivery_id, *, delivered):
    with _CLIENT_DELIVERIES_LOCK:
        entry = _CLIENT_DELIVERIES.get(delivery_id)
        if not entry:
            return
        entry["delivered"] = bool(delivered)
        entry["event"].set()


def _finalize_client_delivery(delivery_id, *, timeout=False):
    with _CLIENT_DELIVERIES_LOCK:
        entry = _CLIENT_DELIVERIES.pop(delivery_id, None)
    if not entry:
        return False
    path = entry.get("path")
    if path and os.path.exists(path):
        try:
            os.remove(path)
        except OSError:
            logging.warning("Client delivery cleanup failed for %s", path)
    if timeout:
        return False
    return bool(entry.get("delivered"))


def load_config(path):
    with open(path, "r") as f:
        return json.load(f)


def _parse_hhmm(value):
    if not value:
        return None
    value = str(value).strip()
    if ":" not in value:
        return None
    hour_str, minute_str = value.split(":", 1)
    if not hour_str.isdigit() or not minute_str.isdigit():
        return None
    hour = int(hour_str)
    minute = int(minute_str)
    if hour < 0 or hour > 23 or minute < 0 or minute > 59:
        return None
    return hour, minute


def _resolve_timezone(value, fallback_tzinfo):
    if not value or str(value).lower() in {"local", "system"}:
        return fallback_tzinfo or timezone.utc
    if str(value).upper() == "UTC":
        return timezone.utc
    try:
        return ZoneInfo(str(value))
    except Exception:
        return fallback_tzinfo or timezone.utc


def _in_downtime(now, start_str, end_str):
    start = _parse_hhmm(start_str)
    end = _parse_hhmm(end_str)
    if not start or not end:
        return False, None
    start_dt = now.replace(hour=start[0], minute=start[1], second=0, microsecond=0)
    end_dt = now.replace(hour=end[0], minute=end[1], second=0, microsecond=0)
    if start_dt <= end_dt:
        in_window = start_dt <= now < end_dt
        next_allowed = end_dt if in_window else None
        return in_window, next_allowed
    if now >= start_dt:
        return True, end_dt + timedelta(days=1)
    if now < end_dt:
        return True, end_dt
    return False, None


def _watch_policy_downtime(config, now=None):
    policy = (config or {}).get("watch_policy") or {}
    downtime = policy.get("downtime") or {}
    if not downtime.get("enabled"):
        return False, None
    local_now = now or datetime.now().astimezone()
    tzinfo = _resolve_timezone(downtime.get("timezone"), local_now.tzinfo)
    now = local_now.astimezone(tzinfo)
    return _in_downtime(now, downtime.get("start"), downtime.get("end"))


def _await_downtime_end(config, *, stop_event=None):
    in_dt, next_allowed = _watch_policy_downtime(config)
    if not in_dt:
        return
    if next_allowed:
        logging.info("Downtime active; deferring download until %s", next_allowed)
    else:
        logging.info("Downtime active; deferring download")
    while in_dt:
        if stop_event and stop_event.is_set():
            return
        if next_allowed:
            sleep_seconds = max(0, (next_allowed - datetime.now(next_allowed.tzinfo)).total_seconds())
        else:
            sleep_seconds = 60
        if sleep_seconds > 0:
            time.sleep(sleep_seconds)
        in_dt, next_allowed = _watch_policy_downtime(config)


def validate_config(config):
    errors = []
    if not isinstance(config, dict):
        return ["config must be a JSON object"]

    accounts = config.get("accounts")
    if accounts is not None and not isinstance(accounts, dict):
        errors.append("accounts must be an object")

    playlists = config.get("playlists")
    if playlists is not None and not isinstance(playlists, list):
        errors.append("playlists must be a list")

    if isinstance(playlists, list):
        for idx, pl in enumerate(playlists):
            if not isinstance(pl, dict):
                errors.append(f"playlists[{idx}] must be an object")
                continue
            if not (pl.get("playlist_id") or pl.get("id")):
                errors.append(f"playlists[{idx}] missing playlist_id")
            if not (pl.get("folder") or pl.get("directory")):
                errors.append(f"playlists[{idx}] missing folder")
            music_mode = pl.get("music_mode")
            if music_mode is not None and not isinstance(music_mode, bool):
                errors.append(f"playlists[{idx}].music_mode must be true/false")
            mode = pl.get("mode")
            if mode is not None and mode not in {"full", "subscribe"}:
                errors.append(f"playlists[{idx}].mode must be 'full' or 'subscribe'")

    schedule = config.get("schedule")
    if schedule is not None:
        if not isinstance(schedule, dict):
            errors.append("schedule must be an object")
        else:
            enabled = schedule.get("enabled")
            if enabled is not None and not isinstance(enabled, bool):
                errors.append("schedule.enabled must be true/false")
            mode = schedule.get("mode", "interval")
            if mode != "interval":
                errors.append("schedule.mode must be 'interval'")
            interval_hours = schedule.get("interval_hours")
            if interval_hours is not None:
                if not isinstance(interval_hours, int):
                    errors.append("schedule.interval_hours must be an integer")
                elif interval_hours < 1:
                    errors.append("schedule.interval_hours must be >= 1")
            if enabled and interval_hours is None:
                errors.append("schedule.interval_hours is required when schedule is enabled")
            run_on_startup = schedule.get("run_on_startup")
            if run_on_startup is not None and not isinstance(run_on_startup, bool):
                errors.append("schedule.run_on_startup must be true/false")

    music_metadata_debug = config.get("music_metadata_debug")
    if music_metadata_debug is not None and not isinstance(music_metadata_debug, bool):
        errors.append("music_metadata_debug must be true/false")
    dry_run = config.get("dry_run")
    if dry_run is not None and not isinstance(dry_run, bool):
        errors.append("dry_run must be true/false")

    cookies_path = config.get("yt_dlp_cookies")
    if cookies_path is not None and not isinstance(cookies_path, str):
        errors.append("yt_dlp_cookies must be a string")

    music_template = config.get("music_filename_template")
    if music_template is not None and not isinstance(music_template, str):
        errors.append("music_filename_template must be a string")

    watch_policy = config.get("watch_policy")
    if watch_policy is not None:
        if not isinstance(watch_policy, dict):
            errors.append("watch_policy must be an object")
        else:
            min_interval = watch_policy.get("min_interval_minutes")
            max_interval = watch_policy.get("max_interval_minutes")
            idle_backoff = watch_policy.get("idle_backoff_factor")
            active_reset = watch_policy.get("active_reset_minutes")
            if min_interval is not None and not isinstance(min_interval, int):
                errors.append("watch_policy.min_interval_minutes must be an integer")
            if max_interval is not None and not isinstance(max_interval, int):
                errors.append("watch_policy.max_interval_minutes must be an integer")
            if idle_backoff is not None and not isinstance(idle_backoff, int):
                errors.append("watch_policy.idle_backoff_factor must be an integer")
            if active_reset is not None and not isinstance(active_reset, int):
                errors.append("watch_policy.active_reset_minutes must be an integer")
            if isinstance(min_interval, int) and min_interval < 1:
                errors.append("watch_policy.min_interval_minutes must be >= 1")
            if isinstance(max_interval, int) and max_interval < 1:
                errors.append("watch_policy.max_interval_minutes must be >= 1")
            if isinstance(min_interval, int) and isinstance(max_interval, int):
                if max_interval < min_interval:
                    errors.append("watch_policy.max_interval_minutes must be >= min_interval_minutes")
            if isinstance(idle_backoff, int) and idle_backoff < 1:
                errors.append("watch_policy.idle_backoff_factor must be >= 1")
            if isinstance(active_reset, int) and active_reset < 1:
                errors.append("watch_policy.active_reset_minutes must be >= 1")
            downtime = watch_policy.get("downtime")
            if downtime is not None:
                if not isinstance(downtime, dict):
                    errors.append("watch_policy.downtime must be an object")
                else:
                    enabled = downtime.get("enabled")
                    if enabled is not None and not isinstance(enabled, bool):
                        errors.append("watch_policy.downtime.enabled must be true/false")
                    for key in ("start", "end"):
                        value = downtime.get(key)
                        if value is not None and not isinstance(value, str):
                            errors.append(f"watch_policy.downtime.{key} must be a string (HH:MM)")
                    timezone_value = downtime.get("timezone")
                    if timezone_value is not None and not isinstance(timezone_value, str):
                        errors.append("watch_policy.downtime.timezone must be a string")

    return errors


def get_status(status):
    if status is None:
        return {
            "run_successes": [],
            "run_failures": [],
            "runtime_warned": False,
            "single_download_ok": None,
            "current_phase": None,
            "last_error_message": None,
            "current_playlist_id": None,
            "current_video_id": None,
            "current_video_title": None,
            "progress_current": None,
            "progress_total": None,
            "progress_percent": None,
            "video_progress_percent": None,
            "video_downloaded_bytes": None,
            "video_total_bytes": None,
            "video_speed": None,
            "video_eta": None,
            "last_completed": None,
            "last_completed_at": None,
            "last_completed_path": None,
            "client_delivery_id": None,
            "client_delivery_filename": None,
            "client_delivery_expires_at": None,
            "client_delivery_mode": None,
        }

    lock = getattr(status, "lock", None)
    if lock:
        with lock:
            successes = list(status.run_successes)
            failures = list(status.run_failures)
    else:
        successes = list(status.run_successes)
        failures = list(status.run_failures)
    return {
        "run_successes": successes,
        "run_failures": failures,
        "runtime_warned": status.runtime_warned,
        "single_download_ok": status.single_download_ok,
        "current_phase": status.current_phase,
        "last_error_message": status.last_error_message,
        "current_playlist_id": status.current_playlist_id,
        "current_video_id": status.current_video_id,
        "current_video_title": status.current_video_title,
        "progress_current": status.progress_current,
        "progress_total": status.progress_total,
        "progress_percent": status.progress_percent,
        "video_progress_percent": status.video_progress_percent,
        "video_downloaded_bytes": status.video_downloaded_bytes,
        "video_total_bytes": status.video_total_bytes,
        "video_speed": status.video_speed,
        "video_eta": status.video_eta,
        "last_completed": status.last_completed,
        "last_completed_at": status.last_completed_at,
        "last_completed_path": status.last_completed_path,
        "client_delivery_id": status.client_delivery_id,
        "client_delivery_filename": status.client_delivery_filename,
        "client_delivery_expires_at": status.client_delivery_expires_at,
        "client_delivery_mode": status.client_delivery_mode,
    }


def read_history(
    db_path,
    limit=None,
    *,
    search=None,
    playlist_id=None,
    date_from=None,
    date_to=None,
    sort_by="date",
    sort_dir="desc",
):
    if not os.path.exists(db_path):
        return []
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    clauses = []
    params = []
    if search:
        like = f"%{search}%"
        clauses.append("(filepath LIKE ? OR video_id LIKE ?)")
        params.extend([like, like])
    if playlist_id:
        clauses.append("playlist_id = ?")
        params.append(playlist_id)
    if date_from:
        clauses.append("downloaded_at >= ?")
        params.append(date_from)
    if date_to:
        clauses.append("downloaded_at <= ?")
        params.append(date_to)

    query = "SELECT video_id, playlist_id, downloaded_at, filepath FROM downloads"
    if clauses:
        query += " WHERE " + " AND ".join(clauses)

    sort_by = (sort_by or "date").lower()
    sort_dir = (sort_dir or "desc").lower()
    desc = sort_dir != "asc"

    if sort_by == "date":
        order_dir = "DESC" if desc else "ASC"
        query += f" ORDER BY downloaded_at {order_dir}"
        if limit:
            query += " LIMIT ?"
            params.append(limit)
        try:
            cur.execute(query, params)
            rows = cur.fetchall()
        except sqlite3.OperationalError:
            rows = []
        conn.close()
        return rows

    try:
        cur.execute(query, params)
        rows = cur.fetchall()
    except sqlite3.OperationalError:
        rows = []
    conn.close()

    if sort_by == "title":
        rows.sort(key=lambda row: os.path.basename(row[3] or "").lower(), reverse=desc)
    elif sort_by == "size":
        def size_key(row):
            size = None
            try:
                size = os.path.getsize(row[3])
            except (OSError, TypeError):
                size = None
            missing = size is None
            size_val = size if size is not None else 0
            if desc:
                size_val = -size_val
            return (missing, size_val)

        rows.sort(key=size_key)

    if limit:
        rows = rows[:limit]
    return rows


# ------------------------------------------------------------------
# DB
# ------------------------------------------------------------------

def init_db(db_path):
    db_dir = os.path.dirname(db_path)
    if db_dir:
        os.makedirs(db_dir, exist_ok=True)
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS downloads (
            video_id TEXT PRIMARY KEY,
            playlist_id TEXT,
            downloaded_at TIMESTAMP,
            filepath TEXT
        )
    """)
    # playlist_videos supports subscribe mode by tracking what each playlist has already seen/downloaded.
    # Invariants:
    # - (playlist_id, video_id) is unique and must never be rewritten casually.
    # - first_seen_at is the first time the playlist surfaced that video.
    # - downloaded only flips to 1 after a successful file write.
    # Do not drop/rename/repurpose this table without a migration.
    cur.execute("""
        CREATE TABLE IF NOT EXISTS playlist_videos (
            playlist_id TEXT NOT NULL,
            video_id TEXT NOT NULL,
            first_seen_at TIMESTAMP,
            downloaded INTEGER DEFAULT 0,
            PRIMARY KEY (playlist_id, video_id)
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_playlist_videos_playlist ON playlist_videos (playlist_id)")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS playlist_watch (
            playlist_id TEXT PRIMARY KEY,
            last_check TIMESTAMP,
            next_check TIMESTAMP,
            idle_count INTEGER DEFAULT 0
        )
    """)
    cur.execute("PRAGMA table_info(playlist_watch)")
    existing_cols = {row[1] for row in cur.fetchall()}
    if "last_checked_at" not in existing_cols:
        cur.execute("ALTER TABLE playlist_watch ADD COLUMN last_checked_at TIMESTAMP")
    if "next_poll_at" not in existing_cols:
        cur.execute("ALTER TABLE playlist_watch ADD COLUMN next_poll_at TIMESTAMP")
    if "current_interval_min" not in existing_cols:
        cur.execute("ALTER TABLE playlist_watch ADD COLUMN current_interval_min INTEGER")
    if "consecutive_no_change" not in existing_cols:
        cur.execute("ALTER TABLE playlist_watch ADD COLUMN consecutive_no_change INTEGER")
    if "last_change_at" not in existing_cols:
        cur.execute("ALTER TABLE playlist_watch ADD COLUMN last_change_at TIMESTAMP")
    if "skip_reason" not in existing_cols:
        cur.execute("ALTER TABLE playlist_watch ADD COLUMN skip_reason TEXT")
    if "last_error" not in existing_cols:
        cur.execute("ALTER TABLE playlist_watch ADD COLUMN last_error TEXT")
    if "last_error_at" not in existing_cols:
        cur.execute("ALTER TABLE playlist_watch ADD COLUMN last_error_at TIMESTAMP")
    if "last_check" in existing_cols and "last_checked_at" in existing_cols:
        cur.execute(
            "UPDATE playlist_watch "
            "SET last_checked_at=COALESCE(last_checked_at, last_check) "
            "WHERE last_checked_at IS NULL"
        )
    if "next_check" in existing_cols and "next_poll_at" in existing_cols:
        cur.execute(
            "UPDATE playlist_watch "
            "SET next_poll_at=COALESCE(next_poll_at, next_check) "
            "WHERE next_poll_at IS NULL"
        )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_playlist_watch_next ON playlist_watch (next_check)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_playlist_watch_next_poll ON playlist_watch (next_poll_at)")
    conn.commit()
    return conn


def discover_playlist_videos(yt_client, playlist_id, *, allow_public=True, cookies_path=None):
    """Discover playlist video IDs using API first, then yt-dlp fallback."""
    videos = []
    fetch_error = False
    fallback_error = False
    refresh_error = False
    if yt_client:
        try:
            videos = get_playlist_videos(yt_client, playlist_id)
        except HttpError:
            logging.exception("Playlist fetch failed %s", playlist_id)
            fetch_error = True
        except RefreshError as e:
            logging.error("OAuth refresh failed while fetching playlist %s: %s", playlist_id, e)
            fetch_error = True
            refresh_error = True
    if not videos and allow_public:
        videos, fallback_error = get_playlist_videos_fallback(playlist_id, cookies_path=cookies_path)
    return videos, fetch_error, fallback_error, refresh_error


def record_playlist_error(conn, playlist_id, message, when=None):
    if not playlist_id:
        return
    timestamp = when or datetime.now(timezone.utc).isoformat()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO playlist_watch (playlist_id, last_error, last_error_at) "
        "VALUES (?, ?, ?) "
        "ON CONFLICT(playlist_id) DO UPDATE SET "
        "last_error=excluded.last_error, last_error_at=excluded.last_error_at",
        (playlist_id, message, timestamp),
    )
    conn.commit()


def playlist_has_seen(conn, playlist_id):
    cur = conn.cursor()
    cur.execute(
        "SELECT 1 FROM playlist_videos WHERE playlist_id=? LIMIT 1",
        (playlist_id,),
    )
    return cur.fetchone() is not None


def is_video_seen(conn, playlist_id, video_id):
    cur = conn.cursor()
    cur.execute(
        "SELECT 1 FROM playlist_videos WHERE playlist_id=? AND video_id=? LIMIT 1",
        (playlist_id, video_id),
    )
    return cur.fetchone() is not None


def mark_video_seen(conn, playlist_id, video_id, *, downloaded=False):
    ts = datetime.utcnow().isoformat()
    cur = conn.cursor()
    cur.execute(
        "INSERT OR IGNORE INTO playlist_videos (playlist_id, video_id, first_seen_at, downloaded) "
        "VALUES (?, ?, ?, ?)",
        (playlist_id, video_id, ts, 1 if downloaded else 0),
    )
    if downloaded:
        cur.execute(
            "UPDATE playlist_videos SET downloaded=1 WHERE playlist_id=? AND video_id=?",
            (playlist_id, video_id),
        )


def mark_video_downloaded(conn, playlist_id, video_id):
    mark_video_seen(conn, playlist_id, video_id, downloaded=True)


def _playlist_sort_key(entry):
    if entry is None:
        return 0
    return entry.get("position") or entry.get("playlist_index") or 0


def is_video_downloaded(conn, video_id):
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM downloads WHERE video_id=? LIMIT 1", (video_id,))
    return cur.fetchone() is not None


def preview_playlist(conn, playlist_id, playlist_name, mode, videos):
    total = len(videos)
    playlist_label = playlist_name or playlist_id
    new_count = 0
    skipped = 0
    reason = ""

    if mode == "subscribe":
        if not playlist_has_seen(conn, playlist_id):
            skipped = total
            reason = "first run (mark seen, download none)"
        else:
            for entry in videos:
                vid = entry.get("videoId") or entry.get("id")
                if not vid:
                    continue
                if is_video_seen(conn, playlist_id, vid):
                    skipped += 1
                    reason = "already seen (stopping scan on first seen)"
                    break
                new_count += 1
            if not reason:
                reason = "no seen videos in current scan"
    else:
        for entry in videos:
            vid = entry.get("videoId") or entry.get("id")
            if not vid:
                continue
            if is_video_downloaded(conn, vid):
                skipped += 1
            else:
                new_count += 1
        reason = "already downloaded"

    logging.info(
        "Preview playlist: %s | mode=%s | total=%d | new=%d | skipped=%d (%s)",
        playlist_label,
        mode,
        total,
        new_count,
        skipped,
        reason,
    )


# ------------------------------------------------------------------
# Filename helpers
# ------------------------------------------------------------------

def sanitize_for_filesystem(name, maxlen=180):
    """Remove characters unsafe for filenames and trim length."""
    if not name:
        return ""
    name = re.sub(r"[\\/:*?\"<>|]+", "", name)
    name = re.sub(r"\s+", " ", name).strip()
    try:
        import unicodedata
        name = unicodedata.normalize("NFC", name)
    except ImportError:
        pass
    if len(name) > maxlen:
        name = name[:maxlen].rstrip()
    return name


def pretty_filename(title, channel, upload_date):
    """Cleaner filename for media servers: 'Title - Channel (MM-YYYY)'"""
    title_s = sanitize_for_filesystem(title)
    channel_s = sanitize_for_filesystem(channel)
    if upload_date and len(upload_date) == 8 and upload_date.isdigit():
        mm = upload_date[4:6]
        yyyy = upload_date[0:4]
        return f"{title_s} - {channel_s} ({mm}-{yyyy})"
    else:
        return f"{title_s} - {channel_s}"


def is_music_url(url):
    if not url:
        return False
    try:
        parsed = urllib.parse.urlparse(url)
        return "music.youtube.com" in (parsed.netloc or "")
    except Exception:
        return False


def build_download_url(video_id, *, music_mode=False, source_url=None):
    vid = extract_video_id(source_url) if source_url else None
    vid = vid or video_id
    if music_mode:
        return f"https://music.youtube.com/watch?v={vid}"
    if source_url and isinstance(source_url, str) and source_url.startswith("http"):
        return source_url
    return f"https://www.youtube.com/watch?v={vid}"


def _has_value(value):
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    return True


def normalize_track_number(value):
    if value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.strip().isdigit():
        return int(value.strip())
    return None


def format_track_number(value):
    num = normalize_track_number(value)
    if num is None:
        return None
    return f"{num:02d}"


def build_music_filename(meta, ext, template=None, fallback_id=None):
    artist = sanitize_for_filesystem(meta.get("artist") or "")
    album = sanitize_for_filesystem(meta.get("album") or "")
    track = sanitize_for_filesystem(meta.get("track") or meta.get("title") or "")
    track_number = format_track_number(meta.get("track_number"))
    album_artist = sanitize_for_filesystem(meta.get("album_artist") or "")
    disc = normalize_track_number(meta.get("disc"))
    release_date = sanitize_for_filesystem(meta.get("release_date") or "")

    values = {
        "artist": artist,
        "album": album,
        "track": track,
        "track_number": track_number or "",
        "album_artist": album_artist,
        "disc": disc or "",
        "release_date": release_date,
        "ext": ext,
    }

    if template:
        try:
            rendered = template % values
            return rendered.lstrip("/\\")
        except Exception:
            pass

    # Default music-safe structure: Artist/Album/NN - Track.ext
    filename = track or (fallback_id or "track")
    if track_number:
        filename = f"{track_number} - {filename}"
    filename = f"{filename}.{ext}"
    if artist and album:
        return os.path.join(artist, album, filename)
    if artist:
        return os.path.join(artist, filename)
    return filename


def build_output_filename(meta, video_id, ext, config, music_mode):
    if music_mode:
        template = config.get("music_filename_template") if config else None
        return build_music_filename(meta, ext, template=template, fallback_id=video_id)

    template = config.get("filename_template") if config else None
    if template:
        try:
            return template % {
                "title": sanitize_for_filesystem(meta.get("title") or video_id),
                "uploader": sanitize_for_filesystem(meta.get("channel") or ""),
                "upload_date": meta.get("upload_date") or "",
                "ext": ext,
            }
        except Exception:
            return f"{pretty_filename(meta.get('title'), meta.get('channel'), meta.get('upload_date'))}_{video_id[:8]}.{ext}"
    return f"{pretty_filename(meta.get('title'), meta.get('channel'), meta.get('upload_date'))}_{video_id[:8]}.{ext}"


def resolve_cookiefile(config):
    cookies = (config or {}).get("yt_dlp_cookies")
    if not cookies:
        return None
    try:
        resolved = resolve_dir(cookies, TOKENS_DIR)
    except ValueError as exc:
        logging.error("Invalid yt-dlp cookies path: %s", exc)
        return None
    if not os.path.exists(resolved):
        logging.warning("yt-dlp cookies file not found: %s", resolved)
        return None
    return resolved


# ------------------------------------------------------------------
# Config + API
# ------------------------------------------------------------------

def load_credentials(token_path):
    with open(token_path, "r") as f:
        data = json.load(f)
    return Credentials(
        token=data.get("token"),
        refresh_token=data.get("refresh_token"),
        token_uri=data.get("token_uri"),
        client_id=data.get("client_id"),
        client_secret=data.get("client_secret"),
        scopes=data.get("scopes"),
    )


def youtube_service(creds):
    return build("youtube", "v3", credentials=creds, cache_discovery=False)


def build_youtube_clients(accounts, config, *, cache=None, refresh_log_state=None):
    """
    Build one YouTube API client per configured account for this run.
    Any account that fails auth is skipped (logged) to avoid aborting the run.
    """
    clients = {}
    if not isinstance(accounts, dict):
        return clients
    cache = cache if isinstance(cache, dict) else {}
    refresh_log_state = refresh_log_state if isinstance(refresh_log_state, set) else set()
    for cached_name in list(cache.keys()):
        if cached_name not in accounts:
            cache.pop(cached_name, None)
    for name, acc in accounts.items():
        token_path = acc.get("token")
        if not token_path:
            logging.error("Account %s has no 'token' path configured; skipping", name)
            continue
        cached = cache.get(name)
        if isinstance(cached, dict) and cached.get("client") and cached.get("creds"):
            creds = cached["creds"]
            if creds and creds.expired and creds.refresh_token:
                try:
                    creds.refresh(Request())
                    if name in refresh_log_state:
                        logging.debug("OAuth refreshed for account=%s", name)
                    else:
                        logging.info("OAuth refreshed for account=%s", name)
                        refresh_log_state.add(name)
                    cached["client"] = youtube_service(creds)
                except RefreshError as e:
                    logging.error("OAuth refresh failed for account %s: %s", name, e)
                    continue
                except Exception:
                    logging.exception("Failed to refresh OAuth for account %s", name)
                    continue
            clients[name] = cached["client"]
            continue
        try:
            creds = load_credentials(token_path)
            if creds and creds.expired and creds.refresh_token:
                try:
                    creds.refresh(Request())
                    if name in refresh_log_state:
                        logging.debug("OAuth refreshed for account=%s", name)
                    else:
                        logging.info("OAuth refreshed for account=%s", name)
                        refresh_log_state.add(name)
                except RefreshError as e:
                    logging.error("OAuth refresh failed for account %s: %s", name, e)
                    continue
            clients[name] = youtube_service(creds)
            cache[name] = {"client": clients[name], "creds": creds}
        except RefreshError as e:
            logging.error("OAuth refresh failed for account %s: %s", name, e)
        except Exception:
            logging.exception("Failed to initialize YouTube client for account %s", name)
    return clients


def normalize_js_runtime(js_runtime):
    """Accept bare binary names or paths; return 'name:/full/path' or None."""
    if not js_runtime:
        return None
    if ":" in js_runtime:
        return js_runtime
    path = shutil.which(js_runtime)
    prefix = "node"
    if path and "deno" in os.path.basename(path).lower():
        prefix = "deno"
    elif path and "node" in os.path.basename(path).lower():
        prefix = "node"
    elif os.path.exists(js_runtime):
        path = js_runtime
        prefix = "deno" if "deno" in os.path.basename(js_runtime).lower() else "node"
    if path:
        return f"{prefix}:{path}"
    return None


def resolve_js_runtime(config, override=None):
    runtime = override or config.get("js_runtime") or os.environ.get("YT_DLP_JS_RUNTIME")
    runtime = normalize_js_runtime(runtime)
    if runtime:
        return runtime

    deno = shutil.which("deno")
    if deno:
        return f"deno:{deno}"

    node = shutil.which("node")
    if node:
        return f"node:{node}"

    return None


def get_playlist_videos(youtube, playlist_id):
    videos = []
    page = None
    while True:
        resp = youtube.playlistItems().list(
            part="snippet,contentDetails",
            playlistId=playlist_id,
            maxResults=50,
            pageToken=page,
        ).execute()
        for item in resp.get("items", []):
            videos.append({
                "videoId": item["contentDetails"].get("videoId"),
                "playlistItemId": item.get("id"),
                "position": item.get("snippet", {}).get("position"),
            })
        page = resp.get("nextPageToken")
        if not page:
            break
    return videos


def get_video_metadata(youtube, video_id):
    """Return basic metadata from YouTube Data API."""
    resp = youtube.videos().list(
        part="snippet,contentDetails",
        id=video_id,
    ).execute(num_retries=2)

    items = resp.get("items")
    if not items:
        return None

    snip = items[0]["snippet"]
    upload_date = snip.get("publishedAt", "")[:10].replace("-", "")

    thumbnails = snip.get("thumbnails", {}) or {}
    thumb_url = (
        thumbnails.get("maxres", {}).get("url")
        or thumbnails.get("standard", {}).get("url")
        or thumbnails.get("high", {}).get("url")
        or thumbnails.get("medium", {}).get("url")
        or thumbnails.get("default", {}).get("url")
    )

    return {
        "video_id": video_id,
        "title": snip.get("title"),
        "channel": snip.get("channelTitle"),
        "artist": snip.get("channelTitle"),
        "album": None,
        "album_artist": None,
        "track": None,
        "track_number": None,
        "disc": None,
        "release_date": None,
        "upload_date": upload_date,
        "description": snip.get("description") or "",
        "tags": snip.get("tags") or [],
        "url": f"https://www.youtube.com/watch?v={video_id}",
        "thumbnail_url": thumb_url,
    }


def extract_video_id(url):
    """Best-effort video ID extraction from a YouTube URL."""
    try:
        parsed = urllib.parse.urlparse(url)
        if parsed.netloc and "youtu.be" in parsed.netloc and parsed.path:
            return parsed.path.strip("/").split("/")[0]
        qs = urllib.parse.parse_qs(parsed.query or "")
        if "v" in qs and qs["v"]:
            return qs["v"][0]
    except Exception:
        pass
    return None


def extract_playlist_id(value):
    """Best-effort playlist ID extraction from a URL or raw ID."""
    if not value:
        return None
    try:
        parsed = urllib.parse.urlparse(value)
        if parsed.scheme and parsed.netloc:
            qs = urllib.parse.parse_qs(parsed.query or "")
            if "list" in qs and qs["list"]:
                return qs["list"][0]
            if parsed.fragment:
                frag_qs = urllib.parse.parse_qs(parsed.fragment)
                if "list" in frag_qs and frag_qs["list"]:
                    return frag_qs["list"][0]
    except Exception:
        pass
    return value


def _resolve_download_format(context):
    final_format = context.get("final_format")
    config = context.get("config")
    audio_only = bool(context.get("audio_only"))
    music_mode = bool(context.get("music_mode"))
    inherited_fmt = None
    if not final_format and config:
        inherited_fmt = config.get("final_format")
    target_fmt = (final_format or inherited_fmt or "").lower() or None
    if music_mode:
        if audio_only:
            audio_mode = True
            if target_fmt not in _AUDIO_FORMATS:
                target_fmt = "mp3"
        elif target_fmt and target_fmt not in _AUDIO_FORMATS:
            audio_mode = False
        else:
            audio_mode = True
            if target_fmt not in _AUDIO_FORMATS:
                target_fmt = "mp3"
    else:
        audio_mode = audio_only or (target_fmt in _AUDIO_FORMATS)

    preferred_exts = []
    if audio_mode:
        format_selector = "bestaudio/best"
        preferred_exts.append(target_fmt or "mp3")
    else:
        format_selector = _FORMAT_MUSIC_VIDEO if music_mode else _FORMAT_WEBM
        if target_fmt:
            preferred_exts.append(target_fmt)
        preferred_exts.extend(["webm", "mp4", "mkv", "m4a", "opus"])

    return {
        "audio_mode": audio_mode,
        "target_fmt": target_fmt,
        "format_selector": format_selector,
        "preferred_exts": preferred_exts,
    }


def _build_audio_postprocessors(target_fmt, music_mode):
    postprocessors = [{
        "key": "FFmpegExtractAudio",
        "preferredcodec": target_fmt or "mp3",
        "preferredquality": "0",
    }]
    if music_mode:
        postprocessors.extend([
            {"key": "FFmpegMetadata"},
            {"key": "EmbedThumbnail"},
        ])
    return postprocessors


def _build_download_attempt_plan(
    strict_format,
    *,
    use_hardened_clients=True,
    cookiefile=None,
    cookies_from_browser=None,
):
    plan = []
    if use_hardened_clients:
        plan.extend([
            {
                "client": "android",
                "headers": {
                    "User-Agent": "com.google.android.youtube/19.42.37 (Linux; Android 14)",
                    "Accept-Language": "en-US,en;q=0.9",
                },
                "extractor_args": {"youtube": ["player_client=android"]},
                "format": strict_format,
            },
            {
                "client": "tv_embedded",
                "headers": {
                    "User-Agent": "Mozilla/5.0 (SmartTV; Linux; Tizen 6.5) AppleWebKit/537.36",
                    "Accept-Language": "en-US,en;q=0.9",
                },
                "extractor_args": {"youtube": ["player_client=tv_embedded"]},
                "format": strict_format,
            },
            {
                "client": "web",
                "headers": {
                    "User-Agent": (
                        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"
                        " AppleWebKit/605.1.15 (KHTML, like Gecko) Safari/605.1.15"
                    ),
                    "Accept-Language": "en-US,en;q=0.9",
                },
                "extractor_args": {"youtube": ["player_client=web"]},
                "format": strict_format,
            },
        ])
    plan.extend([
        {
            "client": "default",
            "headers": None,
            "extractor_args": None,
            "format": strict_format,
        },
        {
            "client": "default",
            "headers": None,
            "extractor_args": None,
            "format": "bestvideo+bestaudio/best",
        },
    ])
    if cookiefile:
        plan.append({
            "client": "default",
            "headers": None,
            "extractor_args": None,
            "format": "best",
            "cookiefile": cookiefile,
            "cookies_from_browser": None,
        })
    elif cookies_from_browser:
        plan.append({
            "client": "default",
            "headers": None,
            "extractor_args": None,
            "format": "best",
            "cookies_from_browser": cookies_from_browser,
        })
    return plan


def _merge_ytdlp_overrides(opts, context):
    overrides = context.get("overrides")
    if overrides:
        try:
            opts.update(overrides)
        except Exception:
            logging.exception("Failed to merge yt_dlp_opts from config")


def _merge_download_overrides(opts, context):
    overrides = context.get("overrides")
    if not overrides:
        return
    if not isinstance(overrides, dict):
        logging.warning("Ignoring yt_dlp_opts for download: expected a JSON object")
        return
    dropped = sorted(set(overrides) - _YTDLP_DOWNLOAD_ALLOWLIST)
    if dropped:
        logging.warning(
            "Dropping unsafe yt_dlp_opts for download: %s",
            ", ".join(dropped),
        )
    for key in _YTDLP_DOWNLOAD_ALLOWLIST:
        if key in overrides:
            opts[key] = overrides[key]


def _fingerprint_ytdlp_opts(opts):
    redacted = dict(opts)
    if "cookiefile" in redacted:
        redacted["cookiefile"] = "<redacted>"
    if "cookiesfrombrowser" in redacted:
        redacted["cookiesfrombrowser"] = "<redacted>"
    if "progress_hooks" in redacted:
        hooks = redacted.get("progress_hooks") or []
        redacted["progress_hooks"] = ["<hook>"] * len(hooks)
    payload = json.dumps(redacted, sort_keys=True, default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:12]


def build_ytdlp_opts(context):
    opts = {
        "quiet": True,
        "forceipv4": True,
    }
    cookies = context.get("cookies")
    if cookies:
        opts["cookiefile"] = cookies
    cookies_from_browser = context.get("cookies_from_browser")
    if cookies_from_browser:
        opts["cookiesfrombrowser"] = cookies_from_browser

    operation = context.get("operation")
    if operation == "playlist":
        opts["skip_download"] = True
        opts["extract_flat"] = True
        _merge_ytdlp_overrides(opts, context)
        return opts
    if operation == "metadata":
        opts["skip_download"] = True
        opts["format"] = "best"
        opts["ignore_no_formats_error"] = True
        _merge_ytdlp_overrides(opts, context)
        return opts
    if operation != "download":
        _merge_ytdlp_overrides(opts, context)
        return opts

    format_ctx = context.get("format_ctx") or _resolve_download_format(context)
    context["format_ctx"] = format_ctx
    format_override = context.get("format_override") if "format_override" in context else None
    extractor_override_set = "extractor_args_override" in context
    extractor_override = context.get("extractor_args_override")

    outtmpl = context.get("outtmpl")
    if outtmpl:
        opts["outtmpl"] = outtmpl
    paths = context.get("paths")
    if paths:
        opts["paths"] = paths
    opts["format"] = format_override if format_override is not None else format_ctx["format_selector"]
    opts["continuedl"] = True
    progress_hook = context.get("progress_hook")
    if progress_hook:
        opts["progress_hooks"] = [progress_hook]
    opts["socket_timeout"] = 120
    opts["retries"] = 5
    opts["remote_components"] = ["ejs:github"]
    if context.get("http_headers"):
        opts["http_headers"] = context["http_headers"]
    if extractor_override_set:
        if extractor_override is not None:
            opts["extractor_args"] = extractor_override
    elif context.get("extractor_args"):
        opts["extractor_args"] = context["extractor_args"]

    if format_ctx["audio_mode"]:
        opts["postprocessors"] = _build_audio_postprocessors(
            format_ctx["target_fmt"],
            context.get("music_mode"),
        )
        if context.get("music_mode"):
            opts["addmetadata"] = True
            opts["embedthumbnail"] = True
            opts["writethumbnail"] = True

    _merge_download_overrides(opts, context)

    opts["format"] = format_override if format_override is not None else format_ctx["format_selector"]
    if extractor_override_set:
        if extractor_override is not None:
            opts["extractor_args"] = extractor_override
        else:
            opts.pop("extractor_args", None)
    if format_ctx["audio_mode"]:
        opts["postprocessors"] = _build_audio_postprocessors(
            format_ctx["target_fmt"],
            context.get("music_mode"),
        )
        if context.get("music_mode"):
            opts["addmetadata"] = True
            opts["embedthumbnail"] = True
            opts["writethumbnail"] = True
    for key in ("download", "skip_download", "simulate", "extract_flat"):
        opts.pop(key, None)

    js_runtime = context.get("js_runtime")
    if js_runtime:
        runtime_name, runtime_path = js_runtime.split(":", 1)
        opts["js_runtimes"] = {runtime_name: {"path": runtime_path}}

    return opts


def get_playlist_videos_fallback(playlist_id, cookies_path=None):
    """Fetch playlist entries without OAuth (yt-dlp extract_flat).
    Returns (videos, had_error).
    """
    playlist_url = f"https://www.youtube.com/playlist?list={playlist_id}"
    context = {
        "operation": "playlist",
        "music_mode": False,
        "final_format": None,
        "cookies": cookies_path,
        "filename_template": None,
        "js_runtime": None,
        "overrides": None,
    }
    opts = build_ytdlp_opts(context)
    try:
        with YoutubeDL(opts) as ydl:
            info = ydl.extract_info(playlist_url, download=False)
            entries = info.get("entries") or []
            vids = []
            for entry in entries:
                vid = entry.get("id") or entry.get("url")
                if vid:
                    vids.append({
                        "videoId": vid,
                        "playlist_index": entry.get("playlist_index"),
                    })
            return vids, False
    except Exception:
        logging.exception("yt-dlp playlist fallback failed for %s", playlist_id)
        return [], True


def get_video_metadata_fallback(video_id_or_url, cookies_path=None):
    """Metadata without OAuth using yt-dlp (no download)."""
    if video_id_or_url.startswith("http"):
        video_url = video_id_or_url
        vid = extract_video_id(video_id_or_url) or video_id_or_url
    else:
        video_url = f"https://www.youtube.com/watch?v={video_id_or_url}"
        vid = video_id_or_url

    context = {
        "operation": "metadata",
        "music_mode": False,
        "final_format": None,
        "cookies": cookies_path,
        "filename_template": None,
        "js_runtime": None,
        "overrides": None,
    }
    opts = build_ytdlp_opts(context)

    try:
        with YoutubeDL(opts) as ydl:
            info = ydl.extract_info(video_url, download=False)
    except Exception:
        logging.exception("yt-dlp metadata fallback failed for %s", video_url)
        if is_music_url(video_url):
            fallback_url = build_download_url(vid, music_mode=False, source_url=video_url)
            try:
                with YoutubeDL(opts) as ydl:
                    info = ydl.extract_info(fallback_url, download=False)
            except Exception:
                logging.exception("yt-dlp metadata fallback failed for %s", fallback_url)
                return None
        else:
            return None

    if not info:
        return None

    upload_date = info.get("upload_date") or ""
    thumb_url = (
        (info.get("thumbnail") or "")
    )
    return {
        "video_id": vid,
        "title": info.get("title"),
        "channel": info.get("uploader"),
        "artist": info.get("artist") or info.get("uploader"),
        "album": info.get("album"),
        "album_artist": info.get("album_artist") or info.get("artist"),
        "track": info.get("track") or info.get("title"),
        "track_number": normalize_track_number(info.get("track_number")),
        "disc": normalize_track_number(info.get("disc_number")),
        "release_date": info.get("release_date") or info.get("release_year"),
        "upload_date": upload_date,
        "description": info.get("description") or "",
        "tags": info.get("tags") or [],
        "url": video_url,
        "thumbnail_url": thumb_url,
    }


def resolve_video_metadata(yt_client, video_id, allow_public_fallback=True, *,
                           music_mode=False, cookies_path=None):
    """Resolve metadata with optional music-mode enrichment."""
    meta = None
    if yt_client:
        try:
            meta = get_video_metadata(yt_client, video_id)
        except HttpError:
            logging.exception("Metadata fetch failed %s", video_id)
        except RefreshError as e:
            logging.error("OAuth refresh failed while fetching video %s: %s", video_id, e)
    if not meta and allow_public_fallback:
        meta = get_video_metadata_fallback(video_id, cookies_path=cookies_path)

    if music_mode:
        music_meta = get_video_metadata_fallback(video_id, cookies_path=cookies_path)
        if music_meta:
            if not meta:
                meta = music_meta
            else:
                for key in (
                    "artist",
                    "album",
                    "album_artist",
                    "track",
                    "track_number",
                    "disc",
                    "release_date",
                    "title",
                    "thumbnail_url",
                    "url",
                ):
                    if _has_value(music_meta.get(key)):
                        meta[key] = music_meta.get(key)
        if meta:
            if not _has_value(meta.get("track")) and _has_value(meta.get("title")):
                meta["track"] = meta.get("title")
            if not _has_value(meta.get("artist")) and _has_value(meta.get("channel")):
                meta["artist"] = meta.get("channel")

    if not meta:
        vid = extract_video_id(video_id) or video_id
        base_url = video_id if isinstance(video_id, str) and str(video_id).startswith("http") else f"https://www.youtube.com/watch?v={vid}"
        meta = {
            "video_id": vid,
            "title": vid,
            "channel": "",
            "artist": "",
            "album": None,
            "album_artist": None,
            "track": None,
            "track_number": None,
            "disc": None,
            "release_date": None,
            "upload_date": "",
            "description": "",
            "tags": [],
            "url": base_url,
            "thumbnail_url": None,
        }
    return meta


def _extract_year(meta):
    if not meta:
        return None
    for key in ("release_date", "upload_date"):
        value = meta.get(key)
        if not value:
            continue
        match = re.match(r"^(\d{4})", str(value))
        if match:
            return match.group(1)
    return None


def _log_music_metadata_quality(meta, video_id, debug_enabled):
    if not debug_enabled:
        return
    if not meta:
        logging.info("Music metadata [%s]: no metadata available", video_id)
        return
    present = []
    missing = []

    def mark(label, value):
        if value:
            present.append(label)
        else:
            missing.append(label)

    mark("artist", meta.get("artist"))
    mark("album", meta.get("album"))
    mark("track", meta.get("track"))
    mark("track_number", meta.get("track_number"))
    mark("year", _extract_year(meta))
    mark("genre", meta.get("genre") or meta.get("genres"))
    mark("thumbnail", meta.get("thumbnail_url"))

    label = meta.get("title") or video_id
    logging.info(
        'Music metadata "%s" (%s): present=%s missing=%s',
        label,
        video_id,
        ", ".join(present) or "none",
        ", ".join(missing) or "none",
    )


# ------------------------------------------------------------------
# Async copy worker
# ------------------------------------------------------------------

def async_copy(src, dst, callback):
    def run():
        try:
            os.makedirs(os.path.dirname(dst), exist_ok=True)
            shutil.copy2(src, dst)
            callback(True, dst)
        except Exception as e:
            logging.exception("Copy failed: %s", e)
            callback(False, dst)

    t = Thread(target=run, daemon=True)
    t.start()
    return t


# ------------------------------------------------------------------
# Telegram notification
# ------------------------------------------------------------------

def telegram_notify(config, message):
    tg = config.get("telegram")
    if not tg:
        return

    token = tg.get("bot_token")
    chat_id = tg.get("chat_id")
    if not token or not chat_id:
        return

    text = urllib.parse.quote_plus(message)
    url = f"https://api.telegram.org/bot{token}/sendMessage?chat_id={chat_id}&text={text}"

    try:
        urllib.request.urlopen(url, timeout=10).read()
    except Exception as e:
        logging.error("Telegram notify failed: %s", e)


# ------------------------------------------------------------------
# Partial file check
# ------------------------------------------------------------------

def is_partial_file_stuck(temp_dir, vid):
    """Detect if partial .part file is frozen or empty."""
    if not os.path.isdir(temp_dir):
        return False
    for f in os.listdir(temp_dir):
        if f.startswith(vid) and f.endswith(".part"):
            p = os.path.join(temp_dir, f)
            try:
                size = os.path.getsize(p)
                # 0 bytes or <512KB after significant time = stuck
                if size < 1024 * 512:
                    return True
            except Exception:
                return True
    return False


# ------------------------------------------------------------------
# Metadata embedding
# ------------------------------------------------------------------

def embed_metadata(local_file, meta, video_id, thumbs_dir):
    """Embed title/channel/date/description/tags/url + thumbnail into local_file (in place)."""
    if not meta:
        return

    title = meta.get("title") or video_id
    channel = meta.get("channel") or ""
    artist = meta.get("artist") or channel
    album = meta.get("album")
    album_artist = meta.get("album_artist")
    track = meta.get("track")
    track_number = meta.get("track_number")
    disc = meta.get("disc")
    release_date = meta.get("release_date")
    upload_date = meta.get("upload_date") or ""
    description = meta.get("description") or ""
    tags = meta.get("tags") or []
    url = meta.get("url") or f"https://www.youtube.com/watch?v={video_id}"
    thumb_url = meta.get("thumbnail_url")

    # Convert YYYYMMDD -> YYYY-MM-DD if possible
    date_tag = ""
    raw_date = release_date or upload_date
    if raw_date and len(str(raw_date)) == 8 and str(raw_date).isdigit():
        raw_date = str(raw_date)
        date_tag = f"{raw_date[0:4]}-{raw_date[4:6]}-{raw_date[6:8]}"

    keywords = ", ".join(tags) if tags else ""
    comment = f"YouTubeID={video_id} URL={url}"

    # Download thumbnail (best effort)
    thumb_path = None
    if thumb_url and thumbs_dir:
        try:
            os.makedirs(thumbs_dir, exist_ok=True)
            thumb_path = os.path.join(thumbs_dir, f"{video_id}.jpg")
            resp = requests.get(thumb_url, timeout=15)
            if resp.ok and resp.content:
                with open(thumb_path, "wb") as f:
                    f.write(resp.content)
            else:
                thumb_path = None
        except Exception:
            logging.exception("Thumbnail download failed for %s", video_id)
            thumb_path = None

    # Keep the same container extension to avoid invalid remuxes (e.g., MP4 into WebM)
    base_ext = os.path.splitext(local_file)[1] or ".webm"
    ext_lower = base_ext.lower()
    audio_only = ext_lower in [".mp3", ".m4a", ".opus", ".aac", ".flac"]
    tmp_fd, tmp_path = tempfile.mkstemp(suffix=f".tagged{base_ext}", dir=os.path.dirname(local_file))
    os.close(tmp_fd)

    try:
        cmd = [
            "ffmpeg",
            "-y",
            "-i",
            local_file,
        ]

        # Attach thumbnail as Matroska attachment if we have one
        if thumb_path and os.path.exists(thumb_path) and not audio_only:
            cmd.extend([
                "-attach", thumb_path,
                "-metadata:s:t", "mimetype=image/jpeg",
                "-metadata:s:t", "filename=cover.jpg",
            ])

        # Core metadata
        if title:
            cmd.extend(["-metadata", f"title={title}"])
        if artist:
            cmd.extend(["-metadata", f"artist={artist}"])
        if album:
            cmd.extend(["-metadata", f"album={album}"])
        if album_artist:
            cmd.extend(["-metadata", f"album_artist={album_artist}"])
        if track:
            cmd.extend(["-metadata", f"track={track}"])
        if track_number is not None:
            cmd.extend(["-metadata", f"track={track_number}"])
        if disc is not None:
            cmd.extend(["-metadata", f"disc={disc}"])
        if date_tag:
            cmd.extend(["-metadata", f"date={date_tag}"])
        if description:
            cmd.extend(["-metadata", f"description={description}"])
        if keywords:
            cmd.extend(["-metadata", f"keywords={keywords}"])
        if comment:
            cmd.extend(["-metadata", f"comment={comment}"])

        # Copy streams, don't re-encode
        cmd.extend([
            "-c",
            "copy",
            tmp_path,
        ])

        subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)
        os.replace(tmp_path, local_file)
        logging.info("[%s] Metadata embedded successfully", video_id)
    except subprocess.CalledProcessError:
        logging.exception("ffmpeg metadata embedding failed for %s", video_id)
        try:
            os.unlink(tmp_path)
        except Exception:
            pass
    except Exception:
        logging.exception("Unexpected error during metadata embedding for %s", video_id)
        try:
            os.unlink(tmp_path)
        except Exception:
            pass
    finally:
        if thumb_path:
            try:
                os.unlink(thumb_path)
            except Exception:
                pass


# ------------------------------------------------------------------
# yt-dlp (WEBM + MP4 fallback)
# ------------------------------------------------------------------

def download_with_ytdlp_native(video_url, temp_dir, js_runtime=None, meta=None, config=None,
                               target_format=None, audio_only=False, *, paths, status=None,
                               stop_event=None, music_mode=False, cookies_path=None):
    vid = extract_video_id(video_url) or (video_url.split("v=")[-1] if "v=" in video_url else "video")
    if meta and meta.get("video_id"):
        vid = meta.get("video_id")
    format_ctx = _resolve_download_format({
        "music_mode": music_mode,
        "final_format": target_format,
        "audio_only": audio_only,
        "config": config,
    })
    audio_mode = format_ctx["audio_mode"]
    target_fmt = format_ctx["target_fmt"]
    preferred_exts = list(format_ctx["preferred_exts"])

    if status is None:
        status = EngineStatus()

    if stop_event and stop_event.is_set():
        logging.warning("[%s] Stop requested before download", vid)
        return None

    def progress_hook(data):
        if stop_event and stop_event.is_set():
            raise RuntimeError("Download cancelled")
        if status is None:
            return
        state = data.get("status")
        if state == "downloading":
            total = data.get("total_bytes") or data.get("total_bytes_estimate")
            downloaded = data.get("downloaded_bytes")
            percent = None
            if total and downloaded is not None:
                percent = int((downloaded / total) * 100)
            _status_set(status, "video_progress_percent", percent)
            _status_set(status, "video_downloaded_bytes", int(downloaded) if downloaded is not None else None)
            _status_set(status, "video_total_bytes", int(total) if total is not None else None)
            _status_set(status, "video_speed", data.get("speed"))
            _status_set(status, "video_eta", data.get("eta"))
        elif state == "finished":
            total = data.get("total_bytes") or data.get("downloaded_bytes")
            _status_set(status, "video_progress_percent", 100)
            _status_set(status, "video_downloaded_bytes", int(total) if total is not None else None)
            _status_set(status, "video_total_bytes", int(total) if total is not None else None)
            _status_set(status, "video_speed", None)
            _status_set(status, "video_eta", None)

    if not js_runtime and not status.runtime_warned:
        logging.warning("No JS runtime configured/detected; set js_runtime in config or pass --js-runtime to reduce SABR/missing format issues.")
        status.runtime_warned = True

    os.makedirs(paths.ytdlp_temp_dir, exist_ok=True)

    if os.path.exists(temp_dir):
        if is_partial_file_stuck(temp_dir, vid):
            logging.warning("[%s] Stuck partial detected, wiping temp_dir", vid)
            shutil.rmtree(temp_dir, ignore_errors=True)

    shutil.rmtree(temp_dir, ignore_errors=True)
    os.makedirs(temp_dir, exist_ok=True)

    opts = {
        "outtmpl": os.path.join(temp_dir, "%(id)s.%(ext)s"),
        "paths": {"temp": paths.ytdlp_temp_dir},
        "continuedl": True,
        "quiet": True,
        "no_warnings": True,
        "logger": logging.getLogger("yt_dlp"),
    }
    opts["progress_hooks"] = [progress_hook]
    node_path = None
    for candidate in ("/bin/node", "/usr/bin/node"):
        if os.path.isfile(candidate) and os.access(candidate, os.X_OK):
            node_path = candidate
            break
    if node_path:
        opts["js_runtimes"] = {
            "node": {
                "path": node_path,
            }
        }
        logging.info("[%s] Native yt-dlp js_runtimes enabled: node (%s)", vid, node_path)
        opts["remote_components"] = ["ejs:github"]
        logging.info("[%s] Native yt-dlp remote JS solver enabled (ejs:github)", vid)
        logging.info("[%s] Native yt-dlp JS solver active: ejs:github", vid)
    else:
        logging.warning("[%s] Native yt-dlp js_runtimes not set: node not found", vid)
    opts["force_ipv4"] = True
    opts["format"] = "bestvideo+bestaudio/best"
    logging.info("[%s] Download path: native (v1.2.0-equivalent semantics)", vid)
    logging.info("[%s] Native yt-dlp opts keys: %s", vid, sorted(opts.keys()))
    assert "download" not in opts, "metadata-only flag leaked into download options: download"

    try:
        with YoutubeDL(opts) as ydl:
            result = ydl.download([video_url])
    except Exception as e:
        logging.warning("[%s] Native download failed: %s", vid, e)
        return None
    if result:
        logging.warning("[%s] Native download reported failures (code=%s)", vid, result)

    chosen = None
    files = [
        name for name in os.listdir(temp_dir)
        if not name.endswith(".part") and os.path.isfile(os.path.join(temp_dir, name))
    ]
    if files:
        search_exts = preferred_exts + ["webm", "mp4", "mkv", "m4a", "opus", "mp3", "aac", "flac"]
        for ext in search_exts:
            candidate = os.path.join(temp_dir, f"{vid}.{ext}")
            if os.path.exists(candidate):
                chosen = candidate
                break
        if not chosen:
            for name in files:
                if name.startswith(vid):
                    chosen = os.path.join(temp_dir, name)
                    break
        if not chosen:
            chosen = os.path.join(temp_dir, files[0])

    if not chosen:
        logging.warning("[%s] Native download produced no usable output", vid)
        return None

    audio_exts = {".m4a", ".mp3", ".opus", ".aac"}
    ext = os.path.splitext(chosen)[1].lower()
    has_video = False
    has_audio = False
    audio_only_output = ext in audio_exts
    try:
        probe = subprocess.run(
            ["ffprobe", "-v", "error", "-show_entries", "stream=codec_type", "-of", "csv=p=0", chosen],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
            text=True,
        )
        for line in probe.stdout.splitlines():
            line = line.strip()
            if line == "video":
                has_video = True
            elif line == "audio":
                has_audio = True
        if not has_video:
            audio_only_output = True
    except Exception:
        audio_only_output = True
    if audio_only_output or not has_audio:
        logging.error("Native download produced audio-only output — rejecting (video required)")
        try:
            os.remove(chosen)
        except Exception:
            pass
        return None

    logging.info("[%s] SUCCESS via native → %s", vid, os.path.basename(chosen))

    if not music_mode:
        _status_set(status, "current_phase", "embedding metadata")
        embed_metadata(chosen, meta, vid, paths.thumbs_dir)

    desired_ext = target_fmt or (config.get("final_format") if config else None)
    if desired_ext:
        current_ext = os.path.splitext(chosen)[1].lstrip(".").lower()
        if current_ext == "mp4" and desired_ext == "webm":
            logging.warning("[%s] Skipping mp4->webm container copy to avoid invalid file; consider final_format=mp4", vid)
        elif current_ext != desired_ext:
            _status_set(status, "current_phase", "converting")
            base = os.path.splitext(chosen)[0]
            converted = f"{base}.{desired_ext}"
            try:
                subprocess.run(
                    ["ffmpeg", "-y", "-i", chosen, "-c", "copy", converted],
                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True
                )
                os.remove(chosen)
                chosen = converted
            except Exception:
                logging.exception("Final format conversion failed for %s", vid)
                try:
                    if os.path.exists(converted):
                        os.remove(converted)
                except Exception:
                    pass
                try:
                    if os.path.exists(chosen):
                        os.remove(chosen)
                except Exception:
                    pass
                return None

    return chosen


def download_with_ytdlp_hardened(video_url, temp_dir, js_runtime=None, meta=None, config=None,
                                 target_format=None, audio_only=False, *, paths, status=None,
                                 stop_event=None, music_mode=False, cookies_path=None):
    vid = extract_video_id(video_url) or (video_url.split("v=")[-1] if "v=" in video_url else "video")
    if meta and meta.get("video_id"):
        vid = meta.get("video_id")
    js_runtime = normalize_js_runtime(js_runtime)
    context = {
        "operation": "download",
        "music_mode": music_mode,
        "final_format": target_format,
        "cookies": cookies_path,
        "filename_template": (config or {}).get("filename_template") if config else None,
        "js_runtime": js_runtime,
        "overrides": (config or {}).get("yt_dlp_opts") if config else None,
        "audio_only": audio_only,
        "config": config,
    }
    format_ctx = _resolve_download_format(context)
    context["format_ctx"] = format_ctx
    audio_mode = format_ctx["audio_mode"]
    target_fmt = format_ctx["target_fmt"]
    preferred_exts = list(format_ctx["preferred_exts"])

    strict_format = format_ctx["format_selector"]
    use_hardened = USE_HARDENED_CLIENTS
    if isinstance(config, dict) and "use_hardened_clients" in config:
        use_hardened = bool(config.get("use_hardened_clients"))
    cookies_from_browser = None
    if isinstance(config, dict):
        browser_value = config.get("yt_dlp_cookies_from_browser")
        if isinstance(browser_value, str) and browser_value.strip():
            cookies_from_browser = browser_value.strip()
        elif browser_value is True:
            cookies_from_browser = "chrome"
    attempt_plan = _build_download_attempt_plan(
        strict_format,
        use_hardened_clients=use_hardened,
        cookiefile=cookies_path,
        cookies_from_browser=cookies_from_browser,
    )
    has_default_client = any(step.get("extractor_args") is None for step in attempt_plan)
    has_permissive_format = any(
        isinstance(step.get("format"), str) and step.get("format").startswith("best")
        for step in attempt_plan
    )
    if not has_default_client or not has_permissive_format:
        missing = []
        if not has_default_client:
            missing.append("default client")
        if not has_permissive_format:
            missing.append("permissive format")
        logging.error(
            "[%s] Attempt plan missing %s; appending fallback attempt",
            vid,
            ", ".join(missing),
        )
        attempt_plan.append({
            "client": "default",
            "headers": None,
            "extractor_args": None,
            "format": "best",
            "cookiefile": None,
            "cookies_from_browser": None,
        })

    if status is None:
        status = EngineStatus()

    if stop_event and stop_event.is_set():
        logging.warning("[%s] Stop requested before download", vid)
        return None

    def progress_hook(data):
        if stop_event and stop_event.is_set():
            raise RuntimeError("Download cancelled")
        if status is None:
            return
        state = data.get("status")
        if state == "downloading":
            total = data.get("total_bytes") or data.get("total_bytes_estimate")
            downloaded = data.get("downloaded_bytes")
            percent = None
            if total and downloaded is not None:
                percent = int((downloaded / total) * 100)
            _status_set(status, "video_progress_percent", percent)
            _status_set(status, "video_downloaded_bytes", int(downloaded) if downloaded is not None else None)
            _status_set(status, "video_total_bytes", int(total) if total is not None else None)
            _status_set(status, "video_speed", data.get("speed"))
            _status_set(status, "video_eta", data.get("eta"))
        elif state == "finished":
            total = data.get("total_bytes") or data.get("downloaded_bytes")
            _status_set(status, "video_progress_percent", 100)
            _status_set(status, "video_downloaded_bytes", int(total) if total is not None else None)
            _status_set(status, "video_total_bytes", int(total) if total is not None else None)
            _status_set(status, "video_speed", None)
            _status_set(status, "video_eta", None)

    if not js_runtime and not status.runtime_warned:
        logging.warning("No JS runtime configured/detected; set js_runtime in config or pass --js-runtime to reduce SABR/missing format issues.")
        status.runtime_warned = True

    os.makedirs(paths.ytdlp_temp_dir, exist_ok=True)

    total_attempts = min(MAX_VIDEO_RETRIES, len(attempt_plan))
    for attempt_idx, attempt in enumerate(attempt_plan[:total_attempts], start=1):
        if stop_event and stop_event.is_set():
            logging.warning("[%s] Stop requested; aborting download loop", vid)
            return None
        client_label = attempt["client"]
        format_selector = attempt["format"]
        cookies_note = ""
        if attempt.get("cookiefile"):
            cookies_note = " cookies=file"
        elif attempt.get("cookies_from_browser"):
            cookies_note = " cookies=browser"
        logging.info(
            "[%s] Download attempt %d/%d client=%s format=%s%s",
            vid,
            attempt_idx,
            total_attempts,
            client_label,
            format_selector,
            cookies_note,
        )

        # Reset temp dir if stuck
        if os.path.exists(temp_dir):
            if is_partial_file_stuck(temp_dir, vid):
                logging.warning("[%s] Stuck partial detected, wiping temp_dir", vid)
                shutil.rmtree(temp_dir, ignore_errors=True)

        shutil.rmtree(temp_dir, ignore_errors=True)
        os.makedirs(temp_dir, exist_ok=True)

        headers = attempt.get("headers")
        if headers:
            context["http_headers"] = headers
        else:
            context.pop("http_headers", None)
        if "cookiefile" in attempt:
            cookies_override = attempt.get("cookiefile")
            if cookies_override is not None:
                context["cookies"] = cookies_override
            else:
                context.pop("cookies", None)
        browser_cookies = attempt.get("cookies_from_browser")
        if browser_cookies:
            context["cookies_from_browser"] = browser_cookies
        else:
            context.pop("cookies_from_browser", None)
        context.update({
            "outtmpl": os.path.join(temp_dir, "%(id)s.%(ext)s"),
            "paths": {"temp": paths.ytdlp_temp_dir},
            "progress_hook": progress_hook,
            "format_override": format_selector,
            "extractor_args_override": attempt.get("extractor_args"),
        })
        opts = build_ytdlp_opts(context)
        suppressors_present = {
            key: key in opts
            for key in ("skip_download", "extract_flat", "simulate", "download")
        }
        cookies_mode = "none"
        if "cookiefile" in opts:
            cookies_mode = "file"
        elif "cookiesfrombrowser" in opts:
            cookies_mode = "browser"
        extractor_args = opts.get("extractor_args") or {}
        yt_extractor_args = extractor_args.get("youtube")
        opts_fingerprint = _fingerprint_ytdlp_opts(opts)
        logging.info(
            "[%s] ytdlp attempt=%d/%d client=%s format=%s suppressors_present=%s "
            "cookies_mode=%s extractor_args.youtube=%s opts_fp=%s",
            vid,
            attempt_idx,
            total_attempts,
            client_label,
            format_selector,
            suppressors_present,
            cookies_mode,
            yt_extractor_args,
            opts_fingerprint,
        )
        assert "download" not in opts, "metadata-only flag leaked into download options: download"

        try:
            with YoutubeDL(opts) as ydl:
                info = ydl.extract_info(video_url, download=True)
        except Exception as e:
            logging.warning("[%s] %s failed: %s", vid, client_label, e)
            probe_context = dict(context)
            probe_context["format_override"] = "best"
            probe_context["extractor_args_override"] = attempt["extractor_args"]
            if headers:
                probe_context["http_headers"] = headers
            else:
                probe_context.pop("http_headers", None)
            try:
                probe_opts = build_ytdlp_opts(probe_context)
                probe_opts["skip_download"] = True
                with YoutubeDL(probe_opts) as ydl:
                    probe_info = ydl.extract_info(video_url, download=False)
                formats = probe_info.get("formats") or []
                format_ids = [fmt.get("format_id") for fmt in formats if fmt.get("format_id")]
                top_ids = ",".join(format_ids[:5]) if format_ids else "-"
                empty = len(formats) == 0
                logging.debug(
                    "[%s] Format probe client=%s formats=%d top_ids=%s empty=%s",
                    vid,
                    client_label,
                    len(formats),
                    top_ids,
                    empty,
                )
                if not empty:
                    logging.debug(
                        "[%s] Format probe client=%s formats exist; selector may be too strict",
                        vid,
                        client_label,
                    )
            except Exception as probe_exc:
                logging.debug(
                    "[%s] Format probe failed client=%s error=%s",
                    vid,
                    client_label,
                    probe_exc,
                )
            continue

        vid_for_files = info.get("id") or vid

        if not info:
            logging.warning("[%s] No info returned from extractor %s", vid, client_label)
            continue

        # Prefer .webm if present, else accept mp4
        chosen = None
        search_exts = preferred_exts + ["webm", "mp4", "mkv", "m4a", "opus", "mp3", "aac", "flac"]
        for ext in search_exts:
            candidate = os.path.join(temp_dir, f"{vid_for_files}.{ext}")
            if os.path.exists(candidate):
                chosen = candidate
                break
        if not chosen:
            for f in os.listdir(temp_dir):
                if f.startswith(vid_for_files) and not f.endswith(".part"):
                    chosen = os.path.join(temp_dir, f)
                    break

        if chosen:
            logging.info("[%s] SUCCESS via %s → %s", vid, client_label, os.path.basename(chosen))

            # Embed metadata for video mode; music mode relies on yt-dlp audio tagging.
            if not music_mode:
                _status_set(status, "current_phase", "embedding metadata")
                embed_metadata(chosen, meta, vid, paths.thumbs_dir)

            # Post-processing final format conversion (if needed)
            desired_ext = target_fmt or (config.get("final_format") if config else None)
            if desired_ext and not audio_mode:
                current_ext = os.path.splitext(chosen)[1].lstrip(".").lower()
                # Avoid container mismatch: don't force mp4 -> webm without re-encode
                if current_ext == "mp4" and desired_ext == "webm":
                    logging.warning("[%s] Skipping mp4->webm container copy to avoid invalid file; consider final_format=mp4", vid)
                elif current_ext != desired_ext:
                    _status_set(status, "current_phase", "converting")
                    base = os.path.splitext(chosen)[0]
                    converted = f"{base}.{desired_ext}"
                    try:
                        subprocess.run(
                            ["ffmpeg", "-y", "-i", chosen, "-c", "copy", converted],
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True
                        )
                        os.remove(chosen)
                        chosen = converted
                    except Exception:
                        logging.exception("Final format conversion failed for %s", vid)

            return chosen

        logging.warning("[%s] Extractor %s produced no usable output", vid, client_label)

    logging.error("[%s] PERMANENT FAILURE after %d attempts.", vid, total_attempts)
    return None


# Coordinator: try native first, fall back to hardened on failure.
def download_with_ytdlp_auto(video_url, temp_dir, js_runtime=None, meta=None, config=None,
                             target_format=None, audio_only=False, *, paths, status=None,
                             stop_event=None, music_mode=False, cookies_path=None):
    vid = extract_video_id(video_url) or (video_url.split("v=")[-1] if "v=" in video_url else "video")
    if meta and meta.get("video_id"):
        vid = meta.get("video_id")
    native_error = None
    try:
        native_file = download_with_ytdlp_native(
            video_url,
            temp_dir,
            js_runtime,
            meta,
            config,
            target_format=target_format,
            audio_only=audio_only,
            paths=paths,
            status=status,
            stop_event=stop_event,
            music_mode=music_mode,
            cookies_path=cookies_path,
        )
    except Exception as exc:
        native_error = exc
        native_file = None
    if native_file:
        return native_file
    if native_error:
        logging.warning("[%s] Native download error: %s", vid, native_error)
    else:
        logging.warning("[%s] Native download produced no file; falling back", vid)
    logging.info("[%s] Download path: hardened fallback (v1.3.0)", vid)
    return download_with_ytdlp_hardened(
        video_url,
        temp_dir,
        js_runtime,
        meta,
        config,
        target_format=target_format,
        audio_only=audio_only,
        paths=paths,
        status=status,
        stop_event=stop_event,
        music_mode=music_mode,
        cookies_path=cookies_path,
    )


def download_with_ytdlp(video_url, temp_dir, js_runtime=None, meta=None, config=None,
                        target_format=None, audio_only=False, *, paths, status=None,
                        stop_event=None, music_mode=False, cookies_path=None):
    return download_with_ytdlp_auto(
        video_url,
        temp_dir,
        js_runtime,
        meta,
        config,
        target_format=target_format,
        audio_only=audio_only,
        paths=paths,
        status=status,
        stop_event=stop_event,
        music_mode=music_mode,
        cookies_path=cookies_path,
    )


# ------------------------------------------------------------------
# Main pipeline
# ------------------------------------------------------------------

def run_single_download(config, video_url, destination=None, final_format_override=None,
                        *, paths, status=None, js_runtime_override=None, stop_event=None,
                        music_mode=False, skip_downtime=False, delivery_mode="server"):
    """Download a single URL (no OAuth required)."""
    dry_run = bool(config.get("dry_run")) if isinstance(config, dict) else False
    if delivery_mode not in {"server", "client"}:
        logging.warning("Unknown single-URL delivery_mode=%s; defaulting to server", delivery_mode)
        delivery_mode = "server"
    logging.info("Single-URL delivery_mode=%s", delivery_mode)
    if is_music_url(video_url):
        music_mode = True
    if dry_run:
        logging.info("Dry-run enabled: no downloads or DB writes will occur")
    js_runtime = resolve_js_runtime(config, override=js_runtime_override)
    cookies_path = resolve_cookiefile(config)
    preview_only = os.environ.get("YT_ARCHIVER_PREVIEW", "").strip().lower() in {"1", "true", "yes", "on"}
    if music_mode and not cookies_path:
        logging.warning("Music mode enabled without yt-dlp cookies; metadata quality may be degraded.")
    meta = resolve_video_metadata(
        None,
        video_url,
        allow_public_fallback=True,
        music_mode=music_mode,
        cookies_path=cookies_path,
    )
    vid = meta.get("video_id") or extract_video_id(video_url) or "video"
    temp_dir = os.path.join(paths.temp_downloads_dir, vid)
    debug_metadata = bool(config.get("music_metadata_debug")) if isinstance(config, dict) else False
    if music_mode:
        _log_music_metadata_quality(meta, vid, debug_metadata)

    if stop_event and stop_event.is_set():
        logging.warning("[%s] Stop requested before single download", vid)
        return False

    _status_set(status, "current_playlist_id", None)
    _status_set(status, "current_video_id", vid)
    _status_set(status, "current_video_title", meta.get("title") or vid)
    _status_set(status, "progress_current", 0)
    _status_set(status, "progress_total", 1)
    _status_set(status, "progress_percent", 0)
    _status_set(status, "last_completed_path", None)
    _reset_video_progress(status)
    _status_set(status, "video_progress_percent", 0)
    _status_set(status, "video_downloaded_bytes", 0)
    _status_set(status, "current_phase", "downloading")
    _status_set(status, "last_error_message", None)

    dest_dir = None
    if delivery_mode == "server":
        try:
            dest_dir = resolve_dir(
                destination or config.get("single_download_folder") or paths.single_downloads_dir,
                paths.single_downloads_dir,
            )
        except ValueError as exc:
            logging.error("Invalid destination path: %s", exc)
            _status_set(status, "last_error_message", f"Invalid destination path: {exc}")
            _status_set(status, "progress_current", 1)
            _status_set(status, "progress_total", 1)
            _status_set(status, "progress_percent", 100)
            _reset_video_progress(status)
            _status_set(status, "current_video_id", None)
            _status_set(status, "current_video_title", None)
            _status_set(status, "current_phase", None)
            return False
        if not dry_run:
            os.makedirs(dest_dir, exist_ok=True)

    if dry_run:
        format_ctx = _resolve_download_format({
            "music_mode": music_mode,
            "final_format": final_format_override,
            "audio_only": False,
            "config": config,
        })
        target_fmt = format_ctx["target_fmt"]
        ext = target_fmt or final_format_override or config.get("final_format") or (
            "mp3" if format_ctx["audio_mode"] else "webm"
        )
        cleaned_name = build_output_filename(meta, vid, ext, config, music_mode)
        final_path = os.path.join(dest_dir or paths.single_downloads_dir, cleaned_name)
        logging.info("Dry-run: would download %s → %s", meta.get("title") or vid, final_path)
        _status_set(status, "progress_current", 1)
        _status_set(status, "progress_total", 1)
        _status_set(status, "progress_percent", 100)
        _reset_video_progress(status)
        _status_set(status, "current_video_id", None)
        _status_set(status, "current_video_title", None)
        _status_set(status, "current_phase", None)
        status.single_download_ok = True
        return True

    download_url = build_download_url(vid, music_mode=music_mode, source_url=video_url)
    local_file = download_with_ytdlp(
        download_url,
        temp_dir,
        js_runtime,
        meta,
        config,
        target_format=final_format_override,
        paths=paths,
        status=status,
        stop_event=stop_event,
        music_mode=music_mode,
        cookies_path=cookies_path,
    )
    if not local_file:
        logging.error("Download FAILED: %s", download_url)
        _status_set(status, "last_error_message", "Single download failed")
        shutil.rmtree(temp_dir, ignore_errors=True)
        _status_set(status, "progress_current", 1)
        _status_set(status, "progress_total", 1)
        _status_set(status, "progress_percent", 100)
        _reset_video_progress(status)
        _status_set(status, "current_video_id", None)
        _status_set(status, "current_video_title", None)
        _status_set(status, "current_phase", None)
        return False

    ext = os.path.splitext(local_file)[1].lstrip(".") or final_format_override or config.get("final_format") or "webm"

    cleaned_name = build_output_filename(meta, vid, ext, config, music_mode)

    if delivery_mode == "client":
        _status_set(status, "current_phase", "finalizing")
        logging.info("Final delivery path: client (HTTP)")
        delivery_dir = os.path.join(paths.temp_downloads_dir, "client_delivery")
        os.makedirs(delivery_dir, exist_ok=True)
        final_path = os.path.join(delivery_dir, cleaned_name)
        os.replace(local_file, final_path)
        shutil.rmtree(temp_dir, ignore_errors=True)

        delivery_id, expires_at, _delivery_event = _register_client_delivery(final_path, cleaned_name)
        logging.info("Client delivery registered: token=%s", delivery_id)
        _status_set(status, "client_delivery_id", delivery_id)
        _status_set(status, "client_delivery_filename", cleaned_name)
        _status_set(status, "client_delivery_expires_at", expires_at.isoformat())
        _status_set(status, "client_delivery_mode", "client")
        _status_set(status, "last_completed", cleaned_name)
        _status_set(status, "last_completed_at", datetime.utcnow().isoformat())
        _status_set(status, "last_completed_path", None)
        _status_set(status, "progress_current", 1)
        _status_set(status, "progress_total", 1)
        _status_set(status, "progress_percent", 100)
        _reset_video_progress(status)
        _status_set(status, "current_video_id", None)
        _status_set(status, "current_video_title", None)
        _status_set(status, "current_phase", "ready for client download")
        logging.info("Returning control to API for client download")
        telegram_notify(config, "✅ Download completed → ready for client download")
        return True

    _status_set(status, "current_phase", "copying")
    final_path = os.path.join(dest_dir, cleaned_name)
    os.makedirs(os.path.dirname(final_path), exist_ok=True)

    shutil.copy2(local_file, final_path)
    shutil.rmtree(temp_dir, ignore_errors=True)

    logging.info("Direct download saved to %s", final_path)
    _status_set(status, "client_delivery_id", None)
    _status_set(status, "client_delivery_filename", None)
    _status_set(status, "client_delivery_expires_at", None)
    _status_set(status, "client_delivery_mode", "server")
    _status_set(status, "last_completed", cleaned_name)
    _status_set(status, "last_completed_at", datetime.utcnow().isoformat())
    _status_set(status, "last_completed_path", final_path)
    _status_set(status, "progress_current", 1)
    _status_set(status, "progress_total", 1)
    _status_set(status, "progress_percent", 100)
    _reset_video_progress(status)
    _status_set(status, "current_video_id", None)
    _status_set(status, "current_video_title", None)
    _status_set(status, "current_phase", None)
    telegram_notify(config, "✅ Download completed → saved to server library")
    return True


def run_single_playlist(config, playlist_value, destination=None, account=None,
                        final_format_override=None, *, paths, status=None,
                        js_runtime_override=None, stop_event=None, music_mode=False,
                        mode="full"):
    """Run a single playlist once (no config mutation)."""
    if not playlist_value:
        logging.error("Playlist ID or URL is required")
        return status
    if is_music_url(playlist_value):
        music_mode = True

    playlist_id = extract_playlist_id(playlist_value)
    if not playlist_id:
        logging.error("Invalid playlist ID or URL: %s", playlist_value)
        return status

    folder = destination or config.get("single_download_folder") or "."
    entry = {
        "playlist_id": playlist_id,
        "folder": folder,
        "remove_after_download": False,
        "mode": mode or "full",
    }
    if account:
        entry["account"] = account
    if final_format_override:
        entry["final_format"] = final_format_override
    if music_mode:
        entry["music_mode"] = True

    run_config = dict(config) if isinstance(config, dict) else {}
    run_config["playlists"] = [entry]

    run_once(
        run_config,
        paths=paths,
        status=status,
        js_runtime_override=js_runtime_override,
        stop_event=stop_event,
    )
    return status


def run_once(config, *, paths, status=None, js_runtime_override=None, stop_event=None):
    lock_file = paths.lock_file

    if status is None:
        status = EngineStatus()

    dry_run = bool(config.get("dry_run")) if isinstance(config, dict) else False
    start_ts = time.monotonic()

    if stop_event and stop_event.is_set():
        logging.warning("Stop requested before run start")
        return

    if os.path.exists(lock_file):
        logging.warning("Lockfile present — skipping run")
        return

    lock_dir = os.path.dirname(lock_file)
    if lock_dir:
        os.makedirs(lock_dir, exist_ok=True)
    with open(lock_file, "w") as f:
        f.write(str(os.getpid()))

    conn = init_db(paths.db_path)
    cur = conn.cursor()

    accounts = config.get("accounts", {}) or {}
    playlists = config.get("playlists", []) or []
    js_runtime = resolve_js_runtime(config, override=js_runtime_override)
    cookies_path = resolve_cookiefile(config)
    global_final_format = config.get("final_format")
    debug_metadata = bool(config.get("music_metadata_debug"))
    preview_only = os.environ.get("YT_ARCHIVER_PREVIEW", "").strip().lower() in {"1", "true", "yes", "on"}
    if dry_run:
        logging.info("Dry-run enabled: no downloads or DB writes will occur")

    pending_copies = []
    yt_clients = build_youtube_clients(accounts, config) if accounts else {}

    try:
        _status_set(status, "current_playlist_id", None)
        _status_set(status, "current_video_id", None)
        _status_set(status, "current_video_title", None)
        _status_set(status, "progress_current", None)
        _status_set(status, "progress_total", None)
        _status_set(status, "progress_percent", None)
        _reset_video_progress(status)
        for pl in playlists:
            if stop_event and stop_event.is_set():
                logging.warning("Stop requested; ending run loop")
                return
            playlist_id = pl.get("playlist_id") or pl.get("id")
            folder_value = pl.get("folder") or pl.get("directory")
            account = pl.get("account")
            remove_after = pl.get("remove_after_download", False)
            playlist_format = pl.get("final_format") or global_final_format
            playlist_mode = (pl.get("mode") or "full").lower()
            if playlist_mode not in {"full", "subscribe"}:
                logging.warning("Playlist %s has invalid mode '%s'; using 'full'", playlist_id, playlist_mode)
                playlist_mode = "full"
            subscribe_mode = playlist_mode == "subscribe"
            playlist_music = bool(pl.get("music_mode"))
            playlist_name = pl.get("name") or ""
            if playlist_music and not cookies_path:
                logging.warning("Playlist %s has music_mode enabled without yt-dlp cookies; metadata quality may be degraded.", playlist_id)

            if not playlist_id or not folder_value:
                logging.error("Playlist entry missing id or folder: %s", pl)
                continue
            _status_set(status, "current_playlist_id", playlist_id)
            _status_set(status, "current_video_id", None)
            _status_set(status, "current_video_title", None)
            try:
                target_folder = resolve_dir(folder_value, paths.single_downloads_dir)
            except ValueError as exc:
                logging.error("Invalid playlist folder path: %s", exc)
                continue

            yt = yt_clients.get(account) if account else None
            allow_public = not account

            if account and not yt:
                logging.error("No valid YouTube client for account '%s'; skipping playlist %s", account, playlist_id)
                _status_append(status, "run_failures", f"{playlist_id} (auth)")
                if not dry_run:
                    record_playlist_error(conn, playlist_id, "oauth missing")
                continue

            videos, fetch_error, fallback_error, refresh_error = discover_playlist_videos(
                yt,
                playlist_id,
                allow_public=allow_public,
                cookies_path=cookies_path,
            )
            if refresh_error and account:
                _status_append(status, "run_failures", f"{playlist_id} (auth)")
                if not dry_run:
                    record_playlist_error(conn, playlist_id, "oauth refresh failed")
                yt_clients[account] = None
                continue

            if not videos:
                if fetch_error or fallback_error:
                    logging.error("No videos found for playlist %s (auth or public fetch failed)", playlist_id)
                    _status_append(status, "run_failures", f"{playlist_id} (auth)")
                    if not dry_run:
                        record_playlist_error(conn, playlist_id, "playlist fetch failed")
                else:
                    logging.info("Playlist %s is empty; skipping.", playlist_id)
                continue

            if subscribe_mode:
                if any("position" in v or "playlist_index" in v for v in videos):
                    videos = sorted(videos, key=_playlist_sort_key, reverse=True)
                else:
                    videos = list(reversed(videos))

            if preview_only and not dry_run:
                preview_playlist(conn, playlist_id, playlist_name, playlist_mode, videos)
                continue

            if subscribe_mode and not playlist_has_seen(conn, playlist_id):
                if dry_run:
                    label = playlist_name or playlist_id
                    logging.info(
                        'Dry-run: playlist "%s" subscribe first run; would mark %d as seen, download none',
                        label,
                        len(videos),
                    )
                    continue
                # First subscribe run: record existing videos as seen, download nothing.
                for entry in videos:
                    vid = entry.get("videoId") or entry.get("id") or entry.get("url")
                    if not vid:
                        continue
                    mark_video_seen(conn, playlist_id, vid, downloaded=False)
                conn.commit()
                label = playlist_name or playlist_id
                logging.info(
                    'Playlist "%s" running in subscribe mode (first run): marking %d videos as seen, downloading none',
                    label,
                    len(videos),
                )
                continue

            total_videos = len(videos)
            completed = 0
            _status_set(status, "progress_total", total_videos)
            _status_set(status, "progress_current", completed)
            _status_set(status, "progress_percent", 0)
            format_ctx = _resolve_download_format({
                "music_mode": playlist_music,
                "final_format": playlist_format,
                "audio_only": False,
                "config": config,
            })
            target_fmt = format_ctx["target_fmt"]
            dry_run_ext = target_fmt or playlist_format or config.get("final_format") or (
                "mp3" if format_ctx["audio_mode"] else "webm"
            )

            for entry in videos:
                if stop_event and stop_event.is_set():
                    logging.warning("Stop requested; stopping after current playlist")
                    return
                vid = entry.get("videoId") or entry.get("id")
                if not vid:
                    continue
                _status_set(status, "current_video_id", vid)
                _status_set(status, "current_video_title", vid)
                _status_set(status, "progress_current", completed)
                _status_set(status, "progress_total", total_videos)
                _status_set(status, "progress_percent", int((completed / total_videos) * 100))
                _reset_video_progress(status)

            if subscribe_mode:
                if is_video_seen(conn, playlist_id, vid):
                    # Optimization: stop after the first known video when in subscribe mode.
                    label = playlist_name or playlist_id
                    logging.info('Subscribe: "%s" reached seen video %s; stopping scan.', label, vid)
                    break
            else:
                cur.execute("SELECT video_id FROM downloads WHERE video_id=?", (vid,))
                if cur.fetchone():
                    completed += 1
                    _status_set(status, "progress_current", completed)
                    _status_set(status, "progress_percent", int((completed / total_videos) * 100))
                    continue

            meta = resolve_video_metadata(
                yt,
                vid,
                allow_public_fallback=allow_public,
                music_mode=playlist_music,
                cookies_path=cookies_path,
            )
            if playlist_music:
                _log_music_metadata_quality(meta, vid, debug_metadata)
            _status_set(status, "current_video_title", meta.get("title") or vid)
            _status_set(status, "video_progress_percent", 0)
            _status_set(status, "video_downloaded_bytes", 0)

            logging.info("START download: %s (%s)", vid, meta.get("title"))

            video_url = build_download_url(vid, music_mode=playlist_music, source_url=meta.get("url"))
            temp_dir = os.path.join(paths.temp_downloads_dir, vid)

            if dry_run:
                cleaned_name = build_output_filename(meta, vid, dry_run_ext, config, playlist_music)
                final_path = os.path.join(target_folder, cleaned_name)
                logging.info("Dry-run: would download %s → %s", meta.get("title") or vid, final_path)
                completed += 1
                _status_set(status, "progress_current", completed)
                _status_set(status, "progress_percent", int((completed / total_videos) * 100))
                _status_set(status, "current_phase", None)
                continue

            _status_set(status, "current_phase", "downloading")
            local_file = download_with_ytdlp(
                video_url,
                temp_dir,
                js_runtime,
                meta,
                config,
                target_format=playlist_format,
                paths=paths,
                status=status,
                stop_event=stop_event,
                music_mode=playlist_music,
                cookies_path=cookies_path,
            )
            _reset_video_progress(status)
            if not local_file:
                logging.warning("Download FAILED: %s", vid)
                _status_set(status, "last_error_message", f"Download failed: {vid}")
                _status_append(status, "run_failures", meta.get("title") or vid)
                if not dry_run:
                    record_playlist_error(conn, playlist_id, f"yt-dlp failed: {vid}")
                shutil.rmtree(temp_dir, ignore_errors=True)
                completed += 1
                _status_set(status, "progress_current", completed)
                _status_set(status, "progress_percent", int((completed / total_videos) * 100))
                _status_set(status, "current_phase", None)
                continue

            # Determine extension based on the resulting file or playlist/default format
            ext = os.path.splitext(local_file)[1].lstrip(".") or playlist_format or "webm"

            # Build filename using filename_template if present
            cleaned_name = build_output_filename(meta, vid, ext, config, playlist_music)

            _status_set(status, "current_phase", "copying")
            final_path = os.path.join(target_folder, cleaned_name)

            def after_copy(success, dst, video_id=vid, playlist=playlist_id,
                           entry_id=entry.get("playlistItemId"),
                           temp=temp_dir, remove=remove_after, yt_service=yt,
                           db_path=paths.db_path, subscribe=subscribe_mode):

                if success:
                    logging.info("Copy OK → %s", dst)
                    _status_append(status, "run_successes", cleaned_name)
                    _status_set(status, "last_completed", cleaned_name)
                    _status_set(status, "last_completed_at", datetime.utcnow().isoformat())
                    _status_set(status, "last_completed_path", dst)
                    try:
                        with sqlite3.connect(db_path, check_same_thread=False) as c:
                            c.execute(
                                "INSERT INTO downloads (video_id, playlist_id, downloaded_at, filepath)"
                                " VALUES (?, ?, ?, ?)",
                                (video_id, playlist, datetime.utcnow(), dst)
                            )
                            if subscribe:
                                mark_video_downloaded(c, playlist, video_id)
                            c.commit()
                    except Exception:
                        logging.exception("DB insert failed for %s", video_id)
                else:
                    logging.error("Copy FAILED for %s", video_id)
                    _status_append(status, "run_failures", cleaned_name)
                    _status_set(status, "last_error_message", f"Copy failed: {cleaned_name}")

                shutil.rmtree(temp, ignore_errors=True)

                if success and remove and entry_id and yt_service:
                    try:
                        yt_service.playlistItems().delete(id=entry_id).execute()
                    except Exception:
                        logging.exception("Failed removing %s", video_id)

            t = async_copy(local_file, final_path, after_copy)
            pending_copies.append(t)
            logging.info("COPY started in background → next download begins")
            completed += 1
            _status_set(status, "progress_current", completed)
            _status_set(status, "progress_percent", int((completed / total_videos) * 100))

        for t in pending_copies:
            t.join()
        logging.info("\n" + ("-" * 80) + "\n")
        logging.info("Run complete.")
        logging.info("\n" + ("-" * 80) + "\n \n \n")

    finally:
        conn.close()
        try:
            _status_set(status, "current_playlist_id", None)
            _status_set(status, "current_video_id", None)
            _status_set(status, "current_video_title", None)
            _status_set(status, "progress_current", None)
            _status_set(status, "progress_total", None)
            _status_set(status, "progress_percent", None)
            _status_set(status, "current_phase", None)
            _reset_video_progress(status)
            # Telegram Summary
            lock = getattr(status, "lock", None)
            if lock:
                with lock:
                    successes = list(status.run_successes)
                    failures = list(status.run_failures)
            else:
                successes = list(status.run_successes)
                failures = list(status.run_failures)

            if successes or failures:
                duration_seconds = max(0, int(time.monotonic() - start_ts))
                status_label = "completed with errors" if failures else "completed"
                max_items = 20
                max_len = 4000
                truncated = False

                def format_duration(seconds):
                    hours = seconds // 3600
                    minutes = (seconds % 3600) // 60
                    secs = seconds % 60
                    if hours:
                        return f"{hours}h {minutes}m {secs}s"
                    if minutes:
                        return f"{minutes}m {secs}s"
                    return f"{secs}s"

                def build_message(limit):
                    parts = [
                        "YouTube Archiver Summary",
                        f"Status: {status_label}",
                        f"✔ Success: {len(successes)}",
                        f"✖ Failed: {len(failures)}",
                        f"Duration: {format_duration(duration_seconds)}",
                        "",
                    ]
                    if successes:
                        parts.append("Downloaded:")
                        for title in successes[:limit]:
                            parts.append(f"• {title}")
                        remaining = len(successes) - limit
                        if remaining > 0:
                            parts.append(f"• (+{remaining} more)")
                    if failures:
                        if successes:
                            parts.append("")
                        parts.append("Failed:")
                        for title in failures[:limit]:
                            parts.append(f"• {title}")
                        remaining = len(failures) - limit
                        if remaining > 0:
                            parts.append(f"• (+{remaining} more)")
                    return "\n".join(parts)

                msg = build_message(max_items)
                if len(successes) > max_items or len(failures) > max_items:
                    truncated = True

                while len(msg) > max_len and max_items > 0:
                    max_items -= 1
                    truncated = True
                    msg = build_message(max_items)

                if truncated:
                    logging.warning("Telegram summary truncated to fit message limits.")

                telegram_notify(config, msg)
            os.remove(lock_file)
        except FileNotFoundError:
            pass


# ------------------------------------------------------------------
# Engine entrypoint
# ------------------------------------------------------------------

def run_archive(config, *, paths, status=None, single_url=None, destination=None,
                final_format_override=None, js_runtime_override=None, stop_event=None,
                run_source="manual", music_mode=False, skip_downtime=False,
                delivery_mode="server"):
    if status is None:
        status = EngineStatus()

    logging.info("Run started (source=%s)", run_source)
    _status_set(status, "current_phase", "starting")
    _status_set(status, "last_error_message", None)

    if single_url:
        ok = run_single_download(
            config,
            single_url,
            destination,
            final_format_override,
            paths=paths,
            status=status,
            js_runtime_override=js_runtime_override,
            stop_event=stop_event,
            music_mode=music_mode,
            skip_downtime=skip_downtime,
            delivery_mode=delivery_mode,
        )
        status.single_download_ok = ok
        return status

    run_once(config, paths=paths, status=status, js_runtime_override=js_runtime_override, stop_event=stop_event)
    return status
