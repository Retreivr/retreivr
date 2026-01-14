#!/usr/bin/env python3
import sys


def _require_python_311():
    if sys.version_info[:2] != (3, 11):
        found = sys.version.split()[0]
        raise SystemExit(
            f"ERROR: youtube-archiver requires Python 3.11.x; found Python {found} "
            f"(executable: {sys.executable})"
        )


_require_python_311()

import asyncio
import functools
import base64
import binascii
import hmac
import json
import logging
import mimetypes
import os
import sqlite3
import subprocess
import tempfile
import threading
import time
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from uuid import uuid4

import anyio
from fastapi import Body, FastAPI, HTTPException, Query, Request
from fastapi.responses import PlainTextResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.errors import HttpError
from pydantic import BaseModel
from uvicorn.middleware.proxy_headers import ProxyHeadersMiddleware
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.date import DateTrigger
from google.auth.exceptions import RefreshError

from engine.core import (
    EngineStatus,
    build_youtube_clients,
    extract_playlist_id,
    get_playlist_videos,
    get_status,
    init_db,
    is_video_downloaded,
    is_video_seen,
    _acquire_client_delivery,
    _finalize_client_delivery,
    _mark_client_delivery,
    load_config,
    mark_video_seen,
    playlist_has_seen,
    read_history,
    run_archive,
    run_single_playlist,
    telegram_notify,
    validate_config,
)
from engine.paths import (
    CONFIG_DIR,
    DATA_DIR,
    DOWNLOADS_DIR,
    LOG_DIR,
    TOKENS_DIR,
    build_engine_paths,
    ensure_dir,
    resolve_config_path,
    resolve_dir,
)
from engine.runtime import get_runtime_info

APP_NAME = "YouTube Archiver API"
STATUS_SCHEMA_VERSION = 1
METRICS_SCHEMA_VERSION = 1
SCHEDULE_SCHEMA_VERSION = 1
_BASIC_AUTH_USER = os.environ.get("YT_ARCHIVER_BASIC_AUTH_USER")
_BASIC_AUTH_PASS = os.environ.get("YT_ARCHIVER_BASIC_AUTH_PASS")
_BASIC_AUTH_ENABLED = bool(_BASIC_AUTH_USER and _BASIC_AUTH_PASS)
_TRUST_PROXY = os.environ.get("YT_ARCHIVER_TRUST_PROXY", "").strip().lower() in {"1", "true", "yes", "on"}
SCHEDULE_JOB_ID = "archive_schedule"
WATCHER_JOB_ID = "playlist_watcher"
DEFERRED_RUN_JOB_ID = "deferred_run"
WATCHER_QUIET_WINDOW_SECONDS = 60
OAUTH_SCOPES = ["https://www.googleapis.com/auth/youtube.readonly"]
OAUTH_SESSION_TTL = timedelta(minutes=15)
_OAUTH_SESSIONS = {}
_OAUTH_LOCK = threading.Lock()
_DEPRECATED_FIELDS = {"poll_interval_hours"}
_DEPRECATED_LOGGED = set()
_MULTI_WORKER_ENV_KEYS = ("UVICORN_WORKERS", "WEB_CONCURRENCY", "GUNICORN_WORKERS")

WEBUI_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "webUI"))


def _env_or_default(name, default):
    value = os.environ.get(name)
    return value if value else default


def _check_basic_auth(header_value):
    if not header_value or not header_value.startswith("Basic "):
        return False
    token = header_value[6:].strip()
    try:
        decoded = base64.b64decode(token.encode("ascii"), validate=True).decode("utf-8")
    except (binascii.Error, UnicodeDecodeError):
        return False
    if ":" not in decoded:
        return False
    user, password = decoded.split(":", 1)
    return hmac.compare_digest(user, _BASIC_AUTH_USER) and hmac.compare_digest(password, _BASIC_AUTH_PASS)


def _setup_logging(log_dir):
    ensure_dir(log_dir)
    root = logging.getLogger("")
    log_path = os.path.join(log_dir, "archiver.log")
    root.setLevel(logging.INFO)
    has_file = False
    for handler in root.handlers:
        if isinstance(handler, logging.FileHandler):
            if os.path.abspath(getattr(handler, "baseFilename", "")) == os.path.abspath(log_path):
                has_file = True
                break
    if not has_file:
        file_handler = logging.FileHandler(log_path)
        file_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
        file_handler.setLevel(logging.INFO)
    root.addHandler(file_handler)


def _detect_worker_count():
    counts = []
    for key in _MULTI_WORKER_ENV_KEYS:
        raw = os.environ.get(key)
        if raw and raw.isdigit():
            counts.append(int(raw))
    cmd_args = os.environ.get("GUNICORN_CMD_ARGS", "")
    if cmd_args:
        match = re.search(r"--workers\s+(\d+)", cmd_args)
        if match:
            counts.append(int(match.group(1)))
    return max(counts) if counts else 1


def _warn_deprecated_fields(config):
    if not isinstance(config, dict):
        return
    for field in sorted(_DEPRECATED_FIELDS):
        if field in config and field not in _DEPRECATED_LOGGED:
            logging.warning("Deprecated config field '%s' ignored and will be removed on save", field)
            _DEPRECATED_LOGGED.add(field)


def _strip_deprecated_fields(config):
    if not isinstance(config, dict):
        return config
    updated = dict(config)
    removed = False
    # Deprecated fields are ignored to avoid changing runtime behavior.
    for field in _DEPRECATED_FIELDS:
        if field in updated:
            updated.pop(field, None)
            removed = True
    if removed:
        _warn_deprecated_fields(config)
    return updated


def _acquire_watcher_lock(lock_dir):
    lock_path = os.path.join(lock_dir, "watcher.lock")
    try:
        import fcntl
    except Exception:
        logging.error("Watcher lock unavailable; watcher disabled")
        return None
    fd = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o644)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        os.close(fd)
        return None
    os.ftruncate(fd, 0)
    os.write(fd, str(os.getpid()).encode("utf-8"))
    return fd


class RunRequest(BaseModel):
    single_url: str | None = None
    playlist_id: str | None = None
    playlist_account: str | None = None
    destination: str | None = None
    final_format_override: str | None = None
    js_runtime: str | None = None
    music_mode: bool | None = None
    delivery_mode: str | None = None


class ConfigPathRequest(BaseModel):
    path: str


class ScheduleRequest(BaseModel):
    enabled: bool | None = None
    mode: str | None = None
    interval_hours: int | None = None
    run_on_startup: bool | None = None


class OAuthStartRequest(BaseModel):
    account: str | None = None
    client_secret: str
    token_out: str


class OAuthCompleteRequest(BaseModel):
    session_id: str
    code: str


def _purge_oauth_sessions():
    now = datetime.now(timezone.utc)
    with _OAUTH_LOCK:
        expired = [key for key, entry in _OAUTH_SESSIONS.items() if entry["expires_at"] <= now]
        for key in expired:
            _OAUTH_SESSIONS.pop(key, None)


app = FastAPI(title=APP_NAME)

if _TRUST_PROXY:
    app.add_middleware(ProxyHeadersMiddleware, trusted_hosts="*")


@app.middleware("http")
async def basic_auth_middleware(request: Request, call_next):
    if not _BASIC_AUTH_ENABLED:
        return await call_next(request)
    if request.method == "OPTIONS":
        return await call_next(request)
    auth_header = request.headers.get("authorization")
    if not _check_basic_auth(auth_header):
        return PlainTextResponse(
            "Unauthorized",
            status_code=401,
            headers={"WWW-Authenticate": "Basic"},
        )
    return await call_next(request)


@app.on_event("startup")
async def startup():
    app.state.paths = build_engine_paths()
    try:
        app.state.config_path = resolve_config_path(os.environ.get("YT_ARCHIVER_CONFIG"))
    except ValueError as exc:
        logging.error("Invalid config override: %s", exc)
        app.state.config_path = resolve_config_path(None)
    app.state.log_path = os.path.join(LOG_DIR, "archiver.log")
    app.state.running = False
    app.state.state = "idle"
    app.state.run_id = None
    app.state.started_at = None
    app.state.finished_at = None
    app.state.last_error = None
    app.state.status = EngineStatus()
    app.state.run_lock = asyncio.Lock()
    app.state.stop_event = threading.Event()
    app.state.run_task = None
    app.state.loop = asyncio.get_running_loop()
    app.state.schedule_lock = threading.Lock()
    app.state.ytdlp_update_lock = threading.Lock()
    app.state.ytdlp_update_running = False
    app.state.cancel_requested = False
    app.state.scheduler = BackgroundScheduler(timezone="UTC")
    app.state.watch_config_cache = None
    app.state.was_in_downtime = False
    app.state.watcher_task = None
    app.state.worker_count = _detect_worker_count()
    app.state.single_worker_enforced = app.state.worker_count <= 1
    ensure_dir(DATA_DIR)
    ensure_dir(CONFIG_DIR)
    ensure_dir(LOG_DIR)
    ensure_dir(DOWNLOADS_DIR)
    ensure_dir(TOKENS_DIR)
    app.state.browse_roots = _browse_root_map()
    _setup_logging(LOG_DIR)
    _init_schedule_db(app.state.paths.db_path)
    state = _read_schedule_state(app.state.paths.db_path)
    app.state.schedule_last_run = state.get("last_run")
    app.state.schedule_next_run = state.get("next_run")
    schedule_config = _default_schedule_config()
    config = _read_config_for_scheduler()
    if config:
        schedule_config = _merge_schedule_config(config.get("schedule"))
    app.state.schedule_config = schedule_config
    app.state.scheduler.start()
    _apply_schedule_config(schedule_config)
    if schedule_config.get("enabled") and schedule_config.get("run_on_startup"):
        asyncio.create_task(_handle_scheduled_run())
    if schedule_config.get("enabled"):
        logging.info("Scheduler active — fixed interval bulk runs")

    init_db(app.state.paths.db_path)
    watch_policy = normalize_watch_policy(config or {})
    app.state.watch_policy = watch_policy
    app.state.watch_config_cache = config
    app.state.watcher_clients_cache = {}
    enable_watcher = bool(config.get("enable_watcher")) if isinstance(config, dict) else False
    if not enable_watcher:
        app.state.watcher_lock = None
        app.state.watcher_task = None
        logging.info("Watcher disabled by config; not starting")
    else:
        if not app.state.single_worker_enforced:
            logging.info(
                "Watcher disabled due to guardrails (multiple workers detected=%d)",
                app.state.worker_count,
            )
            app.state.watcher_lock = None
        else:
            app.state.watcher_lock = _acquire_watcher_lock(DATA_DIR)
        if app.state.watcher_lock is None:
            logging.info("Watcher disabled due to guardrails (lock unavailable)")
        else:
            app.state.watcher_task = asyncio.create_task(_watcher_supervisor())
            logging.info("Watcher active — adaptive monitoring enabled")

    app.state.watcher_status = {
        "state": "disabled" if app.state.watcher_lock is None else "idle",
        "last_poll_ts": None,
        "next_poll_ts": None,
        "pending_playlists_count": 0,
        "quiet_window_remaining_sec": None,
        "batch_active": False,
    }

    downtime_active, _ = _check_downtime(config or {})
    logging.info(
        "Watcher startup: enabled=%s single_worker=%s downtime_active=%s",
        bool(app.state.watcher_lock),
        app.state.single_worker_enforced,
        downtime_active,
    )


@app.on_event("shutdown")
async def shutdown():
    if app.state.running:
        app.state.stop_event.set()
        task = app.state.run_task
        if task:
            try:
                await asyncio.wait_for(task, timeout=30)
            except asyncio.TimeoutError:
                logging.warning("Shutdown timeout while waiting for archive run to stop")
    scheduler = app.state.scheduler
    if scheduler:
        scheduler.shutdown(wait=False)
    watcher_task = getattr(app.state, "watcher_task", None)
    if watcher_task:
        logging.info("Watcher shutdown")
        watcher_task.cancel()
        try:
            await watcher_task
        except asyncio.CancelledError:
            pass
    lock_fd = getattr(app.state, "watcher_lock", None)
    if lock_fd is not None:
        try:
            os.close(lock_fd)
        except OSError:
            pass
    logging.shutdown()


def _browse_root_map():
    return {
        "downloads": os.path.realpath(DOWNLOADS_DIR),
        "config": os.path.realpath(CONFIG_DIR),
        "tokens": os.path.realpath(TOKENS_DIR),
    }


def _path_allowed(path, roots):
    real = os.path.realpath(path)
    for root in roots:
        try:
            if os.path.commonpath([real, root]) == root:
                return True
        except ValueError:
            continue
    return False


def _resolve_browse_path(root_base, rel_path):
    rel_path = (rel_path or "").strip()
    if os.path.isabs(rel_path):
        logging.warning("Browse blocked: absolute path %s", rel_path)
        raise HTTPException(status_code=400, detail="path must be relative")
    normalized = os.path.normpath(rel_path)
    if normalized in (".", os.curdir):
        normalized = ""
    if normalized.startswith(".."):
        logging.warning("Browse blocked: path escape attempt %s", rel_path)
        raise HTTPException(status_code=403, detail="path not allowed")
    abs_path = os.path.realpath(os.path.join(root_base, normalized))
    base = os.path.realpath(root_base)
    if os.path.commonpath([abs_path, base]) != base:
        logging.warning("Browse blocked: path outside root %s", rel_path)
        raise HTTPException(status_code=403, detail="path not allowed")
    return normalized, abs_path


def _list_browse_entries(base, directory, mode, ext, limit=None):
    entries = []
    with os.scandir(directory) as it:
        for entry in it:
            if entry.name.startswith("."):
                continue
            is_dir = entry.is_dir(follow_symlinks=False)
            is_file = entry.is_file(follow_symlinks=False)
            if mode == "dir":
                if not is_dir:
                    continue
            else:
                if not (is_dir or is_file):
                    continue
                if is_file and ext and not entry.name.lower().endswith(ext):
                    continue
            rel_entry = os.path.relpath(entry.path, base)
            entries.append(
                {
                    "name": entry.name,
                    "path": rel_entry if rel_entry != "." else "",
                    "abs_path": entry.path,
                    "type": "dir" if is_dir else "file",
                }
            )
            if limit and len(entries) >= limit:
                break
    entries.sort(key=lambda item: (item["type"] != "dir", item["name"].lower()))
    return entries


def _tail_lines(path, lines, max_bytes=1_000_000):
    if not os.path.exists(path):
        return ""
    with open(path, "rb") as f:
        f.seek(0, os.SEEK_END)
        size = f.tell()
        block = min(size, max_bytes)
        if block <= 0:
            return ""
        f.seek(-block, os.SEEK_END)
        data = f.read().splitlines()
    tail = data[-lines:] if lines else data
    return b"\n".join(tail).decode("utf-8", errors="replace")


def _normalize_date(value, end_of_day=False):
    if not value:
        return None
    value = value.strip()
    if not value:
        return None
    if len(value) == 10 and value[4] == "-" and value[7] == "-":
        suffix = " 23:59:59" if end_of_day else " 00:00:00"
        return f"{value}{suffix}"
    return value


def _encode_file_id(rel_path):
    token = base64.urlsafe_b64encode(rel_path.encode("utf-8")).decode("ascii")
    return token.rstrip("=")


def _decode_file_id(file_id):
    padded = file_id + "=" * (-len(file_id) % 4)
    raw = base64.urlsafe_b64decode(padded.encode("ascii")).decode("utf-8")
    return raw


def _safe_filename(name):
    cleaned = name.replace('"', "'").replace("\n", " ").replace("\r", " ").strip()
    return cleaned or "download"


def _file_id_from_path(path):
    if not path:
        return None
    full = os.path.abspath(path)
    if not _path_allowed(full, [DOWNLOADS_DIR]):
        return None
    rel = os.path.relpath(full, DOWNLOADS_DIR)
    return _encode_file_id(rel)


def _iter_file(path, chunk_size=1024 * 1024):
    with open(path, "rb") as handle:
        while True:
            chunk = handle.read(chunk_size)
            if not chunk:
                break
            yield chunk


def _yt_dlp_script_path():
    return os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "scripts", "update_yt_dlp.sh"))


def _list_download_files(base_dir):
    if not os.path.isdir(base_dir):
        return []
    results = []
    for root, dirs, files in os.walk(base_dir):
        dirs[:] = [d for d in dirs if not d.startswith(".")]
        for name in files:
            if name.startswith("."):
                continue
            full_path = os.path.join(root, name)
            try:
                stat = os.stat(full_path)
            except OSError:
                continue
            rel = os.path.relpath(full_path, base_dir)
            results.append(
                {
                    "id": _encode_file_id(rel),
                    "name": name,
                    "relative_path": rel,
                    "size_bytes": stat.st_size,
                    "modified_at": datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).isoformat(),
                }
            )
    results.sort(key=lambda item: item["modified_at"], reverse=True)
    return results


def _downloads_metrics(base_dir):
    total_files = 0
    total_bytes = 0
    if not os.path.isdir(base_dir):
        return total_files, total_bytes
    for root, dirs, files in os.walk(base_dir):
        dirs[:] = [d for d in dirs if not d.startswith(".")]
        for name in files:
            if name.startswith("."):
                continue
            file_path = os.path.join(root, name)
            try:
                total_bytes += os.path.getsize(file_path)
                total_files += 1
            except OSError:
                continue
    return total_files, total_bytes


def _disk_usage(path):
    try:
        stat = os.statvfs(path)
    except OSError:
        return {
            "total_bytes": None,
            "free_bytes": None,
            "used_bytes": None,
            "free_percent": None,
        }
    total = stat.f_frsize * stat.f_blocks
    free = stat.f_frsize * stat.f_bavail
    used = total - free
    free_percent = (free / total) * 100 if total else None
    return {
        "total_bytes": total,
        "free_bytes": free,
        "used_bytes": used,
        "free_percent": round(free_percent, 1) if free_percent is not None else None,
    }


def _init_schedule_db(db_path):
    db_dir = os.path.dirname(db_path)
    if db_dir:
        os.makedirs(db_dir, exist_ok=True)
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute(
        "CREATE TABLE IF NOT EXISTS schedule_state (key TEXT PRIMARY KEY, value TEXT)"
    )
    conn.commit()
    conn.close()


def _read_schedule_state(db_path):
    if not os.path.exists(db_path):
        return {"last_run": None, "next_run": None}
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("SELECT key, value FROM schedule_state WHERE key IN ('last_run', 'next_run')")
    rows = cur.fetchall()
    conn.close()
    state = {"last_run": None, "next_run": None}
    for key, value in rows:
        state[key] = value
    return state


def _write_schedule_state(db_path, *, last_run=None, next_run=None):
    if last_run is None and next_run is None:
        return
    _init_schedule_db(db_path)
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    for key, value in (("last_run", last_run), ("next_run", next_run)):
        if value is None:
            cur.execute("DELETE FROM schedule_state WHERE key=?", (key,))
        else:
            cur.execute(
                "INSERT INTO schedule_state (key, value) VALUES (?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, value),
            )
    conn.commit()
    conn.close()


def _default_schedule_config():
    return {
        "enabled": False,
        "mode": "interval",
        "interval_hours": 6,
        "run_on_startup": False,
    }


def _default_watch_policy():
    return {
        "min_interval_minutes": 5,
        "max_interval_minutes": 360,
        "idle_backoff_factor": 2,
        "active_reset_minutes": 5,
        "downtime": {
            "enabled": False,
            "start": "23:00",
            "end": "09:00",
            "timezone": "local",
        },
    }


def _merge_schedule_config(schedule):
    merged = _default_schedule_config()
    if isinstance(schedule, dict):
        for key in ("enabled", "mode", "interval_hours", "run_on_startup"):
            if key in schedule:
                merged[key] = schedule[key]
    return merged


def _merge_watch_policy(policy):
    merged = _default_watch_policy()
    if isinstance(policy, dict):
        for key in ("min_interval_minutes", "max_interval_minutes", "idle_backoff_factor", "active_reset_minutes"):
            if key in policy:
                merged[key] = policy[key]
        downtime = policy.get("downtime")
        if isinstance(downtime, dict):
            merged_downtime = dict(merged["downtime"])
            for key in ("enabled", "start", "end", "timezone"):
                if key in downtime:
                    merged_downtime[key] = downtime[key]
            merged["downtime"] = merged_downtime
    return merged


def normalize_watch_policy(raw_config):
    """Apply defaults only when watch_policy is missing; otherwise require full config."""
    last_good = getattr(app.state, "watch_policy", None) or _default_watch_policy()
    normalize_watch_policy.valid = True

    if not isinstance(raw_config, dict):
        logging.error("Invalid watch_policy config; using last-known-good policy")
        normalize_watch_policy.valid = False
        return last_good

    if "watch_policy" not in raw_config:
        # Defaults only when missing entirely.
        return _default_watch_policy()

    policy = raw_config.get("watch_policy")
    if not isinstance(policy, dict):
        logging.error("Invalid watch_policy: must be an object")
        normalize_watch_policy.valid = False
        return last_good

    required = {"min_interval_minutes", "max_interval_minutes", "idle_backoff_factor", "active_reset_minutes", "downtime"}
    missing = sorted(key for key in required if key not in policy)
    if missing:
        logging.error("Invalid watch_policy: missing fields %s", ", ".join(missing))
        normalize_watch_policy.valid = False
        return last_good

    downtime = policy.get("downtime")
    if not isinstance(downtime, dict):
        logging.error("Invalid watch_policy.downtime: must be an object")
        normalize_watch_policy.valid = False
        return last_good

    required_dt = {"enabled", "start", "end", "timezone"}
    missing_dt = sorted(key for key in required_dt if key not in downtime)
    if missing_dt:
        logging.error("Invalid watch_policy.downtime: missing fields %s", ", ".join(missing_dt))
        normalize_watch_policy.valid = False
        return last_good

    errors = _validate_watch_policy(policy)
    if errors:
        logging.error("Invalid watch_policy: %s", errors)
        normalize_watch_policy.valid = False
        return last_good

    tz_value = downtime.get("timezone")
    if tz_value not in {"local", "system", "UTC"}:
        try:
            ZoneInfo(tz_value)
        except Exception:
            logging.error("Invalid watch_policy.downtime.timezone: %s", tz_value)
            normalize_watch_policy.valid = False
            return last_good

    return {
        "min_interval_minutes": policy["min_interval_minutes"],
        "max_interval_minutes": policy["max_interval_minutes"],
        "idle_backoff_factor": policy["idle_backoff_factor"],
        "active_reset_minutes": policy["active_reset_minutes"],
        "downtime": {
            "enabled": downtime["enabled"],
            "start": downtime["start"],
            "end": downtime["end"],
            "timezone": downtime["timezone"],
        },
    }


def _validate_schedule_config(schedule):
    errors = []
    if schedule is None:
        return errors
    if not isinstance(schedule, dict):
        return ["schedule must be an object"]
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
    return errors


def _validate_watch_policy(policy):
    if policy is None:
        return []
    if not isinstance(policy, dict):
        return ["watch_policy must be an object"]
    errors = []
    min_interval = policy.get("min_interval_minutes")
    max_interval = policy.get("max_interval_minutes")
    idle_backoff = policy.get("idle_backoff_factor")
    active_reset = policy.get("active_reset_minutes")
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
    if isinstance(min_interval, int) and isinstance(max_interval, int) and max_interval < min_interval:
        errors.append("watch_policy.max_interval_minutes must be >= min_interval_minutes")
    if isinstance(idle_backoff, int) and idle_backoff < 1:
        errors.append("watch_policy.idle_backoff_factor must be >= 1")
    if isinstance(active_reset, int) and active_reset < 1:
        errors.append("watch_policy.active_reset_minutes must be >= 1")
    downtime = policy.get("downtime")
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


def _cleanup_dir(path):
    deleted_files = 0
    deleted_bytes = 0
    if not os.path.isdir(path):
        return deleted_files, deleted_bytes
    for root, dirs, files in os.walk(path, topdown=False):
        for name in files:
            file_path = os.path.join(root, name)
            try:
                deleted_bytes += os.path.getsize(file_path)
            except OSError:
                pass
            try:
                os.remove(file_path)
                deleted_files += 1
            except OSError:
                pass
        for name in dirs:
            dir_path = os.path.join(root, name)
            try:
                os.rmdir(dir_path)
            except OSError:
                pass
    ensure_dir(path)
    return deleted_files, deleted_bytes


def _read_config_or_404():
    config_path = app.state.config_path
    if not os.path.exists(config_path):
        raise HTTPException(status_code=404, detail=f"Config not found: {config_path}")
    try:
        config = load_config(config_path)
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=400, detail=f"Invalid JSON in config: {exc}") from exc
    except OSError as exc:
        raise HTTPException(status_code=500, detail=f"Failed to read config: {exc}") from exc
    errors = validate_config(config)
    if errors:
        raise HTTPException(status_code=400, detail={"errors": errors})
    _warn_deprecated_fields(config)
    return _strip_deprecated_fields(config)


def _read_config_for_scheduler():
    config_path = app.state.config_path
    if not os.path.exists(config_path):
        logging.error("Schedule skipped: config not found at %s", config_path)
        return None
    try:
        config = load_config(config_path)
    except json.JSONDecodeError as exc:
        logging.error("Schedule skipped: invalid JSON in config: %s", exc)
        return None
    except OSError as exc:
        logging.error("Schedule skipped: failed to read config: %s", exc)
        return None
    errors = validate_config(config)
    if errors:
        logging.error("Schedule skipped: invalid config: %s", errors)
        return None
    _warn_deprecated_fields(config)
    return _strip_deprecated_fields(config)


def _read_config_for_watcher():
    config_path = app.state.config_path
    cached = app.state.watch_config_cache
    if not os.path.exists(config_path):
        logging.error("Watcher skipped: config not found at %s", config_path)
        return cached
    try:
        with open(config_path, "r") as handle:
            data = handle.read()
        config = json.loads(data)
    except json.JSONDecodeError as exc:
        logging.error("Watcher skipped: invalid JSON in config: %s", exc)
        return cached
    except OSError as exc:
        logging.error("Watcher skipped: failed to read config: %s", exc)
        return cached
    errors = validate_config(config)
    if errors:
        logging.error("Watcher skipped: invalid config: %s", errors)
        return cached
    _warn_deprecated_fields(config)
    config = _strip_deprecated_fields(config)
    policy = normalize_watch_policy(config)
    if not getattr(normalize_watch_policy, "valid", True):
        return cached or config
    config["watch_policy"] = policy
    app.state.watch_policy = policy
    app.state.watch_config_cache = config
    return config


async def _start_run_with_config(
    config,
    *,
    single_url=None,
    playlist_id=None,
    playlist_account=None,
    playlist_mode=None,
    destination=None,
    final_format_override=None,
    js_runtime=None,
    music_mode=None,
    run_source="api",
    skip_downtime=False,
    run_id_override=None,
    now=None,
    delivery_mode=None,
):
    async with app.state.run_lock:
        if app.state.running:
            return "busy", None

        app.state.running = True
        app.state.state = "running"
        app.state.run_id = run_id_override or str(uuid4())
        app.state.started_at = datetime.now(timezone.utc).isoformat()
        app.state.finished_at = None
        app.state.last_error = None
        status = EngineStatus()
        app.state.status = status
        app.state.stop_event = threading.Event()

        async def _runner():
            try:
                if run_source == "watcher":
                    logging.info("Watcher-triggered run starting")
                elif run_source == "scheduled":
                    logging.info("Scheduled run starting")
                else:
                    logging.info("Manual run starting (source=%s)", run_source)
                if playlist_id:
                    run_callable = functools.partial(
                        run_single_playlist,
                        config,
                        playlist_id,
                        destination,
                        playlist_account,
                        final_format_override,
                        paths=app.state.paths,
                        status=status,
                        js_runtime_override=js_runtime,
                        stop_event=app.state.stop_event,
                        music_mode=bool(music_mode) if music_mode is not None else False,
                        mode=playlist_mode or "full",
                    )
                else:
                    run_callable = functools.partial(
                    run_archive,
                    config,
                    paths=app.state.paths,
                    status=status,
                    single_url=single_url,
                    destination=destination,
                    final_format_override=final_format_override,
                    js_runtime_override=js_runtime,
                    stop_event=app.state.stop_event,
                    run_source=run_source,
                    music_mode=bool(music_mode) if music_mode is not None else False,
                    skip_downtime=skip_downtime,
                    delivery_mode=delivery_mode or "server",
                )
                await anyio.to_thread.run_sync(run_callable)
                if app.state.stop_event.is_set():
                    if app.state.cancel_requested:
                        app.state.last_error = "Downloads cancelled by user"
                    else:
                        app.state.last_error = "Run stopped"
                    app.state.state = "error"
            except Exception as exc:
                logging.exception("Archive run failed: %s", exc)
                app.state.last_error = str(exc)
                app.state.state = "error"
            finally:
                app.state.running = False
                app.state.finished_at = datetime.now(timezone.utc).isoformat()
                if app.state.cancel_requested:
                    logging.info("Active downloads killed")
                    app.state.cancel_requested = False
                    app.state.state = "idle"
                    app.state.last_error = "Downloads cancelled by user"
                    _cleanup_dir(app.state.paths.temp_downloads_dir)
                    _cleanup_dir(app.state.paths.ytdlp_temp_dir)
                    logging.info("Downloads cancelled by user")
                    logging.info("State reset to idle")
                elif app.state.state == "running":
                    app.state.state = "idle"

        app.state.run_task = asyncio.create_task(_runner())

    return "started", None


def _get_next_run_iso():
    scheduler = app.state.scheduler
    if not scheduler:
        return None
    job = scheduler.get_job(SCHEDULE_JOB_ID)
    if not job or not job.next_run_time:
        return None
    next_run = job.next_run_time
    if next_run.tzinfo is None:
        next_run = next_run.replace(tzinfo=timezone.utc)
    return next_run.astimezone(timezone.utc).isoformat()


_UNSET = object()


def _set_schedule_state(*, last_run=_UNSET, next_run=_UNSET):
    with app.state.schedule_lock:
        if last_run is not _UNSET:
            app.state.schedule_last_run = last_run
        if next_run is not _UNSET:
            app.state.schedule_next_run = next_run
    db_last = None if last_run is _UNSET else last_run
    db_next = None if next_run is _UNSET else next_run
    _write_schedule_state(app.state.paths.db_path, last_run=db_last, next_run=db_next)


def _schedule_tick():
    # Scheduler = periodic full runs based on schedule config.
    loop = app.state.loop
    if not loop or loop.is_closed():
        return
    asyncio.run_coroutine_threadsafe(_handle_scheduled_run(), loop)


async def _handle_scheduled_run():
    if app.state.running:
        logging.info("Scheduled run skipped; run already active")
        _set_schedule_state(next_run=_get_next_run_iso())
        return
    config = _read_config_for_scheduler()
    if not config:
        _set_schedule_state(next_run=_get_next_run_iso())
        return
    result, next_allowed = await _start_run_with_config(config, run_source="scheduled")
    if result == "started":
        logging.info("Scheduled run starting")
        now = datetime.now(timezone.utc).isoformat()
        _set_schedule_state(last_run=now, next_run=_get_next_run_iso())
    elif result == "deferred":
        _set_schedule_state(next_run=_format_iso(next_allowed))
    else:
        _set_schedule_state(next_run=_get_next_run_iso())


def _apply_schedule_config(schedule):
    scheduler = app.state.scheduler
    if not scheduler:
        return
    job = scheduler.get_job(SCHEDULE_JOB_ID)
    if job:
        scheduler.remove_job(SCHEDULE_JOB_ID)

    if schedule.get("enabled"):
        interval = schedule.get("interval_hours") or 1
        start_date = datetime.now(timezone.utc) + timedelta(hours=interval)
        scheduler.add_job(
            _schedule_tick,
            trigger=IntervalTrigger(hours=interval, start_date=start_date),
            id=SCHEDULE_JOB_ID,
            replace_existing=True,
            max_instances=1,
            coalesce=True,
            misfire_grace_time=30,
        )
        _set_schedule_state(next_run=_get_next_run_iso())
    else:
        _set_schedule_state(next_run=None)


def _schedule_response():
    with app.state.schedule_lock:
        last_run = app.state.schedule_last_run
        next_run = app.state.schedule_next_run
    schedule = app.state.schedule_config
    return {
        "schema_version": SCHEDULE_SCHEMA_VERSION,
        "server_time": datetime.now(timezone.utc).isoformat(),
        "schedule": schedule,
        "enabled": schedule.get("enabled", False),
        "last_run": last_run,
        "next_run": next_run,
    }


def _parse_iso(value):
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed


def _format_iso(dt):
    if not dt:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat()


def _parse_hhmm(value):
    if not value:
        return None
    value = value.strip()
    if not value or ":" not in value:
        return None
    parts = value.split(":", 1)
    if len(parts) != 2:
        return None
    if not parts[0].isdigit() or not parts[1].isdigit():
        return None
    hour = int(parts[0])
    minute = int(parts[1])
    if hour < 0 or hour > 23 or minute < 0 or minute > 59:
        return None
    return hour, minute


def _resolve_timezone(value, fallback_tzinfo):
    if not value or value.lower() in {"local", "system"}:
        return fallback_tzinfo or timezone.utc
    if value.upper() == "UTC":
        return timezone.utc
    try:
        return ZoneInfo(value)
    except Exception:
        logging.warning("Invalid watch_policy timezone %s; falling back to local", value)
        return fallback_tzinfo or timezone.utc


def in_downtime(now, start_str, end_str):
    start = _parse_hhmm(start_str)
    end = _parse_hhmm(end_str)
    if not start or not end:
        return False, None
    # Compare in the same timezone; handles windows that cross midnight.
    start_dt = now.replace(hour=start[0], minute=start[1], second=0, microsecond=0)
    end_dt = now.replace(hour=end[0], minute=end[1], second=0, microsecond=0)
    if start_dt <= end_dt:
        in_window = start_dt <= now < end_dt
        next_allowed = end_dt if in_window else None
        return in_window, next_allowed
    # Window crosses midnight.
    if now >= start_dt:
        return True, end_dt + timedelta(days=1)
    if now < end_dt:
        return True, end_dt
    return False, None


def _check_downtime(config, *, now=None):
    policy = normalize_watch_policy(config)
    downtime = policy.get("downtime") or {}
    if not downtime.get("enabled"):
        return False, None
    if now is None:
        local_now = datetime.now().astimezone()
    else:
        local_now = now
    tzinfo = _resolve_timezone(downtime.get("timezone"), local_now.tzinfo)
    now = local_now.astimezone(tzinfo)
    return in_downtime(now, downtime.get("start"), downtime.get("end"))


def _deferred_run_tick(payload):
    loop = app.state.loop
    if not loop or loop.is_closed():
        return
    asyncio.run_coroutine_threadsafe(_handle_deferred_run(payload), loop)


async def _handle_deferred_run(payload):
    if app.state.running:
        delay = datetime.now(timezone.utc) + timedelta(minutes=1)
        scheduler = app.state.scheduler
        if scheduler:
            scheduler.add_job(
                _deferred_run_tick,
                trigger=DateTrigger(run_date=delay),
                args=[payload],
                id=f"{DEFERRED_RUN_JOB_ID}_{uuid4()}",
                replace_existing=False,
                max_instances=1,
                misfire_grace_time=30,
            )
        return
    config = payload.get("config") if isinstance(payload, dict) else None
    if config:
        in_dt, next_allowed = _check_downtime(config)
        if in_dt and next_allowed:
            _schedule_deferred_run(payload, next_allowed)
            return
    await _start_run_with_config(**payload, skip_downtime=True)


def _schedule_deferred_run(payload, next_allowed):
    scheduler = app.state.scheduler
    if not scheduler or not next_allowed:
        return
    scheduler.add_job(
        _deferred_run_tick,
        trigger=DateTrigger(run_date=next_allowed),
        args=[payload],
        id=f"{DEFERRED_RUN_JOB_ID}_{uuid4()}",
        replace_existing=False,
        max_instances=1,
        misfire_grace_time=30,
    )


def _read_watch_state(db_path):
    rows = {}
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute(
        "SELECT playlist_id, last_checked_at, next_poll_at, idle_count, "
        "current_interval_min, consecutive_no_change, last_change_at, skip_reason, "
        "last_error, last_error_at "
        "FROM playlist_watch"
    )
    for (
        playlist_id,
        last_checked_at,
        next_poll_at,
        idle_count,
        interval,
        consecutive,
        last_change_at,
        skip_reason,
        last_error,
        last_error_at,
    ) in cur.fetchall():
        effective_consecutive = consecutive if consecutive is not None else (idle_count or 0)
        rows[playlist_id] = {
            "last_checked_at": last_checked_at,
            "next_poll_at": next_poll_at,
            "idle_count": idle_count or 0,
            "current_interval_min": interval,
            "consecutive_no_change": effective_consecutive or 0,
            "last_change_at": last_change_at,
            "skip_reason": skip_reason,
            "last_error": last_error,
            "last_error_at": last_error_at,
        }
    conn.close()
    return rows


def _write_watch_state(
    db_path,
    playlist_id,
    *,
    last_checked_at=None,
    next_poll_at=None,
    idle_count=None,
    current_interval_min=None,
    consecutive_no_change=None,
    last_change_at=None,
    skip_reason=None,
    last_error=None,
    last_error_at=None,
):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO playlist_watch ("
        "playlist_id, last_checked_at, next_poll_at, idle_count, "
        "current_interval_min, consecutive_no_change, last_change_at, skip_reason, "
        "last_error, last_error_at"
        ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
        "ON CONFLICT(playlist_id) DO UPDATE SET "
        "last_checked_at=excluded.last_checked_at, next_poll_at=excluded.next_poll_at, "
        "idle_count=excluded.idle_count, current_interval_min=excluded.current_interval_min, "
        "consecutive_no_change=excluded.consecutive_no_change, last_change_at=excluded.last_change_at, "
        "skip_reason=excluded.skip_reason, last_error=excluded.last_error, "
        "last_error_at=excluded.last_error_at",
        (
            playlist_id,
            last_checked_at,
            next_poll_at,
            idle_count,
            current_interval_min,
            consecutive_no_change,
            last_change_at,
            skip_reason,
            last_error,
            last_error_at,
        ),
    )
    conn.commit()
    conn.close()


def _playlist_label(playlist_id, playlist_name):
    label = playlist_name or playlist_id
    return label if label else "unknown"


def _log_skip_reason(playlist_id, reason, watch, *, next_check=None):
    prev = watch.get("skip_reason")
    if reason != prev:
        if next_check:
            logging.debug("Watcher skipped (%s) playlist_id=%s next_check=%s", reason, playlist_id, next_check)
        else:
            logging.debug("Watcher skipped (%s) playlist_id=%s", reason, playlist_id)
    return reason


async def _poll_single_playlist(config, now, policy, pl, watch, yt_clients, batch_state):
    playlist_key = pl.get("playlist_id") or pl.get("id")
    if not playlist_key:
        return
    playlist_id = extract_playlist_id(playlist_key) or playlist_key
    playlist_name = pl.get("name") or ""

    min_interval = policy.get("min_interval_minutes") or 5
    max_interval = policy.get("max_interval_minutes") or min_interval
    backoff_factor = policy.get("idle_backoff_factor") or 2
    active_reset = policy.get("active_reset_minutes") or min_interval
    active_interval = max(min_interval, min(active_reset, max_interval))

    consecutive_no_change = watch.get("consecutive_no_change") or 0
    current_interval = watch.get("current_interval_min")
    if not isinstance(current_interval, int) or current_interval < min_interval:
        current_interval = min_interval

    if watch.get("current_interval_min") is None:
        _write_watch_state(
            app.state.paths.db_path,
            playlist_id,
            last_checked_at=watch.get("last_checked_at"),
            next_poll_at=watch.get("next_poll_at"),
            idle_count=consecutive_no_change,
            current_interval_min=current_interval,
            consecutive_no_change=consecutive_no_change,
            last_change_at=watch.get("last_change_at"),
            skip_reason=watch.get("skip_reason"),
            last_error=watch.get("last_error"),
            last_error_at=watch.get("last_error_at"),
        )

    account = pl.get("account")
    yt = yt_clients.get(account) if account else None
    if not yt:
        skip_reason = _log_skip_reason(playlist_id, "oauth missing", watch)
        error_at = _format_iso(now)
        consecutive_no_change += 1
        current_interval = min(current_interval * backoff_factor, max_interval)
        logging.debug(
            "Watcher backoff applied playlist_id=%s interval_min=%s no_change=%s",
            playlist_id,
            current_interval,
            consecutive_no_change,
        )
        _write_watch_state(
            app.state.paths.db_path,
            playlist_id,
            last_checked_at=watch.get("last_checked_at"),
            next_poll_at=_format_iso(now + timedelta(minutes=current_interval)),
            idle_count=consecutive_no_change,
            current_interval_min=current_interval,
            consecutive_no_change=consecutive_no_change,
            last_change_at=watch.get("last_change_at"),
            skip_reason=skip_reason,
            last_error="oauth missing",
            last_error_at=error_at,
        )
        return

    try:
        videos = get_playlist_videos(yt, playlist_id)
    except RefreshError as exc:
        logging.error("Watcher poll error playlist_id=%s error=%s", playlist_id, exc)
        skip_reason = _log_skip_reason(playlist_id, "poll error", watch)
        error_at = _format_iso(now)
        consecutive_no_change += 1
        current_interval = min(current_interval * backoff_factor, max_interval)
        logging.debug(
            "Watcher backoff applied playlist_id=%s interval_min=%s no_change=%s",
            playlist_id,
            current_interval,
            consecutive_no_change,
        )
        _write_watch_state(
            app.state.paths.db_path,
            playlist_id,
            last_checked_at=_format_iso(now),
            next_poll_at=_format_iso(now + timedelta(minutes=current_interval)),
            idle_count=consecutive_no_change,
            current_interval_min=current_interval,
            consecutive_no_change=consecutive_no_change,
            last_change_at=watch.get("last_change_at"),
            skip_reason=skip_reason,
            last_error=f"api error: {exc}",
            last_error_at=error_at,
        )
        return
    except HttpError as exc:
        logging.error("Watcher poll error playlist_id=%s error=%s", playlist_id, exc)
        skip_reason = _log_skip_reason(playlist_id, "poll error", watch)
        error_at = _format_iso(now)
        consecutive_no_change += 1
        current_interval = min(current_interval * backoff_factor, max_interval)
        logging.debug(
            "Watcher backoff applied playlist_id=%s interval_min=%s no_change=%s",
            playlist_id,
            current_interval,
            consecutive_no_change,
        )
        _write_watch_state(
            app.state.paths.db_path,
            playlist_id,
            last_checked_at=_format_iso(now),
            next_poll_at=_format_iso(now + timedelta(minutes=current_interval)),
            idle_count=consecutive_no_change,
            current_interval_min=current_interval,
            consecutive_no_change=consecutive_no_change,
            last_change_at=watch.get("last_change_at"),
            skip_reason=skip_reason,
            last_error=f"api error: {exc}",
            last_error_at=error_at,
        )
        return

    if not videos:
        logging.debug("Watcher polled playlist_id=%s items=0", playlist_id)
        consecutive_no_change += 1
        current_interval = min(current_interval * backoff_factor, max_interval)
        logging.debug(
            "Watcher backoff applied playlist_id=%s interval_min=%s no_change=%s",
            playlist_id,
            current_interval,
            consecutive_no_change,
        )
        _write_watch_state(
            app.state.paths.db_path,
            playlist_id,
            last_checked_at=_format_iso(now),
            next_poll_at=_format_iso(now + timedelta(minutes=current_interval)),
            idle_count=consecutive_no_change,
            current_interval_min=current_interval,
            consecutive_no_change=consecutive_no_change,
            last_change_at=watch.get("last_change_at"),
            skip_reason=None,
            last_error=watch.get("last_error"),
            last_error_at=watch.get("last_error_at"),
        )
        return

    if any("position" in v or "playlist_index" in v for v in videos):
        videos = sorted(
            videos,
            key=lambda v: v.get("position") or v.get("playlist_index") or 0,
            reverse=True,
        )
    else:
        videos = list(reversed(videos))

    playlist_mode = (pl.get("mode") or "full").lower()
    if playlist_mode not in {"full", "subscribe"}:
        playlist_mode = "full"
    subscribe_mode = playlist_mode == "subscribe"
    label = _playlist_label(playlist_id, playlist_name)
    logging.debug("Watcher polled playlist_id=%s items=%s", playlist_id, len(videos))

    with sqlite3.connect(app.state.paths.db_path) as conn:
        if subscribe_mode and not playlist_has_seen(conn, playlist_id):
            # First watcher pass for subscribe mode marks items as seen without downloading.
            for entry in videos:
                vid = entry.get("videoId") or entry.get("id")
                if not vid:
                    continue
                mark_video_seen(conn, playlist_id, vid, downloaded=False)
            conn.commit()
            logging.debug(
                'Watcher subscribe first run playlist_id=%s label="%s" seen=%d download=0',
                playlist_id,
                label,
                len(videos),
            )
            consecutive_no_change = 0
            current_interval = active_interval
            _write_watch_state(
                app.state.paths.db_path,
                playlist_id,
                last_checked_at=_format_iso(now),
                next_poll_at=_format_iso(now + timedelta(minutes=current_interval)),
                idle_count=consecutive_no_change,
                current_interval_min=current_interval,
                consecutive_no_change=consecutive_no_change,
                last_change_at=watch.get("last_change_at"),
                skip_reason=None,
                last_error=watch.get("last_error"),
                last_error_at=watch.get("last_error_at"),
            )
            return

    new_ids = []
    with sqlite3.connect(app.state.paths.db_path) as conn:
        if subscribe_mode:
            for entry in videos:
                vid = entry.get("videoId") or entry.get("id")
                if not vid:
                    continue
                if is_video_seen(conn, playlist_id, vid):
                    break
                new_ids.append(vid)
        else:
            for entry in videos:
                vid = entry.get("videoId") or entry.get("id")
                if not vid:
                    continue
                if not is_video_downloaded(conn, vid):
                    new_ids.append(vid)

    if new_ids:
        pending = batch_state["pending_playlists"]
        pending.add(playlist_id)
        batch_state["last_detection_ts"] = time.monotonic()
        logging.info(
            "Watcher: detected updates playlist_id=%s pending=%s",
            playlist_id,
            len(pending),
        )
        current_interval = active_interval
        consecutive_no_change = 0
        last_change_at = _format_iso(now)
        next_poll_at = now + timedelta(minutes=current_interval)
        _write_watch_state(
            app.state.paths.db_path,
            playlist_id,
            last_checked_at=_format_iso(now),
            next_poll_at=_format_iso(next_poll_at),
            idle_count=consecutive_no_change,
            current_interval_min=current_interval,
            consecutive_no_change=consecutive_no_change,
            last_change_at=last_change_at,
            skip_reason=None,
            last_error=watch.get("last_error"),
            last_error_at=watch.get("last_error_at"),
        )
        return

    consecutive_no_change += 1
    current_interval = min(current_interval * backoff_factor, max_interval)
    logging.debug(
        "Watcher backoff applied playlist_id=%s interval_min=%s no_change=%s",
        playlist_id,
        current_interval,
        consecutive_no_change,
    )
    _write_watch_state(
        app.state.paths.db_path,
        playlist_id,
        last_checked_at=_format_iso(now),
        next_poll_at=_format_iso(now + timedelta(minutes=current_interval)),
        idle_count=consecutive_no_change,
        current_interval_min=current_interval,
        consecutive_no_change=consecutive_no_change,
        last_change_at=watch.get("last_change_at"),
        skip_reason=None,
        last_error=watch.get("last_error"),
        last_error_at=watch.get("last_error_at"),
    )


async def _watcher_supervisor():
    # Watcher = adaptive per-playlist monitoring that triggers runs when changes are detected.
    logging.info("Watcher started")
    _set_watcher_status("idle")
    startup_logged = False
    last_candidate_state = None
    batch_state = {
        "pending_playlists": set(),
        "last_detection_ts": None,
        "batch_active": False,
    }
    while True:
        if getattr(app.state, "watcher_lock", None) is None:
            _set_watcher_status("disabled", batch_active=False, pending_playlists_count=0, quiet_window_remaining_sec=None)
            return
        config = _read_config_for_watcher()
        if not config:
            _set_watcher_status("idle", next_poll_ts=None, pending_playlists_count=0, quiet_window_remaining_sec=None, batch_active=False)
            await asyncio.sleep(60)
            continue

        policy = normalize_watch_policy(config)
        downtime = policy.get("downtime") or {}
        local_now = datetime.now().astimezone()
        tzinfo = _resolve_timezone(downtime.get("timezone"), local_now.tzinfo)
        now = local_now.astimezone(tzinfo)

        downtime_active = False
        if downtime.get("enabled"):
            downtime_active, _next_allowed = in_downtime(
                now,
                downtime.get("start"),
                downtime.get("end"),
            )
            if downtime_active and not app.state.was_in_downtime:
                logging.info("Watcher entering downtime window")
            if not downtime_active and app.state.was_in_downtime:
                logging.info("Watcher exiting downtime window")
            if downtime_active:
                app.state.was_in_downtime = True

        playlists = config.get("playlists") or []
        if not playlists:
            if last_candidate_state != "no_playlists":
                logging.info("Watcher: no candidates (playlists=0)")
                last_candidate_state = "no_playlists"
            if not startup_logged:
                logging.info(
                    "Watcher startup diag: playlists=0 downtime_enabled=%s downtime_active=%s "
                    "timezone=%s next_poll_at=%s delta_sec=%s lock=%s",
                    bool(downtime.get("enabled")),
                    bool(downtime_active),
                    downtime.get("timezone") or "local",
                    None,
                    None,
                    bool(getattr(app.state, "watcher_lock", None)),
                )
                startup_logged = True
            _set_watcher_status("idle", next_poll_ts=None, pending_playlists_count=0, quiet_window_remaining_sec=None, batch_active=False)
            await asyncio.sleep(60)
            continue
        playlist_map = {}
        for pl in playlists:
            playlist_key = pl.get("playlist_id") or pl.get("id")
            if not playlist_key:
                continue
            playlist_id = extract_playlist_id(playlist_key) or playlist_key
            playlist_map[playlist_id] = pl

        pending_count = len(batch_state["pending_playlists"])
        quiet_remaining = None
        if batch_state["last_detection_ts"] is not None and not batch_state["batch_active"]:
            quiet_remaining = max(
                0,
                int(WATCHER_QUIET_WINDOW_SECONDS - (time.monotonic() - batch_state["last_detection_ts"])),
            )
        if (pending_count and batch_state["last_detection_ts"] is not None
                and not batch_state["batch_active"]):
            elapsed = time.monotonic() - batch_state["last_detection_ts"]
            if elapsed >= WATCHER_QUIET_WINDOW_SECONDS:
                _set_watcher_status(
                    "batch_ready",
                    pending_playlists_count=pending_count,
                    quiet_window_remaining_sec=0,
                    batch_active=False,
                )
                logging.info(
                    "Watcher: quiet window elapsed (%ss), preparing batch run",
                    WATCHER_QUIET_WINDOW_SECONDS,
                )
                batch_state["batch_active"] = True
                batch_playlists = list(batch_state["pending_playlists"])
                batch_state["pending_playlists"].clear()
                _set_watcher_status(
                    "running_batch",
                    pending_playlists_count=len(batch_playlists),
                    quiet_window_remaining_sec=None,
                    batch_active=True,
                )
                logging.info(
                    "Watcher: starting batch run playlists=%s",
                    ",".join(batch_playlists),
                )
                batch_start = time.monotonic()
                total_downloaded = 0
                for playlist_id in batch_playlists:
                    pl = playlist_map.get(playlist_id)
                    if not pl:
                        logging.warning("Watcher: batch playlist missing config playlist_id=%s", playlist_id)
                        continue
                    logging.info("Watcher: batch downloading playlist_id=%s", playlist_id)
                    result, _next_allowed = await _start_run_with_config(
                        config,
                        playlist_id=playlist_id,
                        playlist_account=pl.get("account"),
                        playlist_mode=(pl.get("mode") or "full"),
                        destination=pl.get("folder") or pl.get("directory"),
                        final_format_override=pl.get("final_format"),
                        music_mode=bool(pl.get("music_mode")),
                        run_source="watcher",
                        now=now,
                    )
                    if result == "started":
                        if app.state.run_task:
                            await app.state.run_task
                        status = app.state.status
                        if status:
                            total_downloaded += len(status.run_successes or [])
                    elif result == "deferred":
                        logging.info("Watcher: batch deferred playlist_id=%s", playlist_id)
                    else:
                        logging.debug("Watcher: batch skipped (run active) playlist_id=%s", playlist_id)
                duration_seconds = max(0, int(time.monotonic() - batch_start))
                logging.info(
                    "Watcher: batch complete playlists=%s videos=%s",
                    len(batch_playlists),
                    total_downloaded,
                )
                if batch_playlists:
                    minutes = duration_seconds // 60
                    seconds = duration_seconds % 60
                    duration_label = f"{minutes}m {seconds}s" if minutes else f"{seconds}s"
                    msg = (
                        "YouTube Archiver Watcher Batch\n"
                        f"Playlists: {len(batch_playlists)}\n"
                        f"Videos downloaded: {total_downloaded}\n"
                        f"Duration: {duration_label}"
                    )
                    telegram_notify(config, msg)
                batch_state["batch_active"] = False
                batch_state["last_detection_ts"] = None
                _set_watcher_status("idle", pending_playlists_count=0, quiet_window_remaining_sec=None, batch_active=False)
                logging.info("Watcher: batch state reset, resuming monitoring")
                continue
            _set_watcher_status(
                "waiting_quiet_window",
                pending_playlists_count=pending_count,
                quiet_window_remaining_sec=quiet_remaining,
                batch_active=False,
            )
        else:
            _set_watcher_status("idle", pending_playlists_count=pending_count, quiet_window_remaining_sec=None, batch_active=batch_state["batch_active"])

        if downtime.get("enabled") and downtime_active:
            await asyncio.sleep(60)
            continue

        watch_state = _read_watch_state(app.state.paths.db_path)
        if app.state.was_in_downtime:
            app.state.was_in_downtime = False
            for pl in playlists:
                playlist_key = pl.get("playlist_id") or pl.get("id")
                if not playlist_key:
                    continue
                playlist_id = extract_playlist_id(playlist_key) or playlist_key
                watch = watch_state.get(playlist_id) or {}
                consecutive_no_change = watch.get("consecutive_no_change") or 0
                current_interval = watch.get("current_interval_min")
                if not isinstance(current_interval, int):
                    current_interval = policy.get("min_interval_minutes") or 5
                _write_watch_state(
                    app.state.paths.db_path,
                    playlist_id,
                    last_checked_at=watch.get("last_checked_at"),
                    next_poll_at=_format_iso(now),
                    idle_count=consecutive_no_change,
                    current_interval_min=current_interval,
                    consecutive_no_change=consecutive_no_change,
                    last_change_at=watch.get("last_change_at"),
                    skip_reason=watch.get("skip_reason"),
                    last_error=watch.get("last_error"),
                    last_error_at=watch.get("last_error_at"),
                )
            watch_state = _read_watch_state(app.state.paths.db_path)

        candidates = []
        for pl in playlists:
            playlist_id = extract_playlist_id(pl.get("playlist_id") or pl.get("id")) or pl.get("playlist_id") or pl.get("id")
            watch = watch_state.get(playlist_id) or {}
            next_poll_at = _parse_iso(watch.get("next_poll_at")) or now
            candidates.append((next_poll_at, pl, watch))

        if not candidates:
            if last_candidate_state != "no_watch_state":
                logging.info("Watcher: no candidates (watch_state=0)")
                last_candidate_state = "no_watch_state"
            if not startup_logged:
                logging.info(
                    "Watcher startup diag: playlists=%s downtime_enabled=%s downtime_active=%s "
                    "timezone=%s next_poll_at=%s delta_sec=%s lock=%s",
                    len(playlists),
                    bool(downtime.get("enabled")),
                    bool(downtime_active),
                    downtime.get("timezone") or "local",
                    None,
                    None,
                    bool(getattr(app.state, "watcher_lock", None)),
                )
                startup_logged = True
            await asyncio.sleep(60)
            continue

        candidates.sort(key=lambda item: item[0])
        next_poll_at, pl, watch = candidates[0]
        if last_candidate_state != "available":
            logging.info("Watcher: candidates available")
            last_candidate_state = "available"
        delta_seconds = int((next_poll_at - now).total_seconds())
        if not startup_logged:
            logging.info(
                "Watcher startup diag: playlists=%s downtime_enabled=%s downtime_active=%s "
                "timezone=%s next_poll_at=%s delta_sec=%s lock=%s",
                len(playlists),
                bool(downtime.get("enabled")),
                bool(downtime_active),
                downtime.get("timezone") or "local",
                _format_iso(next_poll_at),
                delta_seconds,
                bool(getattr(app.state, "watcher_lock", None)),
            )
            startup_logged = True
        min_interval_minutes = policy.get("min_interval_minutes") or 5
        interval_seconds = max(60, int(min_interval_minutes * 60))
        max_sleep_seconds = max(interval_seconds * 3, 900)
        if delta_seconds > max_sleep_seconds:
            clamped = now + timedelta(seconds=min(30, interval_seconds))
            logging.warning(
                "Watcher: next_poll_at skew detected; clamping from %s to %s (delta=%s)",
                _format_iso(next_poll_at),
                _format_iso(clamped),
                delta_seconds,
            )
            playlist_key = pl.get("playlist_id") or pl.get("id")
            playlist_id = extract_playlist_id(playlist_key) or playlist_key
            consecutive_no_change = watch.get("consecutive_no_change") or 0
            current_interval = watch.get("current_interval_min")
            if not isinstance(current_interval, int):
                current_interval = min_interval_minutes
            _write_watch_state(
                app.state.paths.db_path,
                playlist_id,
                last_checked_at=watch.get("last_checked_at"),
                next_poll_at=_format_iso(clamped),
                idle_count=watch.get("idle_count") or 0,
                current_interval_min=current_interval,
                consecutive_no_change=consecutive_no_change,
                last_change_at=watch.get("last_change_at"),
                skip_reason=watch.get("skip_reason"),
                last_error=watch.get("last_error"),
                last_error_at=watch.get("last_error_at"),
            )
            next_poll_at = clamped
        _set_watcher_status(
            state=getattr(app.state, "watcher_status", {}).get("state") or "idle",
            next_poll_ts=_format_iso(next_poll_at),
        )
        if now < next_poll_at:
            sleep_seconds = max(0.0, (next_poll_at - now).total_seconds())
            if (batch_state["pending_playlists"] and batch_state["last_detection_ts"] is not None
                    and not batch_state["batch_active"]):
                quiet_remaining = WATCHER_QUIET_WINDOW_SECONDS - (time.monotonic() - batch_state["last_detection_ts"])
                if quiet_remaining > 0:
                    sleep_seconds = min(sleep_seconds, quiet_remaining)
                else:
                    sleep_seconds = 0
            if sleep_seconds > 0:
                await asyncio.sleep(sleep_seconds)
            continue

        accounts = config.get("accounts") or {}
        refresh_log_state = set()
        yt_clients = (
            build_youtube_clients(
                accounts,
                config,
                cache=getattr(app.state, "watcher_clients_cache", {}),
                refresh_log_state=refresh_log_state,
            )
            if accounts
            else {}
        )
        pending_before = len(batch_state["pending_playlists"])
        _set_watcher_status(
            "polling",
            last_poll_ts=_format_iso(now),
            pending_playlists_count=pending_before,
            quiet_window_remaining_sec=None,
            batch_active=batch_state["batch_active"],
        )
        await _poll_single_playlist(config, now, policy, pl, watch, yt_clients, batch_state)
        pending_after = len(batch_state["pending_playlists"])
        if pending_after > pending_before:
            logging.info("Watcher poll complete: updates detected")
        else:
            logging.info("Watcher poll complete: no updates detected")
        if batch_state["last_detection_ts"] is not None and not batch_state["batch_active"]:
            quiet_remaining = max(
                0,
                int(WATCHER_QUIET_WINDOW_SECONDS - (time.monotonic() - batch_state["last_detection_ts"])),
            )
            _set_watcher_status(
                "waiting_quiet_window",
                pending_playlists_count=pending_after,
                quiet_window_remaining_sec=quiet_remaining,
                batch_active=False,
            )
        else:
            _set_watcher_status(
                "idle",
                pending_playlists_count=pending_after,
                quiet_window_remaining_sec=None,
                batch_active=batch_state["batch_active"],
            )


def _apply_watch_policy(policy):
    if getattr(app.state, "watcher_lock", None) is None:
        return
    # Supervisor loop reads watch_policy each iteration.
    app.state.watch_policy = policy


def _set_watcher_status(state=None, **fields):
    status = getattr(app.state, "watcher_status", None)
    if status is None:
        status = {}
        app.state.watcher_status = status
    prev_state = status.get("state")
    if state and state != prev_state:
        status["state"] = state
        if state == "polling":
            logging.info("Watcher state → polling")
        elif state == "waiting_quiet_window":
            remaining = fields.get("quiet_window_remaining_sec")
            if remaining is None:
                remaining = WATCHER_QUIET_WINDOW_SECONDS
            logging.info("Watcher state → waiting_quiet_window (%ss)", remaining)
        elif state == "batch_ready":
            logging.info("Watcher state → batch_ready")
        elif state == "running_batch":
            count = fields.get("pending_playlists_count")
            if count is None:
                count = 0
            logging.info("Watcher state → running_batch playlists=%s", count)
        elif state == "disabled":
            logging.info("Watcher state → disabled")
        else:
            logging.info("Watcher state → %s", state)
    for key, value in fields.items():
        status[key] = value


@app.get("/api/status")
async def api_status():
    status = get_status(app.state.status)
    last_path = status.pop("last_completed_path", None)
    status["last_completed_file_id"] = _file_id_from_path(last_path) if last_path else None
    watcher_errors = []
    try:
        watch_state = _read_watch_state(app.state.paths.db_path)
        for playlist_id, entry in watch_state.items():
            last_error = entry.get("last_error")
            if last_error:
                watcher_errors.append({
                    "playlist_id": playlist_id,
                    "last_error": last_error,
                    "last_error_at": entry.get("last_error_at"),
                })
    except Exception:
        logging.exception("Failed to read watcher error state")
    watcher_policy = getattr(app.state, "watch_policy", _default_watch_policy())
    watcher_downtime = watcher_policy.get("downtime") or {}
    local_now = datetime.now().astimezone()
    tzinfo = _resolve_timezone(watcher_downtime.get("timezone"), local_now.tzinfo)
    now = local_now.astimezone(tzinfo)
    paused = False
    if watcher_downtime.get("enabled"):
        paused, _ = in_downtime(now, watcher_downtime.get("start"), watcher_downtime.get("end"))
    watcher_status = dict(getattr(app.state, "watcher_status", {}) or {})
    if not bool(getattr(app.state, "watcher_lock", None)):
        watcher_status["state"] = "disabled"
    return {
        "schema_version": STATUS_SCHEMA_VERSION,
        "server_time": datetime.now(timezone.utc).isoformat(),
        "state": app.state.state,
        "running": app.state.running,
        "run_id": app.state.run_id,
        "started_at": app.state.started_at,
        "finished_at": app.state.finished_at,
        "error": app.state.last_error,
        "watcher": {
            "enabled": bool(getattr(app.state, "watcher_lock", None)),
            "paused": bool(paused),
        },
        "watcher_status": watcher_status,
        "scheduler": {
            "enabled": bool((app.state.schedule_config or {}).get("enabled", False)),
        },
        "watcher_errors": watcher_errors,
        "status": status,
    }


@app.get("/api/schedule")
async def api_get_schedule():
    return _schedule_response()


@app.post("/api/schedule")
async def api_update_schedule(payload: ScheduleRequest):
    config = _read_config_or_404()
    current = _merge_schedule_config(config.get("schedule"))
    updates = payload.dict(exclude_unset=True)
    current.update(updates)
    errors = _validate_schedule_config(current)
    if errors:
        raise HTTPException(status_code=400, detail={"errors": errors})
    config["schedule"] = current

    config_path = app.state.config_path
    config_dir = os.path.dirname(config_path) or "."
    os.makedirs(config_dir, exist_ok=True)

    tmp = tempfile.NamedTemporaryFile("w", delete=False, dir=config_dir)
    try:
        json.dump(config, tmp, indent=4)
        tmp.flush()
        os.fsync(tmp.fileno())
        tmp.close()
        os.replace(tmp.name, config_path)
    finally:
        try:
            tmp.close()
        except Exception:
            pass
        if os.path.exists(tmp.name):
            try:
                os.unlink(tmp.name)
            except Exception:
                pass

    app.state.schedule_config = current
    _apply_schedule_config(current)
    return _schedule_response()


@app.get("/api/metrics")
async def api_metrics():
    files_count, bytes_count = _downloads_metrics(DOWNLOADS_DIR)
    disk = _disk_usage(DOWNLOADS_DIR)
    return {
        "schema_version": METRICS_SCHEMA_VERSION,
        "server_time": datetime.now(timezone.utc).isoformat(),
        "downloads_dir": DOWNLOADS_DIR,
        "downloads_files": files_count,
        "downloads_bytes": bytes_count,
        "disk_total_bytes": disk["total_bytes"],
        "disk_free_bytes": disk["free_bytes"],
        "disk_used_bytes": disk["used_bytes"],
        "disk_free_percent": disk["free_percent"],
    }


@app.get("/api/version")
async def api_version():
    return get_runtime_info()


@app.post("/api/yt-dlp/update")
async def api_update_ytdlp():
    script_path = _yt_dlp_script_path()
    if not os.path.exists(script_path):
        raise HTTPException(status_code=404, detail="update_yt_dlp.sh not found")

    with app.state.ytdlp_update_lock:
        if app.state.ytdlp_update_running:
            raise HTTPException(status_code=409, detail="yt-dlp update already running")
        app.state.ytdlp_update_running = True

    def _run_update():
        try:
            logging.info("yt-dlp update started")
            subprocess.run(["bash", script_path], check=False)
            logging.info("yt-dlp update finished")
        finally:
            app.state.ytdlp_update_running = False

    asyncio.create_task(anyio.to_thread.run_sync(_run_update))
    return {"status": "started"}


@app.get("/api/paths")
async def api_paths():
    return {
        "config_dir": CONFIG_DIR,
        "data_dir": DATA_DIR,
        "downloads_dir": DOWNLOADS_DIR,
        "log_dir": LOG_DIR,
        "tokens_dir": TOKENS_DIR,
        "browse_roots": app.state.browse_roots,
    }


@app.post("/api/oauth/start")
async def api_oauth_start(payload: OAuthStartRequest):
    _purge_oauth_sessions()
    try:
        client_secret_file = resolve_dir(payload.client_secret, TOKENS_DIR)
        token_file = resolve_dir(payload.token_out, TOKENS_DIR)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    ensure_dir(TOKENS_DIR)
    flow = InstalledAppFlow.from_client_secrets_file(client_secret_file, OAUTH_SCOPES)
    flow.redirect_uri = "urn:ietf:wg:oauth:2.0:oob"
    auth_url, _ = flow.authorization_url(access_type="offline", prompt="consent")
    session_id = uuid4().hex
    with _OAUTH_LOCK:
        _OAUTH_SESSIONS[session_id] = {
            "flow": flow,
            "token_file": token_file,
            "account": payload.account or "",
            "expires_at": datetime.now(timezone.utc) + OAUTH_SESSION_TTL,
        }
    return {"session_id": session_id, "auth_url": auth_url}


@app.post("/api/oauth/complete")
async def api_oauth_complete(payload: OAuthCompleteRequest):
    _purge_oauth_sessions()
    with _OAUTH_LOCK:
        session = _OAUTH_SESSIONS.pop(payload.session_id, None)
    if not session:
        raise HTTPException(status_code=404, detail="OAuth session not found or expired")
    code = (payload.code or "").strip()
    if not code:
        raise HTTPException(status_code=400, detail="Authorization code is required")
    flow = session["flow"]
    try:
        flow.fetch_token(code=code)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"OAuth failed: {exc}") from exc
    creds = flow.credentials
    token_file = session["token_file"]
    token_data = {
        "token": creds.token,
        "refresh_token": creds.refresh_token,
        "token_uri": creds.token_uri,
        "client_id": creds.client_id,
        "client_secret": creds.client_secret,
        "scopes": creds.scopes,
    }
    ensure_dir(os.path.dirname(token_file))
    with open(token_file, "w") as f:
        json.dump(token_data, f, indent=2)
    account = session.get("account") or "unknown"
    logging.info("OAuth token saved for account %s to %s", account, token_file)
    return {"status": "ok", "token_path": token_file}


@app.get("/api/config/path")
async def api_get_config_path():
    return {"path": app.state.config_path}


@app.put("/api/config/path")
async def api_put_config_path(payload: ConfigPathRequest):
    path = payload.path.strip()
    if not path:
        raise HTTPException(status_code=400, detail="Config path is required")
    try:
        target = resolve_config_path(path)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    if not os.path.exists(target):
        raise HTTPException(status_code=404, detail=f"Config not found: {target}")
    try:
        config = load_config(target)
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=400, detail=f"Invalid JSON in config: {exc}") from exc
    except OSError as exc:
        raise HTTPException(status_code=500, detail=f"Failed to read config: {exc}") from exc
    errors = validate_config(config)
    if errors:
        raise HTTPException(status_code=400, detail={"errors": errors})
    app.state.config_path = target
    return {"path": app.state.config_path}


@app.post("/api/run", status_code=202)
async def api_run(request: RunRequest):
    config = _read_config_or_404()
    if request.single_url and request.playlist_id:
        raise HTTPException(status_code=400, detail="Provide either single_url or playlist_id, not both")
    if request.playlist_account:
        accounts = (config.get("accounts") or {}) if isinstance(config, dict) else {}
        if request.playlist_account not in accounts:
            raise HTTPException(status_code=400, detail="playlist_account not found in config")
    logging.info(
        "Manual run requested (source=api) single_url=%s playlist_id=%s",
        bool(request.single_url),
        request.playlist_id or "-",
    )
    result, _next_allowed = await _start_run_with_config(
        config,
        single_url=request.single_url,
        playlist_id=request.playlist_id,
        playlist_account=request.playlist_account,
        destination=request.destination,
        final_format_override=request.final_format_override,
        js_runtime=request.js_runtime,
        music_mode=request.music_mode,
        run_source="api",
        skip_downtime=bool(request.single_url),
        delivery_mode=request.delivery_mode,
    )
    if result == "busy":
        raise HTTPException(status_code=409, detail="Archive run already in progress")
    return {"run_id": app.state.run_id, "status": "started"}


@app.post("/api/cancel")
async def api_cancel():
    logging.info("Cancel requested by user")
    if not app.state.running:
        return {"status": "idle"}
    app.state.cancel_requested = True
    app.state.stop_event.set()
    status = app.state.status
    if status:
        lock = getattr(status, "lock", None)
        if lock:
            with lock:
                status.last_error_message = "Downloads cancelled by user"
        else:
            status.last_error_message = "Downloads cancelled by user"
    return {"status": "cancelling"}


@app.get("/api/logs", response_class=PlainTextResponse)
async def api_logs(lines: int = Query(200, ge=1, le=5000)):
    return _tail_lines(app.state.log_path, lines)


@app.get("/api/config")
async def api_get_config():
    return _read_config_or_404()


@app.put("/api/config")
async def api_put_config(payload: dict = Body(...)):
    payload = _strip_deprecated_fields(payload)
    errors = validate_config(payload)
    if errors:
        raise HTTPException(status_code=400, detail={"errors": errors})

    config_path = app.state.config_path
    config_dir = os.path.dirname(config_path) or "."
    os.makedirs(config_dir, exist_ok=True)

    tmp = tempfile.NamedTemporaryFile("w", delete=False, dir=config_dir)
    try:
        json.dump(payload, tmp, indent=4)
        tmp.flush()
        os.fsync(tmp.fileno())
        tmp.close()
        os.replace(tmp.name, config_path)
    finally:
        try:
            tmp.close()
        except Exception:
            pass
        if os.path.exists(tmp.name):
            try:
                os.unlink(tmp.name)
            except Exception:
                pass

    if "schedule" in payload:
        schedule = _merge_schedule_config(payload.get("schedule"))
        app.state.schedule_config = schedule
        _apply_schedule_config(schedule)
    policy = normalize_watch_policy(payload)
    if getattr(normalize_watch_policy, "valid", True):
        app.state.watch_policy = policy
        _apply_watch_policy(policy)
        app.state.watch_config_cache = payload

    return {"status": "updated"}


@app.get("/api/history")
async def api_history(
    limit: int = Query(200, ge=1, le=5000),
    search: str | None = Query(None, max_length=200),
    playlist_id: str | None = Query(None, max_length=200),
    date_from: str | None = Query(None, max_length=32),
    date_to: str | None = Query(None, max_length=32),
    sort_by: str = Query("date", max_length=20),
    sort_dir: str = Query("desc", max_length=4),
):
    sort_by = (sort_by or "date").lower()
    sort_dir = (sort_dir or "desc").lower()
    if sort_by not in {"date", "title", "size"}:
        raise HTTPException(status_code=400, detail="sort_by must be date, title, or size")
    if sort_dir not in {"asc", "desc"}:
        raise HTTPException(status_code=400, detail="sort_dir must be asc or desc")
    search_value = search.strip() if search else None
    playlist_value = playlist_id.strip() if playlist_id else None
    rows = read_history(
        app.state.paths.db_path,
        limit=limit,
        search=search_value,
        playlist_id=playlist_value,
        date_from=_normalize_date(date_from, end_of_day=False),
        date_to=_normalize_date(date_to, end_of_day=True),
        sort_by=sort_by,
        sort_dir=sort_dir,
    )
    return [
        {
            "video_id": row[0],
            "playlist_id": row[1],
            "downloaded_at": row[2],
            "filepath": row[3],
            "file_id": _file_id_from_path(row[3]),
        }
        for row in rows
    ]


@app.get("/api/files")
async def api_files():
    return _list_download_files(DOWNLOADS_DIR)


@app.get("/api/files/{file_id}/download")
async def api_file_download(file_id: str):
    try:
        rel = _decode_file_id(file_id)
    except (ValueError, UnicodeDecodeError, binascii.Error):
        raise HTTPException(status_code=400, detail="Invalid file id")

    candidate = os.path.abspath(os.path.join(DOWNLOADS_DIR, rel))
    if not _path_allowed(candidate, [DOWNLOADS_DIR]):
        raise HTTPException(status_code=403, detail="File not allowed")
    if not os.path.isfile(candidate):
        raise HTTPException(status_code=404, detail="File not found")

    filename = _safe_filename(os.path.basename(candidate))
    content_type, _ = mimetypes.guess_type(candidate)
    headers = {"Content-Disposition": f'attachment; filename="{filename}"'}
    return StreamingResponse(_iter_file(candidate), media_type=content_type or "application/octet-stream", headers=headers)


@app.get("/api/deliveries/{delivery_id}/download")
async def api_delivery_download(delivery_id: str):
    entry = _acquire_client_delivery(delivery_id)
    if not entry:
        raise HTTPException(status_code=404, detail="Delivery not available")
    candidate = entry.get("path")
    if not candidate or not os.path.isfile(candidate):
        _mark_client_delivery(delivery_id, delivered=False)
        raise HTTPException(status_code=404, detail="Delivery file not found")

    filename = _safe_filename(entry.get("filename") or os.path.basename(candidate))
    content_type, _ = mimetypes.guess_type(candidate)
    headers = {"Content-Disposition": f'attachment; filename="{filename}"'}
    logging.info("HTTP client download started delivery_id=%s", delivery_id)

    def stream():
        completed = False
        try:
            with open(candidate, "rb") as f:
                while True:
                    chunk = f.read(1024 * 1024)
                    if not chunk:
                        break
                    yield chunk
            completed = True
        except Exception:
            logging.exception("Client delivery stream failed delivery_id=%s", delivery_id)
            raise
        finally:
            _mark_client_delivery(delivery_id, delivered=completed)
            if completed:
                logging.info("HTTP client download complete → cleanup")
            else:
                logging.warning("HTTP client download incomplete → cleanup")
            _finalize_client_delivery(delivery_id, timeout=False)

    return StreamingResponse(stream(), media_type=content_type or "application/octet-stream", headers=headers)


@app.post("/api/cleanup")
async def api_cleanup():
    paths = app.state.paths
    deleted_files = 0
    deleted_bytes = 0
    results = {}
    for label, target in (
        ("temp_downloads", paths.temp_downloads_dir),
        ("ytdlp_temp", paths.ytdlp_temp_dir),
    ):
        files_count, bytes_count = _cleanup_dir(target)
        deleted_files += files_count
        deleted_bytes += bytes_count
        results[label] = {
            "path": target,
            "deleted_files": files_count,
            "deleted_bytes": bytes_count,
        }
    return {
        "deleted_files": deleted_files,
        "deleted_bytes": deleted_bytes,
        "details": results,
    }


@app.get("/api/browse")
async def api_browse(
    root: str = Query(..., description="downloads, config, or tokens"),
    path: str = Query("", description="Relative path within the root"),
    mode: str = Query("dir", description="dir or file"),
    ext: str = Query("", description="Optional file extension filter, e.g. .json"),
    limit: int | None = Query(None, ge=1, le=5000, description="Optional max entries"),
):
    root = (root or "").strip().lower()
    roots = app.state.browse_roots
    if root not in roots:
        raise HTTPException(status_code=400, detail="root must be downloads, config, or tokens")

    mode = mode.lower()
    if mode not in {"dir", "file"}:
        raise HTTPException(status_code=400, detail="mode must be dir or file")

    ext = ext.strip().lower()
    if ext and not ext.startswith("."):
        ext = f".{ext}"

    base = roots[root]
    rel_path, target = _resolve_browse_path(base, path)
    logging.info("Browse opened root=%s path=%s mode=%s", root, rel_path or "/", mode)
    if not os.path.exists(target):
        raise HTTPException(status_code=404, detail=f"Path not found: {target}")
    if os.path.isfile(target):
        target = os.path.dirname(target)
        rel_path = os.path.relpath(target, base)
        if rel_path == ".":
            rel_path = ""

    parent = None
    if rel_path:
        parent = os.path.dirname(rel_path)
        if parent == ".":
            parent = ""

    try:
        entries = _list_browse_entries(base, target, mode, ext, limit=limit)
    except OSError as exc:
        raise HTTPException(status_code=500, detail=f"Failed to read directory: {exc}") from exc

    return {
        "root": root,
        "path": rel_path,
        "abs_path": target,
        "parent": parent,
        "entries": entries,
    }


if os.path.isdir(WEBUI_DIR):
    app.mount("/", StaticFiles(directory=WEBUI_DIR, html=True), name="webui")


if __name__ == "__main__":
    import uvicorn

    host = _env_or_default("YT_ARCHIVER_HOST", "127.0.0.1")
    port = int(_env_or_default("YT_ARCHIVER_PORT", "8000"))
    uvicorn.run("api.main:app", host=host, port=port, reload=False)
