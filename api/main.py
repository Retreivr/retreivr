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
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import anyio
from fastapi import Body, FastAPI, HTTPException, Query, Request
from fastapi.responses import PlainTextResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from google_auth_oauthlib.flow import InstalledAppFlow
from pydantic import BaseModel
from uvicorn.middleware.proxy_headers import ProxyHeadersMiddleware
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger

from engine.core import EngineStatus, get_status, load_config, read_history, run_archive, validate_config
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
OAUTH_SCOPES = ["https://www.googleapis.com/auth/youtube.readonly"]
OAUTH_SESSION_TTL = timedelta(minutes=15)
_OAUTH_SESSIONS = {}
_OAUTH_LOCK = threading.Lock()

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


class RunRequest(BaseModel):
    single_url: str | None = None
    destination: str | None = None
    final_format_override: str | None = None
    js_runtime: str | None = None


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
    app.state.scheduler = BackgroundScheduler(timezone="UTC")
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
        raise HTTPException(status_code=400, detail="path must be relative")
    normalized = os.path.normpath(rel_path)
    if normalized in (".", os.curdir):
        normalized = ""
    if normalized.startswith(".."):
        raise HTTPException(status_code=403, detail="path not allowed")
    abs_path = os.path.realpath(os.path.join(root_base, normalized))
    base = os.path.realpath(root_base)
    if os.path.commonpath([abs_path, base]) != base:
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


def _merge_schedule_config(schedule):
    merged = _default_schedule_config()
    if isinstance(schedule, dict):
        for key in ("enabled", "mode", "interval_hours", "run_on_startup"):
            if key in schedule:
                merged[key] = schedule[key]
    return merged


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
    return config


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
    return config


async def _start_run_with_config(
    config,
    *,
    single_url=None,
    destination=None,
    final_format_override=None,
    js_runtime=None,
    run_source="api",
):
    async with app.state.run_lock:
        if app.state.running:
            return False

        app.state.running = True
        app.state.state = "running"
        app.state.run_id = str(uuid4())
        app.state.started_at = datetime.now(timezone.utc).isoformat()
        app.state.finished_at = None
        app.state.last_error = None
        status = EngineStatus()
        app.state.status = status
        app.state.stop_event = threading.Event()

        async def _runner():
            try:
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
                )
                await anyio.to_thread.run_sync(run_callable)
                if app.state.stop_event.is_set():
                    app.state.last_error = "Run stopped"
                    app.state.state = "error"
            except Exception as exc:
                logging.exception("Archive run failed: %s", exc)
                app.state.last_error = str(exc)
                app.state.state = "error"
            finally:
                app.state.running = False
                app.state.finished_at = datetime.now(timezone.utc).isoformat()
                if app.state.state == "running":
                    app.state.state = "idle"

        app.state.run_task = asyncio.create_task(_runner())

    return True


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
    started = await _start_run_with_config(config, run_source="scheduled")
    if started:
        now = datetime.now(timezone.utc).isoformat()
        _set_schedule_state(last_run=now, next_run=_get_next_run_iso())
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


@app.get("/api/status")
async def api_status():
    status = get_status(app.state.status)
    last_path = status.pop("last_completed_path", None)
    status["last_completed_file_id"] = _file_id_from_path(last_path) if last_path else None
    return {
        "schema_version": STATUS_SCHEMA_VERSION,
        "server_time": datetime.now(timezone.utc).isoformat(),
        "state": app.state.state,
        "running": app.state.running,
        "run_id": app.state.run_id,
        "started_at": app.state.started_at,
        "finished_at": app.state.finished_at,
        "error": app.state.last_error,
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
    started = await _start_run_with_config(
        config,
        single_url=request.single_url,
        destination=request.destination,
        final_format_override=request.final_format_override,
        js_runtime=request.js_runtime,
        run_source="api",
    )
    if not started:
        raise HTTPException(status_code=409, detail="Archive run already in progress")
    return {"run_id": app.state.run_id, "status": "started"}


@app.get("/api/logs", response_class=PlainTextResponse)
async def api_logs(lines: int = Query(200, ge=1, le=5000)):
    return _tail_lines(app.state.log_path, lines)


@app.get("/api/config")
async def api_get_config():
    return _read_config_or_404()


@app.put("/api/config")
async def api_put_config(payload: dict = Body(...)):
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
