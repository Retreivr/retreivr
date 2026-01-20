import json
import logging
import os
import re
import sqlite3
import threading
import time
import urllib.parse
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from threading import Thread
from uuid import uuid4

import requests
from google.auth.exceptions import RefreshError
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from yt_dlp import YoutubeDL

from engine.job_queue import (
    DownloadWorkerEngine,
    DownloadJobStore,
    build_output_template,
    ensure_download_jobs_table,
    resolve_media_intent,
    resolve_media_type,
    resolve_source,
)
from engine.paths import EnginePaths, TOKENS_DIR, resolve_dir

CLIENT_DELIVERY_TIMEOUT_SECONDS = 600

_GOOGLE_AUTH_RETRY = re.compile(r"Refreshing credentials due to a 401 response\. Attempt (\d+)/(\d+)\.")

_SPOTIFY_PLAYLIST_RE = re.compile(
    r"^(?:https?://open\.spotify\.com/playlist/|spotify:playlist:)([A-Za-z0-9]+)"
)


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
            mode = pl.get("mode")
            if mode is not None and mode not in {"full", "subscribe"}:
                errors.append(f"playlists[{idx}].mode must be 'full' or 'subscribe'")
            media_type = pl.get("media_type")
            if media_type is not None and media_type not in {"music", "audio", "video"}:
                errors.append(f"playlists[{idx}].media_type must be 'music', 'audio', or 'video'")

    spotify_playlists = config.get("spotify_playlists")
    if spotify_playlists is not None and not isinstance(spotify_playlists, list):
        errors.append("spotify_playlists must be a list")
    if isinstance(spotify_playlists, list):
        for idx, entry in enumerate(spotify_playlists):
            if not isinstance(entry, dict):
                errors.append(f"spotify_playlists[{idx}] must be an object")
                continue
            url = entry.get("playlist_url")
            if not url:
                errors.append(f"spotify_playlists[{idx}].playlist_url is required")
            elif not isinstance(url, str) or not _SPOTIFY_PLAYLIST_RE.match(url):
                errors.append(f"spotify_playlists[{idx}].playlist_url must be a Spotify playlist URL")
            name = entry.get("name")
            if name is not None and not isinstance(name, str):
                errors.append(f"spotify_playlists[{idx}].name must be a string")
            destination = entry.get("destination")
            if destination is not None and not isinstance(destination, str):
                errors.append(f"spotify_playlists[{idx}].destination must be a string")
            auto_download = entry.get("auto_download")
            if auto_download is not None and not isinstance(auto_download, bool):
                errors.append(f"spotify_playlists[{idx}].auto_download must be true/false")
            min_score = entry.get("min_match_score")
            if min_score is not None:
                try:
                    value = float(min_score)
                except (TypeError, ValueError):
                    errors.append(f"spotify_playlists[{idx}].min_match_score must be a number")
                else:
                    if not (0 <= value <= 1):
                        errors.append(
                            f"spotify_playlists[{idx}].min_match_score must be between 0 and 1"
                        )

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

    cookie_file = config.get("yt_dlp_cookies")
    if cookie_file is not None and not isinstance(cookie_file, str):
        errors.append("yt_dlp_cookies must be a string")

    filename_template = config.get("filename_template")
    if filename_template is not None and not isinstance(filename_template, str):
        errors.append("filename_template must be a string")

    final_format = config.get("final_format")
    music_folder = config.get("music_download_folder")
    if music_folder is not None and not isinstance(music_folder, str):
        errors.append("music_download_folder must be a string")

    if final_format is not None and not isinstance(final_format, str):
        errors.append("final_format must be a string")

    media_type = config.get("media_type")
    if media_type is not None and media_type not in {"music", "audio", "video"}:
        errors.append("media_type must be 'music', 'audio', or 'video'")

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
    }


def read_history(
    db_path,
    *,
    playlist_id=None,
    start_date=None,
    end_date=None,
    date_from=None,
    date_to=None,
    search=None,
    sort_by="date",
    sort_dir="desc",
    limit=None,
):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    query = "SELECT video_id, playlist_id, downloaded_at, filepath FROM downloads"
    clauses = []
    params = []

    # Normalize API parameter aliases
    if date_from and not start_date:
        start_date = date_from
    if date_to and not end_date:
        end_date = date_to

    if playlist_id:
        clauses.append("playlist_id=?")
        params.append(playlist_id)
    if start_date:
        clauses.append("downloaded_at>=?")
        params.append(start_date)
    if end_date:
        clauses.append("downloaded_at<=?")
        params.append(end_date)
    if search:
        clauses.append("filepath LIKE ?")
        params.append(f"%{search}%")

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


def init_db(db_path):
    db_dir = os.path.dirname(db_path)
    if db_dir:
        os.makedirs(db_dir, exist_ok=True)
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS downloads (
            video_id TEXT PRIMARY KEY,
            playlist_id TEXT,
            downloaded_at TEXT,
            filepath TEXT
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS playlist_videos (
            playlist_id TEXT NOT NULL,
            video_id TEXT NOT NULL,
            first_seen_at TEXT,
            downloaded INTEGER DEFAULT 0,
            PRIMARY KEY (playlist_id, video_id)
        )
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_playlist_videos_playlist ON playlist_videos (playlist_id)")
    ensure_download_jobs_table(conn)
    conn.commit()
    return conn


def discover_playlist_videos(yt_client, playlist_id, *, allow_public=True, cookie_file=None):
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
        except RefreshError as exc:
            logging.error("OAuth refresh failed while fetching playlist %s: %s", playlist_id, exc)
            fetch_error = True
            refresh_error = True
    if not videos and allow_public:
        videos, fallback_error = get_playlist_videos_fallback(playlist_id, cookie_file=cookie_file)
    return videos, fetch_error, fallback_error, refresh_error


def record_playlist_error(_conn, playlist_id, message, when=None):
    if not playlist_id:
        return
    timestamp = when or datetime.now(timezone.utc).isoformat()
    logging.error("Playlist %s error at %s: %s", playlist_id, timestamp, message)


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


def is_video_downloaded(conn, video_id):
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM downloads WHERE video_id=? LIMIT 1", (video_id,))
    return cur.fetchone() is not None


def resolve_cookie_file(config):
    cookie_value = (config or {}).get("yt_dlp_cookies")
    if not cookie_value:
        return None
    try:
        resolved = resolve_dir(cookie_value, TOKENS_DIR)
    except ValueError as exc:
        logging.error("Invalid yt-dlp cookies path: %s", exc)
        return None
    if not os.path.exists(resolved):
        logging.warning("yt-dlp cookies file not found: %s", resolved)
        return None
    return resolved


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
                except RefreshError as exc:
                    logging.error("OAuth refresh failed for account %s: %s", name, exc)
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
                except RefreshError as exc:
                    logging.error("OAuth refresh failed for account %s: %s", name, exc)
                    continue
            clients[name] = youtube_service(creds)
            cache[name] = {"client": clients[name], "creds": creds}
        except RefreshError as exc:
            logging.error("OAuth refresh failed for account %s: %s", name, exc)
        except Exception:
            logging.exception("Failed to initialize YouTube client for account %s", name)
    return clients


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


def get_playlist_videos_fallback(playlist_id, *, cookie_file=None):
    playlist_url = f"https://www.youtube.com/playlist?list={playlist_id}"
    opts = {
        "skip_download": True,
        "extract_flat": True,
        "quiet": True,
        "no_warnings": True,
    }
    if cookie_file:
        opts["cookiefile"] = cookie_file
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


def extract_video_id(url):
    if not url:
        return None
    if "youtube.com" in url:
        match = re.search(r"v=([a-zA-Z0-9_-]{6,})", url)
        if match:
            return match.group(1)
    if "youtu.be" in url:
        parsed = urllib.parse.urlparse(url)
        if parsed.path:
            return parsed.path.lstrip("/").split("/")[0]
    return None


def extract_playlist_id(value):
    if not value:
        return None
    if "list=" in value:
        parsed = urllib.parse.urlparse(value)
        query = urllib.parse.parse_qs(parsed.query)
        return query.get("list", [None])[0]
    if value.startswith("PL") or value.startswith("UU"):
        return value
    return None


def build_video_url(video_id):
    if not video_id:
        return None
    if video_id.startswith("http://") or video_id.startswith("https://"):
        return video_id
    return f"https://www.youtube.com/watch?v={video_id}"


def telegram_notify(config, message):
    telegram = config.get("telegram") if isinstance(config, dict) else None
    if not telegram or not message:
        return False
    bot_token = telegram.get("bot_token")
    chat_id = telegram.get("chat_id")
    if not bot_token or not chat_id:
        return False
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {"chat_id": chat_id, "text": message}
    try:
        resp = requests.post(url, json=payload, timeout=15)
        if resp.ok:
            return True
        logging.warning("Telegram notify failed: %s", resp.text)
    except Exception:
        logging.exception("Telegram notify failed")
    return False


def run_single_download(
    config,
    video_url,
    destination=None,
    final_format_override=None,
    *,
    paths: EnginePaths,
    status=None,
    music_mode=None,
    **_ignored,
):
    if status is None:
        status = EngineStatus()

    if not video_url:
        _status_set(status, "last_error_message", "Missing URL")
        status.single_download_ok = False
        return False

    conn = init_db(paths.db_path)
    try:
        store = DownloadJobStore(paths.db_path)
        origin = "manual"
        origin_id = extract_video_id(video_url) or video_url
        effective_config = dict(config) if isinstance(config, dict) else {}
        if music_mode is True:
            effective_config["media_type"] = "music"
        else:
            effective_config["media_type"] = "video"
        media_type = resolve_media_type(effective_config, url=video_url)
        media_intent = resolve_media_intent(origin, media_type)
        source = resolve_source(video_url)

        try:
            output_template = build_output_template(
                config,
                destination=destination,
                base_dir=paths.single_downloads_dir,
            )
        except ValueError as exc:
            _status_set(status, "last_error_message", f"Invalid destination path: {exc}")
            status.single_download_ok = False
            return False
        if final_format_override:
            output_template["final_format"] = final_format_override

        resolved_destination = output_template.get("output_dir")
        job_id, created, _dedupe_reason = store.enqueue_job(
            origin=origin,
            origin_id=origin_id,
            media_type=media_type,
            media_intent=media_intent,
            source=source,
            url=video_url,
            output_template=output_template,
            resolved_destination=resolved_destination,
            log_duplicate_event=False,
        )
        if created:
            _status_append(status, "run_successes", job_id)
        status.single_download_ok = job_id is not None
        return status.single_download_ok
    finally:
        conn.close()


def run_direct_url_self_test(
    config,
    *,
    paths: EnginePaths,
    url="https://youtu.be/PmtGDk0c-JM",
    final_format_override="webm",
):
    logging.info("RETREIVR_DIAG: enqueueing direct URL self-test for %s", url)
    status = EngineStatus()
    ok = run_single_download(
        config,
        url,
        destination=None,
        final_format_override=final_format_override,
        paths=paths,
        status=status,
        music_mode=False,
    )
    if not ok or not status.run_successes:
        logging.error("RETREIVR_DIAG: enqueue failed for %s", url)
        return False
    job_id = status.run_successes[-1]
    store = DownloadJobStore(paths.db_path)
    job = store.claim_job_by_id(job_id)
    if not job:
        logging.error("RETREIVR_DIAG: unable to claim job %s", job_id)
        return False
    worker = DownloadWorkerEngine(paths.db_path, config or {}, paths)
    worker._execute_job(job)
    return True


def run_single_playlist(
    config,
    playlist_value,
    destination=None,
    account=None,
    final_format_override=None,
    *,
    paths: EnginePaths,
    status=None,
    **_ignored,
):
    if not playlist_value:
        logging.error("Playlist ID or URL is required")
        return status

    playlist_id = extract_playlist_id(playlist_value) or playlist_value
    if not playlist_id:
        logging.error("Invalid playlist ID or URL: %s", playlist_value)
        return status

    folder = destination or config.get("music_download_folder") or config.get("single_download_folder") or "."
    entry = {
        "playlist_id": playlist_id,
        "folder": folder,
        "remove_after_download": False,
        "mode": "full",
    }
    if account:
        entry["account"] = account
    if final_format_override:
        entry["final_format"] = final_format_override

    run_config = dict(config) if isinstance(config, dict) else {}
    run_config["playlists"] = [entry]

    run_once(
        run_config,
        paths=paths,
        status=status,
    )
    return status


def run_once(config, *, paths: EnginePaths, status=None, stop_event=None, **_ignored):
    if status is None:
        status = EngineStatus()

    if stop_event and stop_event.is_set():
        logging.warning("Stop requested before run start")
        return status

    conn = init_db(paths.db_path)
    store = DownloadJobStore(paths.db_path)

    accounts = config.get("accounts", {}) or {}
    playlists = config.get("playlists", []) or []
    cookie_file = resolve_cookie_file(config)

    yt_clients = build_youtube_clients(accounts, config) if accounts else {}

    try:
        _status_set(status, "current_phase", "enqueueing")
        _status_set(status, "current_playlist_id", None)
        _status_set(status, "current_video_id", None)
        _status_set(status, "current_video_title", None)
        _status_set(status, "progress_current", 0)
        _status_set(status, "progress_total", 0)
        _status_set(status, "progress_percent", 0)
        _reset_video_progress(status)

        for pl in playlists:
            if stop_event and stop_event.is_set():
                logging.warning("Stop requested; ending run loop")
                return status
            playlist_key = pl.get("playlist_id") or pl.get("id")
            folder_value = pl.get("folder") or pl.get("directory")
            account = pl.get("account")

            if not playlist_key or not folder_value:
                logging.error("Playlist entry missing id or folder: %s", pl)
                continue

            playlist_id = extract_playlist_id(playlist_key) or playlist_key
            _status_set(status, "current_playlist_id", playlist_id)

            yt = yt_clients.get(account) if account else None
            allow_public = not account

            if account and not yt:
                logging.error("No valid YouTube client for account '%s'; skipping playlist %s", account, playlist_id)
                _status_append(status, "run_failures", f"{playlist_id} (auth)")
                record_playlist_error(conn, playlist_id, "oauth missing")
                continue

            videos, fetch_error, fallback_error, refresh_error = discover_playlist_videos(
                yt,
                playlist_id,
                allow_public=allow_public,
                cookie_file=cookie_file,
            )
            if refresh_error and account:
                _status_append(status, "run_failures", f"{playlist_id} (auth)")
                record_playlist_error(conn, playlist_id, "oauth refresh failed")
                yt_clients[account] = None
                continue

            if not videos:
                if fetch_error or fallback_error:
                    logging.error("No videos found for playlist %s (fetch failed)", playlist_id)
                    _status_append(status, "run_failures", f"{playlist_id} (fetch failed)")
                    record_playlist_error(conn, playlist_id, "playlist fetch failed")
                else:
                    logging.info("Playlist %s is empty; skipping.", playlist_id)
                continue

            total_videos = len(videos)
            _status_set(status, "progress_total", total_videos)
            _status_set(status, "progress_current", 0)
            _status_set(status, "progress_percent", 0)

            for entry in videos:
                if stop_event and stop_event.is_set():
                    logging.warning("Stop requested; stopping after current playlist")
                    return status
                vid = entry.get("videoId") or entry.get("id") or entry.get("url")
                if not vid:
                    continue
                video_id = extract_video_id(vid) or vid
                if is_video_downloaded(conn, video_id):
                    continue

                video_url = build_video_url(vid)
                if not video_url:
                    continue

                media_type = resolve_media_type(config, playlist_entry=pl, url=video_url)
                media_intent = resolve_media_intent("playlist", media_type, playlist_entry=pl)
                source = resolve_source(video_url)

                job_entry = dict(pl)
                job_entry["playlistItemId"] = entry.get("playlistItemId")
                job_entry["account"] = account

                try:
                    output_template = build_output_template(
                        config,
                        playlist_entry=job_entry,
                        base_dir=paths.single_downloads_dir,
                    )
                except ValueError as exc:
                    logging.error("Invalid playlist folder path: %s", exc)
                    continue

                resolved_destination = output_template.get("output_dir")

                job_id, created, _dedupe_reason = store.enqueue_job(
                    origin="playlist",
                    origin_id=playlist_id,
                    media_type=media_type,
                    media_intent=media_intent,
                    source=source,
                    url=video_url,
                    output_template=output_template,
                    resolved_destination=resolved_destination,
                )
                if created:
                    _status_append(status, "run_successes", job_id)

                current = (status.progress_current or 0) + 1
                _status_set(status, "progress_current", current)
                _status_set(status, "progress_percent", int((current / total_videos) * 100))

    finally:
        conn.close()
        _status_set(status, "current_phase", None)
        _status_set(status, "current_playlist_id", None)
        _status_set(status, "current_video_id", None)
        _status_set(status, "current_video_title", None)
        _reset_video_progress(status)

    return status


def run_archive(
    config,
    *,
    paths: EnginePaths,
    status=None,
    single_url=None,
    destination=None,
    final_format_override=None,
    delivery_mode=None,
    stop_event=None,
    run_source="manual",
    music_mode=None,
    **_ignored,
):
    if status is None:
        status = EngineStatus()

    logging.info("Run started (source=%s)", run_source)
    _status_set(status, "current_phase", "enqueueing")
    _status_set(status, "last_error_message", None)

    if single_url:
        if delivery_mode == "search_only":
            # Invariant B: Search-only requests must never enqueue or run direct downloads.
            _status_set(
                status,
                "last_error_message",
                "Search Only does not start direct URL downloads.",
            )
            status.single_download_ok = False
            return status
        ok = run_single_download(
            config,
            single_url,
            destination,
            final_format_override,
            paths=paths,
            status=status,
            stop_event=stop_event,
            music_mode=music_mode,
        )
        status.single_download_ok = ok
        return status

    run_once(config, paths=paths, status=status, stop_event=stop_event)
    return status
