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
from datetime import datetime
from threading import Thread

import requests
from google.oauth2.credentials import Credentials
from google.auth.exceptions import RefreshError
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from yt_dlp import YoutubeDL

from engine.paths import EnginePaths, resolve_dir

MAX_VIDEO_RETRIES = 4        # Hard cap per video
EXTRACTOR_RETRIES = 2        # Times to retry each extractor before moving on

_GOOGLE_AUTH_RETRY = re.compile(r"Refreshing credentials due to a 401 response\\. Attempt (\\d+)/(\\d+)\\.")


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
        logger._yt_archiver_filter = True


_install_google_auth_filter()


@dataclass
class EngineStatus:
    run_successes: list[str] = field(default_factory=list)
    run_failures: list[str] = field(default_factory=list)
    runtime_warned: bool = False
    single_download_ok: bool | None = None
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

    return errors


def get_status(status):
    if status is None:
        return {
            "run_successes": [],
            "run_failures": [],
            "runtime_warned": False,
            "single_download_ok": None,
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
    conn.commit()
    return conn


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


def build_youtube_clients(accounts, config):
    """
    Build one YouTube API client per configured account for this run.
    Any account that fails auth is skipped (logged) to avoid aborting the run.
    """
    clients = {}
    if not isinstance(accounts, dict):
        return clients
    for name, acc in accounts.items():
        token_path = acc.get("token")
        if not token_path:
            logging.error("Account %s has no 'token' path configured; skipping", name)
            continue
        try:
            creds = load_credentials(token_path)
            clients[name] = youtube_service(creds)
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
            })
        page = resp.get("nextPageToken")
        if not page:
            break
    return videos


def get_video_metadata(youtube, video_id):
    """Return title, channel, upload_date (YYYYMMDD), description, tags, url, thumbnail_url."""
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


def get_playlist_videos_fallback(playlist_id):
    """Fetch playlist entries without OAuth (yt-dlp extract_flat).
    Returns (videos, had_error).
    """
    playlist_url = f"https://www.youtube.com/playlist?list={playlist_id}"
    opts = {
        "quiet": True,
        "skip_download": True,
        "extract_flat": True,
        "forceipv4": True,
    }
    try:
        with YoutubeDL(opts) as ydl:
            info = ydl.extract_info(playlist_url, download=False)
            entries = info.get("entries") or []
            vids = []
            for entry in entries:
                vid = entry.get("id") or entry.get("url")
                if vid:
                    vids.append({"videoId": vid})
            return vids, False
    except Exception:
        logging.exception("yt-dlp playlist fallback failed for %s", playlist_id)
        return [], True


def get_video_metadata_fallback(video_id_or_url):
    """Metadata without OAuth using yt-dlp (no download)."""
    if video_id_or_url.startswith("http"):
        video_url = video_id_or_url
        vid = extract_video_id(video_id_or_url) or video_id_or_url
    else:
        video_url = f"https://www.youtube.com/watch?v={video_id_or_url}"
        vid = video_id_or_url

    opts = {
        "quiet": True,
        "skip_download": True,
        "forceipv4": True,
    }

    try:
        with YoutubeDL(opts) as ydl:
            info = ydl.extract_info(video_url, download=False)
    except Exception:
        logging.exception("yt-dlp metadata fallback failed for %s", video_url)
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
        "upload_date": upload_date,
        "description": info.get("description") or "",
        "tags": info.get("tags") or [],
        "url": video_url,
        "thumbnail_url": thumb_url,
    }


def resolve_video_metadata(yt_client, video_id, allow_public_fallback=True):
    """Try OAuth API first, then yt-dlp fallback (if allowed), then stub metadata."""
    meta = None
    if yt_client:
        try:
            meta = get_video_metadata(yt_client, video_id)
        except HttpError:
            logging.exception("Metadata fetch failed %s", video_id)
        except RefreshError as e:
            logging.error("OAuth refresh failed while fetching video %s: %s", video_id, e)
    if not meta and allow_public_fallback:
        meta = get_video_metadata_fallback(video_id)

    if not meta:
        vid = extract_video_id(video_id) or video_id
        base_url = video_id if isinstance(video_id, str) and str(video_id).startswith("http") else f"https://www.youtube.com/watch?v={vid}"
        meta = {
            "video_id": vid,
            "title": vid,
            "channel": "",
            "upload_date": "",
            "description": "",
            "tags": [],
            "url": base_url,
            "thumbnail_url": None,
        }
    return meta


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
    upload_date = meta.get("upload_date") or ""
    description = meta.get("description") or ""
    tags = meta.get("tags") or []
    url = meta.get("url") or f"https://www.youtube.com/watch?v={video_id}"
    thumb_url = meta.get("thumbnail_url")

    # Convert YYYYMMDD -> YYYY-MM-DD if possible
    date_tag = ""
    if len(upload_date) == 8 and upload_date.isdigit():
        date_tag = f"{upload_date[0:4]}-{upload_date[4:6]}-{upload_date[6:8]}"

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
        if channel:
            cmd.extend(["-metadata", f"artist={channel}"])
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

def download_with_ytdlp(video_url, temp_dir, js_runtime=None, meta=None, config=None,
                        target_format=None, audio_only=False, *, paths, status=None, stop_event=None):
    vid = extract_video_id(video_url) or (video_url.split("v=")[-1] if "v=" in video_url else "video")
    if meta and meta.get("video_id"):
        vid = meta.get("video_id")
    js_runtime = normalize_js_runtime(js_runtime)

    FORMAT_WEBM = (
        # Preferred: WebM (VP9/Opus)
        "bestvideo[ext=webm][height<=1080]+bestaudio[ext=webm]/"
        "bestvideo[ext=webm][height<=720]+bestaudio[ext=webm]/"
        # Fallback: MP4 (H.264/AAC)
        "bestvideo[ext=mp4][height<=1080]+bestaudio[ext=m4a]/"
        "bestvideo[ext=mp4][height<=720]+bestaudio[ext=m4a]"
    )

    audio_formats = {"mp3", "m4a", "aac", "opus", "flac"}
    inherited_fmt = None
    if not target_format and config:
        inherited_fmt = config.get("final_format")
    target_fmt = (target_format or inherited_fmt or "").lower() or None
    audio_mode = audio_only or (target_fmt in audio_formats)
    preferred_exts = []

    if audio_mode:
        format_selector = "bestaudio/best"
        preferred_exts.append(target_fmt or "mp3")
    else:
        format_selector = FORMAT_WEBM
        if target_fmt:
            preferred_exts.append(target_fmt)
        preferred_exts.extend(["webm", "mp4", "mkv", "m4a", "opus"])

    extractor_chain = [
        ("android", {
            "User-Agent": "com.google.android.youtube/19.42.37 (Linux; Android 14)",
            "Accept-Language": "en-US,en;q=0.9",
        }),
        ("tv_embedded", {
            "User-Agent": "Mozilla/5.0 (SmartTV; Linux; Tizen 6.5) AppleWebKit/537.36",
            "Accept-Language": "en-US,en;q=0.9",
        }),
        ("web", {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"
                " AppleWebKit/605.1.15 (KHTML, like Gecko) Safari/605.1.15"
            ),
            "Accept-Language": "en-US,en;q=0.9",
        }),
    ]

    if status is None:
        status = EngineStatus()

    if stop_event and stop_event.is_set():
        logging.warning("[%s] Stop requested before download", vid)
        return None

    def progress_hook(data):
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

    for attempt in range(MAX_VIDEO_RETRIES):
        if stop_event and stop_event.is_set():
            logging.warning("[%s] Stop requested; aborting download loop", vid)
            return None
        logging.info(f"[{vid}] Download attempt {attempt+1}/{MAX_VIDEO_RETRIES}")

        for client_name, headers in extractor_chain:
            if stop_event and stop_event.is_set():
                logging.warning("[%s] Stop requested; aborting extractor loop", vid)
                return None
            logging.info(f"[{vid}] Trying extractor: {client_name}")

            for _ in range(EXTRACTOR_RETRIES):
                if stop_event and stop_event.is_set():
                    logging.warning("[%s] Stop requested; aborting retries", vid)
                    return None
                # Reset temp dir if stuck
                if os.path.exists(temp_dir):
                    if is_partial_file_stuck(temp_dir, vid):
                        logging.warning(f"[{vid}] Stuck partial detected, wiping temp_dir")
                        shutil.rmtree(temp_dir, ignore_errors=True)

                shutil.rmtree(temp_dir, ignore_errors=True)
                os.makedirs(temp_dir, exist_ok=True)

                opts = {
                    "outtmpl": os.path.join(temp_dir, "%(id)s.%(ext)s"),
                    "paths": {"temp": paths.ytdlp_temp_dir},
                    "format": format_selector,
                    "quiet": True,
                    "continuedl": True,
                    "progress_hooks": [progress_hook],
                    "socket_timeout": 120,
                    "retries": 5,
                    "forceipv4": True,
                    "http_headers": headers,
                    "extractor_args": {"youtube": [f"player_client={client_name}"]},
                    "remote_components": ["ejs:github"],
                }

                if audio_mode:
                    opts["postprocessors"] = [{
                        "key": "FFmpegExtractAudio",
                        "preferredcodec": target_fmt or "mp3",
                        "preferredquality": "0",
                    }]

                # Allow caller to inject/override yt-dlp options via config (non-critical settings)
                if config and config.get("yt_dlp_opts"):
                    try:
                        user_opts = config.get("yt_dlp_opts") or {}
                        opts.update(user_opts)
                    except Exception:
                        logging.exception("Failed to merge yt_dlp_opts from config")

                # Enforce the format selector even if user opts provided their own format
                opts["format"] = format_selector
                if audio_mode:
                    opts["postprocessors"] = [{
                        "key": "FFmpegExtractAudio",
                        "preferredcodec": target_fmt or "mp3",
                        "preferredquality": "0",
                    }]

                if js_runtime:
                    runtime_name, runtime_path = js_runtime.split(":", 1)
                    opts["js_runtimes"] = {runtime_name: {"path": runtime_path}}

                try:
                    with YoutubeDL(opts) as ydl:
                        info = ydl.extract_info(video_url, download=True)
                except Exception as e:
                    logging.warning(f"[{vid}] {client_name} failed: {e}")
                    continue

                vid_for_files = info.get("id") or vid

                if not info:
                    logging.warning(f"[{vid}] No info returned from extractor {client_name}")
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
                    logging.info(f"[{vid}] SUCCESS via {client_name} → {os.path.basename(chosen)}")

                    # Embed metadata first
                    embed_metadata(chosen, meta, vid, paths.thumbs_dir)

                    # Post-processing final format conversion (if needed)
                    desired_ext = target_fmt or (config.get("final_format") if config else None)
                    if desired_ext and not audio_mode:
                        current_ext = os.path.splitext(chosen)[1].lstrip(".").lower()
                        # Avoid container mismatch: don't force mp4 -> webm without re-encode
                        if current_ext == "mp4" and desired_ext == "webm":
                            logging.warning("[%s] Skipping mp4->webm container copy to avoid invalid file; consider final_format=mp4", vid)
                        elif current_ext != desired_ext:
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

                logging.warning(f"[{vid}] Extractor {client_name} produced no usable output")

        logging.warning(f"[{vid}] All extractors failed this attempt.")

    logging.error(f"[{vid}] PERMANENT FAILURE after {MAX_VIDEO_RETRIES} attempts.")
    return None


# ------------------------------------------------------------------
# Main pipeline
# ------------------------------------------------------------------

def run_single_download(config, video_url, destination=None, final_format_override=None,
                        *, paths, status=None, js_runtime_override=None, stop_event=None):
    """Download a single URL (no OAuth required)."""
    js_runtime = resolve_js_runtime(config, override=js_runtime_override)
    meta = resolve_video_metadata(None, extract_video_id(video_url) or video_url)

    vid = meta.get("video_id") or extract_video_id(video_url) or "video"
    temp_dir = os.path.join(paths.temp_downloads_dir, vid)

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

    try:
        dest_dir = resolve_dir(
            destination or config.get("single_download_folder") or paths.single_downloads_dir,
            paths.single_downloads_dir,
        )
    except ValueError as exc:
        logging.error("Invalid destination path: %s", exc)
        _status_set(status, "progress_current", 1)
        _status_set(status, "progress_total", 1)
        _status_set(status, "progress_percent", 100)
        _reset_video_progress(status)
        _status_set(status, "current_video_id", None)
        _status_set(status, "current_video_title", None)
        return False
    os.makedirs(dest_dir, exist_ok=True)

    local_file = download_with_ytdlp(
        video_url,
        temp_dir,
        js_runtime,
        meta,
        config,
        target_format=final_format_override,
        paths=paths,
        status=status,
        stop_event=stop_event,
    )
    if not local_file:
        logging.error("Download FAILED: %s", video_url)
        shutil.rmtree(temp_dir, ignore_errors=True)
        _status_set(status, "progress_current", 1)
        _status_set(status, "progress_total", 1)
        _status_set(status, "progress_percent", 100)
        _reset_video_progress(status)
        _status_set(status, "current_video_id", None)
        _status_set(status, "current_video_title", None)
        return False

    ext = os.path.splitext(local_file)[1].lstrip(".") or final_format_override or config.get("final_format") or "webm"

    template = config.get("filename_template")
    if template:
        try:
            cleaned_name = template % {
                "title": sanitize_for_filesystem(meta.get("title") or vid),
                "uploader": sanitize_for_filesystem(meta.get("channel") or ""),
                "upload_date": meta.get("upload_date") or "",
                "ext": ext
            }
        except Exception:
            cleaned_name = f"{pretty_filename(meta.get('title'), meta.get('channel'), meta.get('upload_date'))}_{vid[:8]}.{ext}"
    else:
        cleaned_name = f"{pretty_filename(meta.get('title'), meta.get('channel'), meta.get('upload_date'))}_{vid[:8]}.{ext}"

    final_path = os.path.join(dest_dir, cleaned_name)
    os.makedirs(os.path.dirname(final_path), exist_ok=True)

    shutil.copy2(local_file, final_path)
    shutil.rmtree(temp_dir, ignore_errors=True)

    logging.info("Direct download saved to %s", final_path)
    _status_set(status, "last_completed", cleaned_name)
    _status_set(status, "last_completed_at", datetime.utcnow().isoformat())
    _status_set(status, "last_completed_path", final_path)
    _status_set(status, "progress_current", 1)
    _status_set(status, "progress_total", 1)
    _status_set(status, "progress_percent", 100)
    _reset_video_progress(status)
    _status_set(status, "current_video_id", None)
    _status_set(status, "current_video_title", None)
    return True


def run_once(config, *, paths, status=None, js_runtime_override=None, stop_event=None):
    lock_file = paths.lock_file

    if status is None:
        status = EngineStatus()

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
    global_final_format = config.get("final_format")

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

            videos = []
            fetch_error = False
            fallback_error = False
            if account and not yt:
                logging.error("No valid YouTube client for account '%s'; skipping playlist %s", account, playlist_id)
                _status_append(status, "run_failures", f"{playlist_id} (auth)")
                continue

            if yt:
                try:
                    videos = get_playlist_videos(yt, playlist_id)
                except HttpError:
                    logging.exception("Playlist fetch failed %s", playlist_id)
                    fetch_error = True
                    _status_append(status, "run_failures", f"{playlist_id} (auth)")
                    continue
                except RefreshError as e:
                    logging.error("OAuth refresh failed for account %s while fetching playlist %s: %s", account, playlist_id, e)
                    _status_append(status, "run_failures", f"{playlist_id} (auth)")
                    yt_clients[account] = None
                    continue

            if not videos and allow_public:
                videos, fallback_error = get_playlist_videos_fallback(playlist_id)

            if not videos:
                if fetch_error or fallback_error:
                    logging.error("No videos found for playlist %s (auth or public fetch failed)", playlist_id)
                    _status_append(status, "run_failures", f"{playlist_id} (auth)")
                else:
                    logging.info("Playlist %s is empty; skipping.", playlist_id)
                continue

            total_videos = len(videos)
            completed = 0
            _status_set(status, "progress_total", total_videos)
            _status_set(status, "progress_current", completed)
            _status_set(status, "progress_percent", 0)

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

                cur.execute("SELECT video_id FROM downloads WHERE video_id=?", (vid,))
                if cur.fetchone():
                    completed += 1
                    _status_set(status, "progress_current", completed)
                    _status_set(status, "progress_percent", int((completed / total_videos) * 100))
                    continue

                meta = resolve_video_metadata(yt, vid, allow_public_fallback=allow_public)
                _status_set(status, "current_video_title", meta.get("title") or vid)
                _status_set(status, "video_progress_percent", 0)
                _status_set(status, "video_downloaded_bytes", 0)

                logging.info("START download: %s (%s)", vid, meta.get("title"))

                video_url = meta.get("url") or f"https://www.youtube.com/watch?v={vid}"
                temp_dir = os.path.join(paths.temp_downloads_dir, vid)

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
                )
                _reset_video_progress(status)
                if not local_file:
                    logging.warning("Download FAILED: %s", vid)
                    _status_append(status, "run_failures", meta.get("title") or vid)
                    shutil.rmtree(temp_dir, ignore_errors=True)
                    completed += 1
                    _status_set(status, "progress_current", completed)
                    _status_set(status, "progress_percent", int((completed / total_videos) * 100))
                    continue

                # Determine extension based on the resulting file or playlist/default format
                ext = os.path.splitext(local_file)[1].lstrip(".") or playlist_format or "webm"

                # Build filename using filename_template if present
                template = config.get("filename_template")
                if template:
                    try:
                        cleaned_name = template % {
                            "title": sanitize_for_filesystem(meta.get("title") or vid),
                            "uploader": sanitize_for_filesystem(meta.get("channel") or ""),
                            "upload_date": meta.get("upload_date") or "",
                            "ext": ext
                        }
                    except Exception:
                        cleaned_name = f"{pretty_filename(meta['title'], meta['channel'], meta['upload_date'])}_{vid[:8]}.{ext}"
                else:
                    cleaned_name = f"{pretty_filename(meta['title'], meta['channel'], meta['upload_date'])}_{vid[:8]}.{ext}"

                final_path = os.path.join(target_folder, cleaned_name)

                def after_copy(success, dst, video_id=vid, playlist=playlist_id,
                               entry_id=entry.get("playlistItemId"),
                               temp=temp_dir, remove=remove_after, yt_service=yt,
                               db_path=paths.db_path):

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
                                c.commit()
                        except Exception:
                            logging.exception("DB insert failed for %s", video_id)
                    else:
                        logging.error("Copy FAILED for %s", video_id)
                        _status_append(status, "run_failures", cleaned_name)

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
                run_source="manual"):
    if status is None:
        status = EngineStatus()

    logging.info("Run started (source=%s)", run_source)

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
        )
        status.single_download_ok = ok
        return status

    run_once(config, paths=paths, status=status, js_runtime_override=js_runtime_override, stop_event=stop_event)
    return status
