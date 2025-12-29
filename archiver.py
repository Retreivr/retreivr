#!/usr/bin/env python3
"""
YouTube playlist archiver with robust retries, metadata embedding, and clean filenames.
- Sequential downloads to avoid throttling; retries across multiple extractor profiles.
- Embedded metadata (title/channel/date/description/tags/URL) and thumbnail as cover art.
- Optional final format copy (webm/mp4/mkv) and filename templating.
- Background copy to destination and SQLite history to avoid duplicate downloads.
- Optional Telegram summary after each run.
"""

import argparse
import json
import logging
import os
import re
import shutil
import sqlite3
import subprocess
import tempfile
import urllib.parse
import urllib.request
from datetime import datetime
from threading import Thread

import requests
from google.oauth2.credentials import Credentials
from google.auth.exceptions import RefreshError
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from yt_dlp import YoutubeDL


os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    filename=os.path.join("logs", "archiver.log"),
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
console = logging.StreamHandler()
console.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
console.setLevel(logging.INFO)
logging.getLogger("").addHandler(console)

DB_PATH = "database/db.sqlite"

MAX_VIDEO_RETRIES = 4        # Hard cap per video
EXTRACTOR_RETRIES = 2        # Times to retry each extractor before moving on


# ─────────────────────────────────────────────────────────────────────────────
# DB
# ─────────────────────────────────────────────────────────────────────────────
def init_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
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


# ─────────────────────────────────────────────────────────────────────────────
# Filename helpers
# ─────────────────────────────────────────────────────────────────────────────
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


# ─────────────────────────────────────────────────────────────────────────────
# Config + API
# ─────────────────────────────────────────────────────────────────────────────
def load_config(path):
    with open(path, "r") as f:
        return json.load(f)


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


def resolve_js_runtime(config):
    runtime = config.get("js_runtime") or os.environ.get("YT_DLP_JS_RUNTIME")
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
    ).execute()

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
        "title": snip.get("title"),
        "channel": snip.get("channelTitle"),
        "upload_date": upload_date,
        "description": snip.get("description") or "",
        "tags": snip.get("tags") or [],
        "url": f"https://www.youtube.com/watch?v={video_id}",
        "thumbnail_url": thumb_url,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Async copy worker
# ─────────────────────────────────────────────────────────────────────────────
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


# ─────────────────────────────────────────────────────────────────────────────
# Telegram notification
# ─────────────────────────────────────────────────────────────────────────────
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


# ─────────────────────────────────────────────────────────────────────────────
# Partial file check
# ─────────────────────────────────────────────────────────────────────────────
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


# ─────────────────────────────────────────────────────────────────────────────
# Metadata embedding
# ─────────────────────────────────────────────────────────────────────────────
def embed_metadata(local_file, meta, video_id):
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
    if thumb_url:
        try:
            os.makedirs("/tmp/yt-dlp/thumbs", exist_ok=True)
            thumb_path = os.path.join("/tmp/yt-dlp/thumbs", f"{video_id}.jpg")
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
        if thumb_path and os.path.exists(thumb_path):
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


# ─────────────────────────────────────────────────────────────────────────────
# yt-dlp (WEBM + MP4 fallback)
# ─────────────────────────────────────────────────────────────────────────────
def download_with_ytdlp(video_url, temp_dir, js_runtime=None, meta=None, config=None):
    vid = video_url.split("v=")[-1]

    FORMAT_WEBM = (
        # Preferred: WebM (VP9/Opus)
        "bestvideo[ext=webm][height<=1080]+bestaudio[ext=webm]/"
        "bestvideo[ext=webm][height<=720]+bestaudio[ext=webm]/"
        # Fallback: MP4 (H.264/AAC)
        "bestvideo[ext=mp4][height<=1080]+bestaudio[ext=m4a]/"
        "bestvideo[ext=mp4][height<=720]+bestaudio[ext=m4a]"
    )

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

    for attempt in range(MAX_VIDEO_RETRIES):
        logging.info(f"[{vid}] Download attempt {attempt+1}/{MAX_VIDEO_RETRIES}")

        for client_name, headers in extractor_chain:
            logging.info(f"[{vid}] Trying extractor: {client_name}")

            for _ in range(EXTRACTOR_RETRIES):
                # Reset temp dir if stuck
                if os.path.exists(temp_dir):
                    if is_partial_file_stuck(temp_dir, vid):
                        logging.warning(f"[{vid}] Stuck partial detected, wiping temp_dir")
                        shutil.rmtree(temp_dir, ignore_errors=True)

                shutil.rmtree(temp_dir, ignore_errors=True)
                os.makedirs(temp_dir, exist_ok=True)

                opts = {
                    "outtmpl": os.path.join(temp_dir, "%(id)s.%(ext)s"),
                    "paths": {"temp": "/tmp/yt-dlp"},
                    "format": FORMAT_WEBM,
                    "quiet": True,
                    "continuedl": True,
                    "socket_timeout": 120,
                    "retries": 5,
                    "forceipv4": True,
                    "http_headers": headers,
                    "extractor_args": {"youtube": [f"player_client={client_name}"]},
                    "remote_components": ["ejs:github"],
                }

                # Allow caller to inject/override yt-dlp options via config
                if config and config.get("yt_dlp_opts"):
                    try:
                        opts.update(config.get("yt_dlp_opts") or {})
                    except Exception:
                        logging.exception("Failed to merge yt_dlp_opts from config")

                if js_runtime:
                    runtime_name, runtime_path = js_runtime.split(":", 1)
                    opts["js_runtimes"] = {runtime_name: {"path": runtime_path}}

                try:
                    with YoutubeDL(opts) as ydl:
                        info = ydl.extract_info(video_url, download=True)
                except Exception as e:
                    logging.warning(f"[{vid}] {client_name} failed: {e}")
                    continue

                if not info:
                    logging.warning(f"[{vid}] No info returned from extractor {client_name}")
                    continue

                # Prefer .webm if present, else accept mp4
                chosen = None
                for f in os.listdir(temp_dir):
                    if f.startswith(vid) and f.endswith(".webm"):
                        chosen = os.path.join(temp_dir, f)
                        break
                if not chosen:
                    for f in os.listdir(temp_dir):
                        if f.startswith(vid) and f.endswith(".mp4"):
                            chosen = os.path.join(temp_dir, f)
                            break

                if chosen:
                    logging.info(f"[{vid}] SUCCESS via {client_name} → {os.path.basename(chosen)}")

                    # Embed metadata first
                    embed_metadata(chosen, meta, vid)

                    # Post-processing final format conversion (if needed)
                    desired_ext = config.get("final_format") if config else None
                    if desired_ext:
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


# ─────────────────────────────────────────────────────────────────────────────
# Main pipeline
# ─────────────────────────────────────────────────────────────────────────────
def run_once(config):
    LOCK_FILE = "/tmp/yt_archiver.lock"

    run_successes = []
    run_failures = []

    if os.path.exists(LOCK_FILE):
        logging.warning("Lockfile present — skipping run")
        return

    with open(LOCK_FILE, "w") as f:
        f.write(str(os.getpid()))

    conn = init_db()
    cur = conn.cursor()

    accounts = config["accounts"]
    playlists = config["playlists"]
    js_runtime = resolve_js_runtime(config)

    pending_copies = []
    yt_clients = build_youtube_clients(accounts, config)

    try:
        for pl in playlists:
            playlist_id = pl["playlist_id"]
            target_folder = pl["folder"]
            account = pl["account"]
            remove_after = pl.get("remove_after_download", False)

            yt = yt_clients.get(account)
            if yt is None:
                logging.error("No valid YouTube client for account '%s'; skipping playlist %s", account, playlist_id)
                run_failures.append(f"{playlist_id} (auth)")
                continue

            try:
                videos = get_playlist_videos(yt, playlist_id)
            except HttpError:
                logging.exception("Playlist fetch failed %s", playlist_id)
                continue
            except RefreshError as e:
                logging.error("OAuth refresh failed for account %s while fetching playlist %s: %s", account, playlist_id, e)
                run_failures.append(f"{playlist_id} (auth)")
                yt_clients[account] = None
                continue

            for entry in videos:
                vid = entry.get("videoId")
                if not vid:
                    continue

                cur.execute("SELECT video_id FROM downloads WHERE video_id=?", (vid,))
                if cur.fetchone():
                    continue

                try:
                    meta = get_video_metadata(yt, vid)
                except HttpError:
                    logging.exception("Metadata fetch failed %s", vid)
                    continue
                except RefreshError as e:
                    logging.error("OAuth refresh failed for account %s while fetching video %s: %s", account, vid, e)
                    run_failures.append(f"{vid} (auth)")
                    yt_clients[account] = None
                    break
                if not meta:
                    logging.warning("Skipping %s: no metadata", vid)
                    continue

                logging.info("START download: %s (%s)", vid, meta["title"])

                video_url = meta["url"]
                temp_dir = os.path.join("temp_downloads", vid)

                local_file = download_with_ytdlp(video_url, temp_dir, js_runtime, meta, config)
                if not local_file:
                    logging.warning("Download FAILED: %s", vid)
                    run_failures.append(meta["title"])
                    shutil.rmtree(temp_dir, ignore_errors=True)
                    continue

                # Determine extension based on config final_format
                ext = config.get("final_format")
                if not ext:
                    ext = os.path.splitext(local_file)[1].lstrip(".") or "webm"

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
                               temp=temp_dir, remove=remove_after, yt_service=yt):

                    if success:
                        logging.info("Copy OK → %s", dst)
                        run_successes.append(cleaned_name)
                        try:
                            with sqlite3.connect(DB_PATH, check_same_thread=False) as c:
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
                        run_failures.append(cleaned_name)

                    shutil.rmtree(temp, ignore_errors=True)

                    if success and remove and entry_id:
                        try:
                            yt_service.playlistItems().delete(id=entry_id).execute()
                        except Exception:
                            logging.exception("Failed removing %s", video_id)

                t = async_copy(local_file, final_path, after_copy)
                pending_copies.append(t)
                logging.info("COPY started in background → next download begins")

        for t in pending_copies:
            t.join()
        logging.info("\n" + ("-" * 80) + "\n")
        logging.info("Run complete.")
        logging.info("\n" + ("-" * 80) + "\n \n \n")

    finally:
        conn.close()
        try:
            # Telegram Summary
            if run_successes or run_failures:
                msg = "YouTube Archiver Summary\n"
                msg += f"✔ Success: {len(run_successes)}\n"
                msg += f"✖ Failed: {len(run_failures)}\n\n"

                if run_successes:
                    msg += "Downloaded:\n" + "\n".join(f"• {t}" for t in run_successes) + "\n\n"
                if run_failures:
                    msg += "Failed:\n" + "\n".join(f"• {t}" for t in run_failures)

                telegram_notify(config, msg)
            os.remove(LOCK_FILE)
        except FileNotFoundError:
            pass


# ─────────────────────────────────────────────────────────────────────────────
# Entry
# ─────────────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="config/config.json")
    args = parser.parse_args()

    if not os.path.exists(args.config):
        logging.error("Config file not found: %s", args.config)
        return

    config = load_config(args.config)
    run_once(config)


if __name__ == "__main__":
    main()
