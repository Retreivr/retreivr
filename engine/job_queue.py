import json
import logging
import os
import re
import shutil
import sqlite3
import subprocess
import tempfile
import traceback
import threading
import time
import urllib.parse
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional
from uuid import uuid4

import requests
from yt_dlp import YoutubeDL
from yt_dlp.utils import DownloadError, ExtractorError

from engine.json_utils import json_sanity_check, safe_json_dumps
from engine.paths import EnginePaths, TOKENS_DIR, resolve_dir
from metadata.queue import enqueue_metadata

JOB_STATUS_QUEUED = "queued"
JOB_STATUS_CLAIMED = "claimed"
JOB_STATUS_DOWNLOADING = "downloading"
JOB_STATUS_POSTPROCESSING = "postprocessing"
JOB_STATUS_COMPLETED = "completed"
JOB_STATUS_SKIPPED_DUPLICATE = "skipped_duplicate"
JOB_STATUS_FAILED = "failed"

TERMINAL_STATUSES = (JOB_STATUS_COMPLETED, JOB_STATUS_FAILED, JOB_STATUS_SKIPPED_DUPLICATE)

_FORMAT_VIDEO = (
    "bestvideo[ext=webm][height<=1080]+bestaudio[ext=webm]/"
    "bestvideo[ext=webm][height<=720]+bestaudio[ext=webm]/"
    "bestvideo[ext=mp4][height<=1080]+bestaudio[ext=m4a]/"
    "bestvideo[ext=mp4][height<=720]+bestaudio[ext=m4a]/"
    "bestvideo*+bestaudio/best"
)
_FORMAT_AUDIO = "bestaudio/best"

_AUDIO_FORMATS = {"mp3", "m4a", "flac"}

_AUDIO_TITLE_CLEAN_RE = re.compile(
    r"\s*[\(\[\{][^)\]\}]*?(official|music video|video|lyric|audio|visualizer|full video|hd|4k)[^)\]\}]*?[\)\]\}]\s*",
    re.IGNORECASE,
)
_AUDIO_TITLE_TRAIL_RE = re.compile(
    r"\s*-\s*(official|music video|video|lyric|audio|visualizer|full video).*$",
    re.IGNORECASE,
)
_AUDIO_ARTIST_VEVO_RE = re.compile(r"(vevo)$", re.IGNORECASE)

_YTDLP_DOWNLOAD_UNSAFE_KEYS = {"download", "skip_download", "simulate", "extract_flat"}

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


@dataclass(frozen=True)
class DownloadJob:
    id: str
    origin: str
    origin_id: str
    media_type: str
    media_intent: str
    source: str
    url: str
    status: str
    queued: str | None
    claimed: str | None
    downloading: str | None
    postprocessing: str | None
    completed: str | None
    failed: str | None
    canceled: str | None
    attempts: int
    max_attempts: int
    created_at: str | None
    updated_at: str | None
    last_error: str | None
    trace_id: str
    output_template: dict | None
    resolved_destination: str | None
    canonical_id: str | None
    file_path: str | None


def utc_now():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _log_event(level, message, **fields):
    payload = {"message": message, **fields}
    try:
        logging.log(level, safe_json_dumps(payload, sort_keys=True))
    except Exception as exc:
        logging.log(level, f"log_event_serialization_failed: {exc} message={message}")


def ensure_download_jobs_table(conn):
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS download_jobs (
            id TEXT PRIMARY KEY,
            origin TEXT NOT NULL,
            origin_id TEXT NOT NULL,
            media_type TEXT NOT NULL,
            media_intent TEXT NOT NULL,
            source TEXT NOT NULL,
            url TEXT NOT NULL,
            status TEXT NOT NULL,
            queued TEXT,
            claimed TEXT,
            downloading TEXT,
            postprocessing TEXT,
            completed TEXT,
            failed TEXT,
            canceled TEXT,
            attempts INTEGER NOT NULL,
            max_attempts INTEGER NOT NULL DEFAULT 3,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            last_error TEXT,
            trace_id TEXT NOT NULL UNIQUE,
            output_template TEXT,
            resolved_destination TEXT,
            canonical_id TEXT,
            file_path TEXT
        )
        """
    )
    cur.execute("PRAGMA table_info(download_jobs)")
    existing_columns = {row[1] for row in cur.fetchall()}
    for column in (
        "claimed",
        "downloading",
        "postprocessing",
        "resolved_destination",
        "canonical_id",
        "file_path",
    ):
        if column not in existing_columns:
            cur.execute(f"ALTER TABLE download_jobs ADD COLUMN {column} TEXT")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_download_jobs_status ON download_jobs (status)")
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_download_jobs_source_status ON download_jobs (source, status)"
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_download_jobs_created ON download_jobs (created_at)")
    conn.commit()

# --- ensure_downloads_table
def ensure_downloads_table(conn):
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS downloads (
            video_id TEXT PRIMARY KEY,
            playlist_id TEXT,
            downloaded_at TEXT NOT NULL,
            filepath TEXT NOT NULL
        )
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_downloads_downloaded_at ON downloads (downloaded_at)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_downloads_playlist_id ON downloads (playlist_id)")
    conn.commit()

def is_music_media_type(value):
    if value is None:
        return False
    value = str(value).strip().lower()
    return value in {"music", "audio"}

def _normalize_audio_format(value: str | None) -> str | None:
    if not value:
        return None
    v = str(value).strip().lower()
    # Accept mp4 as a user-facing synonym for m4a (AAC in MP4 container)
    if v == "mp4":
        return "m4a"
    return v


def _normalize_format(value: str | None) -> str | None:
    if not value:
        return None
    return str(value).strip().lower()

class DownloadJobStore:
    def __init__(self, db_path):
        self.db_path = db_path

    def _connect(self):
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        return conn

    def _row_to_job(self, row):
        if not row:
            return None
        output_template = row["output_template"]
        parsed_output = None
        if output_template:
            try:
                parsed_output = json.loads(output_template)
            except json.JSONDecodeError:
                parsed_output = None
        return DownloadJob(
            id=row["id"],
            origin=row["origin"],
            origin_id=row["origin_id"],
            media_type=row["media_type"],
            media_intent=row["media_intent"],
            source=row["source"],
            url=row["url"],
            status=row["status"],
            queued=row["queued"],
            claimed=row["claimed"],
            downloading=row["downloading"],
            postprocessing=row["postprocessing"],
            completed=row["completed"],
            failed=row["failed"],
            canceled=row["canceled"],
            attempts=row["attempts"],
            max_attempts=row["max_attempts"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            last_error=row["last_error"],
            trace_id=row["trace_id"],
            output_template=parsed_output,
            resolved_destination=row["resolved_destination"],
            canonical_id=row["canonical_id"],
            file_path=row.get("file_path"),
        )

    def list_sources_with_queued_jobs(self, *, now=None):
        now = now or utc_now()
        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT DISTINCT source FROM download_jobs WHERE status=? AND (queued IS NULL OR queued<=?)",
                (JOB_STATUS_QUEUED, now),
            )
            return [row[0] for row in cur.fetchall()]
        finally:
            conn.close()

    def get_job_status(self, job_id):
        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute("SELECT status FROM download_jobs WHERE id=?", (job_id,))
            row = cur.fetchone()
            return row[0] if row else None
        finally:
            conn.close()

    def job_exists(self, origin, origin_id, url):
        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT 1 FROM download_jobs WHERE origin=? AND origin_id=? AND url=? LIMIT 1",
                (origin, origin_id, url),
            )
            return cur.fetchone() is not None
        finally:
            conn.close()

    def find_active_job(self, origin, origin_id, url):
        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute(
                f"""
                SELECT * FROM download_jobs
                WHERE origin=? AND origin_id=? AND url=? AND status NOT IN ({', '.join('?' for _ in TERMINAL_STATUSES)})
                ORDER BY created_at DESC
                LIMIT 1
                """,
                (origin, origin_id, url, *TERMINAL_STATUSES),
            )
            row = cur.fetchone()
            if not row:
                return None
            return self._row_to_job(row)
        finally:
            conn.close()

    def _row_has_valid_output(self, row):
        if not row:
            return False
        if row["status"] != JOB_STATUS_COMPLETED:
            return False
        path = row.get("file_path")
        if not path:
            return False
        try:
            if not os.path.exists(path):
                return False
            return os.path.getsize(path) > 0
        except OSError:
            return False

    def find_duplicate_job(self, *, canonical_id=None, url=None, destination=None):
        if not canonical_id and not url:
            return None
        conn = self._connect()
        try:
            cur = conn.cursor()
            clauses = []
            params = []
            dest_clause = "resolved_destination=?" if destination is not None else "resolved_destination IS NULL"
            if canonical_id:
                clauses.append(f"(canonical_id=? AND {dest_clause})")
                params.append(canonical_id)
                if destination is not None:
                    params.append(destination)
            if url:
                clauses.append(f"(url=? AND {dest_clause})")
                params.append(url)
                if destination is not None:
                    params.append(destination)
            if not clauses:
                return None
            query = f"""
                SELECT * FROM download_jobs
                WHERE ({' OR '.join(clauses)}) AND status=?
                ORDER BY created_at DESC
                LIMIT 1
            """
            params.append(JOB_STATUS_COMPLETED)
            cur.execute(query, tuple(params))
            row = cur.fetchone()
            if not row or not self._row_has_valid_output(row):
                return None
            return self._row_to_job(row)
        finally:
            conn.close()

    def claim_job_by_id(self, job_id, *, now=None):
        now = now or utc_now()
        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute("BEGIN IMMEDIATE")
            cur.execute(
                "SELECT * FROM download_jobs WHERE id=? AND status=? LIMIT 1",
                (job_id, JOB_STATUS_QUEUED),
            )
            row = cur.fetchone()
            if not row:
                conn.commit()
                return None
            cur.execute(
                """
                UPDATE download_jobs
                SET status=?, claimed=?, updated_at=?
                WHERE id=? AND status=?
                """,
                (JOB_STATUS_CLAIMED, now, now, job_id, JOB_STATUS_QUEUED),
            )
            if cur.rowcount != 1:
                conn.commit()
                return None
            conn.commit()
            updated_row = dict(row)
            updated_row["status"] = JOB_STATUS_CLAIMED
            updated_row["claimed"] = now
            updated_row["updated_at"] = now
            return self._row_to_job(updated_row)
        finally:
            conn.close()

    def enqueue_job(
        self,
        *,
        origin,
        origin_id,
        media_type,
        media_intent,
        source,
        url,
        output_template=None,
        max_attempts=3,
        trace_id=None,
        resolved_destination=None,
        canonical_id=None,
        log_duplicate_event=True,
    ):
        origin_id = origin_id or ""
        destination = resolved_destination
        if destination is None and output_template:
            destination = (output_template or {}).get("output_dir")

        duplicate = self.find_duplicate_job(
            canonical_id=canonical_id,
            url=url,
            destination=destination,
        )
        if duplicate:
            if log_duplicate_event:
                _log_event(
                    logging.INFO,
                    "job_skipped_duplicate",
                    job_id=duplicate.id,
                    trace_id=duplicate.trace_id,
                    origin=origin,
                    origin_id=origin_id,
                    source=source,
                    url=url,
                    destination=destination,
                    canonical_id=canonical_id,
                    status=duplicate.status,
                )
            return duplicate.id, False, "duplicate"

        job_id = uuid4().hex
        now = utc_now()
        trace_id = trace_id or uuid4().hex
        output_template_json = safe_json_dumps(output_template) if output_template else None

        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute(
                """
                INSERT INTO download_jobs (
                    id, origin, origin_id, media_type, media_intent, source, url,
                    status, queued, claimed, downloading, postprocessing, completed,
                    failed, canceled, attempts, max_attempts, created_at, updated_at,
                    last_error, trace_id, output_template, resolved_destination, canonical_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    job_id,
                    origin,
                    origin_id,
                    media_type,
                    media_intent,
                    source,
                    url,
                    JOB_STATUS_QUEUED,
                    now,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    0,
                    max_attempts,
                    now,
                    now,
                    None,
                    trace_id,
                    output_template_json,
                    destination,
                    canonical_id,
                ),
            )
            conn.commit()
            return job_id, True, None
        finally:
            conn.close()

    def claim_next_job(self, source, *, now=None):
        now = now or utc_now()
        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute("BEGIN IMMEDIATE")
            cur.execute(
                "SELECT 1 FROM download_jobs WHERE status=? AND source=? LIMIT 1",
                (JOB_STATUS_DOWNLOADING, source),
            )
            if cur.fetchone():
                conn.commit()
                return None
            cur.execute(
                """
                SELECT * FROM download_jobs
                WHERE status=? AND source=? AND (queued IS NULL OR queued<=?)
                ORDER BY created_at ASC
                LIMIT 1
                """,
                (JOB_STATUS_QUEUED, source, now),
            )
            row = cur.fetchone()
            if not row:
                conn.commit()
                return None
            job_id = row["id"]
            cur.execute(
                """
                UPDATE download_jobs
                SET status=?, claimed=?, updated_at=?
                WHERE id=? AND status=?
                """,
                (JOB_STATUS_CLAIMED, now, now, job_id, JOB_STATUS_QUEUED),
            )
            if cur.rowcount != 1:
                conn.commit()
                return None
            conn.commit()
            updated_row = dict(row)
            updated_row["status"] = JOB_STATUS_CLAIMED
            updated_row["claimed"] = now
            updated_row["updated_at"] = now
            return self._row_to_job(updated_row)
        finally:
            conn.close()
            
    def mark_completed(self, job_id, *, file_path=None):
        now = utc_now()
        conn = self._connect()
        try:
            cur = conn.cursor()
            if file_path:
                cur.execute(
                    """
                    UPDATE download_jobs
                    SET status=?, completed=?, updated_at=?, file_path=?
                    WHERE id=? AND status!=?
                    """,
                    (JOB_STATUS_COMPLETED, now, now, file_path, job_id, JOB_STATUS_FAILED),
                )
            else:
                cur.execute(
                    """
                    UPDATE download_jobs
                    SET status=?, completed=?, updated_at=?
                    WHERE id=? AND status!=?
                    """,
                    (JOB_STATUS_COMPLETED, now, now, job_id, JOB_STATUS_FAILED),
                )
            conn.commit()
        finally:
            conn.close()

    def mark_canceled(self, job_id, *, reason=None):
        now = utc_now()
        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute(
                """
                UPDATE download_jobs
                SET status=?, canceled=?, failed=?, updated_at=?, last_error=?
                WHERE id=?
                """,
                (JOB_STATUS_FAILED, now, now, now, reason, job_id),
            )
            conn.commit()
        finally:
            conn.close()

    def cancel_active_jobs(self, *, reason=None):
        now = utc_now()
        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute(
                """
                UPDATE download_jobs
                SET status=?, canceled=?, failed=?, updated_at=?, last_error=?
                WHERE status IN (?, ?, ?, ?)
                """,
                (
                    JOB_STATUS_FAILED,
                    now,
                    now,
                    now,
                    reason,
                    JOB_STATUS_QUEUED,
                    JOB_STATUS_CLAIMED,
                    JOB_STATUS_DOWNLOADING,
                    JOB_STATUS_POSTPROCESSING,
                ),
            )
            conn.commit()
            return cur.rowcount
        finally:
            conn.close()

    def record_failure(self, job, *, error_message, retryable, retry_delay_seconds):
        attempts = job.attempts + 1
        now = utc_now()
        conn = self._connect()
        try:
            cur = conn.cursor()
            if retryable and attempts < job.max_attempts:
                next_ready = (datetime.now(timezone.utc) + timedelta(seconds=retry_delay_seconds))
                queued_at = next_ready.replace(microsecond=0).isoformat()
                cur.execute(
                    """
                    UPDATE download_jobs
                    SET status=?, queued=?, updated_at=?, attempts=?, last_error=?
                    WHERE id=?
                    """,
                    (JOB_STATUS_QUEUED, queued_at, now, attempts, error_message, job.id),
                )
                conn.commit()
                return JOB_STATUS_QUEUED

            cur.execute(
                """
                UPDATE download_jobs
                SET status=?, failed=?, updated_at=?, attempts=?, last_error=?
                WHERE id=?
                """,
                (JOB_STATUS_FAILED, now, now, attempts, error_message, job.id),
            )
            conn.commit()
            return JOB_STATUS_FAILED
        finally:
            conn.close()

    def mark_downloading(self, job_id):
        now = utc_now()
        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute(
                """
                UPDATE download_jobs
                SET status=?, downloading=?, updated_at=?
                WHERE id=?
                """,
                (JOB_STATUS_DOWNLOADING, now, now, job_id),
            )
            conn.commit()
        finally:
            conn.close()

    def mark_postprocessing(self, job_id):
        now = utc_now()
        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute(
                """
                UPDATE download_jobs
                SET status=?, postprocessing=?, updated_at=?
                WHERE id=? AND status!=?
                """,
                (JOB_STATUS_POSTPROCESSING, now, now, job_id, JOB_STATUS_FAILED),
            )
            conn.commit()
        finally:
            conn.close()


class DownloadWorkerEngine:
    def __init__(
        self,
        db_path,
        config,
        paths: EnginePaths,
        *,
        retry_delay_seconds=30,
        adapters=None,
    ):
        self.db_path = db_path
        self.config = config or {}
        self.paths = paths
        self.retry_delay_seconds = retry_delay_seconds
        self.store = DownloadJobStore(db_path)
        self.adapters = adapters or default_adapters()
        # Ensure required DB tables exist (idempotent).
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        try:
            ensure_download_jobs_table(conn)
            ensure_downloads_table(conn)
        finally:
            conn.close()
        self._locks = {}
        self._locks_lock = threading.Lock()

    def run_once(self, *, stop_event=None):
        sources = self.store.list_sources_with_queued_jobs()
        threads = []
        for source in sources:
            if stop_event and stop_event.is_set():
                break
            lock = self._get_source_lock(source)
            if not lock.acquire(blocking=False):
                continue
            thread = threading.Thread(
                target=self._run_source_once,
                args=(source, lock, stop_event),
                daemon=False,
            )
            thread.start()
            threads.append(thread)
        for thread in threads:
            thread.join()

    def run_loop(self, *, poll_seconds=5, stop_event=None):
        json_sanity_check()
        _log_event(logging.INFO, "worker_started")
        _log_event(logging.INFO, "queue_polling_started")
        while True:
            if stop_event and stop_event.is_set():
                return
            self.run_once(stop_event=stop_event)
            time.sleep(poll_seconds)

    def _get_source_lock(self, source):
        with self._locks_lock:
            lock = self._locks.get(source)
            if not lock:
                lock = threading.Lock()
                self._locks[source] = lock
            return lock

    def _run_source_once(self, source, lock, stop_event):
        try:
            if stop_event and stop_event.is_set():
                return
            job = self.store.claim_next_job(source)
            if not job:
                return
            _log_event(
                logging.INFO,
                "job_claimed",
                job_id=job.id,
                trace_id=job.trace_id,
                source=job.source,
                origin=job.origin,
                media_intent=job.media_intent,
            )
            self._execute_job(job, stop_event=stop_event)
        finally:
            lock.release()

    def _execute_job(self, job, *, stop_event=None):
        if job.status != JOB_STATUS_CLAIMED:
            _log_event(
                logging.ERROR,
                "job_not_running",
                job_id=job.id,
                trace_id=job.trace_id,
                source=job.source,
                origin=job.origin,
                media_intent=job.media_intent,
            )
            return
        if stop_event and stop_event.is_set():
            error_message = "cancel_requested"
            self.store.record_failure(
                job,
                error_message=error_message,
                retryable=False,
                retry_delay_seconds=self.retry_delay_seconds,
            )
            _log_event(
                logging.ERROR,
                "job_failed",
                job_id=job.id,
                trace_id=job.trace_id,
                source=job.source,
                origin=job.origin,
                media_intent=job.media_intent,
                retryable=False,
                status=JOB_STATUS_FAILED,
                error=error_message,
            )
            return
        adapter = self.adapters.get(job.source)
        if not adapter:
            _log_event(
                logging.ERROR,
                "adapter_missing",
                job_id=job.id,
                trace_id=job.trace_id,
                source=job.source,
            )
            self.store.record_failure(
                job,
                error_message=f"adapter_missing:{job.source}",
                retryable=False,
                retry_delay_seconds=self.retry_delay_seconds,
            )
            return
        self.store.mark_downloading(job.id)
        _log_event(
            logging.INFO,
            "job_started",
            job_id=job.id,
            trace_id=job.trace_id,
            source=job.source,
            origin=job.origin,
            media_intent=job.media_intent,
        )
        try:
            result = adapter.execute(
                job,
                self.config,
                self.paths,
                stop_event=stop_event,
                media_type=job.media_type,
                media_intent=job.media_intent,
            )
            if not result:
                raise RuntimeError("adapter_execute_failed")
            final_path, meta = result
            if self.store.get_job_status(job.id) == JOB_STATUS_FAILED:
                return
            if stop_event and stop_event.is_set():
                error_message = "cancel_requested"
                self.store.record_failure(
                    job,
                    error_message=error_message,
                    retryable=False,
                    retry_delay_seconds=self.retry_delay_seconds,
                )
                _log_event(
                    logging.ERROR,
                    "job_failed",
                    job_id=job.id,
                    trace_id=job.trace_id,
                    source=job.source,
                    origin=job.origin,
                    media_intent=job.media_intent,
                    retryable=False,
                    status=JOB_STATUS_FAILED,
                    error=error_message,
                )
                return
            self.store.mark_postprocessing(job.id)
            record_download_history(
                self.db_path,
                job,
                final_path,
                meta=meta,
            )
            self.store.mark_completed(job.id, file_path=final_path)
            _log_event(
                logging.INFO,
                "job_completed",
                job_id=job.id,
                trace_id=job.trace_id,
                source=job.source,
                origin=job.origin,
                media_intent=job.media_intent,
                path=final_path,
            )
        except Exception as exc:
            if (
                isinstance(exc, TypeError)
                and "set" in str(exc)
                and "JSON" in str(exc)
            ):
                _log_event(
                    logging.ERROR,
                    "json_set_serialization_trace",
                    job_id=job.id,
                    url=job.url,
                    error=str(exc),
                    traceback=traceback.format_exc(),
                )
            error_message = f"{type(exc).__name__}: {exc}"
            retryable = is_retryable_error(exc)
            new_status = self.store.record_failure(
                job,
                error_message=error_message,
                retryable=retryable,
                retry_delay_seconds=self.retry_delay_seconds,
            )
            _log_event(
                logging.ERROR,
                "job_failed",
                job_id=job.id,
                trace_id=job.trace_id,
                source=job.source,
                origin=job.origin,
                media_intent=job.media_intent,
                retryable=retryable,
                status=new_status,
                error=error_message,
            )


class YouTubeAdapter:
    def execute(self, job, config, paths, *, stop_event=None, media_type=None, media_intent=None):
        output_template = job.output_template or {}
        output_dir = output_template.get("output_dir") or paths.single_downloads_dir
        raw_final_format = output_template.get("final_format")
        normalized_format = _normalize_format(raw_final_format)
        normalized_audio_format = _normalize_audio_format(raw_final_format)

        # Strict separation:
        # - music_mode controls whether we run music metadata enrichment.
        # - audio_only controls whether we download audio-only / extract audio via ffmpeg.
        # IMPORTANT: Do NOT let a global/default final_format="mp3" force *video* jobs into audio-only.
        music_mode = is_music_media_type(job.media_type)
        audio_only_requested = bool(output_template.get("audio_only")) or bool(output_template.get("audio_mode"))
        audio_mode = bool(audio_only_requested) or (music_mode and normalized_audio_format in _AUDIO_FORMATS)

        # If we're in audio_mode, final_format is the requested audio codec (mp3/m4a/flac).
        # Otherwise final_format is treated as a container preference (webm/mp4/mkv) or None.
        final_format = normalized_audio_format if audio_mode else normalized_format
        filename_template = output_template.get("filename_template")
        audio_template = output_template.get("audio_filename_template")

        resolved_dir = resolve_dir(output_dir, paths.single_downloads_dir)
        temp_dir = os.path.join(paths.temp_downloads_dir, job.id)
        os.makedirs(temp_dir, exist_ok=True)

        try:
            cookie_file = resolve_cookie_file(config)
            info, local_file = download_with_ytdlp(
                job.url,
                temp_dir,
                config,
                audio_mode=audio_mode,
                final_format=final_format,
                cookie_file=cookie_file,
                stop_event=stop_event,
                media_type=media_type,
                media_intent=media_intent,
                job_id=job.id,
                origin=job.origin,
                resolved_destination=job.resolved_destination,
            )
            if not info or not local_file:
                shutil.rmtree(temp_dir, ignore_errors=True)
                return None

            meta = extract_meta(info, fallback_url=job.url)
            video_id = meta.get("video_id") or job.id
            ext = os.path.splitext(local_file)[1].lstrip(".")
            if audio_mode:
                ext = final_format or "mp3"
            elif not ext:
                ext = final_format or "webm"
            template = audio_template if audio_mode else filename_template
            cleaned_name = build_output_filename(meta, video_id, ext, template, audio_mode)

            if stop_event and stop_event.is_set():
                shutil.rmtree(temp_dir, ignore_errors=True)
                return None

            if not audio_mode:
                embed_metadata(local_file, meta, video_id, paths.thumbs_dir)

            final_path = os.path.join(resolved_dir, cleaned_name)
            os.makedirs(os.path.dirname(final_path), exist_ok=True)
            atomic_move(local_file, final_path)
            shutil.rmtree(temp_dir, ignore_errors=True)

            size = None
            try:
                size = os.path.getsize(final_path)
            except OSError:
                size = None
            if size is None or size == 0:
                try:
                    os.remove(final_path)
                except OSError:
                    pass
                raise RuntimeError("empty_output_file")

            # Only enqueue metadata if music_mode is True
            if music_mode:
                try:
                    enqueue_media_metadata(final_path, meta, config)
                except Exception:
                    # Never raise or affect download success
                    pass

            return final_path, meta
        except Exception:
            shutil.rmtree(temp_dir, ignore_errors=True)
            raise


def default_adapters():
    adapter = YouTubeAdapter()
    return {
        "youtube": adapter,
        "youtube_music": adapter,
        "soundcloud": adapter,
        "bandcamp": adapter,
        "direct": adapter,
        "unknown": adapter,
    }


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


def resolve_media_type(config, *, playlist_entry=None, url=None):
    media_type = None
    if isinstance(playlist_entry, dict):
        media_type = playlist_entry.get("media_type") or playlist_entry.get("media")
    if not media_type and isinstance(config, dict):
        media_type = config.get("media_type")
    if media_type:
        media_type = str(media_type).strip().lower()
        if media_type in {"music", "audio"}:
            return "music"
        if media_type == "video":
            return "video"

    legacy_audio = None
    if isinstance(playlist_entry, dict):
        legacy_audio = playlist_entry.get("audio_only")
        if legacy_audio is None:
            legacy_audio = playlist_entry.get("music_mode")
    if legacy_audio is None and isinstance(config, dict):
        legacy_audio = config.get("audio_only")
        if legacy_audio is None:
            legacy_audio = config.get("music_mode")
    if legacy_audio is True:
        return "music"
    # Invariant A: default to video when no explicit media_type is provided.
    return "video"


def resolve_media_intent(origin, media_type, *, playlist_entry=None):
    if isinstance(playlist_entry, dict):
        value = playlist_entry.get("media_intent")
        if value in {"track", "album", "playlist", "episode", "movie"}:
            return value
    if origin == "playlist":
        return "playlist"
    if is_music_media_type(media_type):
        return "track"
    return "episode"


def build_output_template(config, *, playlist_entry=None, destination=None, base_dir=None):
    base_dir = base_dir or "."
    config = config or {}
    entry = playlist_entry if isinstance(playlist_entry, dict) else {}
    output_dir = destination or entry.get("folder") or entry.get("directory")
    if not output_dir:
        # Prefer the correct default folder based on media type intent.
        # If the run is not explicitly music/audio, do NOT default into the music folder.
        media_type = str(entry.get("media_type") or entry.get("media") or "").strip().lower()
        if media_type in {"music", "audio"}:
            output_dir = config.get("music_download_folder") or config.get("single_download_folder") or base_dir
        else:
            output_dir = config.get("single_download_folder") or config.get("music_download_folder") or base_dir
    output_dir = resolve_dir(output_dir, base_dir)

    final_format = entry.get("final_format") or config.get("final_format")

    filename_template = entry.get("filename_template") or config.get("filename_template")
    audio_template = entry.get("audio_filename_template") or config.get("audio_filename_template")
    if not audio_template:
        audio_template = config.get("music_filename_template")

    return {
        "output_dir": output_dir,
        "final_format": final_format,
        "filename_template": filename_template,
        "audio_filename_template": audio_template,
        "remove_after_download": bool(entry.get("remove_after_download")),
        "playlist_item_id": entry.get("playlistItemId") or entry.get("playlist_item_id"),
        "source_account": entry.get("account"),
    }


def resolve_source(url):
    if not url:
        return "unknown"
    parsed = urllib.parse.urlparse(url)
    host = (parsed.netloc or "").lower()
    if "music.youtube.com" in host:
        return "youtube_music"
    if "youtube.com" in host or "youtu.be" in host:
        return "youtube"
    return "unknown"


def is_youtube_music_url(url):
    parsed = urllib.parse.urlparse(url)
    return "music.youtube.com" in (parsed.netloc or "").lower()


def build_ytdlp_opts(context):
    operation = context.get("operation") or "download"
    audio_mode = bool(context.get("audio_mode"))
    target_format = context.get("final_format")
    output_template = context.get("output_template")
    overrides = context.get("overrides") or {}
    allow_chapter_outtmpl = bool(context.get("allow_chapter_outtmpl"))

    def _url_looks_like_playlist(u: str | None) -> bool:
        if not u:
            return False
        try:
            parsed = urllib.parse.urlparse(u)
            q = urllib.parse.parse_qs(parsed.query)
            # YouTube playlist patterns
            if "list" in q and q.get("list"):
                return True
            # Common non-YouTube playlist patterns (best-effort)
            if any(k in q for k in ("playlist", "pl", "set")):
                return True
            # Path-based playlist URLs
            if "/playlist" in (parsed.path or ""):
                return True
        except Exception:
            return False
        return False

    # Playlists: let yt-dlp expand playlists itself when the run/job indicates playlist intent.
    # This must override the previous "single item" behavior; forcing noplaylist=True breaks scheduler/watcher.
    allow_playlist = bool(context.get("allow_playlist"))
    if not allow_playlist:
        if context.get("media_intent") == "playlist":
            allow_playlist = True
        elif context.get("origin") == "playlist":
            allow_playlist = True
        elif operation in {"playlist", "playlist_probe", "playlist_metadata"}:
            allow_playlist = True
        elif _url_looks_like_playlist(context.get("url")):
            allow_playlist = True

    if isinstance(output_template, dict) and not allow_chapter_outtmpl:
        default_template = output_template.get("default")
        if isinstance(default_template, str) and default_template.strip():
            output_template = default_template
        else:
            output_template = "%(title).200s-%(id)s.%(ext)s"

    opts = {
        "quiet": True,
        "no_warnings": True,
        # CLI parity: allow playlist expansion when requested; otherwise behave like a single-URL download.
        "noplaylist": False if allow_playlist else True,
        "outtmpl": output_template,
        # Avoid chapter workflows unless explicitly enabled.
        # (Chapter outtmpl dicts can trigger unexpected behavior in the Python API path.)
        "no_chapters": True if not allow_chapter_outtmpl else False,
        "retries": 3,
        "fragment_retries": 3,
        "overwrites": True,
    }

    cookie_file = context.get("cookie_file")
    # Cookies OFF by default. Only enable when explicitly allowed or when running in music mode.
    # This prevents YouTube/SABR edge cases that produced empty/403 fragment downloads in the worker path.
    allow_cookies = bool(context.get("allow_cookies")) or (
        bool(context.get("audio_mode")) and is_music_media_type(context.get("media_type"))
    )
    if cookie_file and allow_cookies:
        opts["cookiefile"] = cookie_file

    if operation == "playlist":
        opts["skip_download"] = True
        opts["extract_flat"] = True
    elif operation == "metadata":
        opts["skip_download"] = True
    else:
        # Only enable addmetadata, embedthumbnail, writethumbnail, and audio postprocessors
        # when both audio_mode and media_type is music/audio
        if audio_mode and is_music_media_type(context.get("media_type")):
            normalized_audio = _normalize_audio_format(target_format)
            if normalized_audio and normalized_audio in _AUDIO_FORMATS:
                opts["postprocessors"] = _build_audio_postprocessors(normalized_audio)
            else:
                opts["postprocessors"] = _build_audio_postprocessors(None)
            opts["format"] = _FORMAT_AUDIO
            opts["addmetadata"] = True
            opts["embedthumbnail"] = True
            opts["writethumbnail"] = True
        elif audio_mode:
            opts["format"] = _FORMAT_AUDIO
        else:
            normalized = _normalize_format(target_format)
            if normalized in {"webm", "mp4", "mkv"}:
                opts["format"] = "bestvideo+bestaudio/best"
                opts["merge_output_format"] = normalized
            elif normalized:
                opts["format"] = normalized
            else:
                opts["format"] = _FORMAT_VIDEO

    opts = _merge_overrides(opts, overrides, operation=operation, lock_format=bool(target_format))

    if operation == "download":
        for key in _YTDLP_DOWNLOAD_UNSAFE_KEYS:
            opts.pop(key, None)

    _log_event(
        logging.INFO,
        "audit_build_ytdlp_opts",
        operation=operation,
        format=opts.get("format"),
        audio_mode=audio_mode,
        media_type=context.get("media_type"),
        media_intent=context.get("media_intent"),
        final_format=target_format,
        postprocessors=bool(opts.get("postprocessors")),
        addmetadata=opts.get("addmetadata"),
        embedthumbnail=opts.get("embedthumbnail"),
        writethumbnail=opts.get("writethumbnail"),
        overrides=context.get("overrides"),
        allow_playlist=allow_playlist,
    )

    return opts


def _build_audio_postprocessors(target_format):
    preferred = _normalize_audio_format(target_format) or "mp3"
    if preferred not in _AUDIO_FORMATS:
        logging.warning("Unsupported audio format %s; defaulting to mp3", preferred)
        preferred = "mp3"
    return [
        {
            "key": "FFmpegExtractAudio",
            "preferredcodec": preferred,
            "preferredquality": "0",
        }
    ]


def _merge_overrides(opts, overrides, *, operation, lock_format=False):
    if not isinstance(overrides, dict):
        return opts
    unsafe = [key for key in overrides if key in _YTDLP_DOWNLOAD_UNSAFE_KEYS]
    if unsafe and operation == "download":
        logging.warning("Dropping unsafe yt_dlp_opts for download: %s", unsafe)
    for key, value in overrides.items():
        if operation == "download" and key in _YTDLP_DOWNLOAD_UNSAFE_KEYS:
            continue
        if operation == "download" and key not in _YTDLP_DOWNLOAD_ALLOWLIST:
            continue
        if lock_format and key in {"format", "merge_output_format"}:
            continue
        opts[key] = value
    return opts


def _redact_ytdlp_opts(opts):
    redacted = {}
    for key, value in (opts or {}).items():
        if key in {"cookiefile", "cookiesfrombrowser"}:
            redacted[key] = "<redacted>"
            continue
        if key in {"http_headers", "headers"}:
            redacted[key] = "<redacted>"
            continue
        redacted[key] = value
    return redacted


def _format_summary(info):
    if not isinstance(info, dict):
        return {"format_count": 0, "exts": [], "has_webm": False}
    formats = info.get("formats") or []
    ext_set = {fmt.get("ext") for fmt in formats if fmt.get("ext")}
    exts = sorted(ext_set)
    return {
        "format_count": len(formats),
        "exts": exts,
        "has_webm": "webm" in ext_set,
    }


def download_with_ytdlp(
    url,
    temp_dir,
    config,
    *,
    audio_mode,
    final_format,
    cookie_file=None,
    stop_event=None,
    media_type=None,
    media_intent=None,
    job_id=None,
    origin=None,
    resolved_destination=None,
):
    if stop_event and stop_event.is_set():
        return None, None
    # Use an ID-based template in temp_dir (CLI parity and avoids title/path edge cases).
    # The final user-facing name is applied later when we move/rename the completed file.
    output_template = os.path.join(temp_dir, "%(id)s.%(ext)s")
    context = {
        "operation": "download",
        "url": url,
        "audio_mode": audio_mode,
        "final_format": final_format,
        "output_template": output_template,
        "cookie_file": cookie_file,
        "overrides": (config or {}).get("yt_dlp_opts") or {},
        "media_type": media_type,
        "media_intent": media_intent,
        "origin": origin,
    }
    opts = build_ytdlp_opts(context)

    _log_event(
        logging.INFO,
        "FINAL_YTDLP_OPTS",
        job_id=job_id,
        url=url,
        origin=origin,
        media_type=media_type,
        media_intent=media_intent,
        audio_mode=audio_mode,
        resolved_destination=resolved_destination,
        final_format=final_format,
        format=opts.get("format"),
        merge_output_format=opts.get("merge_output_format"),
        outtmpl=opts.get("outtmpl"),
        noplaylist=opts.get("noplaylist"),
        postprocessors=opts.get("postprocessors"),
        cookiefile=opts.get("cookiefile"),
        opts=_redact_ytdlp_opts(opts),
    )

    # HARD GUARD: yt-dlp download path must use string outtmpl (CLI parity)
    outtmpl = opts.get("outtmpl")
    if isinstance(outtmpl, dict):
        logging.error(
            "COERCING_DICT_OUTTMPL download_with_ytdlp job_id=%s outtmpl=%r",
            job_id,
            outtmpl,
        )
        default_tmpl = outtmpl.get("default") if isinstance(outtmpl, dict) else None
        opts["outtmpl"] = (
            default_tmpl
            if isinstance(default_tmpl, str) and default_tmpl.strip()
            else "%(title).200s-%(id)s.%(ext)s"
        )

    logging.info(
        "PRE_YTDLP_EXEC job_id=%s outtmpl_type=%s outtmpl=%r",
        job_id,
        type(opts.get("outtmpl")).__name__,
        opts.get("outtmpl"),
    )

    def _is_empty_download_error(e: Exception) -> bool:
        msg = str(e) or ""
        msg_l = msg.lower()
        return (
            "downloaded file is empty" in msg_l
            or "http error 403" in msg_l
            or "forbidden" in msg_l
        )

    info = None
    try:
        # yt-dlp may mutate the options dict internally; always pass a copy.
        opts_for_run = dict(opts)
        # HARD GUARD again on the copy.
        if isinstance(opts_for_run.get("outtmpl"), dict):
            default_tmpl = opts_for_run["outtmpl"].get("default")
            opts_for_run["outtmpl"] = (
                default_tmpl
                if isinstance(default_tmpl, str) and default_tmpl.strip()
                else "%(title).200s-%(id)s.%(ext)s"
            )
        with YoutubeDL(opts_for_run) as ydl:
            info = ydl.extract_info(url, download=True)
    except Exception as exc:
        # If a cookiefile is present and yt-dlp reports an empty download, retry once WITHOUT cookies.
        # This matches the observed CLI behavior (no cookies) and avoids poisoning downloads with bad/expired cookies.
        if _is_empty_download_error(exc) and opts.get("cookiefile"):
            retry_opts = dict(opts)
            retry_opts.pop("cookiefile", None)
            # Keep worker logs quiet, but allow yt-dlp to behave normally.
            _log_event(
                logging.WARNING,
                "YTDLP_EMPTY_FILE_RETRY_NO_COOKIES",
                job_id=job_id,
                url=url,
                origin=origin,
                media_type=media_type,
                media_intent=media_intent,
                audio_mode=audio_mode,
                final_format=final_format,
                format=opts.get("format"),
                merge_output_format=opts.get("merge_output_format"),
                outtmpl=opts.get("outtmpl"),
                noplaylist=opts.get("noplaylist"),
            )
            try:
                retry_opts = dict(opts)
                retry_opts.pop("cookiefile", None)
                if isinstance(retry_opts.get("outtmpl"), dict):
                    default_tmpl = retry_opts["outtmpl"].get("default")
                    retry_opts["outtmpl"] = (
                        default_tmpl
                        if isinstance(default_tmpl, str) and default_tmpl.strip()
                        else "%(title).200s-%(id)s.%(ext)s"
                    )
                with YoutubeDL(retry_opts) as ydl:
                    info = ydl.extract_info(url, download=True)
            except Exception as retry_exc:
                _log_event(
                    logging.ERROR,
                    "YTDLP_EMPTY_FILE_RETRY_FAILED",
                    job_id=job_id,
                    url=url,
                    origin=origin,
                    media_type=media_type,
                    media_intent=media_intent,
                    audio_mode=audio_mode,
                    final_format=final_format,
                    error=str(retry_exc),
                    opts=_redact_ytdlp_opts(retry_opts),
                )
                # Fall through to normal failure handling using the original exception.

        if info is None:
            _log_event(
                logging.ERROR,
                "YTDLP_FAILED",
                job_id=job_id,
                url=url,
                origin=origin,
                media_type=media_type,
                media_intent=media_intent,
                audio_mode=audio_mode,
                final_format=final_format,
                format=opts.get("format"),
                merge_output_format=opts.get("merge_output_format"),
                outtmpl=opts.get("outtmpl"),
                noplaylist=opts.get("noplaylist"),
                error=str(exc),
                opts=_redact_ytdlp_opts(opts),
            )
            _log_event(
                logging.ERROR,
                "ytdlp_download_failed",
                job_id=job_id,
                url=url,
                origin=origin,
                media_type=media_type,
                media_intent=media_intent,
                audio_mode=audio_mode,
                final_format=final_format,
                format=opts.get("format"),
                merge_output_format=opts.get("merge_output_format"),
                outtmpl=opts.get("outtmpl"),
                noplaylist=opts.get("noplaylist"),
                error=str(exc),
            )
            if "Requested format is not available" in str(exc):
                try:
                    list_opts = dict(opts)
                    list_opts["skip_download"] = True
                    with YoutubeDL(list_opts) as ydl:
                        info_probe = ydl.extract_info(url, download=False)
                    summary = _format_summary(info_probe)
                    _log_event(
                        logging.ERROR,
                        "ytdlp_format_summary",
                        job_id=job_id,
                        url=url,
                        format=opts.get("format"),
                        merge_output_format=opts.get("merge_output_format"),
                        format_count=summary["format_count"],
                        exts=summary["exts"],
                        has_webm=summary["has_webm"],
                    )
                except Exception as probe_exc:
                    _log_event(
                        logging.ERROR,
                        "ytdlp_format_summary_failed",
                        job_id=job_id,
                        url=url,
                        format=opts.get("format"),
                        error=str(probe_exc),
                    )
            raise RuntimeError(f"yt_dlp_download_failed: {exc}")

    if stop_event and stop_event.is_set():
        return None, None

    local_path = None
    if isinstance(info, dict):
        local_path = info.get("_filename")
        if not local_path and info.get("requested_downloads"):
            for req in info.get("requested_downloads"):
                local_path = req.get("filepath") or req.get("filename")
                if local_path:
                    break
    if local_path and os.path.exists(local_path):
        return info, local_path

    candidates = []
    for entry in os.listdir(temp_dir):
        # Ignore yt-dlp temporary/partial artifacts
        if entry.endswith((".part", ".ytdl", ".temp")):
            continue
        candidate = os.path.join(temp_dir, entry)
        if os.path.isfile(candidate):
            try:
                size = os.path.getsize(candidate)
            except OSError:
                size = 0
            if size > 0:
                candidates.append((size, candidate))

    if candidates:
        candidates.sort(reverse=True)
        return info, candidates[0][1]

    raise RuntimeError("yt_dlp_no_output")


def preview_direct_url(url, config):
    cookie_file = resolve_cookie_file(config or {})
    context = {
        "operation": "metadata",
        "url": url,
        "audio_mode": False,
        "final_format": None,
        "output_template": None,
        "cookie_file": cookie_file,
        "overrides": (config or {}).get("yt_dlp_opts") or {},
        "media_type": "video",
        "media_intent": "episode",
    }
    opts = build_ytdlp_opts(context)
    with YoutubeDL(opts) as ydl:
        info = ydl.extract_info(url, download=False)
    meta = extract_meta(info, fallback_url=url)
    return {
        "title": meta.get("title"),
        "uploader": meta.get("channel") or meta.get("artist"),
        "thumbnail_url": meta.get("thumbnail_url"),
        "url": meta.get("url") or url,
        "source": resolve_source(url),
        "duration_sec": info.get("duration") if isinstance(info, dict) else None,
    }


def extract_meta(info, *, fallback_url=None):
    if not isinstance(info, dict):
        return {}
    tags = info.get("tags") or []
    if isinstance(tags, set):
        tags = sorted(tags)
    elif isinstance(tags, tuple):
        tags = list(tags)
    elif not isinstance(tags, list):
        tags = [str(tags)]
    return {
        "video_id": info.get("id"),
        "title": info.get("title"),
        "channel": info.get("channel") or info.get("uploader"),
        "artist": info.get("artist") or info.get("uploader"),
        "album": info.get("album"),
        "album_artist": info.get("album_artist"),
        "track": info.get("track") or info.get("title"),
        "track_number": info.get("track_number"),
        "disc": info.get("disc_number"),
        "release_date": info.get("release_date"),
        "upload_date": info.get("upload_date"),
        "description": info.get("description") or "",
        "tags": tags,
        "url": info.get("webpage_url") or fallback_url,
        "thumbnail_url": info.get("thumbnail"),
    }


def sanitize_for_filesystem(name, maxlen=180):
    if not name:
        return ""
    safe = re.sub(r"[\\/\\?%*:|\"<>]", "_", str(name)).strip()
    safe = re.sub(r"\s+", " ", safe)
    return safe[:maxlen].strip()


def pretty_filename(title, channel, upload_date):
    safe_title = sanitize_for_filesystem(title or "")
    safe_channel = sanitize_for_filesystem(channel or "")
    date = upload_date or ""
    if safe_channel and date:
        return f"{safe_title} - {safe_channel} - {date}".strip(" -")
    if safe_channel:
        return f"{safe_title} - {safe_channel}".strip(" -")
    if date:
        return f"{safe_title} - {date}".strip(" -")
    return safe_title or "media"


def _clean_audio_title(value):
    if not value:
        return ""
    cleaned = _AUDIO_TITLE_CLEAN_RE.sub(" ", value)
    cleaned = _AUDIO_TITLE_TRAIL_RE.sub("", cleaned)
    return re.sub(r"\s+", " ", cleaned).strip()


def _clean_audio_artist(value):
    if not value:
        return ""
    cleaned = _AUDIO_ARTIST_VEVO_RE.sub("", value).strip()
    return re.sub(r"\s+", " ", cleaned).strip()


def normalize_track_number(value):
    if value is None:
        return None
    if isinstance(value, int):
        return value
    value = str(value).strip()
    if not value:
        return None
    if value.isdigit():
        return int(value)
    match = re.match(r"(\d+)", value)
    if match:
        return int(match.group(1))
    return None


def format_track_number(value):
    normalized = normalize_track_number(value)
    if normalized is None:
        return ""
    return f"{normalized:02d}"


def build_audio_filename(meta, ext, *, template=None, fallback_id=None):
    artist = sanitize_for_filesystem(_clean_audio_artist(meta.get("artist") or ""))
    album = sanitize_for_filesystem(_clean_audio_title(meta.get("album") or ""))
    track = sanitize_for_filesystem(_clean_audio_title(meta.get("track") or meta.get("title") or ""))
    track_number = format_track_number(meta.get("track_number"))
    fallback = (fallback_id or "media")[:8]

    fmt = {
        "artist": artist,
        "album": album,
        "track": track,
        "track_number": track_number,
        "ext": ext,
        "id": fallback,
    }

    if template:
        try:
            rendered = template % fmt
            rendered = rendered.strip("/\t ")
            if rendered:
                return rendered
        except Exception:
            pass

    if artist and album:
        if track_number:
            return f"{artist}/{album}/{track_number} - {track}.{ext}"
        return f"{artist}/{album}/{track}.{ext}"
    if artist:
        if track_number:
            return f"{artist}/{track_number} - {track}.{ext}"
        return f"{artist}/{track}.{ext}"
    return f"{track or fallback}.{ext}"


def build_output_filename(meta, fallback_id, ext, template, audio_mode):
    if audio_mode:
        return build_audio_filename(meta, ext, template=template, fallback_id=fallback_id)
    if template:
        try:
            rendered = template % {
                "title": sanitize_for_filesystem(meta.get("title") or fallback_id),
                "uploader": sanitize_for_filesystem(meta.get("channel") or ""),
                "upload_date": meta.get("upload_date") or "",
                "ext": ext,
                "id": fallback_id,
            }
            if rendered:
                return rendered
        except Exception:
            pass
    return f"{pretty_filename(meta.get('title'), meta.get('channel'), meta.get('upload_date'))}_{fallback_id[:8]}.{ext}"


def atomic_move(src, dst):
    try:
        os.replace(src, dst)
    except OSError:
        shutil.copy2(src, dst)
        os.remove(src)


def embed_metadata(local_file, meta, video_id, thumbs_dir):
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

    date_tag = ""
    raw_date = release_date or upload_date
    if raw_date and len(str(raw_date)) == 8 and str(raw_date).isdigit():
        raw_date = str(raw_date)
        date_tag = f"{raw_date[0:4]}-{raw_date[4:6]}-{raw_date[6:8]}"

    keywords = ", ".join(tags) if tags else ""
    comment = f"YouTubeID={video_id} URL={url}"

    thumb_path = None
    if thumb_url and thumbs_dir:
        try:
            os.makedirs(thumbs_dir, exist_ok=True)
            thumb_path = os.path.join(thumbs_dir, f"{video_id}.jpg")
            resp = requests.get(thumb_url, timeout=15)
            if resp.ok and resp.content:
                with open(thumb_path, "wb") as handle:
                    handle.write(resp.content)
            else:
                thumb_path = None
        except Exception:
            logging.exception("Thumbnail download failed for %s", video_id)
            thumb_path = None

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

        if thumb_path and os.path.exists(thumb_path) and not audio_only:
            cmd.extend([
                "-attach",
                thumb_path,
                "-metadata:s:t",
                "mimetype=image/jpeg",
                "-metadata:s:t",
                "filename=cover.jpg",
            ])

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


def enqueue_media_metadata(file_path, meta, config):
    try:
        enqueue_metadata(file_path, meta, config)
    except Exception:
        logging.exception("Metadata enqueue failed for %s", file_path)


def record_download_history(db_path, job, filepath, *, meta=None):
    if not filepath:
        return
    video_id = None
    if meta:
        video_id = meta.get("video_id")
    if not video_id:
        video_id = extract_video_id(job.url) or job.id
    playlist_id = job.origin_id if job.origin == "playlist" else None
    conn = sqlite3.connect(db_path, check_same_thread=False)
    try:
        ensure_downloads_table(conn)
        cur = conn.cursor()
        cur.execute(
            "INSERT OR REPLACE INTO downloads (video_id, playlist_id, downloaded_at, filepath) VALUES (?, ?, ?, ?)",
            (video_id, playlist_id, utc_now(), filepath),
        )
        conn.commit()
    finally:
        conn.close()


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


def is_retryable_error(error):
    if isinstance(error, TypeError):
        return False
    if isinstance(error, (DownloadError, ExtractorError)):
        message = str(error).lower()
    else:
        message = str(error).lower()
    if "json serializable" in message or "not json" in message:
        return False
    if "drm" in message:
        return False
    if "http error 403" in message or "http error 404" in message:
        return False
    if "403" in message and "http" in message:
        return False
    if "404" in message and "http" in message:
        return False
    if "not available" in message or "private" in message:
        return False
    if "unavailable" in message and "region" in message:
        return False
    if "extractor" in message or "unable to extract" in message:
        return True
    if "timed out" in message or "timeout" in message:
        return True
    if "temporary failure" in message or "connection reset" in message:
        return True
    return True
