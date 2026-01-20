import json
import logging
import os
import sqlite3
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from uuid import uuid4

from engine.job_queue import DownloadJobStore, build_output_template, ensure_download_jobs_table
from engine.paths import DATA_DIR
from engine.search_adapters import default_adapters
from engine.search_scoring import rank_candidates, score_candidate, select_best_candidate
from metadata.canonical import CanonicalMetadataResolver

REQUEST_STATUSES = {"pending", "resolving", "completed", "completed_with_skips", "failed"}
ITEM_STATUSES = {
    "queued",
    "searching",
    "candidate_found",
    "selected",
    "enqueued",
    "skipped",
    "failed",
}

DEFAULT_SOURCE_PRIORITY = ["bandcamp", "youtube_music", "soundcloud"]


@dataclass(frozen=True)
class SearchRequest:
    id: str
    intent: str
    media_type: str
    artist: str
    album: str | None
    track: str | None
    destination_dir: str | None
    auto_enqueue: bool
    min_match_score: float
    duration_hint_sec: int | None
    source_priority: list[str]
    max_candidates_per_source: int



def _utc_now():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _log_event(level, message, **fields):
    payload = {"message": message, **fields}
    logging.log(level, json.dumps(payload, sort_keys=True))


def resolve_search_db_path(queue_db_path, config=None):
    env_path = os.environ.get("RETREIVR_SEARCH_DB_PATH")
    if env_path:
        return os.path.abspath(env_path)
    if isinstance(config, dict):
        override = config.get("search_db_path")
        if override:
            return os.path.abspath(override)
    base_dir = os.path.dirname(queue_db_path) or DATA_DIR
    return os.path.join(base_dir, "search_jobs.sqlite")


def _normalize_media_type(value, *, default="generic"):
    # Invariant A: media_type defaults to general/generic unless explicitly set.
    if value is None:
        return default
    value = str(value).strip().lower()
    if not value:
        return default
    if value in {"music", "audio"}:
        return "music"
    if value == "video":
        return "video"
    if value in {"generic", "general"}:
        return "generic"
    return None


def _extract_canonical_id(metadata):
    if not isinstance(metadata, dict):
        return None
    external_ids = metadata.get("external_ids") or {}
    for key in ("spotify_id", "isrc", "musicbrainz_recording_id", "musicbrainz_release_id"):
        value = external_ids.get(key)
        if value:
            return str(value)
    return None



def ensure_search_tables(conn):
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS search_requests (
            id TEXT PRIMARY KEY,
            created_at TEXT,
            updated_at TEXT,
            created_by TEXT,
            intent TEXT,
            media_type TEXT,
            artist TEXT,
            album TEXT,
            track TEXT,
            destination_dir TEXT,
            include_albums INTEGER DEFAULT 1,
            include_singles INTEGER DEFAULT 1,
            min_match_score REAL DEFAULT 0.92,
            duration_hint_sec INTEGER,
            quality_min_bitrate_kbps INTEGER,
            lossless_only INTEGER DEFAULT 0,
            auto_enqueue INTEGER DEFAULT 1,
            source_priority_json TEXT,
            max_candidates_per_source INTEGER DEFAULT 5,
            status TEXT,
            error TEXT
        )
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_search_requests_status ON search_requests (status)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_search_requests_created ON search_requests (created_at)")
    cur.execute("PRAGMA table_info(search_requests)")
    existing_requests = {row[1] for row in cur.fetchall()}
    if "destination_dir" not in existing_requests:
        cur.execute("ALTER TABLE search_requests ADD COLUMN destination_dir TEXT")
    if "auto_enqueue" not in existing_requests:
        cur.execute("ALTER TABLE search_requests ADD COLUMN auto_enqueue INTEGER DEFAULT 1")


    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS search_items (
            id TEXT PRIMARY KEY,
            request_id TEXT,
            position INTEGER,
            item_type TEXT,
            media_type TEXT,
            artist TEXT,
            album TEXT,
            track TEXT,
            duration_hint_sec INTEGER,
            status TEXT,
            chosen_source TEXT,
            chosen_url TEXT,
            chosen_score REAL,
            error TEXT,
            FOREIGN KEY (request_id) REFERENCES search_requests(id)
        )
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_search_items_request_status ON search_items (request_id, status)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_search_items_status ON search_items (status)")

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS search_candidates (
            id TEXT PRIMARY KEY,
            item_id TEXT,
            source TEXT,
            url TEXT,
            title TEXT,
            uploader TEXT,
            artist_detected TEXT,
            album_detected TEXT,
            track_detected TEXT,
            duration_sec INTEGER,
            artwork_url TEXT,
            raw_meta_json TEXT,
            canonical_json TEXT,
            score_artist REAL,
            score_track REAL,
            score_album REAL,
            score_duration REAL,
            source_modifier REAL,
            penalty_multiplier REAL,
            final_score REAL,
            rank INTEGER,
            FOREIGN KEY (item_id) REFERENCES search_items(id)
        )
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_search_candidates_item_score ON search_candidates (item_id, final_score DESC)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_search_candidates_source ON search_candidates (source)")

    cur.execute("PRAGMA table_info(search_candidates)")
    existing = {row[1] for row in cur.fetchall()}
    if "canonical_json" not in existing:
        cur.execute("ALTER TABLE search_candidates ADD COLUMN canonical_json TEXT")

    conn.commit()


class SearchJobStore:
    def __init__(self, db_path):
        self.db_path = db_path

    def _connect(self):
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        return conn

    def ensure_schema(self):
        conn = self._connect()
        try:
            ensure_search_tables(conn)
        finally:
            conn.close()

    def create_request(self, payload):
        if not isinstance(payload, dict):
            raise ValueError("payload must be an object")
        intent = payload.get("intent")
        if intent not in {"track", "album", "artist", "artist_collection"}:
            raise ValueError("intent must be track, album, artist, or artist_collection")
        media_type = _normalize_media_type(payload.get("media_type") or "generic")
        if media_type not in {"music", "video", "generic"}:
            raise ValueError("media_type must be music, video, or generic")
        artist = payload.get("artist")
        if not artist:
            raise ValueError("artist is required")
        if intent == "track" and not payload.get("track"):
            raise ValueError("track is required for track intent")
        if intent == "album" and not payload.get("album"):
            raise ValueError("album is required for album intent")

        req_id = uuid4().hex
        now = _utc_now()
        source_priority = _normalize_source_priority(payload.get("source_priority"))
        min_match_score = float(payload.get("min_match_score") or 0.92)
        max_candidates = int(payload.get("max_candidates_per_source") or 5)
        destination_dir = payload.get("destination_dir")
        if destination_dir is not None:
            destination_dir = str(destination_dir).strip() or None
        auto_enqueue = bool(payload.get("auto_enqueue", True))

        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute(
                """
                INSERT INTO search_requests (
                    id, created_at, updated_at, created_by, intent, media_type, artist,
                    album, track, destination_dir, include_albums, include_singles, min_match_score,
                    duration_hint_sec, quality_min_bitrate_kbps, lossless_only, auto_enqueue,
                    source_priority_json, max_candidates_per_source, status, error
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    req_id,
                    now,
                    now,
                    payload.get("created_by") or "api",
                    intent,
                    media_type,
                    artist,
                    payload.get("album"),
                    payload.get("track"),
                    destination_dir,
                    1 if payload.get("include_albums", True) else 0,
                    1 if payload.get("include_singles", True) else 0,
                    min_match_score,
                    payload.get("duration_hint_sec"),
                    payload.get("quality_min_bitrate_kbps"),
                    1 if payload.get("lossless_only") else 0,
                    1 if auto_enqueue else 0,
                    json.dumps(source_priority),
                    max_candidates,
                    "pending",
                    None,
                ),
            )
            conn.commit()
        finally:
            conn.close()
        return req_id

    def get_request(self, request_id):
        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute("SELECT * FROM search_requests WHERE id=?", (request_id,))
            row = cur.fetchone()
            if not row:
                return None
            request = dict(row)
            request["source_priority"] = _parse_source_priority(row["source_priority_json"])

            cur.execute("SELECT * FROM search_items WHERE request_id=? ORDER BY position", (request_id,))
            items = [dict(item) for item in cur.fetchall()]
            for item in items:
                cur.execute(
                    "SELECT COUNT(*) FROM search_candidates WHERE item_id=?",
                    (item["id"],),
                )
                item["candidate_count"] = cur.fetchone()[0]
            return {"request": request, "items": items}
        finally:
            conn.close()

    def get_request_row(self, request_id):
        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute("SELECT * FROM search_requests WHERE id=?", (request_id,))
            row = cur.fetchone()
            return dict(row) if row else None
        finally:
            conn.close()

    def get_item(self, item_id):
        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute("SELECT * FROM search_items WHERE id=?", (item_id,))
            row = cur.fetchone()
            return dict(row) if row else None
        finally:
            conn.close()

    def get_candidate(self, candidate_id):
        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute("SELECT * FROM search_candidates WHERE id=?", (candidate_id,))
            row = cur.fetchone()
            return dict(row) if row else None
        finally:
            conn.close()

    def list_requests(self, *, status=None, limit=None):
        conn = self._connect()
        try:
            cur = conn.cursor()
            params = []
            query = "SELECT * FROM search_requests"
            if status:
                query += " WHERE status=?"
                params.append(status)
            query += " ORDER BY created_at ASC"
            if limit:
                query += " LIMIT ?"
                params.append(limit)
            cur.execute(query, params)
            rows = []
            for row in cur.fetchall():
                entry = dict(row)
                entry["source_priority"] = _parse_source_priority(row["source_priority_json"])
                entry["resolved_destination"] = self._resolve_request_destination(entry.get("destination_dir"))
                rows.append(entry)
            return rows
        finally:
            conn.close()

    def cancel_request(self, request_id):
        conn = self._connect()
        try:
            cur = conn.cursor()
            now = _utc_now()
            cur.execute(
                """
                UPDATE search_requests
                SET status=?, updated_at=?, error=?
                WHERE id=? AND status NOT IN ('completed', 'completed_with_skips', 'failed')
                """,
                ("failed", now, "request_canceled", request_id),
            )
            cur.execute(
                """
                UPDATE search_items
                SET status=?, error=?
                WHERE request_id=? AND status NOT IN ('enqueued', 'failed')
                """,
                ("skipped", "request_canceled", request_id),
            )
            conn.commit()
            return cur.rowcount > 0
        finally:
            conn.close()

    def claim_next_request(self):
        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute("BEGIN IMMEDIATE")
            cur.execute(
                "SELECT * FROM search_requests WHERE status=? ORDER BY created_at ASC LIMIT 1",
                ("pending",),
            )
            row = cur.fetchone()
            if not row:
                conn.commit()
                return None
            now = _utc_now()
            cur.execute(
                "UPDATE search_requests SET status=?, updated_at=? WHERE id=? AND status=?",
                ("resolving", now, row["id"], "pending"),
            )
            if cur.rowcount != 1:
                conn.commit()
                return None
            conn.commit()
            return dict(row)
        finally:
            conn.close()

    def claim_request(self, request_id):
        if not request_id:
            return self.claim_next_request()
        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute("BEGIN IMMEDIATE")
            cur.execute(
                "SELECT * FROM search_requests WHERE id=? AND status=?",
                (request_id, "pending"),
            )
            row = cur.fetchone()
            if not row:
                conn.commit()
                return None
            now = _utc_now()
            cur.execute(
                "UPDATE search_requests SET status=?, updated_at=? WHERE id=? AND status=?",
                ("resolving", now, request_id, "pending"),
            )
            if cur.rowcount != 1:
                conn.commit()
                return None
            conn.commit()
            return dict(row)
        finally:
            conn.close()

    def create_items_for_request(self, request_row):
        req_id = request_row["id"]
        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM search_items WHERE request_id=?", (req_id,))
            if cur.fetchone()[0] > 0:
                return
            item_type = "track" if request_row["intent"] == "track" else "album"
            item_id = uuid4().hex
            media_type = _normalize_media_type(request_row.get("media_type")) or "generic"
            cur.execute(
                """
                INSERT INTO search_items (
                    id, request_id, position, item_type, media_type, artist,
                    album, track, duration_hint_sec, status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    item_id,
                    req_id,
                    1,
                    item_type,
                    media_type,
                    request_row["artist"],
                    request_row["album"],
                    request_row["track"],
                    request_row["duration_hint_sec"],
                    "queued",
                ),
            )
            conn.commit()
        finally:
            conn.close()

    def list_candidates(self, item_id):
        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT * FROM search_candidates WHERE item_id=? ORDER BY rank",
                (item_id,),
            )
            rows = []
            for row in cur.fetchall():
                entry = dict(row)
                canonical_raw = entry.get("canonical_json")
                if canonical_raw:
                    try:
                        entry["canonical_metadata"] = json.loads(canonical_raw)
                    except json.JSONDecodeError:
                        entry["canonical_metadata"] = None
                rows.append(entry)
            return rows
        finally:
            conn.close()

    def list_items(self, request_id):
        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute("SELECT * FROM search_items WHERE request_id=? ORDER BY position", (request_id,))
            return [dict(item) for item in cur.fetchall()]
        finally:
            conn.close()

    def reset_candidates_for_item(self, item_id):
        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute("DELETE FROM search_candidates WHERE item_id=?", (item_id,))
            conn.commit()
        finally:
            conn.close()

    def insert_candidates(self, item_id, candidates):
        conn = self._connect()
        try:
            cur = conn.cursor()
            for candidate in candidates:
                cur.execute(
                    """
                    INSERT INTO search_candidates (
                        id, item_id, source, url, title, uploader, artist_detected,
                        album_detected, track_detected, duration_sec, artwork_url,
                        raw_meta_json, canonical_json, score_artist, score_track, score_album,
                        score_duration, source_modifier, penalty_multiplier, final_score, rank
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        candidate["id"],
                        item_id,
                        candidate.get("source"),
                        candidate.get("url"),
                        candidate.get("title"),
                        candidate.get("uploader"),
                        candidate.get("artist_detected"),
                        candidate.get("album_detected"),
                        candidate.get("track_detected"),
                        candidate.get("duration_sec"),
                        candidate.get("artwork_url"),
                        candidate.get("raw_meta_json"),
                        candidate.get("canonical_json"),
                        candidate.get("score_artist"),
                        candidate.get("score_track"),
                        candidate.get("score_album"),
                        candidate.get("score_duration"),
                        candidate.get("source_modifier"),
                        candidate.get("penalty_multiplier"),
                        candidate.get("final_score"),
                        candidate.get("rank"),
                    ),
                )
            conn.commit()
        finally:
            conn.close()

    def update_item_status(self, item_id, status, *, chosen=None, error=None):
        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute(
                """
                UPDATE search_items
                SET status=?, chosen_source=?, chosen_url=?, chosen_score=?, error=?
                WHERE id=?
                """,
                (
                    status,
                    chosen.get("source") if chosen else None,
                    chosen.get("url") if chosen else None,
                    chosen.get("final_score") if chosen else None,
                    error,
                    item_id,
                ),
            )
            conn.commit()
        finally:
            conn.close()

    def update_request_status(self, request_id, status, *, error=None):
        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute(
                "UPDATE search_requests SET status=?, updated_at=?, error=? WHERE id=?",
                (status, _utc_now(), error, request_id),
            )
            conn.commit()
        finally:
            conn.close()


class SearchResolutionService:
    def __init__(self, *, search_db_path, queue_db_path, adapters=None, config=None, paths=None, canonical_resolver=None):
        self.search_db_path = search_db_path
        self.queue_db_path = queue_db_path
        self.adapters = adapters or default_adapters()
        self.config = config or {}
        self.paths = paths
        self.store = SearchJobStore(search_db_path)
        self.store.ensure_schema()
        self.queue_store = DownloadJobStore(queue_db_path)
        self._ensure_queue_schema()
        self.canonical_resolver = canonical_resolver or CanonicalMetadataResolver(config=self.config)

    def _resolve_request_destination(self, destination):
        if not self.paths:
            return destination
        try:
            template = build_output_template(
                self.config,
                destination=destination,
                base_dir=self.paths.single_downloads_dir,
            )
            return template.get("output_dir")
        except ValueError:
            return destination


    def _ensure_queue_schema(self):
        conn = sqlite3.connect(self.queue_db_path)
        try:
            ensure_download_jobs_table(conn)
        finally:
            conn.close()

    def create_search_request(self, payload):
        return self.store.create_request(payload)

    def get_search_request(self, request_id):
        result = self.store.get_request(request_id)
        if not result:
            return None
        request = result.get("request") or {}
        request["resolved_destination"] = self._resolve_request_destination(request.get("destination_dir"))
        return result

    def list_item_candidates(self, item_id):
        return self.store.list_candidates(item_id)

    def list_search_requests(self, status=None, limit=None):
        return self.store.list_requests(status=status, limit=limit)

    def cancel_search_request(self, request_id):
        return self.store.cancel_request(request_id)

    def run_search_resolution_once(self, *, stop_event=None, request_id=None):
        request_row = self.store.claim_request(request_id)
        if not request_row:
            return None

        request_id = request_row["id"]
        intent = request_row["intent"]

        auto_enqueue_value = request_row.get("auto_enqueue")
        auto_enqueue = True if auto_enqueue_value is None else bool(auto_enqueue_value)
        destination_dir = request_row.get("destination_dir") or None
        created_by = (request_row.get("created_by") or "").strip()
        playlist_id = None
        if created_by.startswith("spotify_playlist:"):
            playlist_id = created_by.split(":", 1)[1]
        job_origin = "spotify_playlist" if playlist_id else "search"
        if intent in {"artist", "artist_collection"}:
            self.store.update_request_status(request_id, "failed", error="not_implemented")
            _log_event(logging.WARNING, "request_failed", request_id=request_id, error="not_implemented")
            return request_id

        self.store.create_items_for_request(request_row)
        items = self.store.list_items(request_id)

        for item in items:
            if stop_event and stop_event.is_set():
                return request_id
            if item.get("status") in {"enqueued", "failed", "skipped"}:
                continue
            self.store.update_item_status(item["id"], "searching")
            _log_event(logging.INFO, "item_searching", request_id=request_id, item_id=item["id"])

            canonical_payload = None
            if self.canonical_resolver:
                if item["item_type"] == "album":
                    canonical_payload = self.canonical_resolver.resolve_album(item["artist"], item.get("album"))
                else:
                    canonical_payload = self.canonical_resolver.resolve_track(
                        item["artist"],
                        item.get("track"),
                        album=item.get("album"),
                    )

            source_priority = _parse_source_priority(request_row.get("source_priority_json"))
            max_candidates = int(request_row.get("max_candidates_per_source") or 5)
            scored = []

            for source in source_priority:
                adapter = self.adapters.get(source)
                if not adapter:
                    _log_event(logging.ERROR, "adapter_missing", request_id=request_id, item_id=item["id"], source=source)
                    continue

                if item["item_type"] == "album":
                    candidates = adapter.search_album(item["artist"], item.get("album"), max_candidates)
                else:
                    candidates = adapter.search_track(
                        item["artist"],
                        item.get("track"),
                        item.get("album"),
                        max_candidates,
                    )

                for cand in candidates:
                    cand["source"] = source
                    cand["canonical_metadata"] = canonical_payload
                    modifier = adapter.source_modifier(cand)
                    scores = score_candidate(item, cand, source_modifier=modifier)
                    cand.update(scores)
                    cand["canonical_json"] = json.dumps(canonical_payload) if canonical_payload else None
                    cand["id"] = uuid4().hex
                    scored.append(cand)

            if not scored:
                self.store.update_item_status(item["id"], "failed", error="no_candidates")
                _log_event(logging.WARNING, "item_failed", request_id=request_id, item_id=item["id"], error="no_candidates")
                continue

            ranked = rank_candidates(scored, source_priority=source_priority)
            self.store.reset_candidates_for_item(item["id"])
            self.store.insert_candidates(item["id"], ranked)
            self.store.update_item_status(item["id"], "candidate_found")
            _log_event(logging.INFO, "item_candidate_found", request_id=request_id, item_id=item["id"])

            min_score = float(request_row.get("min_match_score") or 0.92)
            request_media_type = request_row.get("media_type") or "generic"
            is_music_request = request_media_type == "music"
            selection_threshold = min_score if is_music_request else 0.0
            chosen = select_best_candidate(ranked, selection_threshold)
            if not chosen:
                self.store.update_item_status(item["id"], "failed", error="no_candidate_above_threshold")
                _log_event(
                    logging.WARNING,
                    "item_failed",
                    request_id=request_id,
                    item_id=item["id"],
                    error="no_candidate_above_threshold",
                )
                continue

            self.store.update_item_status(item["id"], "selected", chosen=chosen)
            _log_event(
                logging.INFO,
                "item_selected",
                request_id=request_id,
                item_id=item["id"],
                source=chosen.get("source"),
                score=chosen.get("final_score"),
            )

            if not auto_enqueue:
                # Invariant B: search-only requests never enqueue download jobs.
                continue

            output_template = None
            resolved_destination = None
            if self.paths is not None:
                try:
                    output_template = build_output_template(
                        self.config,
                        destination=destination_dir,
                        base_dir=self.paths.single_downloads_dir,
                    )
                    resolved_destination = output_template.get("output_dir")
                except ValueError as exc:
                    self.store.update_item_status(item["id"], "failed", error="invalid_destination")
                    _log_event(
                        logging.ERROR,
                        "item_failed",
                        request_id=request_id,
                        item_id=item["id"],
                        error=f"invalid_destination: {exc}",
                    )
                    continue

            canonical_for_job = chosen.get("canonical_metadata") if isinstance(chosen, dict) else None
            if not canonical_for_job and isinstance(chosen, dict) and chosen.get("canonical_json"):
                try:
                    canonical_for_job = json.loads(chosen.get("canonical_json"))
                except json.JSONDecodeError:
                    canonical_for_job = None
            if canonical_for_job:
                if output_template is None:
                    output_template = {}
                output_template["canonical_metadata"] = canonical_for_job

            canonical_id = _extract_canonical_id(canonical_for_job or chosen)
            trace_id = uuid4().hex
            media_intent = "album" if item["item_type"] == "album" else "track"
            job_id, created, dedupe_reason = self.queue_store.enqueue_job(
                origin=job_origin,
                origin_id=request_id,
                media_type=item["media_type"],
                media_intent=media_intent,
                source=chosen["source"],
                url=chosen["url"],
                output_template=output_template,
                trace_id=trace_id,
                resolved_destination=resolved_destination,
                canonical_id=canonical_id,
            )

            if not created and dedupe_reason == "duplicate":
                self.store.update_item_status(
                    item["id"],
                    "skipped",
                    chosen=chosen,
                    error="duplicate",
                )
                _log_event(
                    logging.INFO,
                    "job_skipped_duplicate",
                    request_id=request_id,
                    item_id=item["id"],
                    job_id=job_id,
                    source=chosen.get("source"),
                    origin=job_origin,
                    destination=resolved_destination,
                    media_type=item["media_type"],
                    dedupe_result=dedupe_reason,
                )
                continue

            if created:
                self.store.update_item_status(item["id"], "enqueued", chosen=chosen)
                _log_event(
                    logging.INFO,
                    "job_enqueued",
                    request_id=request_id,
                    item_id=item["id"],
                    job_id=job_id,
                    trace_id=trace_id,
                    source=chosen.get("source"),
                    origin=job_origin,
                    destination=resolved_destination,
                    media_type=item["media_type"],
                )
                if job_origin == "spotify_playlist" and playlist_id:
                    _log_event(
                        logging.INFO,
                        "search_enqueued",
                        playlist_id=playlist_id,
                        request_id=request_id,
                        job_id=job_id,
                        source=chosen.get("source"),
                        destination=resolved_destination,
                    )
            else:
                self.store.update_item_status(item["id"], "enqueued", chosen=chosen)
            _log_event(
                logging.INFO,
                "job_exists",
                request_id=request_id,
                item_id=item["id"],
                job_id=job_id,
                source=chosen.get("source"),
                destination=resolved_destination,
                media_type=item["media_type"],
            )

        items = self.store.list_items(request_id)
        if not auto_enqueue:
            has_candidates = any(
                item.get("status") in {"candidate_found", "selected", "enqueued", "skipped"}
                or item.get("error") == "no_candidate_above_threshold"
                for item in items
            )
            if has_candidates:
                self.store.update_request_status(request_id, "completed")
            else:
                self.store.update_request_status(request_id, "failed", error="no_candidates")
            return request_id

        has_enqueued = any(item.get("status") == "enqueued" for item in items)
        has_skipped = any(item.get("status") == "skipped" for item in items)
        if has_enqueued:
            status_value = "completed_with_skips" if has_skipped else "completed"
            self.store.update_request_status(request_id, status_value)
        elif has_skipped:
            self.store.update_request_status(request_id, "completed_with_skips")
        else:
            self.store.update_request_status(request_id, "failed", error="no_items_enqueued")
        return request_id

    def enqueue_item_candidate(self, item_id, candidate_id):
        item = self.store.get_item(item_id)
        if not item:
            return None
        candidate = self.store.get_candidate(candidate_id)
        if not candidate or candidate.get("item_id") != item_id:
            return None
        if not candidate.get("url"):
            return None
        request = self.store.get_request_row(item.get("request_id"))
        if not request:
            return None
        if request.get("auto_enqueue") in (0, False):
            # Invariant B: search-only requests must never enqueue jobs.
            raise ValueError("search_only_request_no_enqueue")

        destination_dir = request.get("destination_dir") or None
        output_template = None
        if self.paths is not None:
            try:
                output_template = build_output_template(
                    self.config,
                    destination=destination_dir,
                    base_dir=self.paths.single_downloads_dir,
                )
            except ValueError as exc:
                raise ValueError(f"invalid_destination: {exc}")

        canonical_payload = None
        canonical_raw = candidate.get("canonical_json")
        if canonical_raw:
            try:
                canonical_payload = json.loads(canonical_raw)
            except json.JSONDecodeError:
                canonical_payload = None
        if canonical_payload:
            if output_template is None:
                output_template = {}
            output_template["canonical_metadata"] = canonical_payload

        resolved_destination = output_template.get("output_dir") if output_template else None
        canonical_id = _extract_canonical_id(canonical_payload or candidate)
        trace_id = uuid4().hex
        media_intent = "album" if item.get("item_type") == "album" else "track"
        job_id, created, dedupe_reason = self.queue_store.enqueue_job(
            origin="search",
            origin_id=request["id"],
            media_type=item.get("media_type") or "generic",
            media_intent=media_intent,
            source=candidate.get("source"),
            url=candidate.get("url"),
            output_template=output_template,
            trace_id=trace_id,
            resolved_destination=resolved_destination,
            canonical_id=canonical_id,
        )

        if not created and dedupe_reason == "duplicate":
            self.store.update_item_status(
                item_id,
                "skipped",
                chosen=candidate,
                error="duplicate",
            )
            _log_event(
                logging.INFO,
                "job_skipped_duplicate",
                request_id=request["id"],
                item_id=item_id,
                job_id=job_id,
                source=candidate.get("source"),
                origin="search",
                destination=resolved_destination,
                media_type=item.get("media_type"),
                dedupe_result=dedupe_reason,
            )
            return {"job_id": job_id, "created": False}

        self.store.update_item_status(item_id, "enqueued", chosen=candidate)
        if created:
            _log_event(
                logging.INFO,
                "job_enqueued",
                request_id=request["id"],
                item_id=item_id,
                job_id=job_id,
                trace_id=trace_id,
                source=candidate.get("source"),
                origin="search",
                destination=resolved_destination,
                media_type=item.get("media_type"),
            )
        else:
            _log_event(
                logging.INFO,
                "job_exists",
                request_id=request["id"],
                item_id=item_id,
                job_id=job_id,
                source=candidate.get("source"),
                destination=resolved_destination,
                media_type=item.get("media_type"),
            )

        if request.get("status") not in {"completed", "completed_with_skips", "failed"}:
            self.store.update_request_status(request["id"], "completed")

        return {"job_id": job_id, "created": created}

    def run_search_resolution_loop(self, *, poll_seconds=5, stop_event=None):
        while True:
            if stop_event and stop_event.is_set():
                return
            request_id = self.run_search_resolution_once(stop_event=stop_event)
            if request_id is None:
                time.sleep(poll_seconds)


def _parse_source_priority(raw_value):
    if not raw_value:
        return list(DEFAULT_SOURCE_PRIORITY)
    if isinstance(raw_value, list):
        return [str(item) for item in raw_value if item]
    if isinstance(raw_value, str):
        try:
            parsed = json.loads(raw_value)
            if isinstance(parsed, list):
                return [str(item) for item in parsed if item]
        except json.JSONDecodeError:
            parts = [part.strip() for part in raw_value.split(",")]
            return [part for part in parts if part]
    return list(DEFAULT_SOURCE_PRIORITY)


def _normalize_source_priority(raw_value):
    if raw_value is None:
        return list(DEFAULT_SOURCE_PRIORITY)
    if isinstance(raw_value, list):
        return [str(item) for item in raw_value if item]
    if isinstance(raw_value, str):
        parts = [part.strip() for part in raw_value.split(",")]
        return [part for part in parts if part]
    return list(DEFAULT_SOURCE_PRIORITY)
