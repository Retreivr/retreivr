import logging
import os
import threading
import time

from . import matcher
from .providers import acoustid as acoustid_provider
from .providers import artwork as artwork_provider
from .providers import musicbrainz as musicbrainz_provider
from .tagger import apply_tags
from .lyric_enrichment import fetch_lyrics


class MetadataWorker(threading.Thread):
    def __init__(self, work_queue):
        super().__init__(daemon=True)
        self._queue = work_queue

    def run(self):
        while True:
            item = self._queue.get()
            try:
                _process_item(item)
            except Exception:
                logging.exception("Music metadata worker failed")
            finally:
                self._queue.task_done()
            rate_limit = item.get("config", {}).get("rate_limit_seconds", 1.5)
            try:
                rate = float(rate_limit)
            except (TypeError, ValueError):
                rate = 1.5
            if rate > 0:
                time.sleep(rate)


def _process_item(item):
    file_path = item.get("file_path")
    if not file_path or not os.path.exists(file_path):
        logging.warning("Music metadata skipped: file missing (%s)", file_path)
        return
    config = item.get("config") or {}
    meta = item.get("meta") or {}
    source = matcher.parse_source(meta, file_path)
    if not source.get("title") or not source.get("artist"):
        logging.warning("Music metadata skipped: missing source artist/title (%s)", file_path)
        return

    duration = matcher.get_duration_seconds(file_path)
    candidates = musicbrainz_provider.search_recordings(
        source["artist"],
        source["title"],
        album=source.get("album"),
    )

    if config.get("use_acoustid"):
        api_key = (config.get("acoustid_api_key") or "").strip()
        if api_key:
            acoustid_hit = acoustid_provider.match_recording(file_path, api_key)
            if acoustid_hit:
                candidates = matcher.merge_candidates(candidates, [acoustid_hit])
        else:
            logging.warning("Music metadata: acoustid enabled but API key is missing")

    best, score = matcher.select_best_match(source, candidates, duration)
    threshold = config.get("confidence_threshold", 70)
    if not best or score < threshold:
        logging.warning(
            "Music metadata skipped (score=%s, threshold=%s) for %s",
            score if best else "none",
            threshold,
            os.path.basename(file_path),
        )
        return

    tags = {
        "artist": best.get("artist"),
        "album": best.get("album"),
        "title": best.get("title"),
        "track_number": best.get("track_number"),
        "year": best.get("year"),
        "genre": best.get("genre"),
        "album_artist": best.get("album_artist") or best.get("artist"),
        "recording_id": best.get("recording_id"),
    }
    release_id = best.get("release_id")
    artwork = None
    if config.get("embed_artwork") and release_id:
        artwork = artwork_provider.fetch_artwork(
            release_id,
            max_size_px=config.get("max_artwork_size_px", 1500),
        )

    display_artist = tags.get("artist") or "-"
    display_title = tags.get("title") or "-"
    display_album = tags.get("album") or "-"
    logging.info(
        "Metadata matched (%s%%) - %s / %s / %s",
        score,
        display_artist,
        display_title,
        display_album,
    )

    # Optional lyrics enrichment (non-fatal)
    if config.get("enable_lyrics"):
        try:
            lyrics_result = fetch_lyrics(
                artist=tags.get("artist") or "",
                title=tags.get("title") or "",
                album=tags.get("album"),
                config=config,
            )
            if lyrics_result and lyrics_result.lyrics:
                tags["lyrics"] = lyrics_result.lyrics
                tags["lyrics_source"] = lyrics_result.source
                tags["lyrics_confidence"] = lyrics_result.confidence
        except Exception:
            logging.exception("Lyrics enrichment failed (non-fatal)")

    dry_run = bool(config.get("dry_run"))
    apply_tags(
        file_path,
        tags,
        artwork,
        source_title=source.get("source_title"),
        allow_overwrite=bool(config.get("allow_overwrite_tags", True)),
        dry_run=dry_run,
    )
