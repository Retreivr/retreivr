import html
import json
import logging
import os
import re
from datetime import datetime, timezone

import requests

from engine.json_utils import safe_json_dump
from engine.paths import DATA_DIR, ensure_dir
from engine.search_engine import _log_event


class SpotifyPlaylistImportError(Exception):
    pass


class SpotifyPlaylistImporter:
    _PLAYLIST_URL_RE = re.compile(
        r"^(?:https?://open\.spotify\.com/playlist/|spotify:playlist:)([A-Za-z0-9]+)"
    )
    _NEXT_DATA_RE = re.compile(
        r'<script[^>]+id="__NEXT_DATA__"[^>]+>(.+?)</script>', re.DOTALL
    )

    def __init__(self):
        self.snapshot_dir = os.path.join(DATA_DIR, "spotify_playlists")
        ensure_dir(self.snapshot_dir)

    def import_playlist(self, playlist_entry, search_service, config):
        playlist_url = playlist_entry.get("playlist_url") or ""
        playlist_id = self._extract_playlist_id(playlist_url)
        playlist_name = playlist_entry.get("name") or playlist_id
        min_score = self._normalize_score(playlist_entry.get("min_match_score"))
        if min_score is None:
            min_score = 0.65
        auto_download = playlist_entry.get("auto_download")
        if auto_download is None:
            auto_download = True
        destination = self._resolve_destination(playlist_entry, config, playlist_name)

        _log_event(
            logging.INFO,
            "playlist_import_started",
            playlist_id=playlist_id,
            playlist_name=playlist_name,
            playlist_url=playlist_url,
        )

        playlist_data = self._fetch_playlist_data(playlist_id, playlist_url)
        tracks = self._extract_tracks(playlist_data)
        self._store_snapshot(playlist_id, playlist_url, playlist_name, tracks)

        queued_total = 0
        duplicates_total = 0
        failed_total = 0
        for track in tracks:
            _log_event(
                logging.INFO,
                "track_imported",
                playlist_id=playlist_id,
                track_title=track["track"],
                artist=track["artist"],
                position=track["position"],
            )
            payload = {
                "intent": "track",
                "media_type": "music",
                "artist": track["artist"] or "",
                "track": track["track"] or "",
                "album": track.get("album"),
                "destination_dir": destination,
                "include_albums": 1,
                "include_singles": 1,
                "min_match_score": min_score,
                "lossless_only": 1,
                "auto_enqueue": bool(auto_download),
                "created_by": f"spotify_playlist:{playlist_id}",
                "max_candidates_per_source": 5,
            }
            request_id = search_service.create_search_request(payload)
            search_service.run_search_resolution_once(request_id=request_id)
            items = search_service.store.list_items(request_id)
            queued = sum(
                1 for item in items if item.get("status") in {"selected", "enqueued"}
            )
            skipped = sum(1 for item in items if item.get("status") == "skipped")
            failed = sum(1 for item in items if item.get("status") == "failed")
            queued_total += queued
            duplicates_total += skipped
            failed_total += failed

        _log_event(
            logging.INFO,
            "playlist_import_completed",
            playlist_id=playlist_id,
            playlist_name=playlist_name,
            tracks_discovered=len(tracks),
            tracks_queued=queued_total,
            tracks_skipped=duplicates_total,
            tracks_failed=failed_total,
        )

        return {
            "playlist_id": playlist_id,
            "playlist_name": playlist_name,
            "destination": destination,
            "tracks_discovered": len(tracks),
            "tracks_queued": queued_total,
            "tracks_skipped": duplicates_total,
            "tracks_failed": failed_total,
            "snapshot_path": os.path.join(self.snapshot_dir, f"{playlist_id}.json"),
        }

    def _resolve_destination(self, entry, config, playlist_name):
        explicit = entry.get("destination")
        if explicit:
            return explicit
        base = config.get("music_download_folder") or config.get("single_download_folder") or "Music"
        if playlist_name:
            return os.path.join(base, playlist_name)
        return base

    def _extract_playlist_id(self, playlist_url):
        if not playlist_url:
            raise SpotifyPlaylistImportError("playlist_url is required")
        match = self._PLAYLIST_URL_RE.match(playlist_url.strip())
        if not match:
            raise SpotifyPlaylistImportError("Invalid Spotify playlist URL")
        return match.group(1)

    def _fetch_playlist_data(self, playlist_id, playlist_url):
        target = f"https://open.spotify.com/playlist/{playlist_id}"
        headers = {
            "User-Agent": "Retreivr/1.0",
            "Accept-Language": "en-US,en;q=0.9",
        }
        try:
            response = requests.get(target, headers=headers, timeout=15)
        except requests.RequestException as exc:
            raise SpotifyPlaylistImportError(f"Failed to fetch playlist: {exc}") from exc
        if response.status_code != 200:
            raise SpotifyPlaylistImportError(
                f"Spotify playlist unavailable ({response.status_code})"
            )
        return self._parse_next_data(response.text, playlist_url)

    def _parse_next_data(self, html_content, playlist_url):
        match = self._NEXT_DATA_RE.search(html_content)
        if not match:
            raise SpotifyPlaylistImportError("Unable to parse Spotify playlist metadata")
        try:
            payload = html.unescape(match.group(1))
            decoded = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise SpotifyPlaylistImportError("Spotify playlist metadata invalid") from exc
        playlist_data = self._find_playlist_data(decoded)
        if not playlist_data:
            raise SpotifyPlaylistImportError("Spotify playlist data not found")
        playlist_data["url"] = playlist_url
        return playlist_data

    def _find_playlist_data(self, obj, depth=0):
        if depth > 12:
            return None
        if isinstance(obj, dict):
            tracks = obj.get("tracks")
            if tracks and isinstance(tracks, dict):
                if "items" in tracks and obj.get("name"):
                    return obj
            for value in obj.values():
                candidate = self._find_playlist_data(value, depth + 1)
                if candidate:
                    return candidate
        elif isinstance(obj, list):
            for entry in obj:
                candidate = self._find_playlist_data(entry, depth + 1)
                if candidate:
                    return candidate
        return None

    def _extract_tracks(self, playlist_data):
        raw_items = playlist_data.get("tracks", {}).get("items") or []
        tracks = []
        for idx, item in enumerate(raw_items, start=1):
            if isinstance(item, dict) and "track" in item:
                track_info = item["track"]
            else:
                track_info = item
            if not track_info or track_info.get("is_local"):
                continue
            title = track_info.get("name")
            if not title:
                continue
            artists = track_info.get("artists") or []
            artist_names = [artist.get("name") for artist in artists if artist.get("name")]
            artist_text = ", ".join(artist_names) if artist_names else ""
            album = track_info.get("album") or {}
            album_title = album.get("name")
            release_date = album.get("release_date") or album.get("releaseDate")
            release_year = None
            if release_date:
                release_year = release_date.split("-")[0]
            images = album.get("images") or []
            artwork = images[0].get("url") if images and images[0].get("url") else None
            tracks.append(
                {
                    "position": idx,
                    "track": title,
                    "artist": artist_text,
                    "album": album_title,
                    "release_year": release_year,
                    "artwork_url": artwork,
                    "duration_ms": track_info.get("duration_ms"),
                }
            )
        return tracks

    def _store_snapshot(self, playlist_id, playlist_url, playlist_name, tracks):
        snapshot = {
            "playlist_id": playlist_id,
            "playlist_name": playlist_name,
            "playlist_url": playlist_url,
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "tracks": tracks,
        }
        snapshot_path = os.path.join(self.snapshot_dir, f"{playlist_id}.json")
        with open(snapshot_path, "w", encoding="utf-8") as handle:
            safe_json_dump(snapshot, handle, indent=2)

    def _normalize_score(self, value):
        if value is None:
            return None
        try:
            score = float(value)
        except (TypeError, ValueError):
            return None
        if 0 <= score <= 1:
            return score
        return None
