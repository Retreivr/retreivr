import logging
from urllib.parse import urlparse

from yt_dlp import YoutubeDL

from engine.json_utils import safe_json_dumps

def _is_http_url(value):
    if not value or not isinstance(value, str):
        return False
    try:
        return urlparse(value).scheme in ("http", "https")
    except Exception:
        return False

class SearchAdapter:
    source = ""

    def search_track(self, artist, track, album=None, limit=5):
        raise NotImplementedError

    def search_album(self, artist, album, limit=5):
        raise NotImplementedError

    def expand_album_to_tracks(self, candidate_album):
        return None

    def source_modifier(self, candidate):
        return 1.0

    def _candidate_thumbnail_url(self, entry):
        return None


class _YtDlpSearchMixin(SearchAdapter):
    search_prefix = ""

    def _search(self, query, limit):
        if not query:
            return []
        search_term = f"{self.search_prefix}{limit}:{query}"
        opts = {
            "skip_download": True,
            "quiet": True,
            "no_warnings": True,
            "ignoreerrors": True,
            "noplaylist": True,
            "cachedir": False,
            "socket_timeout": 10,
        }
        try:
            with YoutubeDL(opts) as ydl:
                info = ydl.extract_info(search_term, download=False)
        except Exception:
            logging.exception("Search failed for source=%s query=%s", self.source, query)
            return []

        entries = info.get("entries") if isinstance(info, dict) else None
        if not entries:
            return []

        results = []
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            url = entry.get("webpage_url")
            if not _is_http_url(url):
                # yt-dlp search extractors often expose internal extractor URLs (e.g. bandcampsearch5)
                # which must never be treated as real URLs.
                logging.debug(
                    "Skipping non-http search result from %s: %r",
                    self.source,
                    entry.get("url"),
                )
                continue

            title = entry.get("title")
            if not title:
                continue
            isrc = entry.get("isrc")
            if not isrc:
                isrcs = entry.get("isrcs")
                if isinstance(isrcs, list) and isrcs:
                    isrc = isrcs[0]
            track_count = entry.get("track_count") or entry.get("n_entries") or entry.get("playlist_count")
            candidate = {
                "source": self.source,
                "url": url,
                "title": title,
                "uploader": entry.get("uploader") or entry.get("channel"),
                "artist_detected": entry.get("artist") or entry.get("uploader") or entry.get("channel"),
                "album_detected": entry.get("album"),
                "track_detected": entry.get("track") or entry.get("title"),
                "duration_sec": entry.get("duration"),
                "artwork_url": entry.get("thumbnail"),
                "raw_meta_json": safe_json_dumps(entry),
                "official": bool(entry.get("is_official")),
                "isrc": isrc,
                "track_count": track_count,
            }
            candidate["thumbnail_url"] = None
            thumbnail_url = self._candidate_thumbnail_url(entry)
            if thumbnail_url:
                candidate["thumbnail_url"] = thumbnail_url
            results.append(candidate)
        return results

    def search_track(self, artist, track, album=None, limit=5):
        query = f"{artist} {track}".strip()
        if album:
            query = f"{query} {album}".strip()
        return self._search(query, limit)

    def search_album(self, artist, album, limit=5):
        query = f"{artist} {album}".strip()
        return self._search(query, limit)


class YouTubeMusicAdapter(_YtDlpSearchMixin):
    source = "youtube_music"
    search_prefix = "ytsearch"

    def _candidate_thumbnail_url(self, entry):
        video_id = entry.get("id")
        if not isinstance(video_id, str):
            return None
        video_id = video_id.strip()
        if not video_id:
            return None
        return f"https://i.ytimg.com/vi/{video_id}/hqdefault.jpg"

    def source_modifier(self, candidate):
        if candidate.get("official"):
            return 1.0
        return 0.90


class YouTubeAdapter(_YtDlpSearchMixin):
    source = "youtube"
    search_prefix = "ytsearch"

    def _candidate_thumbnail_url(self, entry):
        video_id = entry.get("id")
        if not isinstance(video_id, str):
            return None
        video_id = video_id.strip()
        if not video_id:
            return None
        return f"https://i.ytimg.com/vi/{video_id}/hqdefault.jpg"

    def source_modifier(self, candidate):
        # Neutral weight for general video content
        return 0.85


class SoundCloudAdapter(_YtDlpSearchMixin):
    source = "soundcloud"
    search_prefix = "scsearch"

    def _candidate_thumbnail_url(self, entry):
        artwork_url = entry.get("artwork_url")
        if not isinstance(artwork_url, str):
            return None
        artwork_url = artwork_url.strip()
        if not artwork_url:
            return None
        upgraded = artwork_url.replace("-large", "-t500x500")
        return upgraded if _is_http_url(upgraded) else None

    def source_modifier(self, candidate):
        return 0.95


class BandcampAdapter(_YtDlpSearchMixin):
    source = "bandcamp"
    search_prefix = "bandcampsearch"

    def _candidate_thumbnail_url(self, entry):
        for key in ("thumbnail_url", "image"):
            value = entry.get(key)
            if isinstance(value, str):
                value = value.strip()
                if value and _is_http_url(value):
                    return value
        return None

    def source_modifier(self, candidate):
        return 1.05


def default_adapters():
    adapters = [BandcampAdapter(), YouTubeMusicAdapter(), YouTubeAdapter(), SoundCloudAdapter()]
    return {adapter.source: adapter for adapter in adapters}
