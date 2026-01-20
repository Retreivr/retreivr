import logging

from yt_dlp import YoutubeDL

from engine.json_utils import safe_json_dumps

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
            url = entry.get("webpage_url") or entry.get("url")
            title = entry.get("title")
            if not url or not title:
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

    def source_modifier(self, candidate):
        if candidate.get("official"):
            return 1.0
        return 0.90


class YouTubeAdapter(_YtDlpSearchMixin):
    source = "youtube"
    search_prefix = "ytsearch"

    def source_modifier(self, candidate):
        # Neutral weight for general video content
        return 0.85


class SoundCloudAdapter(_YtDlpSearchMixin):
    source = "soundcloud"
    search_prefix = "scsearch"

    def source_modifier(self, candidate):
        return 0.95


class BandcampAdapter(_YtDlpSearchMixin):
    source = "bandcamp"
    search_prefix = "bandcampsearch"

    def source_modifier(self, candidate):
        return 1.05


def default_adapters():
    adapters = [BandcampAdapter(), YouTubeMusicAdapter(), YouTubeAdapter(), SoundCloudAdapter()]
    return {adapter.source: adapter for adapter in adapters}
