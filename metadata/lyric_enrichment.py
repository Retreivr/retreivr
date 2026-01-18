"""
Lyrics enrichment module for Retreivr.

Responsibilities:
- Given normalized metadata (artist, title, album optional)
- Attempt to retrieve lyrics from public sources
- Return lyrics + metadata (confidence, source)
- Never hard-fail ingestion
- Never write files directly unless instructed

This module is optional and safe to disable entirely.
"""

from typing import Optional, Dict
import logging

try:
    import lyricsgenius
except ImportError:
    lyricsgenius = None

log = logging.getLogger(__name__)


class LyricsResult:
    def __init__(
        self,
        lyrics: str,
        source: str,
        confidence: float,
        language: Optional[str] = None,
    ):
        self.lyrics = lyrics
        self.source = source
        self.confidence = confidence
        self.language = language


def fetch_lyrics(
    *,
    artist: str,
    title: str,
    album: Optional[str] = None,
    config: Dict,
) -> Optional[LyricsResult]:
    """
    Attempt to fetch lyrics for a track.

    Returns:
        LyricsResult on success
        None on failure (non-fatal)
    """

    if not config.get("enable_lyrics", False):
        return None

    if lyricsgenius is None:
        log.warning("lyricsgenius not installed; lyrics enrichment skipped")
        return None

    genius_token = config.get("genius_api_token")
    if not genius_token:
        log.debug("No Genius API token configured; lyrics skipped")
        return None

    try:
        genius = lyricsgenius.Genius(
            genius_token,
            skip_non_songs=True,
            remove_section_headers=True,
            timeout=10,
            retries=1,
        )

        song = genius.search_song(title=title, artist=artist)
        if not song or not song.lyrics:
            return None

        lyrics_text = song.lyrics.strip()

        # Very simple confidence heuristic (intentionally conservative)
        confidence = 0.85
        if album and album.lower() in (song.album or "").lower():
            confidence += 0.05

        return LyricsResult(
            lyrics=lyrics_text,
            source="genius",
            confidence=min(confidence, 0.95),
            language="en",  # Genius does not expose language reliably
        )

    except Exception as e:
        log.debug("Lyrics lookup failed: %s", e)
        return None