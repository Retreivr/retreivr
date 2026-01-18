import logging
import os

from mutagen import File as MutagenFile
from mutagen.id3 import APIC, ID3, TCON, TDRC, TIT2, TPE1, TPE2, TALB, TRCK, TXXX, USLT
from mutagen.mp4 import MP4, MP4Cover


def apply_tags(file_path, tags, artwork, *, source_title=None, allow_overwrite=False, dry_run=False):
    if dry_run:
        logging.info("Music metadata dry-run tags for %s: %s", os.path.basename(file_path), _format_tags(tags))
        return
    ext = os.path.splitext(file_path)[1].lower()
    if ext == ".mp3":
        _apply_id3_tags(file_path, tags, artwork, source_title, allow_overwrite)
        return
    if ext in {".m4a", ".mp4", ".m4b"}:
        _apply_mp4_tags(file_path, tags, artwork, source_title, allow_overwrite)
        return
    _apply_generic_tags(file_path, tags, artwork, source_title, allow_overwrite)


def _apply_id3_tags(file_path, tags, artwork, source_title, allow_overwrite):
    try:
        audio = ID3(file_path)
    except Exception:
        audio = ID3()
    changed = False
    changed |= _set_id3_text(audio, "TPE1", tags.get("artist"), allow_overwrite)
    changed |= _set_id3_text(audio, "TALB", tags.get("album"), allow_overwrite)
    changed |= _set_id3_text(audio, "TIT2", tags.get("title"), allow_overwrite)
    changed |= _set_id3_text(audio, "TPE2", tags.get("album_artist"), allow_overwrite)
    changed |= _set_id3_text(audio, "TRCK", tags.get("track_number"), allow_overwrite)
    changed |= _set_id3_text(audio, "TDRC", tags.get("year"), allow_overwrite)
    changed |= _set_id3_text(audio, "TCON", tags.get("genre"), allow_overwrite)
    changed |= _set_id3_txxx(audio, "SOURCE", "YouTube", allow_overwrite)
    if source_title:
        changed |= _set_id3_txxx(audio, "SOURCE_TITLE", source_title, allow_overwrite)
    if tags.get("recording_id"):
        changed |= _set_id3_txxx(audio, "MBID", tags.get("recording_id"), allow_overwrite)
    lyrics = tags.get("lyrics")
    if lyrics:
        if allow_overwrite:
            audio.delall("USLT")
        if allow_overwrite or not audio.getall("USLT"):
            audio.add(USLT(encoding=3, lang="eng", desc="Lyrics", text=str(lyrics)))
            changed = True
    if artwork and (allow_overwrite or not audio.getall("APIC")):
        if allow_overwrite:
            for frame in audio.getall("APIC"):
                audio.delall("APIC")
        changed = True
        audio.add(
            APIC(
                encoding=3,
                mime=artwork.get("mime") or "image/jpeg",
                type=3,
                desc="cover",
                data=artwork.get("data"),
            )
        )
    if changed:
        audio.save(file_path)


def _apply_mp4_tags(file_path, tags, artwork, source_title, allow_overwrite):
    audio = MP4(file_path)
    mp4_tags = audio.tags or {}
    changed = False
    changed |= _set_mp4_value(mp4_tags, "\xa9ART", tags.get("artist"), allow_overwrite)
    changed |= _set_mp4_value(mp4_tags, "\xa9alb", tags.get("album"), allow_overwrite)
    changed |= _set_mp4_value(mp4_tags, "\xa9nam", tags.get("title"), allow_overwrite)
    changed |= _set_mp4_value(mp4_tags, "\xa9lyr", tags.get("lyrics"), allow_overwrite)
    changed |= _set_mp4_value(mp4_tags, "aART", tags.get("album_artist"), allow_overwrite)
    track_number = _normalize_track(tags.get("track_number"))
    if track_number and (allow_overwrite or "trkn" not in mp4_tags):
        mp4_tags["trkn"] = [(track_number, 0)]
        changed = True
    year = tags.get("year")
    if year and (allow_overwrite or "\xa9day" not in mp4_tags):
        mp4_tags["\xa9day"] = [str(year)]
        changed = True
    genre = tags.get("genre")
    if genre and (allow_overwrite or "\xa9gen" not in mp4_tags):
        mp4_tags["\xa9gen"] = [str(genre)]
        changed = True
    changed |= _set_mp4_freeform(mp4_tags, "SOURCE", "YouTube", allow_overwrite)
    if source_title:
        changed |= _set_mp4_freeform(mp4_tags, "SOURCE_TITLE", source_title, allow_overwrite)
    if tags.get("recording_id"):
        changed |= _set_mp4_freeform(mp4_tags, "MBID", tags.get("recording_id"), allow_overwrite)
    if artwork and (allow_overwrite or "covr" not in mp4_tags):
        img_data = artwork.get("data")
        if img_data:
            cover = MP4Cover(img_data)
            mp4_tags["covr"] = [cover]
            changed = True
    if changed:
        audio.tags = mp4_tags
        audio.save()


def _apply_generic_tags(file_path, tags, artwork, source_title, allow_overwrite):
    audio = MutagenFile(file_path)
    if not audio:
        logging.warning("Music metadata tagging skipped: unsupported file %s", file_path)
        return
    if audio.tags is None:
        audio.add_tags()
    changed = False
    changed |= _set_generic(audio.tags, "artist", tags.get("artist"), allow_overwrite)
    changed |= _set_generic(audio.tags, "album", tags.get("album"), allow_overwrite)
    changed |= _set_generic(audio.tags, "title", tags.get("title"), allow_overwrite)
    changed |= _set_generic(audio.tags, "lyrics", tags.get("lyrics"), allow_overwrite)
    changed |= _set_generic(audio.tags, "albumartist", tags.get("album_artist"), allow_overwrite)
    changed |= _set_generic(audio.tags, "tracknumber", tags.get("track_number"), allow_overwrite)
    changed |= _set_generic(audio.tags, "date", tags.get("year"), allow_overwrite)
    changed |= _set_generic(audio.tags, "genre", tags.get("genre"), allow_overwrite)
    changed |= _set_generic(audio.tags, "source", "YouTube", allow_overwrite)
    if source_title:
        changed |= _set_generic(audio.tags, "source_title", source_title, allow_overwrite)
    if tags.get("recording_id"):
        changed |= _set_generic(audio.tags, "mbid", tags.get("recording_id"), allow_overwrite)
    if changed:
        audio.save()


def _set_id3_text(audio, frame_id, value, allow_overwrite):
    if value is None or value == "":
        return False
    if audio.getall(frame_id):
        if not allow_overwrite:
            return False
        audio.delall(frame_id)
    text_value = str(value)
    frame_map = {
        "TPE1": TPE1,
        "TALB": TALB,
        "TIT2": TIT2,
        "TPE2": TPE2,
        "TRCK": TRCK,
        "TDRC": TDRC,
        "TCON": TCON,
    }
    frame_cls = frame_map.get(frame_id)
    if not frame_cls:
        return False
    audio.add(frame_cls(encoding=3, text=[text_value]))
    return True


def _set_id3_txxx(audio, desc, value, allow_overwrite):
    if value is None or value == "":
        return False
    for frame in audio.getall("TXXX"):
        if frame.desc == desc:
            if not allow_overwrite:
                return False
            audio.delall("TXXX")
            break
    audio.add(TXXX(encoding=3, desc=desc, text=[str(value)]))
    return True


def _set_mp4_value(tags, key, value, allow_overwrite):
    if value is None or value == "":
        return False
    if key in tags:
        if not allow_overwrite:
            return False
    tags[key] = [str(value)]
    return True


def _set_mp4_freeform(tags, key, value, allow_overwrite):
    if value is None or value == "":
        return False
    atom = f"----:com.apple.iTunes:{key}"
    if atom in tags:
        if not allow_overwrite:
            return False
    tags[atom] = [str(value).encode("utf-8")]
    return True


def _set_generic(tags, key, value, allow_overwrite):
    if value is None or value == "":
        return False
    if key in tags:
        existing = tags.get(key)
        if existing and not allow_overwrite:
            return False
    tags[key] = [str(value)]
    return True


def _normalize_track(value):
    if value is None or value == "":
        return None
    try:
        return int(str(value).split("/")[0])
    except Exception:
        return None


def _format_tags(tags):
    compact = {}
    for key, value in (tags or {}).items():
        if value is None or value == "":
            continue
        compact[key] = value
    return compact
