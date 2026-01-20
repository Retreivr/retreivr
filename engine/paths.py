import os
from dataclasses import dataclass
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent.parent


def _env_path(name, default):
    value = os.environ.get(name)
    if value:
        return os.path.abspath(value)
    return os.path.abspath(default)


# Base directories for all file access. Override via env for container mounts.
CONFIG_DIR = _env_path("RETREIVR_CONFIG_DIR", PROJECT_ROOT / "config")
DATA_DIR = _env_path("RETREIVR_DATA_DIR", PROJECT_ROOT)
DOWNLOADS_DIR = _env_path("RETREIVR_DOWNLOADS_DIR", PROJECT_ROOT / "downloads")
LOG_DIR = _env_path("RETREIVR_LOG_DIR", PROJECT_ROOT / "logs")
TOKENS_DIR = _env_path("RETREIVR_TOKENS_DIR", PROJECT_ROOT / "tokens")


@dataclass(frozen=True)
class EnginePaths:
    log_dir: str
    db_path: str
    temp_downloads_dir: str
    single_downloads_dir: str
    lock_file: str
    ytdlp_temp_dir: str
    thumbs_dir: str


def ensure_dir(path):
    if path:
        os.makedirs(path, exist_ok=True)


def resolve_config_path(path):
    if not path:
        resolved = os.path.join(CONFIG_DIR, "config.json")
    elif os.path.isabs(path):
        resolved = os.path.abspath(path)
    else:
        resolved = os.path.abspath(os.path.join(CONFIG_DIR, path))
    if not _is_within_base(resolved, CONFIG_DIR):
        raise ValueError(f"Config path must be within CONFIG_DIR: {CONFIG_DIR}")
    return resolved


def _is_within_base(path, base_dir):
    real = os.path.realpath(path)
    base = os.path.realpath(base_dir)
    return os.path.commonpath([real, base]) == base


def resolve_dir(path, base_dir):
    if not path:
        return base_dir
    if os.path.isabs(path):
        resolved = os.path.abspath(path)
    else:
        resolved = os.path.abspath(os.path.join(base_dir, path))
    if not _is_within_base(resolved, base_dir):
        # Enforce container-safe paths: all writes stay under explicit base dirs.
        raise ValueError(f"Path must be within base directory: {base_dir}")
    return resolved


def build_engine_paths():
    db_path = os.path.join(DATA_DIR, "database", "db.sqlite")
    temp_downloads_dir = os.path.join(DATA_DIR, "temp_downloads")
    lock_file = os.path.join(DATA_DIR, "tmp", "yt_archiver.lock")
    ytdlp_temp_dir = os.path.join(DATA_DIR, "tmp", "yt-dlp")
    thumbs_dir = os.path.join(ytdlp_temp_dir, "thumbs")
    return EnginePaths(
        log_dir=LOG_DIR,
        db_path=db_path,
        temp_downloads_dir=temp_downloads_dir,
        single_downloads_dir=DOWNLOADS_DIR,
        lock_file=lock_file,
        ytdlp_temp_dir=ytdlp_temp_dir,
        thumbs_dir=thumbs_dir,
    )
