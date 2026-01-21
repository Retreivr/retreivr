import os
from dataclasses import dataclass
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = Path(os.environ.get("RETREIVR_DATA_DIR", PROJECT_ROOT / "data")).resolve()

CONFIG_DIR = Path(os.environ.get("RETREIVR_CONFIG_DIR", DATA_DIR / "config")).resolve()
DOWNLOADS_DIR = Path(os.environ.get("RETREIVR_DOWNLOADS_DIR", DATA_DIR / "downloads")).resolve()
LOG_DIR = Path(os.environ.get("RETREIVR_LOG_DIR", DATA_DIR / "logs")).resolve()
TOKENS_DIR = Path(os.environ.get("RETREIVR_TOKENS_DIR", DATA_DIR / "tokens")).resolve()


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
    db_path = DATA_DIR / "database" / "db.sqlite"
    temp_downloads_dir = DATA_DIR / "temp_downloads"
    lock_file = DATA_DIR / "tmp" / "retreivr.lock"
    ytdlp_temp_dir = DATA_DIR / "tmp" / "yt-dlp"
    thumbs_dir = ytdlp_temp_dir / "thumbs"

    # Ensure required directories exist
    for d in (
        db_path.parent,
        temp_downloads_dir,
        lock_file.parent,
        ytdlp_temp_dir,
        thumbs_dir,
        LOG_DIR,
        DOWNLOADS_DIR,
        TOKENS_DIR,
        CONFIG_DIR,
    ):
        ensure_dir(d)

    return EnginePaths(
        log_dir=str(LOG_DIR),
        db_path=str(db_path),
        temp_downloads_dir=str(temp_downloads_dir),
        single_downloads_dir=str(DOWNLOADS_DIR),
        lock_file=str(lock_file),
        ytdlp_temp_dir=str(ytdlp_temp_dir),
        thumbs_dir=str(thumbs_dir),
    )
