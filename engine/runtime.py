import os
import sys

from yt_dlp.version import __version__ as ytdlp_version


def get_runtime_info():
    return {
        "app_version": os.environ.get("RETREIVR_VERSION", "0.9.1"),
        "python_version": sys.version.split()[0],
        "yt_dlp_version": ytdlp_version,
    }
