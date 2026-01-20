import json
import os
import threading
import time

from engine.json_utils import safe_json_dump

class JsonCache:
    def __init__(self, path, *, ttl_seconds=86400):
        self.path = path
        self.ttl_seconds = ttl_seconds
        self._loaded = False
        self._lock = threading.Lock()
        self._entries = {}

    def _load(self):
        if self._loaded:
            return
        self._loaded = True
        if not self.path or not os.path.exists(self.path):
            return
        try:
            with open(self.path, "r") as handle:
                payload = json.load(handle)
        except Exception:
            return
        entries = payload.get("entries")
        if isinstance(entries, dict):
            self._entries = entries

    def _save(self):
        if not self.path:
            return
        directory = os.path.dirname(self.path)
        if directory:
            os.makedirs(directory, exist_ok=True)
        tmp_path = f"{self.path}.tmp"
        payload = {"version": 1, "entries": self._entries}
        with open(tmp_path, "w") as handle:
            safe_json_dump(payload, handle)
        os.replace(tmp_path, self.path)

    def _is_valid(self, entry):
        if not isinstance(entry, dict):
            return False
        ts = entry.get("ts")
        if ts is None:
            return False
        if self.ttl_seconds is None:
            return True
        return (time.time() - float(ts)) <= float(self.ttl_seconds)

    def get(self, key):
        with self._lock:
            self._load()
            entry = self._entries.get(key)
            if not entry:
                return None
            if self._is_valid(entry):
                return entry.get("value")
            self._entries.pop(key, None)
            self._save()
            return None

    def set(self, key, value):
        with self._lock:
            self._load()
            self._entries[key] = {"ts": time.time(), "value": value}
            self._save()
