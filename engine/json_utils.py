import json
import logging


def sanitize_for_json(value):
    if isinstance(value, dict):
        return {str(key): sanitize_for_json(val) for key, val in value.items()}
    if isinstance(value, (list, tuple)):
        return [sanitize_for_json(item) for item in value]
    if isinstance(value, set):
        return [sanitize_for_json(item) for item in value]
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return str(value)


def safe_json(value):
    return sanitize_for_json(value)


def safe_json_dumps(value, **kwargs):
    kwargs.setdefault("default", str)
    try:
        return json.dumps(sanitize_for_json(value), **kwargs)
    except TypeError:
        # Last-ditch guard: never allow JSON serialization to crash logging.
        return json.dumps(str(value), **kwargs)


def safe_json_dump(value, fp, **kwargs):
    kwargs.setdefault("default", str)
    try:
        return json.dump(sanitize_for_json(value), fp, **kwargs)
    except TypeError:
        return json.dump(str(value), fp, **kwargs)


def json_sanity_check():
    payload = {"message": "json_sanity_check", "sample_set": {"alpha", "beta"}}
    logging.info(safe_json_dumps(payload, sort_keys=True))
