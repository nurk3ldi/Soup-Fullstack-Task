"""Structured JSON logs. Nothing here may ever receive an API key or a stream token."""
import json
import logging
import re
import sys
from collections import deque
from datetime import datetime, timezone

_TOKEN_RE = re.compile(r"(?i)(token=)[^&\s\"]*")
_API_KEY_RE = re.compile(r"sk_[A-Za-z0-9_-]{16,}")
_STREAM_TOKEN_RE = re.compile(r"[0-9a-f]{32}\.[0-9a-f]{32}\.[0-9a-f]{32}\.\d+\.[0-9a-f]{64}")

# Query parameters whose values are safe and useful to see in logs. Everything else is masked,
# so a secret put in the URL by mistake (?api_key=...) is not written down either.
_SAFE_QUERY_PARAMS = {"since_seq", "limit"}


def safe_query(query: str) -> str:
    parts = []
    for pair in query.split("&"):
        if not pair:
            continue
        name, _, value = pair.partition("=")
        parts.append(f"{name}={value}" if name in _SAFE_QUERY_PARAMS else f"{name}=REDACTED")
    return "&".join(parts)


def redact(text: str) -> str:
    """Last line of defence, applied to every finished log line: mask anything shaped like a
    stream token or an API key, wherever it ended up (path, message, traceback)."""
    text = _TOKEN_RE.sub(r"\1REDACTED", text)
    text = _STREAM_TOKEN_RE.sub("STREAM_TOKEN_REDACTED", text)
    return _API_KEY_RE.sub("sk_REDACTED", text)


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        out = {
            "ts": datetime.fromtimestamp(record.created, timezone.utc).isoformat(timespec="milliseconds"),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        out.update(getattr(record, "fields", {}))
        if record.exc_info:
            out["exc"] = self.formatException(record.exc_info)
        return redact(json.dumps(out, default=str))


class RingHandler(logging.Handler):
    """TEST_MODE only: keeps the most recent log lines, exactly as written to stdout, so the test
    suite can scan them for leaked secrets."""

    def __init__(self, capacity: int = 20000):
        super().__init__()
        self.lines: deque[str] = deque(maxlen=capacity)

    def emit(self, record: logging.LogRecord) -> None:
        self.lines.append(self.format(record))


ring: RingHandler | None = None


def setup(test_mode: bool = False) -> None:
    global ring
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger()
    root.handlers = [handler]
    if test_mode:
        ring = RingHandler()
        ring.setFormatter(JsonFormatter())
        root.addHandler(ring)
    root.setLevel(logging.INFO)
    # uvicorn installs its own plain-text handlers; route them through ours instead.
    for name in ("uvicorn", "uvicorn.error"):
        lg = logging.getLogger(name)
        lg.handlers = []
        lg.propagate = True
    # The access log prints full URLs (including ?token=); our middleware logs requests instead.
    access = logging.getLogger("uvicorn.access")
    access.handlers = []
    access.propagate = False
    access.disabled = True


log = logging.getLogger("app")
