import os

# Test-only hooks (fault injection, debug endpoints) exist only when this is exactly "1".
TEST_MODE = os.environ.get("TEST_MODE") == "1"

# Largest request body the API reads. The biggest valid batch (1000 points with 64-character
# names, every character JSON-escaped) is under 1 MiB; anything above this is refused with 413.
MAX_BODY_BYTES = int(os.environ.get("MAX_BODY_BYTES", str(2 * 1024 * 1024)))
