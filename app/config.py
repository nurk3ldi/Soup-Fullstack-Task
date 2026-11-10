import os

# Test-only hooks (fault injection, debug endpoints) exist only when this is exactly "1".
TEST_MODE = os.environ.get("TEST_MODE") == "1"
