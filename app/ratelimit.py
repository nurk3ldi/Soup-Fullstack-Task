"""In-process rate limiting (token buckets).

Two limits, both checked before a request body is read or a query is run:

* per project: how fast one project's keys may call the API. Protects the other projects from
  one runaway or abusive client.
* per client address: how many *failed* credentials an address may present. Once the allowance
  is used up, every request from that address that needs a credential is refused without
  looking at the credential, so guessing is slowed to the refill rate. Legitimate callers behind
  the same address are refused too while that lasts; that is the price of not telling a guesser
  whether a guess was right.

The state lives in this process. With several workers each has its own buckets, so the
effective limits are multiplied by the number of workers; a shared limit belongs in the proxy.
The address is the TCP peer: behind a proxy every client has the proxy's address unless uvicorn
is told which proxy to trust (--forwarded-allow-ips).
"""
import os
import time

from app.errors import ApiError

MAX_BUCKETS = 10_000  # per limiter; beyond this, full buckets are dropped (they carry no state)


class Limiter:
    def __init__(self, rate: float, burst: float):
        self.rate = rate  # tokens added per second
        self.burst = burst  # bucket size
        self.enabled = rate > 0 and burst >= 1
        self._buckets: dict[object, tuple[float, float]] = {}  # key -> (tokens, time of that count)

    def _tokens(self, key, now: float) -> float:
        tokens, stamp = self._buckets.get(key, (self.burst, now))
        return min(self.burst, tokens + (now - stamp) * self.rate)

    def wait(self, key) -> float:
        """Seconds until a token is available; 0 if one is available now. Takes nothing."""
        if not self.enabled:
            return 0.0
        tokens = self._tokens(key, time.monotonic())
        return 0.0 if tokens >= 1 else (1 - tokens) / self.rate

    def take(self, key) -> float:
        """Take one token. Returns 0 on success, else the seconds to wait (and takes nothing)."""
        if not self.enabled:
            return 0.0
        now = time.monotonic()
        tokens = self._tokens(key, now)
        if tokens < 1:
            return (1 - tokens) / self.rate
        if len(self._buckets) >= MAX_BUCKETS and key not in self._buckets:
            self._prune(now)
        self._buckets[key] = (tokens - 1, now)
        return 0.0

    def _prune(self, now: float) -> None:
        # A bucket that has refilled completely is the same as no bucket at all.
        self._buckets = {k: v for k, v in self._buckets.items() if self._tokens(k, now) < self.burst}
        if len(self._buckets) >= MAX_BUCKETS:
            self._buckets.clear()  # fail open rather than grow without bound


class Stats:
    rate_limited_project_total = 0
    rate_limited_address_total = 0


def _env(name: str, default: str) -> float:
    return float(os.environ.get(name, default))


def configure(project_rps=None, project_burst=None, auth_fail_rps=None, auth_fail_burst=None) -> dict:
    """(Re)create both limiters, empty. Arguments left as None come from the environment."""
    global projects, failures
    p_rate = _env("RATE_LIMIT_PROJECT_RPS", "600") if project_rps is None else float(project_rps)
    p_burst = _env("RATE_LIMIT_PROJECT_BURST", str(2 * p_rate)) if project_burst is None else float(project_burst)
    f_rate = _env("RATE_LIMIT_AUTH_FAIL_RPS", "1") if auth_fail_rps is None else float(auth_fail_rps)
    f_burst = _env("RATE_LIMIT_AUTH_FAIL_BURST", "20") if auth_fail_burst is None else float(auth_fail_burst)
    projects = Limiter(p_rate, p_burst)
    failures = Limiter(f_rate, f_burst)
    return settings()


def settings() -> dict:
    return {
        "project_rps": projects.rate if projects.enabled else 0,
        "project_burst": projects.burst if projects.enabled else 0,
        "auth_fail_rps": failures.rate if failures.enabled else 0,
        "auth_fail_burst": failures.burst if failures.enabled else 0,
    }


projects: Limiter
failures: Limiter
configure()


def _limited(seconds: float) -> ApiError:
    return ApiError(429, "rate_limited", headers={"Retry-After": str(max(1, int(seconds + 0.999)))})


def address_of(request) -> str:
    return request.client.host if request.client else "-"


def guard_address(request) -> None:
    """Refuse an address that has used up its allowance of failed credentials."""
    seconds = failures.wait(address_of(request))
    if seconds:
        Stats.rate_limited_address_total += 1
        raise _limited(seconds)


def credential_failed(request) -> None:
    failures.take(address_of(request))


def guard_project(project_id) -> None:
    seconds = projects.take(project_id)
    if seconds:
        Stats.rate_limited_project_total += 1
        raise _limited(seconds)
