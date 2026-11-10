"""Fan-out of Postgres NOTIFY to the SSE connections of this process.

A notification is only a wake-up call: it carries no data anyone relies on. Streams always re-read
the database, so a lost, duplicated or late notification can delay delivery (until the next safety
poll) but can never corrupt it.
"""
import asyncio
from collections import defaultdict

import asyncpg

from app.logging import log

RUN_CHANNEL = "run_events"  # payload "<run_id>:<last_seq>"
KEY_CHANNEL = "key_revoked"  # payload "<key_id>"


class Subscription:
    __slots__ = ("run_id", "key_id", "event", "auth_dirty")

    def __init__(self, run_id: str, key_id: str):
        self.run_id = run_id
        self.key_id = key_id
        self.event = asyncio.Event()
        self.auth_dirty = False  # set when this stream's key may have just been revoked


class Bus:
    def __init__(self, dsn: str):
        self._dsn = dsn
        self._by_run: dict[str, set[Subscription]] = defaultdict(set)
        self._by_key: dict[str, set[Subscription]] = defaultdict(set)
        self._task: asyncio.Task | None = None
        self._ready = asyncio.Event()
        self.connected = False
        self.reconnects = 0

    async def start(self) -> None:
        self._task = asyncio.create_task(self._run())
        await asyncio.wait_for(self._ready.wait(), timeout=10)

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)

    def subscribe(self, run_id: str, key_id: str) -> Subscription:
        sub = Subscription(run_id, key_id)
        self._by_run[run_id].add(sub)
        self._by_key[key_id].add(sub)
        return sub

    def unsubscribe(self, sub: Subscription) -> None:
        for index, key in ((self._by_run, sub.run_id), (self._by_key, sub.key_id)):
            subs = index.get(key)
            if subs is not None:
                subs.discard(sub)
                if not subs:
                    del index[key]

    def _on_run(self, _conn, _pid, _channel, payload: str) -> None:
        for sub in self._by_run.get(payload.partition(":")[0], ()):
            sub.event.set()

    def _on_key(self, _conn, _pid, _channel, payload: str) -> None:
        for sub in self._by_key.get(payload, ()):
            sub.auth_dirty = True
            sub.event.set()

    def _wake_all(self) -> None:
        for subs in self._by_run.values():
            for sub in subs:
                sub.auth_dirty = True
                sub.event.set()

    async def _run(self) -> None:
        """Hold one dedicated LISTEN connection; reconnect forever if it drops."""
        while True:
            conn = None
            try:
                conn = await asyncpg.connect(self._dsn)
                await conn.add_listener(RUN_CHANNEL, self._on_run)
                await conn.add_listener(KEY_CHANNEL, self._on_key)
                self.connected = True
                self._ready.set()
                # Notifications sent while we were not listening are gone: make everyone re-read.
                self._wake_all()
                while True:
                    await asyncio.sleep(5)
                    await conn.fetchval("SELECT 1", timeout=5)  # notice a dead connection
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.error("listen connection lost", extra={"fields": {"error": type(exc).__name__}})
            finally:
                self.connected = False
                if conn is not None and not conn.is_closed():
                    conn.terminate()
            self.reconnects += 1
            await asyncio.sleep(1)
