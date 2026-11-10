"""A small TCP proxy that can misbehave on command, placed between the browser and the API.

Playwright's `context.set_offline(True)` turned out not to interrupt an already-open SSE response
in Chromium, so it cannot simulate a network drop. This proxy breaks real TCP connections.

Modes for NEW connections:
  pass      - forward normally
  refuse    - reset immediately (network down / server unreachable)
  blackhole - accept, then never answer (packets silently dropped)

Actions on EXISTING connections:
  cut()     - reset both sides abruptly
  freeze()  - stop forwarding in both directions but keep the sockets open: the client gets no
              data and no error, like a half-open connection or a proxy that stopped forwarding
"""
import asyncio


class _Conn:
    def __init__(self, client_writer):
        self.client_writer = client_writer
        self.upstream_writer = None
        self.frozen = False
        self.tasks: list[asyncio.Task] = []

    def abort(self):
        for writer in (self.client_writer, self.upstream_writer):
            if writer is not None:
                writer.transport.abort()
        for task in self.tasks:
            task.cancel()


class FlakyProxy:
    def __init__(self, upstream_host: str, upstream_port: int):
        self.upstream = (upstream_host, upstream_port)
        self.mode = "pass"
        self.conns: set[_Conn] = set()
        self.accepted = 0
        self.refused = 0
        self.blackholed = 0
        self.cut_count = 0
        self.frozen_count = 0
        self._server = None
        self._never = asyncio.Event()

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self._server.sockets[0].getsockname()[1]}"

    async def start(self):
        self._server = await asyncio.start_server(self._handle, "127.0.0.1", 0)
        return self

    async def stop(self):
        self._server.close()
        for conn in list(self.conns):
            conn.abort()
        self.conns.clear()

    def cut(self) -> int:
        n = len(self.conns)
        for conn in list(self.conns):
            conn.abort()
        self.conns.clear()
        self.cut_count += n
        return n

    def freeze(self) -> int:
        n = 0
        for conn in self.conns:
            if not conn.frozen:
                conn.frozen = True
                n += 1
        self.frozen_count += n
        return n

    async def _handle(self, reader, writer):
        conn = _Conn(writer)
        if self.mode == "refuse":
            self.refused += 1
            writer.transport.abort()
            return
        self.conns.add(conn)
        if self.mode == "blackhole":
            self.blackholed += 1
            conn.frozen = True
            conn.tasks.append(asyncio.current_task())
            try:
                await self._never.wait()  # hold the socket open and say nothing
            except asyncio.CancelledError:
                pass
            return
        self.accepted += 1
        try:
            up_reader, up_writer = await asyncio.open_connection(*self.upstream)
        except OSError:
            writer.transport.abort()
            self.conns.discard(conn)
            return
        conn.upstream_writer = up_writer
        conn.tasks = [
            asyncio.create_task(self._pump(conn, reader, up_writer)),
            asyncio.create_task(self._pump(conn, up_reader, writer)),
        ]
        await asyncio.gather(*conn.tasks, return_exceptions=True)
        if not conn.frozen:
            conn.abort()
            self.conns.discard(conn)

    async def _pump(self, conn: _Conn, reader, writer):
        try:
            while True:
                data = await reader.read(65536)
                if conn.frozen:
                    await self._never.wait()  # swallow it; pass nothing on, not even the close
                if not data:
                    break
                writer.write(data)
                await writer.drain()
        except (ConnectionError, OSError):
            pass
        finally:
            if not conn.frozen:
                # One side ended: end the other too, so a server-side drop reaches the browser.
                for task in conn.tasks:
                    if task is not asyncio.current_task():
                        task.cancel()
