#!/usr/bin/env python3
#
# olefied — a concurrency + stability front-end for HeinleinSupport's olefy.
#
# Upstream olefy.py is a single-threaded asyncio server that runs olevba as a
# *blocking* subprocess on the event loop with no timeout. That is fine for one
# message at a time, but a mail pipeline doing ~1000 msg/s needs:
#   * concurrency        — olevba is CPU-bound, so we run a POOL of olefy worker
#                          processes (one in-flight scan each) and load-balance.
#   * a hard scan timeout — a malformed/poison document can wedge olevba; upstream
#                          never recovers that worker. We bound every scan and
#                          KILL+RESPAWN any worker that blows the timeout.
#   * backpressure        — cap concurrent scans to the worker count; queue briefly,
#                          then return a 503-style error instead of unbounded growth.
#   * an input cap        — reject oversized uploads instead of buffering forever.
#
# olefy.py itself is shipped verbatim from upstream and used unmodified as the
# worker; all of the above lives here, at the runtime layer.
#
# Protocol is transparent: we read the whole client request (the olefy line
# protocol ends when the client half-closes the connection), hand it to a free
# worker, and stream the worker's reply back. PING\n\n -> PONG still works
# (answered by whichever worker handles it).

import asyncio
import hashlib
import logging
import os
import shutil
import signal
import sys

try:
    import redis.asyncio as aioredis  # optional; only needed if OLEFIED_REDIS_URL set
except ImportError:
    aioredis = None


# Config clamps are collected here and logged from main(), because most _int()
# calls run at import time before logging is configured.
_clamps = []


def _int(name, default, minimum=None):
    """Parse an int env var. A non-numeric value falls back to default; a value
    below `minimum` (when given) is clamped up to it. Clamping a zero/negative
    operational knob stops it from crashing startup (Semaphore(-1)), running with
    zero workers, or making every request instantly busy/timeout."""
    try:
        v = int(os.getenv(name, str(default)))
    except ValueError:
        v = default
    if minimum is not None and v < minimum:
        _clamps.append((name, v, minimum))
        v = minimum
    return v


# ---- public listener -------------------------------------------------------
BIND_ADDR = os.getenv("OLEFY_BINDADDRESS", "0.0.0.0") or "0.0.0.0"
BIND_PORT = _int("OLEFY_BINDPORT", 10050, minimum=1)

# ---- worker pool -----------------------------------------------------------
WORKERS = _int("OLEFIED_WORKERS", os.cpu_count() or 4, minimum=1)
WORKER_HOST = "127.0.0.1"
WORKER_BASE_PORT = _int("OLEFIED_WORKER_BASE_PORT", 10100, minimum=1)
OLEFY_PY = os.getenv("OLEFIED_OLEFY_PATH", "/usr/local/bin/olefy.py")
PYTHON = os.getenv("OLEFY_PYTHON_PATH", sys.executable)

# ---- limits / timeouts (seconds, bytes) ------------------------------------
REQUEST_TIMEOUT = _int("OLEFIED_REQUEST_TIMEOUT", 60, minimum=1)   # max time for one scan
READ_TIMEOUT = _int("OLEFIED_READ_TIMEOUT", 30, minimum=1)         # max time to read client upload
QUEUE_TIMEOUT = _int("OLEFIED_QUEUE_TIMEOUT", 15, minimum=1)       # max wait for a free worker
HEALTH_INTERVAL = _int("OLEFIED_HEALTH_INTERVAL", 30, minimum=1)   # idle worker PING cadence
HEALTH_TIMEOUT = _int("OLEFIED_HEALTH_TIMEOUT", 5, minimum=1)
READY_TIMEOUT = _int("OLEFIED_READY_TIMEOUT", 30, minimum=1)       # startup readiness per worker
MAX_REQUEST_BYTES = _int("OLEFIED_MAX_REQUEST_BYTES", 50 * 1024 * 1024, minimum=1024)
# cap simultaneously-accepted connections so per-connection upload buffers
# (up to MAX_REQUEST_BYTES each) can't exhaust memory under a connection flood.
MAX_CONNS = _int("OLEFIED_MAX_CONNS", max(WORKERS * 4, 16), minimum=1)

TMP_BASE = os.getenv("OLEFY_TMPDIR", "/tmp")

# ---- result cache (optional, redis) ----------------------------------------
# Cache successful olevba results keyed by document hash. Identical attachments
# recur constantly across a mail stream, and olevba is the expensive step — a
# hit skips the worker entirely. Shared redis also dedupes across all replicas.
# Disabled unless OLEFIED_REDIS_URL is set; any redis error is non-fatal (we
# just scan). Only SUCCESSFUL scans are cached, never error/timeout/busy.
REDIS_URL = os.getenv("OLEFIED_REDIS_URL", "").strip()
CACHE_TTL = _int("OLEFIED_CACHE_TTL", 86400, minimum=0)  # seconds; 0 = no expiry
CACHE_PREFIX = os.getenv("OLEFIED_CACHE_PREFIX", "olefied")
CACHE_OP_TIMEOUT = _int("OLEFIED_CACHE_OP_TIMEOUT", 2, minimum=1)  # per redis op, seconds
CACHE_KEY_VERSION = "1"  # bump if the request/reply format changes

LOGLVL = _int("OLEFY_LOGLVL", 20)
logging.basicConfig(
    stream=sys.stdout, level=LOGLVL,
    format="olefied %(levelname)s %(message)s",
)
log = logging.getLogger("olefied")

PONG = b"PONG"
ERR_BUSY = b'[ { "error": "olefied busy: no free worker" } ]\t\n\n\t'
ERR_TIMEOUT = b'[ { "error": "olefied scan timeout" } ]\t\n\n\t'
ERR_TOO_BIG = b'[ { "error": "olefied request too large" } ]\t\n\n\t'
ERR_INTERNAL = b'[ { "error": "olefied internal error" } ]\t\n\n\t'


def _valid_reply(reply):
    """A well-formed worker reply is either a JSON array (olevba result, starts
    with '[') or PONG (health check). Empty or anything else means the worker
    died/closed before producing output — caller treats it as an internal error
    and must not cache it."""
    s = reply.lstrip()
    return s.startswith(b"[") or s.strip() == PONG


def oletools_version():
    """oletools version → part of the cache key, so a oletools bump auto-
    invalidates every entry. olefyd runs in the same venv as oletools, so read
    it in-process. Best-effort; 'unknown' on failure."""
    try:
        from importlib.metadata import version
        return version("oletools")
    except Exception:  # noqa: BLE001
        return "unknown"


class Cache:
    """Optional redis result cache. Entirely non-fatal: any error → cache miss,
    the scan still happens. Only successful olevba replies are stored."""

    def __init__(self, ns):
        self.ns = ns                       # olevba version → key namespace
        self.r = None
        self.enabled = bool(REDIS_URL)
        self.hits = 0
        self.misses = 0
        if REDIS_URL and aioredis is None:
            log.error("OLEFIED_REDIS_URL set but 'redis' package not installed — cache OFF")
            self.enabled = False

    async def connect(self):
        if not self.enabled:
            log.info("result cache disabled (set OLEFIED_REDIS_URL to enable)")
            return
        try:
            self.r = aioredis.from_url(
                REDIS_URL, socket_timeout=CACHE_OP_TIMEOUT,
                socket_connect_timeout=CACHE_OP_TIMEOUT,
            )
            await asyncio.wait_for(self.r.ping(), CACHE_OP_TIMEOUT)
            log.info("result cache ON (redis, ns=oletools-%s, ttl=%ds)", self.ns, CACHE_TTL)
        except Exception as exc:  # noqa: BLE001
            log.error("redis connect failed (%s) — cache OFF", exc)
            self.enabled = False
            self.r = None

    def key_for(self, data):
        """Cache key for an oletools scan, keyed on the DOCUMENT BODY only (so a
        varying Rspamd-ID header doesn't fragment the cache). None = don't cache."""
        if not self.enabled:
            return None
        if data[:4] == b"PING":
            return None
        sep = data.find(b"\n\n")
        if sep < 0:
            return None
        if b"Method: oletools" not in data[:sep]:
            return None
        body = data[sep + 2:]
        if not body:
            return None
        h = hashlib.sha256(body).hexdigest()
        return f"{CACHE_PREFIX}:v{CACHE_KEY_VERSION}:{self.ns}:{h}"

    async def get(self, key):
        if not self.enabled:
            return None
        try:
            val = await asyncio.wait_for(self.r.get(key), CACHE_OP_TIMEOUT)
        except Exception as exc:  # noqa: BLE001
            log.warning("cache get failed (%s) — treating as miss", exc)
            return None
        if val is None:
            self.misses += 1
        else:
            self.hits += 1
        return val

    async def set(self, key, value):
        if not self.enabled:
            return
        try:
            await asyncio.wait_for(
                self.r.set(key, value, ex=CACHE_TTL or None), CACHE_OP_TIMEOUT)
        except Exception as exc:  # noqa: BLE001
            log.warning("cache set failed (%s) — ignored", exc)

    async def close(self):
        if self.r is not None:
            try:
                await self.r.aclose()
            except Exception as exc:  # noqa: BLE001
                log.debug("redis close failed (%s) — ignored", exc)


class Worker:
    """One upstream olefy.py process listening on 127.0.0.1:<port>."""

    def __init__(self, index):
        self.index = index
        self.port = WORKER_BASE_PORT + index
        self.tmpdir = os.path.join(TMP_BASE, "olefied", f"w{self.port}")
        self.proc = None
        # accounting for the supervisor: a worker is either in-flight (busy),
        # available (queued), or — if neither — leaked and must be re-recycled.
        self.busy = False
        self.queued = False
        self.recycling = False

    def _env(self):
        env = dict(os.environ)
        env["OLEFY_BINDADDRESS"] = WORKER_HOST   # loopback only — never exposed
        env["OLEFY_BINDPORT"] = str(self.port)
        env["OLEFY_TMPDIR"] = self.tmpdir        # per-worker scratch, mode 0700
        return env

    async def start(self):
        os.makedirs(self.tmpdir, mode=0o700, exist_ok=True)
        self.proc = await asyncio.create_subprocess_exec(
            PYTHON, OLEFY_PY, env=self._env(),
        )
        log.info("worker %d started (pid %s, port %d)", self.index, self.proc.pid, self.port)

    async def stop(self):
        if self.proc and self.proc.returncode is None:
            try:
                self.proc.kill()
            except ProcessLookupError:
                pass
            try:
                await asyncio.wait_for(self.proc.wait(), 5)
            except asyncio.TimeoutError:
                pass

    async def ping(self, timeout=HEALTH_TIMEOUT):
        try:
            r, w = await asyncio.wait_for(
                asyncio.open_connection(WORKER_HOST, self.port), timeout)
        except (OSError, asyncio.TimeoutError):
            return False
        try:
            w.write(b"PING\n\n")
            if w.can_write_eof():
                w.write_eof()
            await asyncio.wait_for(w.drain(), timeout)
            resp = await asyncio.wait_for(r.read(), timeout)
            return resp.strip() == PONG
        except (OSError, asyncio.TimeoutError):
            return False
        finally:
            w.close()

    async def wait_ready(self):
        loop = asyncio.get_running_loop()
        deadline = loop.time() + READY_TIMEOUT
        delay = 0.2
        while loop.time() < deadline:
            if self.proc.returncode is not None:
                raise RuntimeError(f"worker {self.index} exited rc={self.proc.returncode} during startup")
            if await self.ping():
                return True
            await asyncio.sleep(delay)
            delay = min(delay * 1.5, 2.0)
        return False

    async def scan(self, data):
        """Send a full request to this worker, return its full reply."""
        r, w = await asyncio.open_connection(WORKER_HOST, self.port)
        try:
            w.write(data)
            if w.can_write_eof():
                w.write_eof()
            await w.drain()
            return await r.read()   # olefy replies then closes the connection
        finally:
            w.close()


class Pool:
    def __init__(self):
        self.workers = [Worker(i) for i in range(WORKERS)]
        self.idle = asyncio.Queue()
        self._lock = asyncio.Lock()
        # bound concurrent connections so buffered uploads can't OOM us
        self.conn_sem = asyncio.Semaphore(MAX_CONNS)
        self.cache = Cache(oletools_version())  # disabled unless OLEFIED_REDIS_URL

    def _make_available(self, worker):
        # flag ordering matters: set queued before clearing busy so a healthy
        # worker is never seen as both not-busy and not-queued (supervise would
        # otherwise mistake the gap for a leak and recycle it).
        worker.queued = True
        self.idle.put_nowait(worker)
        worker.busy = False

    async def start(self):
        await asyncio.gather(*(w.start() for w in self.workers))
        results = await asyncio.gather(*(w.wait_ready() for w in self.workers),
                                       return_exceptions=True)
        ready = 0
        for w, ok in zip(self.workers, results):
            if ok is True:
                self._make_available(w)
                ready += 1
            else:
                log.error("worker %d not ready: %s", w.index, ok)
        if ready == 0:
            raise RuntimeError("no workers became ready")
        log.info("%d/%d workers ready", ready, WORKERS)

    async def acquire(self):
        # A worker can die while sitting idle in the queue (crash, OOM-kill).
        # Don't hand a corpse to a client — it would accept the connection, fail
        # the scan, and only then get recycled. Check the process is alive before
        # returning it; recycle any dead one and keep waiting, bounded by the
        # overall QUEUE_TIMEOUT.
        loop = asyncio.get_running_loop()
        deadline = loop.time() + QUEUE_TIMEOUT
        while True:
            remaining = deadline - loop.time()
            if remaining <= 0:
                raise asyncio.TimeoutError
            worker = await asyncio.wait_for(self.idle.get(), remaining)
            worker.queued = False
            if worker.proc is None or worker.proc.returncode is not None:
                rc = None if worker.proc is None else worker.proc.returncode
                log.warning("idle worker %d was dead (rc=%s) — recycling, retrying",
                            worker.index, rc)
                asyncio.create_task(self.recycle(worker))
                continue
            worker.busy = True
            return worker

    def release(self, worker):
        self._make_available(worker)

    async def recycle(self, worker):
        """A worker timed out, errored, died or leaked — kill and respawn it."""
        if worker.recycling:
            return
        worker.recycling = True
        worker.busy = False
        async with self._lock:
            log.warning("recycling worker %d (port %d)", worker.index, worker.port)
            await worker.stop()
            try:
                await worker.start()
                if await worker.wait_ready():
                    self._make_available(worker)
                else:
                    log.error("worker %d failed to come back ready — will retry", worker.index)
            except Exception as exc:  # noqa: BLE001
                log.error("worker %d respawn failed: %s — will retry", worker.index, exc)
            finally:
                worker.recycling = False

    async def stop(self):
        await asyncio.gather(*(w.stop() for w in self.workers), return_exceptions=True)

    async def supervise(self):
        """Recycle any worker that is neither in-flight nor available — i.e.
        a process that died, or a respawn that never came back ready (mute but
        alive). Healthy idle/busy/recycling workers are left untouched."""
        while True:
            await asyncio.sleep(HEALTH_INTERVAL)
            for w in self.workers:
                if w.busy or w.queued or w.recycling:
                    continue
                log.warning("worker %d not in pool (proc rc=%s) — recycling",
                            w.index, None if w.proc is None else w.proc.returncode)
                await self.recycle(w)


async def read_request(reader):
    """Read the whole client upload until EOF, bounded by size + time."""
    buf = bytearray()

    async def _pump():
        while True:
            chunk = await reader.read(65536)
            if not chunk:
                return
            buf.extend(chunk)
            if len(buf) > MAX_REQUEST_BYTES:
                raise OverflowError
    await asyncio.wait_for(_pump(), READ_TIMEOUT)
    return bytes(buf)


def handler(pool):
    async def handle(reader, writer):
        peer = writer.get_extra_info("peername")
        reply = ERR_INTERNAL
        # admission control: don't even start reading until we hold a connection
        # slot, so total buffered upload bytes stay bounded under a flood.
        try:
            await asyncio.wait_for(pool.conn_sem.acquire(), QUEUE_TIMEOUT)
        except asyncio.TimeoutError:
            log.error("%s connection slot unavailable within %ds", peer, QUEUE_TIMEOUT)
            try:
                writer.write(ERR_BUSY)
                await writer.drain()
            except (OSError, ConnectionError):
                pass
            writer.close()
            return
        try:
            try:
                data = await read_request(reader)
            except OverflowError:
                reply = ERR_TOO_BIG
                log.error("%s request exceeded %d bytes", peer, MAX_REQUEST_BYTES)
                return
            except asyncio.TimeoutError:
                log.error("%s read timeout", peer)
                return

            # result cache: a document-hash hit skips the worker entirely
            cache_key = pool.cache.key_for(data)
            if cache_key:
                hit = await pool.cache.get(cache_key)
                if hit is not None:
                    reply = hit
                    log.debug("%s cache hit", peer)
                    return

            try:
                worker = await pool.acquire()
            except asyncio.TimeoutError:
                reply = ERR_BUSY
                log.error("%s no free worker within %ds", peer, QUEUE_TIMEOUT)
                return

            try:
                raw = await asyncio.wait_for(worker.scan(data), REQUEST_TIMEOUT)
                if not _valid_reply(raw):
                    # A worker that accepts the connection then exits/closes
                    # before emitting olefy output returns b""/truncated bytes.
                    # Treat that as an internal error: return ERR_INTERNAL, recycle
                    # the (likely wedged) worker, and never cache the garbage.
                    log.error("%s worker %d returned empty/malformed reply (%d bytes) — recycling",
                              peer, worker.index, len(raw))
                    reply = ERR_INTERNAL
                    asyncio.create_task(pool.recycle(worker))
                else:
                    reply = raw
                    pool.release(worker)
                    # cache only successful scans — never error/timeout/busy replies.
                    # olefy/olefied errors are the shape `[ { "error": ... } ]`, so
                    # test the prefix (real olevba JSON may mention "error" deeper in).
                    if cache_key and b'"error"' not in reply[:64]:
                        await pool.cache.set(cache_key, reply)
            except asyncio.TimeoutError:
                reply = ERR_TIMEOUT
                log.error("%s scan timeout (%ds) — recycling worker %d",
                          peer, REQUEST_TIMEOUT, worker.index)
                asyncio.create_task(pool.recycle(worker))
            except Exception as exc:  # noqa: BLE001
                log.error("%s worker %d scan error: %s — recycling", peer, worker.index, exc)
                asyncio.create_task(pool.recycle(worker))
        finally:
            pool.conn_sem.release()
            try:
                writer.write(reply)
                await writer.drain()
            except (OSError, ConnectionError):
                pass
            writer.close()
    return handle


async def main():
    for name, bad, clamped in _clamps:
        log.warning("invalid %s=%s — clamped to %s", name, bad, clamped)

    # scrub any stale per-worker scratch from a previous run
    stale = os.path.join(TMP_BASE, "olefied")
    if os.path.isdir(stale):
        shutil.rmtree(stale, ignore_errors=True)

    pool = Pool()
    await pool.cache.connect()
    await pool.start()

    server = await asyncio.start_server(handler(pool), BIND_ADDR, BIND_PORT)
    for sock in server.sockets:
        log.info("olefied serving on %s (workers=%d, scan_timeout=%ds, max_bytes=%d)",
                 sock.getsockname(), WORKERS, REQUEST_TIMEOUT, MAX_REQUEST_BYTES)

    sup = asyncio.ensure_future(pool.supervise())

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:
            pass

    async with server:
        await stop.wait()

    log.info("shutting down (cache hits=%d misses=%d)", pool.cache.hits, pool.cache.misses)
    sup.cancel()
    server.close()
    await pool.stop()
    await pool.cache.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
