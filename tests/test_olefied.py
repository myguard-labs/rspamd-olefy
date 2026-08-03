#!/usr/bin/env python3
#
# Dispatcher tests for olefyd.py. Uses tests/fake_olefy.py as the worker, so the
# pool / timeout / backpressure / recycle logic is exercised deterministically
# with no oletools dependency. Stdlib only; run as: python tests/test_olefied.py
# Real-olevba behaviour is covered separately by tests/itest_image.py.

import os
import signal
import socket
import subprocess
import sys
import threading
import time

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
OLEFYD = os.path.join(ROOT, "docker", "olefyd.py")
FAKE = os.path.join(HERE, "fake_olefy.py")

SCAN_REQ = b"OLEFY/1.0\nMethod: oletools\nRspamd-ID: test01\n\n" + b"x" * 600
_failures = []


def req(port, payload, timeout=20):
    s = socket.create_connection(("127.0.0.1", port), timeout=timeout)
    s.settimeout(timeout)
    s.sendall(payload)
    s.shutdown(socket.SHUT_WR)
    buf = b""
    while True:
        try:
            c = s.recv(65536)
        except TimeoutError:
            break
        if not c:
            break
        buf += c
    s.close()
    return buf


def start(port, worker_base, **env):
    e = dict(os.environ)
    e.update(
        OLEFY_BINDADDRESS="127.0.0.1",
        OLEFY_BINDPORT=str(port),
        OLEFIED_WORKER_BASE_PORT=str(worker_base),
        OLEFIED_OLEFY_PATH=FAKE,
        OLEFY_PYTHON_PATH=sys.executable,
        OLEFY_TMPDIR=os.path.join("/tmp", f"olefied-test-{port}"),
        OLEFY_LOGLVL="30",
        # cache OFF by default so scenarios are deterministic and don't pollute
        # each other via the shared SCAN_REQ; test_cache opts back in explicitly.
        OLEFIED_REDIS_URL="",
    )
    e.update({k: str(v) for k, v in env.items()})
    # own session/process group so teardown can reap the spawned workers too,
    # not just the dispatcher (orphaned workers would hold their loopback ports).
    p = subprocess.Popen(
        [sys.executable, OLEFYD], env=e, start_new_session=True,
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    # wait until the dispatcher serves PONG
    deadline = time.time() + 20
    while time.time() < deadline:
        try:
            if req(port, b"PING\n\n", timeout=2).strip() == b"PONG":
                return p
        except OSError:
            pass
        time.sleep(0.3)
    stop(p)
    raise RuntimeError(f"olefyd on {port} never became ready")


def stop(p):
    """Kill the dispatcher and every worker it spawned (whole process group)."""
    try:
        os.killpg(os.getpgid(p.pid), signal.SIGKILL)
    except (ProcessLookupError, PermissionError):
        p.kill()
    try:
        p.wait(timeout=5)
    except subprocess.TimeoutExpired:
        pass


def check(name, cond, detail=""):
    if cond:
        print(f"  ok   {name}")
    else:
        print(f"  FAIL {name} {detail}")
        _failures.append(name)


def test_ping():
    p = start(10070, 10200, OLEFIED_WORKERS=2)
    try:
        check("ping_pong", req(10070, b"PING\n\n").strip() == b"PONG")
    finally:
        stop(p)


def test_scan():
    p = start(10071, 10210, OLEFIED_WORKERS=2)
    try:
        r = req(10071, SCAN_REQ)
        check("happy_scan", b'"ok": true' in r, r[:80])
    finally:
        stop(p)


def test_concurrency():
    p = start(10072, 10220, OLEFIED_WORKERS=2)
    try:
        out = [None] * 12
        def one(i):
            out[i] = req(10072, SCAN_REQ)
        ts = [threading.Thread(target=one, args=(i,)) for i in range(12)]
        [t.start() for t in ts]
        [t.join() for t in ts]
        check("concurrency_12_over_2_workers",
              all(o and b'"ok": true' in o for o in out))
    finally:
        stop(p)


def test_too_big():
    p = start(10073, 10230, OLEFIED_WORKERS=1, OLEFIED_MAX_REQUEST_BYTES=1024)
    try:
        r = req(10073, b"OLEFY/1.0\n\n" + b"x" * 4096)
        check("too_big_rejected", b"too large" in r, r[:80])
    finally:
        stop(p)


def test_busy():
    # 1 worker, each scan sleeps 3s; queue wait 1s → 2nd request must report busy
    p = start(10074, 10240, OLEFIED_WORKERS=1, OLEFIED_QUEUE_TIMEOUT=1,
              OLEFIED_REQUEST_TIMEOUT=10, FAKE_SLEEP=3)
    try:
        bg = threading.Thread(target=req, args=(10074, SCAN_REQ))
        bg.start()
        time.sleep(0.5)                       # let the bg scan occupy the worker
        t0 = time.time()
        r = req(10074, SCAN_REQ)
        check("busy_when_saturated", b"busy" in r, r[:80])
        check("busy_returns_fast", time.time() - t0 < 5)
        bg.join()
    finally:
        stop(p)


def test_timeout_and_recycle():
    # scan sleeps 5s, dispatcher timeout 1s → timeout error, worker recycled,
    # then a PING must succeed (proves the respawned worker serves again).
    p = start(10075, 10250, OLEFIED_WORKERS=1, OLEFIED_REQUEST_TIMEOUT=1,
              OLEFIED_QUEUE_TIMEOUT=8, FAKE_SLEEP=5)
    try:
        r = req(10075, SCAN_REQ, timeout=10)
        check("scan_timeout_reported", b"scan timeout" in r, r[:80])
        time.sleep(2)                         # allow recycle to respawn worker
        check("pool_recovers_after_timeout",
              req(10075, b"PING\n\n", timeout=8).strip() == b"PONG")
    finally:
        stop(p)


def test_empty_reply_is_internal_error():
    # A worker that closes without emitting olevba JSON must yield an internal
    # error to the client, not an empty/garbage success, and must not hang.
    p = start(10077, 10270, OLEFIED_WORKERS=1, FAKE_EMPTY="1",
              OLEFIED_QUEUE_TIMEOUT=8)
    try:
        r = req(10077, SCAN_REQ, timeout=10)
        check("empty_reply_internal_error", b"internal error" in r, r[:80])
        # the worker is recycled after a bad reply; PING must work again
        time.sleep(2)
        check("pool_recovers_after_empty_reply",
              req(10077, b"PING\n\n", timeout=8).strip() == b"PONG")
    finally:
        stop(p)


def test_config_clamped():
    # Zero/negative operational knobs must be clamped to safe minimums so the
    # dispatcher still starts and serves instead of crashing (Semaphore(-1)),
    # running zero workers, or instant-timeouting every request.
    p = start(10078, 10280, OLEFIED_WORKERS=0, OLEFIED_MAX_CONNS=-5,
              OLEFIED_QUEUE_TIMEOUT=0)
    try:
        check("starts_with_clamped_config",
              req(10078, b"PING\n\n").strip() == b"PONG")
        check("scans_with_clamped_config", b'"ok": true' in req(10078, SCAN_REQ))
    finally:
        stop(p)


def _redis_url():
    url = os.getenv("OLEFIED_REDIS_URL", "").strip()
    if not url:
        return None
    try:
        import redis
        c = redis.from_url(url, socket_connect_timeout=2)
        c.ping()
        c.flushdb()
        return url
    except Exception:  # noqa: BLE001
        return None


def _n(reply):
    import re as _re
    m = _re.search(rb'"n":\s*(\d+)', reply)
    return int(m.group(1)) if m else None


def test_cache():
    url = _redis_url()
    if not url:
        print("  skip cache (no reachable OLEFIED_REDIS_URL / redis pkg)")
        return
    p = start(10076, 10260, OLEFIED_WORKERS=1, OLEFIED_REDIS_URL=url)
    try:
        body1 = b"OLEFY/1.0\nMethod: oletools\nRspamd-ID: aaaaaa\n\n" + b"DOC-ONE" * 100
        # same document, different Rspamd-ID header → must still be one cache key
        body1b = b"OLEFY/1.0\nMethod: oletools\nRspamd-ID: bbbbbb\n\n" + b"DOC-ONE" * 100
        body2 = b"OLEFY/1.0\nMethod: oletools\nRspamd-ID: aaaaaa\n\n" + b"DOC-TWO" * 100
        n1 = _n(req(10076, body1))
        n1b = _n(req(10076, body1b))
        n2 = _n(req(10076, body2))
        check("cache_hit_same_doc", n1 is not None and n1b == n1, f"{n1} {n1b}")
        check("cache_key_ignores_rspamd_id", n1b == n1)
        check("cache_miss_diff_doc", n2 is not None and n2 != n1, f"{n1} {n2}")
    finally:
        stop(p)


if __name__ == "__main__":
    for t in (test_ping, test_scan, test_concurrency, test_too_big,
              test_busy, test_timeout_and_recycle, test_empty_reply_is_internal_error,
              test_config_clamped, test_cache):
        print(t.__name__)
        t()
    if _failures:
        print(f"\nFAILED: {_failures}")
        sys.exit(1)
    print("\nall dispatcher tests passed")
