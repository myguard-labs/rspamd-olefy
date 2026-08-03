#!/usr/bin/env python3
#
# Test double for olefy.py: speaks the same line protocol (PING -> PONG, else
# read-until-EOF then reply-and-close) but with no oletools dependency, so the
# olefyd dispatcher can be tested deterministically.
#
# Honoured env (set per worker by olefyd): OLEFY_BINDADDRESS, OLEFY_BINDPORT.
# Test knobs: FAKE_SLEEP (seconds before replying to a scan, to force a
# dispatcher timeout), FAKE_EXIT (exit code to die immediately, to test respawn).

import asyncio
import os
import sys

ADDR = os.getenv("OLEFY_BINDADDRESS", "127.0.0.1") or "127.0.0.1"
PORT = int(os.getenv("OLEFY_BINDPORT", "10050"))
SLEEP = float(os.getenv("FAKE_SLEEP", "0"))
# FAKE_EMPTY: reply to a scan with nothing (close the connection without emitting
# olevba JSON) to exercise the dispatcher's empty/malformed-reply handling.
EMPTY = os.getenv("FAKE_EMPTY", "") not in ("", "0")

_count = 0


async def handle(reader, writer):
    global _count
    data = await reader.read()          # client half-closes to signal EOF
    if data[:4] == b"PING":
        writer.write(b"PONG")
    elif EMPTY:
        pass  # close without producing any output (worker died mid-scan)
    else:
        if SLEEP:
            await asyncio.sleep(SLEEP)
        # per-scan counter: lets a cache test prove a hit (same n on a 2nd
        # identical request) vs a miss (n increments → reached the worker).
        _count += 1
        writer.write(b'[ { "type": "fake", "ok": true, "n": %d } ]\t\n\n\t' % _count)
    try:
        await writer.drain()
    except (OSError, ConnectionError):
        pass
    writer.close()


async def main():
    server = await asyncio.start_server(handle, ADDR, PORT)
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    code = int(os.getenv("FAKE_EXIT", "0"))
    if code:
        sys.exit(code)
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
