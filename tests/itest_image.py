#!/usr/bin/env python3
#
# Integration smoke against a running olefied container (real oletools/olevba).
# Usage: python tests/itest_image.py [host] [port]   (default 127.0.0.1 10050)

import os
import socket
import sys
import threading

HOST = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1"
PORT = int(sys.argv[2]) if len(sys.argv) > 2 else 10050
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TESTMSG = os.path.join(ROOT, "Test-Messages", "oletools.test")
_failures = []


def req(payload, timeout=30):
    s = socket.create_connection((HOST, PORT), timeout=timeout)
    s.sendall(payload)
    s.shutdown(socket.SHUT_WR)
    buf = b""
    while True:
        c = s.recv(65536)
        if not c:
            break
        buf += c
    s.close()
    return buf


def check(name, cond, detail=""):
    print(f"  {'ok  ' if cond else 'FAIL'} {name} {detail if not cond else ''}")
    if not cond:
        _failures.append(name)


def main():
    check("ping", req(b"PING\n\n").strip() == b"PONG")

    with open(TESTMSG, "rb") as fh:
        body = fh.read()
    scan = b"OLEFY/1.0\nMethod: oletools\nRspamd-ID: itest1\n\n" + body
    r = req(scan)
    check("real_olevba_scan", b"olevba" in r and b"MetaInformation" in r, r[:120])

    out = [None] * 8
    def one(i):
        out[i] = req(scan)
    ts = [threading.Thread(target=one, args=(i,)) for i in range(8)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    check("concurrent_real_scans", all(o and b"olevba" in o for o in out))

    if _failures:
        print(f"\nFAILED: {_failures}")
        sys.exit(1)
    print("\nimage integration smoke passed")


if __name__ == "__main__":
    main()
