#!/usr/bin/env python3
"""Tiny DNS UDP client used by the demo/self-test scripts and as a CLI.

Usage:
    python scripts/dns_client.py [--server 127.0.0.1] [--port 5354] NAME [A|AAAA|...]
"""

from __future__ import annotations

import argparse
import random
import socket
import sys
import time
from pathlib import Path

# Make the repository root importable when run directly.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.dns_codec import (  # noqa: E402
    CLASS_IN,
    TYPE_A,
    build_query,
    parse_message,
    qtype_name,
    rcode_name,
)

_QTYPES = {"A": 1, "AAAA": 28, "CNAME": 5, "SOA": 6, "NS": 2, "PTR": 12}


def query_once(
    name: str,
    qtype: int | str = "A",
    *,
    server: str = "127.0.0.1",
    port: int = 5354,
    timeout: float = 2.0,
    qclass: int = CLASS_IN,
) -> dict:
    if isinstance(qtype, str):
        qtype = _QTYPES.get(qtype.upper(), None)
        if qtype is None:
            raise ValueError(f"unsupported qtype {qtype!r}")

    packet = build_query(name, qtype=qtype, qclass=qclass, msg_id=random.randint(0, 65535))
    started = time.monotonic()
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        sock.settimeout(timeout)
        sock.sendto(packet, (server, port))
        data, _addr = sock.recvfrom(65535)
        elapsed_ms = (time.monotonic() - started) * 1000
        msg = parse_message(data)
        a_records = [
            {"ip": rr.ip, "ttl": rr.ttl}
            for rr in msg.answers
            if rr.rtype == TYPE_A and rr.ip is not None
        ]
        return {
            "ok": True,
            "rcode": msg.rcode,
            "rcode_name": rcode_name(msg.rcode),
            "answers": a_records,
            "answer_count": msg.ancount,
            "elapsed_ms": elapsed_ms,
        }
    except Exception as exc:
        return {
            "ok": False,
            "error": f"{type(exc).__name__}: {exc}",
            "elapsed_ms": (time.monotonic() - started) * 1000,
        }
    finally:
        sock.close()


def format_result(name: str, qtype: str, result: dict) -> str:
    if not result["ok"]:
        return f"{name} {qtype}: ERROR {result['error']} ({result['elapsed_ms']:.0f}ms)"
    if result["answers"]:
        addrs = ", ".join(f"{a['ip']} ttl={a['ttl']}" for a in result["answers"])
    else:
        addrs = "<no A records>"
    return (
        f"{name} {qtype}: {result['rcode_name']} {addrs} "
        f"({result['elapsed_ms']:.0f}ms)"
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Minimal DNS UDP query client")
    parser.add_argument("name")
    parser.add_argument("qtype", nargs="?", default="A")
    parser.add_argument("--server", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=int(__import__("os").environ.get("DNS_PORT", "5354")))
    parser.add_argument("--timeout", type=float, default=2.0)
    args = parser.parse_args()

    result = query_once(
        args.name, args.qtype, server=args.server, port=args.port, timeout=args.timeout
    )
    print(format_result(args.name, args.qtype.upper(), result))
    return 0 if result.get("ok") and result.get("rcode") == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
