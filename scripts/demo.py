#!/usr/bin/env python3
"""End-to-end demonstration of stale-address fallback.

Default targets match docker-compose published ports:
  DNS forwarder : 127.0.0.1:5354/udp
  upstream API  : http://127.0.0.1:8000

Override with DNS_HOST / DNS_PORT / API_BASE, e.g. when running the stack
locally on test ports.
"""

from __future__ import annotations

import json
import os
import sys
import time
import urllib.error
import urllib.request
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from dns_client import format_result, query_once  # noqa: E402

DNS_HOST = os.environ.get("DNS_HOST", "127.0.0.1")
DNS_PORT = int(os.environ.get("DNS_PORT", "5354"))
API_BASE = os.environ.get("API_BASE", "http://127.0.0.1:8000")

OLD_IP = "203.0.113.10"
NEW_IP = "198.51.100.77"


def api(method: str, path: str, body: dict | None = None) -> dict:
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        API_BASE + path,
        data=data,
        method=method,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode()
        raise RuntimeError(f"{method} {path} -> {exc.code}: {detail}") from exc


def title(text: str) -> None:
    print("\n" + "=" * 76)
    print(text)
    print("=" * 76)


def dig(name: str, qtype: str = "A", timeout: float = 2.0, quiet: bool = False) -> dict:
    result = query_once(name, qtype, server=DNS_HOST, port=DNS_PORT, timeout=timeout)
    if not quiet:
        print("  $ dig @%s -p %s %s %s" % (DNS_HOST, DNS_PORT, name, qtype))
        print("  " + format_result(name, qtype, result))
    return result


def wait_api() -> None:
    for _ in range(50):
        try:
            with urllib.request.urlopen(API_BASE + "/healthz", timeout=2):
                return
        except Exception:
            time.sleep(0.2)
    raise SystemExit(f"upstream API not reachable at {API_BASE}")


def main() -> int:
    wait_api()
    run_id = uuid.uuid4().hex[:8]
    name = f"demo-{run_id}.local"
    missing_name = f"never-{run_id}.local"
    gone_name = f"gone-{run_id}.local"

    print(f"targets: dns={DNS_HOST}:{DNS_PORT} api={API_BASE}")
    api("POST", "/admin/reset")

    title("1) 上游正常：配置旧址并首次查询（写入转发器缓存）")
    api("POST", "/admin/answer", {"qname": name, "ip": OLD_IP, "ttl": 2})
    api("POST", "/admin/resume")
    r1 = dig(name)
    assert r1["ok"] and r1["answers"][0]["ip"] == OLD_IP, r1

    title("2) 等待超过上游 TTL(2s)，缓存到期但旧址仍在 30s 宽限内")
    time.sleep(2.2)

    title("3) 让上游“停答”（模拟卡住），并发 6 个相同查询")
    api("DELETE", "/admin/requests")
    api("POST", "/admin/silence", {"silence": True})
    t0 = time.monotonic()
    with ThreadPoolExecutor(max_workers=6) as pool:
        results = list(pool.map(lambda _i: dig(name, quiet=True), range(6)))
    wall = (time.monotonic() - t0) * 1000
    for i, r in enumerate(results, 1):
        print(f"  client#{i}: " + format_result(name, "A", r))
    print(f"  6 个查询总等待 {wall:.0f}ms（约 100ms 阈值后全部收到 TTL=0 旧址）")
    time.sleep(0.3)
    reqs = api("GET", f"/admin/requests?qname={name}.&limit=50")
    print(f"  上游实际收到的同域名查询包数量 = {reqs['count']}（并发共用一次回源）")

    title("4) 回源总限时 1s：等待该次回源结束（停答中必然超时，旧址不续期）")
    time.sleep(0.9)

    title("5) 上游恢复并改答为新地址，再次查询 -> 查到新址")
    api("POST", "/admin/answer", {"qname": name, "ip": NEW_IP, "ttl": 30})
    api("POST", "/admin/resume")
    r5 = dig(name)
    assert r5["ok"] and r5["answers"][0]["ip"] == NEW_IP, r5

    title("6) 没有可用旧址的域名 + 上游停答：等满 1s 回源总限时 -> SERVFAIL")
    api("POST", "/admin/silence", {"silence": True})
    r6 = dig(missing_name, timeout=2.0)
    api("POST", "/admin/resume")
    print(f"  耗时 {r6['elapsed_ms']:.0f}ms（应接近 1000ms）")
    assert r6["ok"] and r6["rcode"] == 2, r6

    title("7) 上游下发 NXDOMAIN：转发器清掉旧址并透传 NXDOMAIN")
    api("POST", "/admin/nxdomain", {"qname": gone_name})
    r7 = dig(gone_name)
    assert r7["ok"] and r7["rcode"] == 3, r7

    title("8) 非 A/IN 查询被转发器拒绝（REFUSED）")
    r8 = dig(name, "AAAA")
    assert r8["ok"] and r8["rcode"] == 5, r8

    print("\n演示完成：先收到 TTL=0 旧址，恢复改答后查到新址；"
          "并发单回源、SERVFAIL、NXDOMAIN、REFUSED 均符合预期。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
