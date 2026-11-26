#!/usr/bin/env python3
"""End-to-end self-test: launches the real FastAPI upstream and forwarders.

Run from the repository root:

    PYTHONPATH=/tmp/pylibs/lib/python3.11/site-packages \\
        python3 tests/selftest_e2e.py

The extra PYTHONPATH is only needed where fastapi/uvicorn are not installed
system-wide.  In Docker they are installed normally.
"""

from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(REPO_ROOT / "scripts"))

from dns_client import query_once  # noqa: E402

CHECKS: list[tuple[str, bool, str]] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    CHECKS.append((name, bool(cond), detail))
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}" + (f" -- {detail}" if detail else ""))


def free_port(kind: int = socket.SOCK_DGRAM) -> int:
    sock = socket.socket(socket.AF_INET, kind)
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


def api_request(base: str, method: str, path: str, body: dict | None = None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        base + path, data=data, method=method,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            raw = resp.read().decode()
            return resp.status, (json.loads(raw) if raw else {})
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode()
        try:
            return exc.code, json.loads(raw)
        except json.JSONDecodeError:
            return exc.code, {"raw": raw}


def wait_http(base: str, timeout: float = 15.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(base + "/healthz", timeout=2):
                return
        except Exception:
            time.sleep(0.2)
    raise RuntimeError(f"HTTP API not ready at {base}")


def wait_dns(port: int, timeout: float = 10.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = query_once("ready-probe.local", port=port, timeout=1.0)
        if result.get("ok"):
            return
        time.sleep(0.2)
    raise RuntimeError(f"DNS forwarder not answering on port {port}")


def upstream_packet_count(base: str, name: str) -> int:
    status, data = api_request(base, "GET", f"/admin/requests?qname={name}.&limit=500")
    return int(data.get("count", -1))


def wait_packet_count(base: str, name: str, expected: int, timeout: float = 2.0) -> int:
    deadline = time.monotonic() + timeout
    count = -1
    while time.monotonic() < deadline:
        count = upstream_packet_count(base, name)
        if count == expected:
            return count
        time.sleep(0.05)
    return count


def start_process(log_path: Path, args: list[str], env_extra: dict[str, str]):
    env = os.environ.copy()
    env["EXIT_ON_STDIN_EOF"] = "1"
    env.update(env_extra)
    log = open(log_path, "wb")
    proc = subprocess.Popen(
        [sys.executable, "-u"] + args,
        cwd=str(REPO_ROOT),
        env=env,
        stdin=subprocess.PIPE,
        stdout=log,
        stderr=subprocess.STDOUT,
    )
    return proc, log


def stop_process(proc, log_handle) -> None:
    try:
        if proc.poll() is None:
            try:
                proc.terminate()
                proc.wait(timeout=5)
            except PermissionError:
                # Some sandboxes deny kill(2) even to a child; the
                # stdin-EOF watchdog makes the process exit itself.
                if proc.stdin is not None:
                    proc.stdin.close()
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    pass
            except subprocess.TimeoutExpired:
                try:
                    proc.kill()
                    proc.wait(timeout=5)
                except (PermissionError, subprocess.TimeoutExpired):
                    pass
    finally:
        try:
            log_handle.close()
        except Exception:
            pass


def tail(path: Path, lines: int = 25) -> str:
    try:
        text = path.read_text(errors="replace").splitlines()
        return "\n".join(text[-lines:])
    except OSError:
        return "(no log)"


def main() -> int:
    run_id = uuid.uuid4().hex[:8]
    updns_port = free_port()
    api_port = free_port(socket.SOCK_STREAM)
    fwd_port = free_port()
    fwd_fast_port = free_port()

    api_base = f"http://127.0.0.1:{api_port}"
    up_log = Path("/tmp/dns-selftest-upstream.log")
    fwd_log = Path("/tmp/dns-selftest-forwarder.log")
    fwd_fast_log = Path("/tmp/dns-selftest-forwarder-fast.log")

    upstream_proc, up_log_h = start_process(
        up_log,
        ["-m", "app.upstream"],
        {
            "UPSTREAM_DNS_HOST": "127.0.0.1",
            "UPSTREAM_DNS_PORT": str(updns_port),
            "API_HOST": "127.0.0.1",
            "API_PORT": str(api_port),
        },
    )
    fwd_proc, fwd_log_h = start_process(
        fwd_log,
        ["-m", "app.forwarder"],
        {
            "DNS_LISTEN_HOST": "127.0.0.1",
            "DNS_LISTEN_PORT": str(fwd_port),
            "UPSTREAM_DNS_HOST": "127.0.0.1",
            "UPSTREAM_DNS_PORT": str(updns_port),
            "STALE_AFTER_MS": "100",
            "UPSTREAM_TIMEOUT_MS": "1000",
            "STALE_GRACE_S": "30",
        },
    )
    fwd_fast_proc, fwd_fast_log_h = start_process(
        fwd_fast_log,
        ["-m", "app.forwarder"],
        {
            "DNS_LISTEN_HOST": "127.0.0.1",
            "DNS_LISTEN_PORT": str(fwd_fast_port),
            "UPSTREAM_DNS_HOST": "127.0.0.1",
            "UPSTREAM_DNS_PORT": str(updns_port),
            "STALE_AFTER_MS": "100",
            "UPSTREAM_TIMEOUT_MS": "400",
            "STALE_GRACE_S": "1",
        },
    )

    failed_start = False
    try:
        print(f"ports: upstream-dns={updns_port} api={api_port} "
              f"forwarder={fwd_port} fast-forwarder={fwd_fast_port}")
        wait_http(api_base)
        wait_dns(fwd_port)
        wait_dns(fwd_fast_port)
        print("all three processes are up\n")

        api_request(api_base, "POST", "/admin/reset")

        # ---- T1 basic answer ------------------------------------------------
        print("T1 基本 A 查询经转发器得到配置地址")
        n1 = f"basic-{run_id}.local"
        api_request(api_base, "POST", "/admin/answer",
                    {"qname": n1, "ip": "203.0.113.10", "ttl": 30})
        r = query_once(n1, port=fwd_port)
        check("返回 NOERROR", r["ok"] and r["rcode"] == 0, str(r))
        check("得到配置 IP", bool(r["answers"]) and r["answers"][0]["ip"] == "203.0.113.10")
        check("TTL 为正且 <=30", bool(r["answers"]) and 0 < r["answers"][0]["ttl"] <= 30)

        # ---- T2 A/IN only policy -------------------------------------------
        print("T2 仅允许 A/IN，AAAA 与 CH 类别被 REFUSED")
        r_aaaa = query_once(n1, "AAAA", port=fwd_port)
        check("AAAA -> REFUSED(5)", r_aaaa["ok"] and r_aaaa["rcode"] == 5, str(r_aaaa))
        r_ch = query_once(n1, "A", qclass=3, port=fwd_port)
        check("A/CH -> REFUSED(5)", r_ch["ok"] and r_ch["rcode"] == 5, str(r_ch))

        # ---- T3 fresh cache hit --------------------------------------------
        print("T3 新鲜缓存命中不再回源")
        n3 = f"hit-{run_id}.local"
        api_request(api_base, "POST", "/admin/answer",
                    {"qname": n3, "ip": "203.0.113.20", "ttl": 60})
        query_once(n3, port=fwd_port)
        time.sleep(0.15)
        api_request(api_base, "DELETE", "/admin/requests")
        query_once(n3, port=fwd_port)
        time.sleep(0.15)
        cnt = upstream_packet_count(api_base, n3)
        check("第二次查询期间上游包数=0", cnt == 0, f"count={cnt}")

        # ---- T4 singleflight + stale + background refresh ------------------
        print("T4 到期后 8 个并发查询共用一次回源，100ms 收 TTL0 旧址，回源继续")
        n4 = f"sf-{run_id}.local"
        api_request(api_base, "POST", "/admin/answer",
                    {"qname": n4, "ip": "203.0.113.30", "ttl": 1})
        query_once(n4, port=fwd_port)
        time.sleep(1.15)  # let TTL expire (stale remains valid)
        api_request(api_base, "POST", "/admin/answer",
                    {"qname": n4, "ip": "203.0.113.30", "ttl": 30})
        api_request(api_base, "POST", "/admin/delay", {"delay_ms": 250})
        api_request(api_base, "DELETE", "/admin/requests")

        t0 = time.monotonic()
        with ThreadPoolExecutor(max_workers=8) as pool:
            rs = list(pool.map(lambda _: query_once(n4, port=fwd_port, timeout=2), range(8)))
        wall_ms = (time.monotonic() - t0) * 1000
        check("8 个查询全部成功", all(x["ok"] and x["rcode"] == 0 for x in rs))
        check("全部拿到旧址 TTL=0",
              all(x["answers"] and x["answers"][0]["ip"] == "203.0.113.30"
                  and x["answers"][0]["ttl"] == 0 for x in rs))
        check("约在 100ms 阈值返回（70-400ms）",
              all(70 <= x["elapsed_ms"] <= 400 for x in rs),
              "ms=" + ",".join(str(int(x["elapsed_ms"])) for x in rs))
        check("整批未被 250ms 延迟拖死（wall<450ms）", wall_ms < 450, f"wall={wall_ms:.0f}ms")
        cnt = wait_packet_count(api_base, n4, 1, timeout=2.0)
        check("上游只收到 1 个同域名包（singleflight）", cnt == 1, f"count={cnt}")

        api_request(api_base, "POST", "/admin/delay", {"delay_ms": 0})
        time.sleep(0.1)
        r4b = query_once(n4, port=fwd_port)
        check("后台回源成功后缓存已续新（TTL>0）",
              r4b["ok"] and r4b["answers"] and r4b["answers"][0]["ttl"] > 10,
              str(r4b["answers"]))

        # ---- T5 stale first, then new address after recovery ---------------
        print("T5 停答 -> 先收 TTL0 旧址；恢复改答后 -> 查到新址")
        n5 = f"switch-{run_id}.local"
        api_request(api_base, "POST", "/admin/answer",
                    {"qname": n5, "ip": "203.0.113.10", "ttl": 1})
        api_request(api_base, "POST", "/admin/resume")
        r5a = query_once(n5, port=fwd_port)
        check("初次查到旧址", r5a["answers"][0]["ip"] == "203.0.113.10", str(r5a))
        time.sleep(1.15)

        api_request(api_base, "POST", "/admin/silence", {"silence": True})
        t0 = time.monotonic()
        r5stale = query_once(n5, port=fwd_port, timeout=2)
        stale_ms = (time.monotonic() - t0) * 1000
        check("停答后收到旧址", r5stale["rcode"] == 0
              and r5stale["answers"][0]["ip"] == "203.0.113.10", str(r5stale))
        check("旧址 TTL=0", r5stale["answers"][0]["ttl"] == 0)
        check("约 100ms 后返回旧址（70-400ms）", 70 <= stale_ms <= 400, f"{stale_ms:.0f}ms")

        time.sleep(1.05)  # let the shared fetch hit the 1s total cap
        api_request(api_base, "POST", "/admin/answer",
                    {"qname": n5, "ip": "198.51.100.77", "ttl": 30})
        api_request(api_base, "POST", "/admin/resume")
        r5new = query_once(n5, port=fwd_port)
        check("恢复改答后查到新址",
              r5new["ok"] and r5new["answers"][0]["ip"] == "198.51.100.77", str(r5new))
        check("新址快速返回（<300ms，未再等 100ms）", r5new["elapsed_ms"] < 300,
              f"{r5new['elapsed_ms']:.0f}ms")

        # ---- T5b delay 中途改答：后台延迟应答带回新址并刷新缓存 ------------
        print("T5b 延迟回源期间改答，后台回包后再查得到新址")
        n5b = f"delay-switch-{run_id}.local"
        api_request(api_base, "POST", "/admin/answer",
                    {"qname": n5b, "ip": "203.0.113.60", "ttl": 1})
        api_request(api_base, "POST", "/admin/delay", {"delay_ms": 0})
        api_request(api_base, "POST", "/admin/resume")
        query_once(n5b, port=fwd_port)
        time.sleep(1.15)
        api_request(api_base, "POST", "/admin/delay", {"delay_ms": 300})
        t0 = time.monotonic()
        r5b_stale = query_once(n5b, port=fwd_port, timeout=2)
        check("延迟场景先回 TTL0 旧址",
              r5b_stale["rcode"] == 0 and r5b_stale["answers"][0]["ttl"] == 0
              and r5b_stale["answers"][0]["ip"] == "203.0.113.60", str(r5b_stale))
        # 在 300ms 延迟应答真正发出前改答；样例上游在延迟结束后读取最新配置。
        api_request(api_base, "POST", "/admin/answer",
                    {"qname": n5b, "ip": "198.51.100.88", "ttl": 30})
        elapsed = time.monotonic() - t0
        time.sleep(max(0.0, 0.45 - elapsed))  # 等后台延迟回包完成并写缓存
        api_request(api_base, "POST", "/admin/delay", {"delay_ms": 0})
        r5b_new = query_once(n5b, port=fwd_port)
        check("后台回源后再查得到中途配置的新址",
              r5b_new["ok"] and r5b_new["answers"]
              and r5b_new["answers"][0]["ip"] == "198.51.100.88"
              and r5b_new["answers"][0]["ttl"] > 10, str(r5b_new))

        # ---- T6 no stale -> wait 1s -> SERVFAIL ----------------------------
        print("T6 从无旧址的域名在停答时等满 1s 收 SERVFAIL")
        n6 = f"nostale-{run_id}.local"
        api_request(api_base, "POST", "/admin/silence", {"silence": True})
        r6 = query_once(n6, port=fwd_port, timeout=2.5)
        api_request(api_base, "POST", "/admin/resume")
        check("返回 SERVFAIL(2)", r6["ok"] and r6["rcode"] == 2, str(r6))
        check("耗时接近 1000ms（850-1500ms）",
              850 <= r6["elapsed_ms"] <= 1500, f"{r6['elapsed_ms']:.0f}ms")

        # ---- T7 NXDOMAIN clears stale (fast forwarder: 400ms timeout) -----
        print("T7 NXDOMAIN 清掉旧址；随后停答不再给旧址 -> SERVFAIL")
        n7 = f"nx-{run_id}.local"
        api_request(api_base, "POST", "/admin/answer",
                    {"qname": n7, "ip": "203.0.113.40", "ttl": 1})
        r7a = query_once(n7, port=fwd_fast_port)
        check("前置查询得到旧址IP", r7a["answers"][0]["ip"] == "203.0.113.40", str(r7a))
        time.sleep(1.15)
        api_request(api_base, "POST", "/admin/nxdomain", {"qname": n7})
        r7nx = query_once(n7, port=fwd_fast_port)
        check("收到 NXDOMAIN(3)", r7nx["ok"] and r7nx["rcode"] == 3, str(r7nx))
        api_request(api_base, "POST", "/admin/silence", {"silence": True})
        r7sf = query_once(n7, port=fwd_fast_port, timeout=2)
        api_request(api_base, "POST", "/admin/resume")
        check("NXDOMAIN 后无旧址可用 -> SERVFAIL", r7sf["ok"] and r7sf["rcode"] == 2, str(r7sf))
        check("SERVFAIL 等待约 400ms 总限时（320-900ms）",
              320 <= r7sf["elapsed_ms"] <= 900, f"{r7sf['elapsed_ms']:.0f}ms")

        # ---- T8 stale beyond grace window ---------------------------------
        print("T8 过期超过宽限（测试实例 grace=1s）后不再使用旧址")
        n8 = f"old-{run_id}.local"
        api_request(api_base, "POST", "/admin/answer",
                    {"qname": n8, "ip": "203.0.113.50", "ttl": 1})
        query_once(n8, port=fwd_fast_port)
        time.sleep(2.2)  # age 2.2s > ttl 1 + grace 1
        api_request(api_base, "POST", "/admin/silence", {"silence": True})
        r8 = query_once(n8, port=fwd_fast_port, timeout=2)
        api_request(api_base, "POST", "/admin/resume")
        check("超宽限旧址不回送 -> SERVFAIL", r8["ok"] and r8["rcode"] == 2, str(r8))
        check("等待回源总限时后返回（>=320ms）", r8["elapsed_ms"] >= 320,
              f"{r8['elapsed_ms']:.0f}ms")

        # ---- T9 API validation --------------------------------------------
        print("T9 管理 API 校验非法 IP 返回 422")
        status, _ = api_request(api_base, "POST", "/admin/answer",
                                {"qname": n1, "ip": "not-an-ip"})
        check("非法 IP -> 422", status == 422, f"status={status}")

        # ---- T10 real resolver library / EDNS compatibility --------------
        print("T10 dnspython（含 EDNS OPT）线路兼容性")
        try:
            import dns.message
            import dns.query
            import dns.rcode
            import dns.rdatatype

            q = dns.message.make_query(n1, dns.rdatatype.A, use_edns=True)
            ans = dns.query.udp(q, "127.0.0.1", port=fwd_port, timeout=2)
            ips = [r.address for rrset in ans.answer for r in rrset]
            check("EDNS A 查询 NOERROR 且含 IP",
                  ans.rcode() == dns.rcode.NOERROR and "203.0.113.10" in ips,
                  f"rcode={dns.rcode.to_text(ans.rcode())} ips={ips}")

            q6 = dns.message.make_query(n1, dns.rdatatype.AAAA, use_edns=True)
            ans6 = dns.query.udp(q6, "127.0.0.1", port=fwd_port, timeout=2)
            check("EDNS AAAA 查询 REFUSED", ans6.rcode() == dns.rcode.REFUSED,
                  dns.rcode.to_text(ans6.rcode()))
        except ImportError:
            print("  [SKIP] dnspython 未安装，跳过真实库兼容性校验")

    except Exception:
        failed_start = True
        import traceback
        traceback.print_exc()
    finally:
        stop_process(upstream_proc, up_log_h)
        stop_process(fwd_proc, fwd_log_h)
        stop_process(fwd_fast_proc, fwd_fast_log_h)

    passed = sum(1 for _, ok, _ in CHECKS if ok)
    total = len(CHECKS)
    print("\n" + "=" * 70)
    print(f"E2E SELF-TEST: {passed}/{total} checks passed")
    if passed != total or failed_start:
        print("-" * 70)
        print("upstream log tail:\n" + tail(up_log))
        print("forwarder log tail:\n" + tail(fwd_log))
        print("fast forwarder log tail:\n" + tail(fwd_fast_log))
        for name, ok, detail in CHECKS:
            if not ok:
                print(f"FAILED: {name} -- {detail}")
        return 1
    print("all e2e scenarios passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
