#!/usr/bin/env python3
"""Run the complete stale-address demo locally without Docker.

Boots ``app.upstream`` (FastAPI + UDP upstream) and ``app.forwarder`` on
high local ports, then executes ``scripts/demo.py`` against them.  Child
processes are stopped on exit (SIGTERM where permitted, otherwise the
stdin-EOF watchdog).

Usage:
    python scripts/run_demo_local.py
"""

from __future__ import annotations

import os
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent

UP_DNS_PORT = int(os.environ.get("LOCAL_UP_DNS_PORT", "15353"))
FWD_DNS_PORT = int(os.environ.get("LOCAL_FWD_DNS_PORT", "15354"))
API_PORT = int(os.environ.get("LOCAL_API_PORT", "18080"))


def wait_http(url: str, timeout: float = 20.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(url + "/healthz", timeout=2):
                return
        except Exception:
            time.sleep(0.2)
    raise SystemExit(f"service not ready: {url}")


def main() -> int:
    env = os.environ.copy()
    env["EXIT_ON_STDIN_EOF"] = "1"
    env.update(
        {
            # Shared setting: upstream binds 127.0.0.1:15353 and forwarder dials it.
            "UPSTREAM_DNS_HOST": "127.0.0.1",
            "UPSTREAM_DNS_PORT": str(UP_DNS_PORT),
            "API_HOST": "127.0.0.1",
            "API_PORT": str(API_PORT),
            "DNS_LISTEN_HOST": "127.0.0.1",
            "DNS_LISTEN_PORT": str(FWD_DNS_PORT),
            "STALE_AFTER_MS": "100",
            "UPSTREAM_TIMEOUT_MS": "1000",
            "STALE_GRACE_S": "30",
            "PYTHONUNBUFFERED": "1",
        }
    )

    upstream = subprocess.Popen(
        [sys.executable, "-u", "-m", "app.upstream"],
        cwd=REPO_ROOT, env=env, stdin=subprocess.PIPE,
    )
    forwarder = subprocess.Popen(
        [sys.executable, "-u", "-m", "app.forwarder"],
        cwd=REPO_ROOT, env=env, stdin=subprocess.PIPE,
    )
    try:
        wait_http(f"http://127.0.0.1:{API_PORT}")
        # Give the UDP endpoints a moment; demo itself retries API only.
        time.sleep(0.5)

        demo_env = os.environ.copy()
        demo_env.update(
            {
                "DNS_HOST": "127.0.0.1",
                "DNS_PORT": str(FWD_DNS_PORT),
                "API_BASE": f"http://127.0.0.1:{API_PORT}",
            }
        )
        result = subprocess.run(
            [sys.executable, "-u", str(REPO_ROOT / "scripts" / "demo.py")],
            cwd=REPO_ROOT, env=demo_env,
        )
        return result.returncode
    finally:
        for proc in (forwarder, upstream):
            if proc.poll() is None:
                try:
                    proc.terminate()
                    proc.wait(timeout=5)
                except PermissionError:
                    if proc.stdin is not None:
                        proc.stdin.close()
                    try:
                        proc.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        pass
                except subprocess.TimeoutExpired:
                    try:
                        proc.kill()
                    except PermissionError:
                        if proc.stdin is not None:
                            proc.stdin.close()


if __name__ == "__main__":
    raise SystemExit(main())
