"""Configuration loaded from environment variables."""

from __future__ import annotations

import os
from dataclasses import dataclass


def _env_str(key: str, default: str) -> str:
    value = os.environ.get(key)
    return value if value not in (None, "") else default


def _env_int(key: str, default: int) -> int:
    raw = os.environ.get(key)
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _env_float(key: str, default: float) -> float:
    raw = os.environ.get(key)
    if raw is None or raw == "":
        return default
    try:
        return float(raw)
    except ValueError:
        return default


@dataclass(frozen=True)
class ForwarderConfig:
    listen_host: str
    listen_port: int
    upstream_host: str
    upstream_port: int
    stale_after_seconds: float  #回源多久无应答后发旧址
    total_timeout_seconds: float  # 回源总限时
    stale_grace_seconds: float  # 过期后允许使用旧址的最长时间

    @staticmethod
    def from_env() -> "ForwarderConfig":
        return ForwarderConfig(
            listen_host=_env_str("DNS_LISTEN_HOST", "0.0.0.0"),
            listen_port=_env_int("DNS_LISTEN_PORT", 53),
            upstream_host=_env_str("UPSTREAM_DNS_HOST", "127.0.0.1"),
            upstream_port=_env_int("UPSTREAM_DNS_PORT", 5353),
            stale_after_seconds=_env_int("STALE_AFTER_MS", 100) / 1000.0,
            total_timeout_seconds=_env_int("UPSTREAM_TIMEOUT_MS", 1000) / 1000.0,
            stale_grace_seconds=_env_float("STALE_GRACE_S", 30.0),
        )


@dataclass(frozen=True)
class UpstreamConfig:
    dns_host: str
    dns_port: int
    api_host: str
    api_port: int

    @staticmethod
    def from_env() -> "UpstreamConfig":
        return UpstreamConfig(
            dns_host=_env_str("UPSTREAM_DNS_HOST", "0.0.0.0"),
            dns_port=_env_int("UPSTREAM_DNS_PORT", 5353),
            api_host=_env_str("API_HOST", "0.0.0.0"),
            api_port=_env_int("API_PORT", 8000),
        )
