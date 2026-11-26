"""Local sample authoritative-ish DNS upstream with a FastAPI control API.

The DNS/UDP behaviour is fully remote-controllable:

* change A records / TTL (and turn a name into NXDOMAIN),
* add a response delay in milliseconds,
* silence the server (receive but never answer, i.e. emulate a stuck upstream).

Run with ``python -m app.upstream``; HTTP and UDP share one asyncio loop.
"""

from __future__ import annotations

import asyncio
import logging
from collections import deque
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any

import uvicorn
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field, field_validator

from .config import UpstreamConfig
from .runtime import install_stdin_eof_watchdog
from .dns_codec import (
    CLASS_IN,
    TYPE_A,
    DnsDecodeError,
    build_response,
    canonical_name,
    encode_name,
    fqdn,
    parse_message,
    qtype_name,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("sample-upstream")

DEFAULT_IP = "203.0.113.10"  # TEST-NET-3 documentation range
NEW_IP = "198.51.100.77"  # TEST-NET-2 documentation range


# --------------------------------------------------------------------------- #
# State
# --------------------------------------------------------------------------- #


class UpstreamState:
    def __init__(self) -> None:
        self.delay_ms: float = 0.0
        self.silence: bool = False
        # Catch-all answer used when no per-name override exists.
        self.default: dict[str, Any] = {
            "ips": [DEFAULT_IP],
            "ttl": 30,
            "rcode": 0,
        }
        # canonical name -> {"ips": [...], "ttl": int, "rcode": int}
        self.records: dict[str, dict[str, Any]] = {}
        self.requests: deque[dict[str, Any]] = deque(maxlen=500)
        self.transport: asyncio.DatagramTransport | None = None
        self.started_at = datetime.now(timezone.utc).isoformat()
        self._seq = 0

    def reset(self) -> None:
        self.delay_ms = 0.0
        self.silence = False
        self.default = {"ips": [DEFAULT_IP], "ttl": 30, "rcode": 0}
        self.records.clear()
        self.requests.clear()
        log.info("state reset to defaults")

    def spec_for(self, name: str | None) -> dict[str, Any]:
        key = canonical_name(name or "")
        if key in self.records:
            return dict(self.records[key])
        return dict(self.default)

    def add_request(self, entry: dict[str, Any]) -> dict[str, Any]:
        self._seq += 1
        entry["seq"] = self._seq
        entry["time"] = datetime.now(timezone.utc).isoformat()
        self.requests.append(entry)
        return entry


STATE = UpstreamState()


# --------------------------------------------------------------------------- #
# DNS UDP server
# --------------------------------------------------------------------------- #


class UpstreamDnsProtocol(asyncio.DatagramProtocol):
    def __init__(self, state: UpstreamState) -> None:
        self.state = state
        self.transport: asyncio.DatagramTransport | None = None

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        self.transport = transport  # type: ignore[assignment]
        self.state.transport = self.transport  # type: ignore[assignment]

    def datagram_received(self, data: bytes, addr) -> None:
        asyncio.create_task(self.handle(data, addr))

    def error_received(self, exc: Exception) -> None:
        log.warning("udp error: %r", exc)

    async def handle(self, data: bytes, addr) -> None:
        delay_ms = self.state.delay_ms
        if delay_ms > 0:
            await asyncio.sleep(delay_ms / 1000.0)

        try:
            message = parse_message(data)
        except DnsDecodeError as exc:
            self.state.add_request(
                {"client": f"{addr[0]}:{addr[1]}", "action": "bad_request", "error": str(exc)}
            )
            return

        question = message.first_question
        if question is None:
            self.state.add_request({"client": f"{addr[0]}:{addr[1]}", "action": "bad_request"})
            return

        qname = question.name
        spec = self.state.spec_for(qname)

        # "Stop answering": still record that the packet arrived, send nothing.
        if self.state.silence:
            self.state.add_request(
                {
                    "client": f"{addr[0]}:{addr[1]}",
                    "qname": fqdn(qname),
                    "qtype": qtype_name(question.qtype),
                    "qclass": question.qclass,
                    "delay_ms": delay_ms,
                    "action": "silenced",
                }
            )
            log.info("SILENCED %s from %s (no reply)", fqdn(qname), addr[0])
            return

        rcode = int(spec.get("rcode", 0))
        if rcode == 3:
            packet = build_response(data, 3)
            action = "nxdomain"
            ips: list[str] = []
        elif question.qtype == TYPE_A and question.qclass == CLASS_IN and rcode == 0:
            ttl = int(spec.get("ttl", 30))
            ips = list(spec.get("ips", []))
            packet = build_response(data, 0, [(ip, ttl) for ip in ips])
            action = "answered"
        else:
            # e.g. AAAA: NOERROR with empty answer (NODATA)
            packet = build_response(data, 0)
            action = "nodata"
            ips = []

        if self.transport is not None and not self.transport.is_closing():
            self.transport.sendto(packet, addr)
        self.state.add_request(
            {
                "client": f"{addr[0]}:{addr[1]}",
                "qname": fqdn(qname),
                "qtype": qtype_name(question.qtype),
                "qclass": question.qclass,
                "delay_ms": delay_ms,
                "action": action,
                "rcode": rcode if action == "nxdomain" else 0,
                "ips": ips,
                "ttl": spec.get("ttl"),
            }
        )
        log.info(
            "%s %s %s -> %s",
            action.upper(),
            fqdn(qname),
            qtype_name(question.qtype),
            ips or f"rcode={rcode}",
        )


# --------------------------------------------------------------------------- #
# HTTP API models
# --------------------------------------------------------------------------- #


def _validate_name(value: str | None) -> str | None:
    if value is None:
        return None
    try:
        encode_name(value)
    except DnsDecodeError as exc:
        raise ValueError(str(exc)) from exc
    return value


def _validate_ipv4_list(value: list[str] | None) -> list[str] | None:
    if value is None:
        return None
    import ipaddress

    out: list[str] = []
    for item in value:
        try:
            out.append(str(ipaddress.IPv4Address(item)))
        except (ipaddress.AddressValueError, ValueError) as exc:
            raise ValueError(f"invalid IPv4 address {item!r}") from exc
    if not out:
        raise ValueError("at least one address is required")
    return out


class AnswerIn(BaseModel):
    qname: str | None = Field(default=None, description="域名；省略则修改兜底应答")
    ip: str | None = Field(default=None, description="单个 IPv4 地址")
    ips: list[str] | None = Field(default=None, description="多个 IPv4 地址")
    ttl: int | None = Field(default=None, ge=0, le=604800)
    rcode: int | None = Field(default=None, ge=0, le=15)

    @field_validator("qname")
    @classmethod
    def _name_ok(cls, v: str | None) -> str | None:
        return _validate_name(v)

    @field_validator("ips")
    @classmethod
    def _ips_ok(cls, v: list[str] | None) -> list[str] | None:
        return _validate_ipv4_list(v)

    @field_validator("ip")
    @classmethod
    def _ip_ok(cls, v: str | None) -> str | None:
        if v is None:
            return None
        return (_validate_ipv4_list([v]) or [v])[0]


class DelayIn(BaseModel):
    delay_ms: float = Field(..., ge=0, le=60000, description="每次应答前延迟毫秒")


class SilenceIn(BaseModel):
    silence: bool = True


class NxdomainIn(BaseModel):
    qname: str

    @field_validator("qname")
    @classmethod
    def _name_ok(cls, v: str) -> str:
        validated = _validate_name(v)
        assert validated is not None
        return validated


class ConfigIn(BaseModel):
    delay_ms: float | None = Field(default=None, ge=0, le=60000)
    silence: bool | None = None
    default_ips: list[str] | None = None
    default_ttl: int | None = Field(default=None, ge=0, le=604800)

    @field_validator("default_ips")
    @classmethod
    def _ips_ok(cls, v: list[str] | None) -> list[str] | None:
        return _validate_ipv4_list(v)


# --------------------------------------------------------------------------- #
# FastAPI app
# --------------------------------------------------------------------------- #


@asynccontextmanager
async def lifespan(app: FastAPI):
    cfg = UpstreamConfig.from_env()
    loop = asyncio.get_running_loop()
    transport, _ = await loop.create_datagram_endpoint(
        lambda: UpstreamDnsProtocol(STATE),
        local_addr=(cfg.dns_host, cfg.dns_port),
    )
    log.info("sample upstream DNS listening on %s:%s/udp", cfg.dns_host, cfg.dns_port)
    try:
        yield
    finally:
        transport.close()


app = FastAPI(
    title="DNS fallback demo - sample upstream control API",
    version="1.0.0",
    description="通过 HTTP 改变样例上游的应答、延迟和停答状态。",
    lifespan=lifespan,
)


@app.get("/healthz")
async def healthz() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/")
async def root() -> dict[str, Any]:
    return {
        "service": "dns-fallback sample-upstream",
        "dns_udp_port": UpstreamConfig.from_env().dns_port,
        "endpoints": [
            "GET  /admin/state",
            "PUT  /admin/config",
            "POST /admin/answer",
            "POST /admin/delay",
            "POST /admin/silence",
            "POST /admin/resume",
            "POST /admin/nxdomain",
            "POST /admin/reset",
            "GET  /admin/requests",
            "DELETE /admin/requests",
        ],
    }


@app.get("/admin/state")
async def get_state() -> dict[str, Any]:
    return {
        "started_at": STATE.started_at,
        "delay_ms": STATE.delay_ms,
        "silence": STATE.silence,
        "default": STATE.default,
        "records": STATE.records,
        "buffered_requests": len(STATE.requests),
    }


@app.put("/admin/config")
async def put_config(body: ConfigIn) -> dict[str, Any]:
    if body.delay_ms is not None:
        STATE.delay_ms = body.delay_ms
    if body.silence is not None:
        STATE.silence = body.silence
    if body.default_ips is not None:
        STATE.default["ips"] = body.default_ips
    if body.default_ttl is not None:
        STATE.default["ttl"] = body.default_ttl
    return await get_state()


@app.post("/admin/answer")
async def set_answer(body: AnswerIn) -> dict[str, Any]:
    ips: list[str] | None = None
    if body.ips is not None:
        ips = body.ips
    elif body.ip is not None:
        ips = [body.ip]

    if body.qname is None:
        spec = STATE.default
        if ips is not None:
            spec["ips"] = ips
        if body.ttl is not None:
            spec["ttl"] = body.ttl
        if body.rcode is not None:
            spec["rcode"] = body.rcode
        key = "<default>"
    else:
        key = canonical_name(body.qname)
        spec = STATE.records.setdefault(key, {"ips": [DEFAULT_IP], "ttl": 30, "rcode": 0})
        if ips is not None:
            spec["ips"] = ips
        if body.ttl is not None:
            spec["ttl"] = body.ttl
        spec["rcode"] = body.rcode if body.rcode is not None else 0

    log.info("answer configured for %s: %s", key, spec)
    return {"ok": True, "key": key, "spec": spec}


@app.post("/admin/delay")
async def set_delay(body: DelayIn) -> dict[str, Any]:
    STATE.delay_ms = body.delay_ms
    return {"ok": True, "delay_ms": STATE.delay_ms}


@app.post("/admin/silence")
async def silence(body: SilenceIn | None = None) -> dict[str, Any]:
    STATE.silence = True if body is None else body.silence
    return {"ok": True, "silence": STATE.silence}


@app.post("/admin/resume")
async def resume() -> dict[str, Any]:
    STATE.silence = False
    return {"ok": True, "silence": STATE.silence}


@app.post("/admin/nxdomain")
async def set_nxdomain(body: NxdomainIn) -> dict[str, Any]:
    key = canonical_name(body.qname)
    STATE.records[key] = {"ips": [], "ttl": 30, "rcode": 3}
    log.info("%s configured as NXDOMAIN", key)
    return {"ok": True, "key": key, "spec": STATE.records[key]}


@app.delete("/admin/record/{qname}")
async def delete_record(qname: str) -> dict[str, Any]:
    key = canonical_name(qname)
    existed = STATE.records.pop(key, None) is not None
    if not existed:
        raise HTTPException(status_code=404, detail=f"no override for {key}")
    return {"ok": True, "key": key, "removed": True}


@app.post("/admin/reset")
async def reset() -> dict[str, Any]:
    STATE.reset()
    return {"ok": True}


@app.get("/admin/requests")
async def list_requests(qname: str | None = None, limit: int = 100) -> dict[str, Any]:
    items = list(STATE.requests)
    if qname is not None:
        wanted = fqdn(qname)
        items = [e for e in items if e.get("qname") == wanted]
    items = items[-max(1, min(limit, 500)) :]
    return {"count": len(items), "entries": items}


@app.delete("/admin/requests")
async def clear_requests() -> dict[str, Any]:
    STATE.requests.clear()
    return {"ok": True}


def main() -> None:
    install_stdin_eof_watchdog()
    cfg = UpstreamConfig.from_env()
    uvicorn.run(app, host=cfg.api_host, port=cfg.api_port, log_level="info")


if __name__ == "__main__":
    main()
