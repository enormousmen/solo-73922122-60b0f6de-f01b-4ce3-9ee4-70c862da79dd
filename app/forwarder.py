"""UDP DNS forwarder entry point.

Accepts only A/IN queries (others receive REFUSED), serves the TTL cache with
stale fallback and coalesces concurrent same-name misses via ``Resolver``.
"""

from __future__ import annotations

import asyncio
import logging
import time

from .cache import AddressCache
from .config import ForwarderConfig
from .dns_codec import (
    CLASS_IN,
    TYPE_A,
    DnsDecodeError,
    build_header_error,
    build_response,
    parse_message,
    qtype_name,
    rcode_name,
    RCODE_FORMERR,
    RCODE_REFUSED,
    RCODE_SERVFAIL,
)
from .resolver import Resolver, UdpUpstream
from .runtime import install_stdin_eof_watchdog

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("forwarder")


class ForwarderProtocol(asyncio.DatagramProtocol):
    def __init__(self, resolver: Resolver) -> None:
        self.resolver = resolver
        self.transport: asyncio.DatagramTransport | None = None

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        self.transport = transport  # type: ignore[assignment]

    def datagram_received(self, data: bytes, addr) -> None:
        # Each query is handled independently; tasks are only kept by the loop.
        asyncio.create_task(self.handle(data, addr))

    def error_received(self, exc: Exception) -> None:
        log.warning("udp socket error: %r", exc)

    def _send(self, packet: bytes, addr) -> None:
        if self.transport is not None and not self.transport.is_closing():
            self.transport.sendto(packet, addr)

    async def handle(self, data: bytes, addr) -> None:
        started = time.monotonic()
        peer = f"{addr[0]}:{addr[1]}"

        try:
            message = parse_message(data)
        except DnsDecodeError as exc:
            log.warning("drop malformed dns from %s: %s", peer, exc)
            if len(data) >= 12:
                self._send(build_header_error(data, RCODE_FORMERR), addr)
            return

        question = message.first_question
        if question is None or message.qdcount != 1:
            log.info("FORMERR from %s: qdcount=%s", peer, message.qdcount)
            self._send(build_header_error(data, RCODE_FORMERR), addr)
            return

        qname = question.name
        # Policy: only A / IN is forwarded and answered from cache.
        if question.qtype != TYPE_A or question.qclass != CLASS_IN:
            kind = qtype_name(question.qtype)
            log.info(
                "REFUSED %s %s/class%s from %s (A/IN only)",
                qname,
                kind,
                question.qclass,
                peer,
            )
            self._send(build_response(data, RCODE_REFUSED), addr)
            return

        try:
            resolution = await self.resolver.resolve(qname)
        except Exception:
            log.exception("resolver failure for %s", qname)
            self._send(build_response(data, RCODE_SERVFAIL), addr)
            return

        elapsed_ms = (time.monotonic() - started) * 1000

        if resolution.kind == "fresh":
            packet = build_response(data, 0, resolution.records)
            log.info(
                "ANSWER  %s A from %s -> %s in %.1fms",
                qname,
                peer,
                [ip for ip, _ in resolution.records],
                elapsed_ms,
            )
        elif resolution.kind == "stale":
            packet = build_response(data, 0, resolution.records)
            log.info(
                "STALE0  %s A from %s -> %s ttl=0 in %.1fms (background fetch continues)",
                qname,
                peer,
                [ip for ip, _ in resolution.records],
                elapsed_ms,
            )
        elif resolution.kind == "nxdomain":
            packet = build_response(data, 3)
            log.info("NXDOMAIN %s A from %s in %.1fms", qname, peer, elapsed_ms)
        elif resolution.kind == "nodata":
            packet = build_response(data, 0)
            log.info("NODATA  %s A from %s in %.1fms", qname, peer, elapsed_ms)
        else:
            packet = build_response(data, RCODE_SERVFAIL)
            log.info(
                "SERVFAIL %s A from %s in %.1fms (%s)",
                qname,
                peer,
                elapsed_ms,
                resolution.reason or "upstream failure",
            )

        self._send(packet, addr)


async def run_forwarder(config: ForwarderConfig | None = None) -> None:
    cfg = config or ForwarderConfig.from_env()
    cache = AddressCache()
    upstream = UdpUpstream(cfg.upstream_host, cfg.upstream_port)
    resolver = Resolver(
        cache,
        upstream,
        stale_after_seconds=cfg.stale_after_seconds,
        total_timeout_seconds=cfg.total_timeout_seconds,
        stale_grace_seconds=cfg.stale_grace_seconds,
    )

    loop = asyncio.get_running_loop()
    transport, _protocol = await loop.create_datagram_endpoint(
        lambda: ForwarderProtocol(resolver),
        local_addr=(cfg.listen_host, cfg.listen_port),
    )
    log.info(
        "DNS forwarder listening on %s:%s/udp (upstream %s:%s, stale_after=%dms, "
        "total_timeout=%dms, stale_grace=%.0fs)",
        cfg.listen_host,
        cfg.listen_port,
        cfg.upstream_host,
        cfg.upstream_port,
        cfg.stale_after_seconds * 1000,
        cfg.total_timeout_seconds * 1000,
        cfg.stale_grace_seconds,
    )
    try:
        await asyncio.Event().wait()
    finally:
        transport.close()


def main() -> None:
    install_stdin_eof_watchdog()
    try:
        asyncio.run(run_forwarder())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
