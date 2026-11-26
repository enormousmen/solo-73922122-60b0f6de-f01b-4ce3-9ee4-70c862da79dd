"""Resolution logic: TTL cache, single-flight upstream fetches and stale fallback."""

from __future__ import annotations

import asyncio
import logging
import random
import time
from dataclasses import dataclass, field
from typing import Protocol

from .cache import AddressCache
from .dns_codec import (
    CLASS_IN,
    TYPE_A,
    DnsMessage,
    canonical_name,
    parse_message,
    build_query,
)

log = logging.getLogger("resolver")


@dataclass(frozen=True)
class Resolution:
    """Outcome of resolving one A/IN qname.

    kind:
      - ``fresh``: positive answer built from a successful upstream fetch/cache
      - ``stale``: expired old addresses, always with TTL 0
      - ``nxdomain``: upstream said NXDOMAIN (cache entry was cleared)
      - ``nodata``: upstream NOERROR with no A records
      - ``servfail``: no answer in time / upstream failure, no stale usable
    """

    kind: str
    records: list[tuple[str, int]] = field(default_factory=list)
    reason: str = ""


class Upstream(Protocol):
    async def exchange(self, qname: str, timeout: float) -> DnsMessage: ...


class UdpUpstream:
    """One-shot UDP DNS exchange against the configured upstream."""

    def __init__(self, host: str, port: int) -> None:
        self.host = host
        self.port = port

    async def exchange(self, qname: str, timeout: float) -> DnsMessage:
        loop = asyncio.get_running_loop()
        query_id = random.randint(0, 0xFFFF)
        packet = build_query(qname, TYPE_A, CLASS_IN, msg_id=query_id)
        finished: asyncio.Future[bytes] = loop.create_future()

        class _Client(asyncio.DatagramProtocol):
            def datagram_received(self, data: bytes, _addr) -> None:
                if not finished.done():
                    finished.set_result(data)

            def error_received(self, exc: Exception) -> None:
                if not finished.done():
                    finished.set_exception(exc)

        transport, _proto = await loop.create_datagram_endpoint(
            _Client, remote_addr=(self.host, self.port)
        )
        try:
            transport.sendto(packet)
            data = await asyncio.wait_for(finished, timeout=timeout)
            message = parse_message(data)
            if message.msg_id != query_id:
                # A well-behaved upstream echoes the id; the one-shot socket
                # only receives packets for this exchange, so tolerate it.
                log.debug("upstream msg id mismatch: got %s want %s", message.msg_id, query_id)
            return message
        finally:
            transport.close()


@dataclass
class _Flight:
    started_at: float
    task: asyncio.Task[Resolution] | None = None


class Resolver:
    """Cache + single-flight fetch coordination.

    Timing model (all deadlines derive from the shared fetch start):

    * ``stale_after`` (100ms): once the shared fetch has been outstanding this
      long without a positive/definitive answer, waiters holding a still-allowed
      stale entry receive the old addresses with TTL 0.
    * ``total_timeout`` (1s): hard cap of the upstream fetch.  After a stale
      reply the caller is done but the shared task keeps running until this cap
      so a late upstream answer can refresh the cache for the next query.
    * ``stale_grace`` (30s): maximum post-expiry age of an old address set.
    """

    def __init__(
        self,
        cache: AddressCache,
        upstream: Upstream,
        *,
        stale_after_seconds: float = 0.1,
        total_timeout_seconds: float = 1.0,
        stale_grace_seconds: float = 30.0,
        time_func=time.monotonic,
    ) -> None:
        self.cache = cache
        self.upstream = upstream
        self.stale_after = stale_after_seconds
        self.total_timeout = total_timeout_seconds
        self.stale_grace = stale_grace_seconds
        self._time = time_func
        self._inflight: dict[str, _Flight] = {}

    def inflight_count(self) -> int:
        return sum(1 for f in self._inflight.values() if f.task and not f.task.done())

    async def resolve(self, raw_name: str) -> Resolution:
        key = canonical_name(raw_name)

        fresh = self.cache.fresh_records(key)
        if fresh is not None:
            return Resolution("fresh", fresh)

        # Snapshot stale addresses for this query; failures/timeouts never
        # mutate fetched_at so this stays valid throughout the wait.
        stale_ips = self.cache.stale_addresses(key, self.stale_grace)

        flight = self._get_or_start_flight(key)
        assert flight.task is not None
        task = flight.task
        stale_deadline = (
            flight.started_at + self.stale_after if stale_ips is not None else None
        )
        hard_deadline = flight.started_at + self.total_timeout

        while True:
            if task.done():
                resolution = self._safe_result(task)
                # A definitive upstream outcome is returned immediately.
                if resolution.kind in ("fresh", "nxdomain", "nodata"):
                    return resolution
                # Fetch failed/timed out.  With stale data, honour the 100ms
                # threshold even if the failure arrived sooner.
                if stale_ips is not None:
                    now = self._time()
                    wait = (stale_deadline or now) - now
                    if wait > 0:
                        await asyncio.sleep(wait)
                    return Resolution("stale", [(ip, 0) for ip in stale_ips])
                return resolution

            now = self._time()
            if stale_deadline is not None and now >= stale_deadline:
                return Resolution("stale", [(ip, 0) for ip in stale_ips])

            wait_until = hard_deadline
            if stale_deadline is not None:
                wait_until = min(wait_until, stale_deadline)
            timeout = wait_until - now
            if timeout <= 0:
                # Total upstream budget exhausted.  The fetch task itself is
                # bounded by the same timeout and will finish/clean up.
                if stale_ips is not None:
                    return Resolution("stale", [(ip, 0) for ip in stale_ips])
                return Resolution("servfail", reason="upstream total timeout")

            try:
                await asyncio.wait_for(asyncio.shield(task), timeout=timeout)
            except asyncio.TimeoutError:
                continue

    def _get_or_start_flight(self, key: str) -> _Flight:
        flight = self._inflight.get(key)
        if flight is not None and flight.task is not None and not flight.task.done():
            return flight

        flight = _Flight(started_at=self._time())
        task = asyncio.create_task(self._fetch(key))
        flight.task = task
        self._inflight[key] = flight

        def _cleanup(_done: asyncio.Task) -> None:
            current = self._inflight.get(key)
            if current is flight:
                self._inflight.pop(key, None)

        task.add_done_callback(_cleanup)
        return flight

    @staticmethod
    def _safe_result(task: asyncio.Task[Resolution]) -> Resolution:
        if task.cancelled():
            return Resolution("servfail", reason="fetch cancelled")
        exc = task.exception()
        if exc is not None:
            return Resolution("servfail", reason=f"fetch error: {exc}")
        return task.result()

    async def _fetch(self, key: str) -> Resolution:
        started = self._time()
        try:
            message = await self.upstream.exchange(key, timeout=self.total_timeout)
        except asyncio.TimeoutError:
            log.info("fetch %s: timeout after %.0fms", key, self.total_timeout * 1000)
            return Resolution("servfail", reason="upstream timeout")
        except Exception as exc:  # network errors / decode errors must not crash loop
            log.info("fetch %s: error %r", key, exc)
            return Resolution("servfail", reason=f"upstream error: {exc}")

        elapsed_ms = (self._time() - started) * 1000

        if message.rcode == 3:  # NXDOMAIN clears any old addresses
            self.cache.delete(key)
            log.info("fetch %s: NXDOMAIN after %.1fms; stale cleared", key, elapsed_ms)
            return Resolution("nxdomain")

        if message.rcode != 0:
            log.info("fetch %s: upstream rcode=%s after %.1fms", key, message.rcode, elapsed_ms)
            return Resolution("servfail", reason=f"upstream rcode {message.rcode}")

        pairs: list[tuple[str, int]] = [
            (rr.ip, rr.ttl)
            for rr in message.answers
            if rr.rtype == TYPE_A and rr.rclass == 1 and rr.ip is not None
        ]
        if not pairs:
            log.info("fetch %s: NODATA after %.1fms", key, elapsed_ms)
            return Resolution("nodata")

        entry = self.cache.put(key, pairs)
        if entry is not None:
            records = [(r.ip, r.ttl) for r in entry.records]
        else:
            records = [(ip, max(0, ttl)) for ip, ttl in pairs]
        log.info(
            "fetch %s: ok after %.1fms %s (ttl=%s)",
            key,
            elapsed_ms,
            [ip for ip, _ in records],
            [ttl for _, ttl in records],
        )
        return Resolution("fresh", records)
