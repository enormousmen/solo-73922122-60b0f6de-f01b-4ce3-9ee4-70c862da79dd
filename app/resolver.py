"""Resolution logic: TTL cache, single-flight upstream fetches and stale fallback.

Timing model (all deadlines derive from one shared flight start timestamp):

* ``stale_after`` (100ms): once the shared fetch has been outstanding this
  long without a positive/definitive answer, waiters holding a still-allowed
  stale entry receive the old addresses with TTL 0.
* ``total_timeout`` (1s): hard cap of one upstream round, measured from the
  moment the flight is started.  The time needed to create the upstream UDP
  endpoint counts against this budget.  When the deadline is reached the
  round is over for every waiter; a same-name query arriving afterwards
  starts a brand-new flight immediately, even if the old fetch task has not
  finished its own cleanup yet.
* ``stale_grace`` (30s): maximum post-expiry age of an old address set.
  Servability is re-checked at every decision point, so an entry whose grace
  window runs out while a fetch is still outstanding is no longer served:
  the waiter keeps waiting on the same fetch and gets SERVFAIL at the hard
  deadline.

Cache integrity: a response that arrives after the flight deadline (a late
answer of a timed-out round) is never written to the cache and never clears
one.  Expired-flight cleanup only removes the inflight registration when it
still owns the slot, so it cannot disturb the next round.
"""

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
    """One-shot UDP DNS exchange against the configured upstream.

    The ``timeout`` budget starts *before* the local UDP endpoint is created,
    so endpoint setup time is included: the receive wait only gets the budget
    remaining after the endpoint exists and the query has been sent.
    """

    def __init__(self, host: str, port: int) -> None:
        self.host = host
        self.port = port

    async def exchange(self, qname: str, timeout: float) -> DnsMessage:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + max(0.0, timeout)
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

        # Budget is already ticking: any time spent here is part of the round.
        transport, _proto = await loop.create_datagram_endpoint(
            _Client, remote_addr=(self.host, self.port)
        )
        try:
            remaining = deadline - loop.time()
            if remaining <= 0:
                raise asyncio.TimeoutError()
            transport.sendto(packet)
            data = await asyncio.wait_for(finished, timeout=remaining)
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
    hard_deadline: float
    task: asyncio.Task[Resolution] | None = None
    # Set once the shared hard deadline has passed.  While True the flight may
    # still be draining in the background, but its result is dead to callers
    # and its responses must not touch the cache.
    expired: bool = False


class Resolver:
    """Cache + single-flight fetch coordination.

    See the module docstring for the timing model.  In short: one flight per
    canonical name, deadlines anchored at flight start, stale servability
    revalidated continuously, and late responses of expired flights dropped.
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

        flight = self._get_or_start_flight(key)
        assert flight.task is not None
        task = flight.task
        stale_deadline = flight.started_at + self.stale_after
        hard_deadline = flight.hard_deadline

        while True:
            now = self._time()

            # Hard cap first: this round is over.  Expire the flight (its
            # background task may still be draining) so the next same-name
            # query starts a fresh round immediately instead of inheriting
            # this dead one.
            if now >= hard_deadline:
                flight.expired = True
                stale = self._stale_resolution(key)
                if stale is not None:
                    return stale
                return Resolution("servfail", reason="upstream total timeout")

            if task.done():
                resolution = self._safe_result(task)
                # A definitive upstream outcome is returned immediately.
                if resolution.kind in ("fresh", "nxdomain", "nodata"):
                    return resolution
                # Fetch failed/timed out.  Stale may only be served once the
                # 100ms threshold has been reached, even on an instant failure.
                if self._time() < stale_deadline:
                    await asyncio.sleep(stale_deadline - self._time())
                stale = self._stale_resolution(key)
                if stale is not None:
                    return stale
                return resolution

            # Stale threshold reached: re-read the cache (the 30s grace window
            # may have ended while we waited) and serve TTL 0 if still allowed.
            if now >= stale_deadline:
                stale = self._stale_resolution(key)
                if stale is not None:
                    return stale
                # Stale became unservable during the wait: keep waiting on the
                # same original fetch until its hard deadline.

            wait_until = hard_deadline
            if self._time() < stale_deadline:
                wait_until = min(wait_until, stale_deadline)
            timeout = wait_until - self._time()
            if timeout <= 0:
                continue
            try:
                await asyncio.wait_for(asyncio.shield(task), timeout=timeout)
            except asyncio.TimeoutError:
                continue

    def _stale_resolution(self, key: str) -> Resolution | None:
        """Return a TTL-0 stale resolution *right now* if still allowed.

        Unlike a one-shot snapshot taken at query entry, this re-reads the
        cache on every decision, so entries whose 30s grace window has ended
        (or that NXDOMAIN removed) are not served.
        """
        stale_ips = self.cache.stale_addresses(key, self.stale_grace)
        if stale_ips is None:
            return None
        return Resolution("stale", [(ip, 0) for ip in stale_ips])

    def _get_or_start_flight(self, key: str) -> _Flight:
        flight = self._inflight.get(key)
        if (
            flight is not None
            and flight.task is not None
            and not flight.task.done()
            and not flight.expired
        ):
            return flight

        started_at = self._time()
        flight = _Flight(
            started_at=started_at,
            hard_deadline=started_at + self.total_timeout,
        )
        task = asyncio.create_task(self._fetch(key, flight))
        flight.task = task
        self._inflight[key] = flight

        def _cleanup(_done: asyncio.Task) -> None:
            # Only remove our own registration; a newer round must not be
            # disturbed by this flight finishing its cleanup afterwards.
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

    def _response_is_on_time(self, flight: _Flight) -> bool:
        """Whether the flight still owns its round when the response arrives."""
        return not flight.expired and self._time() < flight.hard_deadline

    async def _fetch(self, key: str, flight: _Flight) -> Resolution:
        started = self._time()
        # The remaining budget already excludes any scheduling/endpoint setup
        # time elapsed since the flight was started.
        timeout = flight.hard_deadline - started
        try:
            message = await self.upstream.exchange(key, timeout=max(0.0, timeout))
        except asyncio.TimeoutError:
            flight.expired = True
            log.info(
                "fetch %s: timeout after %.0fms budget",
                key,
                self.total_timeout * 1000,
            )
            return Resolution("servfail", reason="upstream timeout")
        except Exception as exc:  # network errors / decode errors must not crash loop
            log.info("fetch %s: error %r", key, exc)
            return Resolution("servfail", reason=f"upstream error: {exc}")

        elapsed_ms = (self._time() - started) * 1000

        # Late answer of an expired round: never write to, nor clear, the
        # cache.  Just log and drop it; the next round owns the name now.
        if not self._response_is_on_time(flight):
            flight.expired = True
            log.info(
                "fetch %s: late %s response after %.1fms discarded (round deadline passed)",
                key,
                message.rcode,
                elapsed_ms,
            )
            return Resolution("servfail", reason="late response after round timeout")

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
