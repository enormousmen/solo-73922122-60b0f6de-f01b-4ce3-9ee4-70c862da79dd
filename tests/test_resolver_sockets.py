"""Real-socket integration tests for resolver timing, flights and cache safety.

These drive the real ``UdpUpstream`` against scripted raw UDP servers (no
fakes inside the data path), covering:

* the 1s hard deadline and 100ms stale threshold boundaries,
* the 30s stale-grace inclusive boundary via an injectable clock,
* late answers of expired rounds never writing to / clearing the cache,
* same-name retries starting a brand-new flight immediately,
* background flights draining after their cap without blocking new rounds.
"""

import asyncio
import time
import unittest

from app.cache import AddressCache
from app.dns_codec import build_response
from app.resolver import Resolver, UdpUpstream


class _SilentUpstream:
    """Raw UDP server that accepts every packet and never answers."""

    def __init__(self) -> None:
        self.transport: asyncio.DatagramTransport | None = None

    def protocol(self):
        outer = self

        class _Protocol(asyncio.DatagramProtocol):
            def connection_made(self, transport):
                outer.transport = transport

            def datagram_received(self, data, addr):
                pass

        return _Protocol()


class _ScriptedUpstream:
    """Raw UDP server that holds queries until the test releases answers."""

    def __init__(self) -> None:
        self.transport: asyncio.DatagramTransport | None = None
        self.pending: list[tuple[bytes, object]] = []
        self.received = 0

    def protocol(self):
        outer = self

        class _Protocol(asyncio.DatagramProtocol):
            def connection_made(self, transport):
                outer.transport = transport

            def datagram_received(self, data, addr):
                outer.received += 1
                outer.pending.append((data, addr))

        return _Protocol()

    def release_all(self, *, answer_ip=None, rcode=0, ttl=30) -> None:
        assert self.transport is not None
        while self.pending:
            data, addr = self.pending.pop(0)
            if answer_ip is None:
                packet = build_response(data, rcode)
            else:
                packet = build_response(data, 0, [(answer_ip, ttl)])
            self.transport.sendto(packet, addr)


class ResolverSocketTimingTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.loop = asyncio.get_running_loop()
        self.silent = _SilentUpstream()
        server_transport, _ = await self.loop.create_datagram_endpoint(
            self.silent.protocol, local_addr=("127.0.0.1", 0)
        )
        self.server_transport = server_transport
        self.port = server_transport.get_extra_info("socket").getsockname()[1]

    async def asyncTearDown(self) -> None:
        self.server_transport.close()

    def make_resolver(self, **kwargs) -> Resolver:
        defaults = dict(
            stale_after_seconds=0.1,
            total_timeout_seconds=1.0,
            stale_grace_seconds=30,
        )
        defaults.update(kwargs)
        return Resolver(AddressCache(), UdpUpstream("127.0.0.1", self.port), **defaults)

    async def test_hard_deadline_one_second_without_stale(self) -> None:
        resolver = self.make_resolver()
        started = time.monotonic()
        result = await resolver.resolve("deadline.nostale.local")
        elapsed_ms = (time.monotonic() - started) * 1000
        self.assertEqual(result.kind, "servfail")
        self.assertGreaterEqual(elapsed_ms, 950)
        self.assertLess(elapsed_ms, 1150)

    async def test_stale_served_at_100ms_with_ttl_zero(self) -> None:
        cache = AddressCache()
        cache.put("stale.local", [("203.0.113.1", 1)], now=time.monotonic() - 3)
        resolver = Resolver(
            cache, UdpUpstream("127.0.0.1", self.port),
            stale_after_seconds=0.1, total_timeout_seconds=1.0,
            stale_grace_seconds=30,
        )
        started = time.monotonic()
        result = await resolver.resolve("stale.local")
        elapsed_ms = (time.monotonic() - started) * 1000
        self.assertEqual(result.kind, "stale")
        self.assertEqual(result.records, [("203.0.113.1", 0)])
        self.assertGreaterEqual(elapsed_ms, 90)
        self.assertLess(elapsed_ms, 250)

    async def test_beyond_grace_window_waits_full_deadline(self) -> None:
        cache = AddressCache()
        # Expired by ~31s: beyond the 30s grace window, not servable.
        cache.put("tooold.local", [("203.0.113.2", 1)], now=time.monotonic() - 32)
        resolver = Resolver(
            cache, UdpUpstream("127.0.0.1", self.port),
            stale_after_seconds=0.1, total_timeout_seconds=1.0,
            stale_grace_seconds=30,
        )
        started = time.monotonic()
        result = await resolver.resolve("tooold.local")
        elapsed_ms = (time.monotonic() - started) * 1000
        self.assertEqual(result.kind, "servfail")
        self.assertEqual(result.records, [])
        self.assertGreaterEqual(elapsed_ms, 950)

    async def test_grace_boundary_inclusive_with_injected_clock(self) -> None:
        # Deterministic inclusive boundary: expired exactly 30s is still stale.
        ticks = [1000.0]
        cache = AddressCache(time_func=lambda: ticks[0])
        cache.put("edge.local", [("203.0.113.3", 10)], now=1000.0)
        ticks[0] = 1040.0  # age 40 = ttl 10 + exactly 30s grace -> still stale
        self.assertEqual(
            cache.stale_addresses("edge.local", 30), ["203.0.113.3"]
        )
        ticks[0] = 1040.001  # a hair beyond 30s grace -> rejected
        self.assertIsNone(cache.stale_addresses("edge.local", 30))


class LateAnswerAndFlightTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.loop = asyncio.get_running_loop()
        self.scripted = _ScriptedUpstream()
        server_transport, _ = await self.loop.create_datagram_endpoint(
            self.scripted.protocol, local_addr=("127.0.0.1", 0)
        )
        self.server_transport = server_transport
        self.port = server_transport.get_extra_info("socket").getsockname()[1]
        self.cache = AddressCache()
        self.resolver = Resolver(
            self.cache,
            UdpUpstream("127.0.0.1", self.port),
            stale_after_seconds=0.1,
            total_timeout_seconds=0.3,
            stale_grace_seconds=30,
        )

    async def asyncTearDown(self) -> None:
        self.server_transport.close()

    async def test_late_positive_answer_discarded_and_retry_refetches(self) -> None:
        result = await self.resolver.resolve("a.late.local")
        self.assertEqual(result.kind, "servfail")
        self.assertIsNone(self.cache.get_entry("a.late.local"))

        # Deliver the dead round's answer after the deadline.
        await asyncio.sleep(0.02)
        self.scripted.release_all(answer_ip="192.0.2.99")
        await asyncio.sleep(0.1)
        self.assertIsNone(
            self.cache.get_entry("a.late.local"),
            "late answer must not populate the cache",
        )

        # The retry opens a new flight and sends a new packet immediately.
        task = asyncio.create_task(self.resolver.resolve("a.late.local"))
        await asyncio.sleep(0.05)
        self.assertEqual(self.scripted.received, 2)
        self.scripted.release_all(answer_ip="198.51.100.5")
        retry = await task
        self.assertEqual(retry.kind, "fresh")
        self.assertEqual(retry.records[0][0], "198.51.100.5")
        self.assertIsNotNone(self.cache.get_entry("a.late.local"))

    async def test_late_nxdomain_does_not_clear_existing_entry(self) -> None:
        self.cache.put(
            "c.late.local", [("203.0.113.7", 1)], now=time.monotonic() - 2
        )
        result = await self.resolver.resolve("c.late.local")
        self.assertEqual(result.kind, "stale")

        # Past the 0.3s hard deadline, deliver a late NXDOMAIN.
        await asyncio.sleep(0.3)
        self.scripted.release_all(rcode=3)
        await asyncio.sleep(0.1)
        self.assertIsNotNone(
            self.cache.get_entry("c.late.local"),
            "late NXDOMAIN must not clear the cache",
        )

    async def test_background_flight_drains_then_retry_starts_new_one(self) -> None:
        self.cache.put(
            "bg.local", [("9.9.9.9", 1)], now=time.monotonic() - 2
        )
        result = await self.resolver.resolve("bg.local")
        self.assertEqual(result.kind, "stale")
        self.assertEqual(self.resolver.inflight_count(), 1)

        # The background flight reaches its cap and removes its registration.
        await asyncio.sleep(0.35)
        self.assertEqual(self.resolver.inflight_count(), 0)

        task = asyncio.create_task(self.resolver.resolve("bg.local"))
        await asyncio.sleep(0.05)
        self.assertEqual(self.scripted.received, 2)  # a fresh packet went out
        self.scripted.release_all(answer_ip="8.8.8.8")
        retry = await task
        self.assertEqual(retry.kind, "fresh")
        self.assertEqual(retry.records[0][0], "8.8.8.8")


if __name__ == "__main__":
    unittest.main()
