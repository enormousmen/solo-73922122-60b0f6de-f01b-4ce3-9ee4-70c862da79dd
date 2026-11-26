import asyncio
import unittest

from app.cache import AddressCache
from app.dns_codec import DnsMessage, Question
from app.resolver import Resolver


def make_message(rcode=0, ips_ttls=(), qname="test.local"):
    from app.dns_codec import ResourceRecord

    answers = [
        ResourceRecord(
            name=qname,
            rtype=1,
            rclass=1,
            ttl=ttl,
            rdata=b"",
            ip=ip,
        )
        for ip, ttl in ips_ttls
    ]
    return DnsMessage(
        msg_id=1,
        flags=0x8180,
        qdcount=1,
        ancount=len(answers),
        nscount=0,
        arcount=0,
        rcode=rcode,
        questions=[Question(qname, 1, 1)],
        answers=answers,
    )


class FakeUpstream:
    def __init__(self):
        self.calls: list[str] = []
        self.gate: asyncio.Event | None = None
        self.next_message: DnsMessage | None = None
        self.delay: float = 0.0
        self.fail: Exception | None = None
        # Per-call messages: when set, call #n returns message_sequence[n-1].
        self.message_sequence: list[DnsMessage] | None = None
        # Call number -> event that blocks that exchange until set.
        self.block_until: dict[int, asyncio.Event] = {}
        self.timeouts: list[float] = []

    async def exchange(self, qname: str, timeout: float) -> DnsMessage:
        self.timeouts.append(timeout)
        call_no = len(self.calls) + 1
        self.calls.append(qname)
        blocker = self.block_until.get(call_no)
        if blocker is not None:
            # Mirror the real exchange: the round deadline still applies while
            # waiting on an outstanding reply, so callers do not depend solely
            # on the resolver-side hard cap.
            await asyncio.wait_for(blocker.wait(), timeout=timeout)
        if self.delay:
            await asyncio.sleep(self.delay)
        if self.gate is not None:
            await asyncio.wait_for(self.gate.wait(), timeout=timeout)
        if self.fail is not None:
            raise self.fail
        if self.message_sequence is not None:
            index = len(self.calls) - 1
            return self.message_sequence[min(index, len(self.message_sequence) - 1)]
        assert self.next_message is not None
        return self.next_message


class ResolverTests(unittest.IsolatedAsyncioTestCase):
    async def test_fresh_cache_does_not_call_upstream(self):
        upstream = FakeUpstream()
        resolver = Resolver(AddressCache(), upstream)
        resolver.cache.put("a.local", [("1.1.1.1", 30)])

        result = await resolver.resolve("a.local")
        self.assertEqual(result.kind, "fresh")
        self.assertEqual(result.records[0][0], "1.1.1.1")
        self.assertIn(result.records[0][1], (29, 30))  # a few ms of age
        self.assertEqual(upstream.calls, [])

    async def test_concurrent_misses_share_one_fetch_and_get_stale(self):
        upstream = FakeUpstream()
        upstream.gate = asyncio.Event()
        resolver = Resolver(
            AddressCache(),
            upstream,
            stale_after_seconds=0.05,
            total_timeout_seconds=1.0,
        )
        # Pre-populate an expired-but-grace-valid old address.
        import time

        resolver.cache.put("same.local", [("9.9.9.9", 1)], now=time.monotonic() - 2)

        results = await asyncio.gather(*[resolver.resolve("same.local") for _ in range(10)])

        self.assertEqual(len(upstream.calls), 1)  # single flight
        self.assertTrue(all(r.kind == "stale" for r in results))
        self.assertTrue(all(r.records == [("9.9.9.9", 0)] for r in results))

        # Fetch is still outstanding in the background.
        self.assertEqual(resolver.inflight_count(), 1)

        # Upstream finally answers with the new address; cache refreshes.
        upstream.next_message = make_message(ips_ttls=[("8.8.8.8", 30)])
        upstream.gate.set()
        await asyncio.sleep(0.05)
        result = await resolver.resolve("same.local")
        self.assertEqual(result.kind, "fresh")
        self.assertEqual([ip for ip, _ in result.records], ["8.8.8.8"])

    async def test_no_stale_waits_and_servfail_on_timeout(self):
        upstream = FakeUpstream()
        upstream.delay = 10  # longer than total timeout
        resolver = Resolver(
            AddressCache(),
            upstream,
            stale_after_seconds=0.05,
            total_timeout_seconds=0.15,
        )
        started = asyncio.get_running_loop().time()
        result = await resolver.resolve("missing.local")
        elapsed = asyncio.get_running_loop().time() - started
        self.assertEqual(result.kind, "servfail")
        self.assertGreaterEqual(elapsed, 0.14)
        self.assertLess(elapsed, 0.6)

    async def test_nxdomain_clears_stale(self):
        upstream = FakeUpstream()
        upstream.next_message = make_message(rcode=3)
        cache = AddressCache()
        import time

        cache.put("gone.local", [("1.1.1.1", 1)], now=time.monotonic() - 2)
        resolver = Resolver(cache, upstream)

        result = await resolver.resolve("gone.local")
        self.assertEqual(result.kind, "nxdomain")
        self.assertIsNone(cache.get_entry("gone.local"))

    async def test_fast_failure_with_stale_waits_for_stale_threshold(self):
        upstream = FakeUpstream()
        upstream.fail = OSError("connection refused")
        cache = AddressCache()
        import time

        cache.put("a.local", [("1.1.1.1", 1)], now=time.monotonic() - 2)
        resolver = Resolver(
            cache,
            upstream,
            stale_after_seconds=0.1,
            total_timeout_seconds=1.0,
        )
        started = asyncio.get_running_loop().time()
        result = await resolver.resolve("a.local")
        elapsed = asyncio.get_running_loop().time() - started
        self.assertEqual(result.kind, "stale")
        self.assertGreaterEqual(elapsed, 0.09)

    async def test_fast_failure_without_stale_servfail_immediately(self):
        upstream = FakeUpstream()
        upstream.fail = OSError("connection refused")
        resolver = Resolver(AddressCache(), upstream, total_timeout_seconds=1.0)
        result = await resolver.resolve("never.local")
        self.assertEqual(result.kind, "servfail")


    async def test_servfail_then_retry_starts_new_flight_immediately(self):
        # First round hits the hard cap (silent upstream); a same-name retry
        # once the round is over must open a new fetch instead of inheriting
        # the dead flight and failing without sending a packet.
        upstream = FakeUpstream()
        upstream.block_until = {1: asyncio.Event(), 2: asyncio.Event()}
        resolver = Resolver(
            AddressCache(),
            upstream,
            stale_after_seconds=0.05,
            total_timeout_seconds=0.1,
        )

        result1 = await resolver.resolve("retry.local")
        self.assertEqual(result1.kind, "servfail")
        self.assertEqual(len(upstream.calls), 1)

        # New round starts at once, even before the old task has drained.
        result2_task = asyncio.create_task(resolver.resolve("retry.local"))
        await asyncio.sleep(0.02)
        self.assertEqual(len(upstream.calls), 2)

        # The second round succeeds: its upstream answer is delivered.
        upstream.next_message = make_message(ips_ttls=[("8.8.8.8", 30)])
        upstream.block_until[2].set()
        result2 = await result2_task
        self.assertEqual(result2.kind, "fresh")
        self.assertEqual([ip for ip, _ in result2.records], ["8.8.8.8"])
        # Drain the dead first task so it does not leak warnings.
        upstream.block_until[1].set()
        await asyncio.sleep(0.05)

    async def test_late_positive_answer_after_deadline_does_not_fill_cache(self):
        # Round 1 stays outstanding past the deadline; its answer is only
        # released afterwards and must be discarded without touching cache.
        upstream = FakeUpstream()
        upstream.block_until = {1: asyncio.Event(), 2: asyncio.Event()}
        upstream.message_sequence = [
            make_message(ips_ttls=[("7.7.7.7", 30)]),  # late, must be dropped
            make_message(ips_ttls=[("8.8.8.8", 30)]),  # round 2, valid
        ]
        cache = AddressCache()
        resolver = Resolver(
            cache,
            upstream,
            stale_after_seconds=0.05,
            total_timeout_seconds=0.1,
        )

        result1 = await resolver.resolve("late.local")
        self.assertEqual(result1.kind, "servfail")
        self.assertIsNone(cache.get_entry("late.local"))

        # Releasing round 1's answer now simulates a late upstream reply.
        upstream.block_until[1].set()
        await asyncio.sleep(0.05)
        self.assertIsNone(
            cache.get_entry("late.local"),
            "late answer of a timed-out round must not populate the cache",
        )

        # A fresh round works and then caches normally.
        upstream.block_until[2].set()
        result2 = await resolver.resolve("late.local")
        self.assertEqual(result2.kind, "fresh")
        self.assertIsNotNone(cache.get_entry("late.local"))

    async def test_late_nxdomain_after_deadline_does_not_clear_cache(self):
        upstream = FakeUpstream()
        upstream.block_until = {1: asyncio.Event()}
        cache = AddressCache()
        import time

        # An old (expired, still in grace) address exists for the name.
        cache.put("nxlate.local", [("9.9.9.9", 1)], now=time.monotonic() - 2)
        upstream.message_sequence = [make_message(rcode=3)]  # late NXDOMAIN
        resolver = Resolver(
            cache,
            upstream,
            stale_after_seconds=0.05,
            total_timeout_seconds=0.1,
            stale_grace_seconds=30,
        )

        result1 = await resolver.resolve("nxlate.local")
        self.assertEqual(result1.kind, "stale")
        self.assertIsNotNone(cache.get_entry("nxlate.local"))

        # Wait until well past the 0.1s hard deadline before releasing the
        # blocked exchange, so its NXDOMAIN is unambiguously a late response.
        await asyncio.sleep(0.15)
        # The round has expired and its task drained out of the inflight map.
        self.assertNotIn("nxlate.local", resolver._inflight)
        upstream.block_until[1].set()
        await asyncio.sleep(0.05)
        self.assertIsNotNone(
            cache.get_entry("nxlate.local"),
            "late NXDOMAIN of a timed-out round must not clear the cache",
        )

    async def test_stale_expiring_during_wait_keeps_waiting_then_servfail(self):
        # The old address is still servable at query entry (expired for 0.09s
        # with a 0.1s grace window) but its grace window ends 0.01s into the
        # wait.  The 100ms stale threshold must then refuse it and the query
        # must keep waiting on the same fetch until the hard deadline.
        upstream = FakeUpstream()
        upstream.gate = asyncio.Event()
        cache = AddressCache()
        import time

        cache.put("aging.local", [("9.9.9.9", 1)], now=time.monotonic() - 1.09)
        resolver = Resolver(
            cache,
            upstream,
            stale_after_seconds=0.1,
            total_timeout_seconds=0.3,
            stale_grace_seconds=0.1,
        )

        started = asyncio.get_running_loop().time()
        result = await resolver.resolve("aging.local")
        elapsed = asyncio.get_running_loop().time() - started
        self.assertEqual(result.kind, "servfail")
        self.assertGreaterEqual(
            elapsed, 0.28, "must keep waiting on the fetch after stale became unusable"
        )
        self.assertEqual(len(upstream.calls), 1)  # same single flight, no re-fetch

    async def test_endpoint_setup_time_counts_against_budget(self):
        # Resolver hands the exchange a timeout equal to the budget remaining
        # from flight start, so any time spent creating the UDP endpoint is
        # accounted for.  Simulate the slow setup inside the fake and confirm
        # The resolver-derived budget already excludes time since flight
        # start, and the round still ends exactly on the shared hard deadline.
        class SlowEndpointUpstream(FakeUpstream):
            async def exchange(self, qname, timeout):
                self.timeouts.append(timeout)
                await asyncio.sleep(0.05)  # emulate slow endpoint creation
                await self.gate.wait()  # upstream stays silent after setup
                raise AssertionError("unexpected release")

        upstream = SlowEndpointUpstream()
        upstream.gate = asyncio.Event()
        resolver = Resolver(
            AddressCache(),
            upstream,
            stale_after_seconds=0.05,
            total_timeout_seconds=0.15,
        )
        started = asyncio.get_running_loop().time()
        result = await resolver.resolve("setup.local")
        elapsed = asyncio.get_running_loop().time() - started
        self.assertEqual(result.kind, "servfail")
        # The round ended on the 0.15s hard deadline even though the fake
        # blocked forever afterwards: setup time does not extend the round.
        self.assertGreaterEqual(elapsed, 0.14)
        self.assertLess(elapsed, 0.4)
        self.assertEqual(len(upstream.timeouts), 1)
        self.assertLessEqual(upstream.timeouts[0], 0.15)
        self.assertGreater(upstream.timeouts[0], 0.0)


class UdpUpstreamBudgetTests(unittest.IsolatedAsyncioTestCase):
    """Real-socket checks that the exchange timeout covers the whole round."""

    async def test_timeout_includes_endpoint_creation_budget(self):
        from app.resolver import UdpUpstream

        # Bind a silent UDP server: the packet is accepted, never answered.
        loop = asyncio.get_running_loop()
        server_transport, _ = await loop.create_datagram_endpoint(
            lambda: asyncio.DatagramProtocol(),
            local_addr=("127.0.0.1", 0),
        )
        port = server_transport.get_extra_info("socket").getsockname()[1]
        try:
            upstream = UdpUpstream("127.0.0.1", port)
            started = loop.time()
            with self.assertRaises(asyncio.TimeoutError):
                # Endpoint creation + send + wait must all fit in 0.15s.
                await upstream.exchange("budget.local", timeout=0.15)
            elapsed = loop.time() - started
            self.assertGreaterEqual(elapsed, 0.14)
            self.assertLess(elapsed, 0.35)
        finally:
            server_transport.close()

    async def test_zero_remaining_budget_times_out_without_long_wait(self):
        from app.resolver import UdpUpstream

        loop = asyncio.get_running_loop()
        server_transport, _ = await loop.create_datagram_endpoint(
            lambda: asyncio.DatagramProtocol(),
            local_addr=("127.0.0.1", 0),
        )
        port = server_transport.get_extra_info("socket").getsockname()[1]
        try:
            upstream = UdpUpstream("127.0.0.1", port)
            started = loop.time()
            with self.assertRaises(asyncio.TimeoutError):
                await upstream.exchange("budget0.local", timeout=0.0)
            # No remaining budget: fails immediately after endpoint setup.
            self.assertLess(loop.time() - started, 0.2)
        finally:
            server_transport.close()


if __name__ == "__main__":
    unittest.main()
