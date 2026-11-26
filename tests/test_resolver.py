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

    async def exchange(self, qname: str, timeout: float) -> DnsMessage:
        self.calls.append(qname)
        if self.delay:
            await asyncio.sleep(self.delay)
        if self.gate is not None:
            await self.gate.wait()
        if self.fail is not None:
            raise self.fail
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


if __name__ == "__main__":
    unittest.main()
