import unittest

from app.cache import AddressCache


class CacheBoundaryTests(unittest.TestCase):
    def setUp(self):
        self.now = 1000.0
        self.cache = AddressCache(time_func=lambda: self.now)

    def test_fresh_returns_remaining_ttl(self):
        self.cache.put("a.local", [("1.1.1.1", 10)], now=self.now)
        self.now += 3
        fresh = self.cache.fresh_records("a.local", now=self.now)
        self.assertEqual(fresh, [("1.1.1.1", 7)])

    def test_expiry_boundary(self):
        self.cache.put("a.local", [("1.1.1.1", 10)], now=self.now)
        # strictly before ttl -> fresh
        self.assertIsNotNone(self.cache.fresh_records("a.local", now=self.now + 9.999))
        # exactly at ttl -> expired
        self.assertIsNone(self.cache.fresh_records("a.local", now=self.now + 10))

    def test_stale_grace_window(self):
        self.cache.put("a.local", [("1.1.1.1", 10)], now=self.now)
        # still fresh -> no stale view
        self.assertIsNone(self.cache.stale_addresses("a.local", 30, now=self.now + 9))
        # expired 1s ago -> stale allowed
        self.assertEqual(
            self.cache.stale_addresses("a.local", 30, now=self.now + 11),
            ["1.1.1.1"],
        )
        # expired exactly 30s ago -> "不超过 30 秒" inclusive
        self.assertEqual(
            self.cache.stale_addresses("a.local", 30, now=self.now + 40),
            ["1.1.1.1"],
        )
        # expired 30.001s ago -> rejected
        self.assertIsNone(self.cache.stale_addresses("a.local", 30, now=self.now + 40.001))

    def test_timeout_does_not_renew_fetched_at(self):
        self.cache.put("a.local", [("1.1.1.1", 10)], now=self.now)
        original = self.cache.get_entry("a.local").fetched_at
        # A timeout path simply never calls put(); advancing time and reading
        # the entry must show the original fetch timestamp.
        self.now += 50
        self.assertEqual(self.cache.get_entry("a.local").fetched_at, original)
        # and the address is now beyond the grace window
        self.assertIsNone(self.cache.stale_addresses("a.local", 30, now=self.now))

    def test_nxdomain_delete_clears_old_address(self):
        self.cache.put("gone.local", [("1.1.1.1", 10)], now=self.now)
        self.cache.delete("gone.local")
        self.assertIsNone(self.cache.get_entry("gone.local"))
        self.assertIsNone(self.cache.stale_addresses("gone.local", 30, now=self.now + 11))

    def test_ttl_zero_is_not_cached(self):
        entry = self.cache.put("zero.local", [("1.1.1.1", 0)], now=self.now)
        self.assertIsNone(entry)
        self.assertIsNone(self.cache.get_entry("zero.local"))

    def test_entry_ttl_is_min_record_ttl(self):
        self.cache.put(
            "multi.local",
            [("1.1.1.1", 30), ("2.2.2.2", 5)],
            now=self.now,
        )
        entry = self.cache.get_entry("multi.local")
        self.assertEqual(entry.ttl, 5)


if __name__ == "__main__":
    unittest.main()
