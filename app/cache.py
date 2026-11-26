"""TTL-bound address cache with a bounded stale (grace) window."""

from __future__ import annotations

import time
from dataclasses import dataclass

from .dns_codec import canonical_name


@dataclass(frozen=True)
class AddressRecord:
    ip: str
    ttl: int  # upstream TTL in seconds, as received


@dataclass
class CacheEntry:
    records: tuple[AddressRecord, ...]
    fetched_at: float  # time_func timestamp of the successful upstream fetch

    @property
    def ttl(self) -> int:
        """Entry freshness lifetime: the smallest upstream record TTL."""
        return min(r.ttl for r in self.records)

    @property
    def ips(self) -> list[str]:
        return [r.ip for r in self.records]


class AddressCache:
    """Dictionary cache keyed by canonical qname.

    Freshness follows the upstream TTL only.  After expiry the entry is kept
    as *stale* for at most ``grace_seconds`` more seconds, but neither serving
    stale data nor timing out an upstream fetch updates ``fetched_at``, so a
    timeout never renews the entry.
    """

    def __init__(self, time_func=time.monotonic) -> None:
        self._entries: dict[str, CacheEntry] = {}
        self._time = time_func

    def get_entry(self, name: str) -> CacheEntry | None:
        return self._entries.get(canonical_name(name))

    def put(self, name: str, pairs: list[tuple[str, int]], now: float | None = None) -> CacheEntry | None:
        """Store positive-TTL ``(ip, ttl)`` pairs.

        Records with TTL 0 are not cached (RFC behaviour).  Returns the stored
        entry, or ``None`` when nothing had a positive TTL.
        """
        key = canonical_name(name)
        records = tuple(
            AddressRecord(ip=ip, ttl=max(0, int(ttl)))
            for ip, ttl in pairs
            if int(ttl) > 0
        )
        if not records:
            return None
        entry = CacheEntry(records=records, fetched_at=self._time() if now is None else now)
        self._entries[key] = entry
        return entry

    def delete(self, name: str) -> None:
        """Remove the entry completely (used on NXDOMAIN)."""
        self._entries.pop(canonical_name(name), None)

    def clear(self) -> None:
        self._entries.clear()

    def fresh_records(
        self, name: str, now: float | None = None
    ) -> list[tuple[str, int]] | None:
        """Return ``(ip, remaining_ttl)`` pairs while the entry is fresh."""
        entry = self.get_entry(name)
        if entry is None:
            return None
        current = self._time() if now is None else now
        age = current - entry.fetched_at
        if age < entry.ttl:
            return [(r.ip, max(0, int(r.ttl - age))) for r in entry.records]
        return None

    def stale_addresses(
        self, name: str, grace_seconds: float, now: float | None = None
    ) -> list[str] | None:
        """Return IPs if the entry is expired but within the stale grace window.

        The grace window is ``0 <= age - ttl <= grace_seconds`` ("expired by no
        more than 30s").  Returns ``None`` for fresh, missing or too-old
        entries.
        """
        entry = self.get_entry(name)
        if entry is None:
            return None
        current = self._time() if now is None else now
        age = current - entry.fetched_at
        if age < entry.ttl:
            return None  # still fresh
        if age <= entry.ttl + grace_seconds:
            return entry.ips
        return None

    def size(self) -> int:
        return len(self._entries)
