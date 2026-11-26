"""Minimal DNS wire codec used by the forwarder and the sample upstream.

Only the pieces needed for A/IN forwarding are implemented, but the parser
understands compressed names and arbitrary answer RDATA well enough to skip
unknown records safely.
"""

from __future__ import annotations

import ipaddress
import struct
from dataclasses import dataclass, field

# Resource record types
TYPE_A = 1
TYPE_NS = 2
TYPE_CNAME = 5
TYPE_SOA = 6
TYPE_PTR = 12
TYPE_AAAA = 28

# Classes
CLASS_IN = 1

# Response codes
RCODE_NOERROR = 0
RCODE_FORMERR = 1
RCODE_SERVFAIL = 2
RCODE_NXDOMAIN = 3
RCODE_NOTIMP = 4
RCODE_REFUSED = 5

_HEADER = struct.Struct("!HHHHHH")


class DnsDecodeError(ValueError):
    """Raised when a DNS message cannot be decoded."""


@dataclass(frozen=True)
class Question:
    name: str
    qtype: int
    qclass: int


@dataclass(frozen=True)
class ResourceRecord:
    name: str
    rtype: int
    rclass: int
    ttl: int
    rdata: bytes
    ip: str | None = None


@dataclass
class DnsMessage:
    msg_id: int
    flags: int
    qdcount: int
    ancount: int
    nscount: int
    arcount: int
    rcode: int
    questions: list[Question] = field(default_factory=list)
    answers: list[ResourceRecord] = field(default_factory=list)

    @property
    def first_question(self) -> Question | None:
        return self.questions[0] if self.questions else None


def canonical_name(name: str) -> str:
    """Normalize a domain name for cache/inflight keys."""
    if name is None:
        return ""
    return name.strip().rstrip(".").lower()


def fqdn(name: str) -> str:
    """Return the lower-cased fully qualified form used in logs/API keys."""
    key = canonical_name(name)
    return f"{key}." if key else "."


def encode_name(name: str) -> bytes:
    """Encode a (possibly unicode) name as uncompressed DNS labels."""
    if name is None:
        raise DnsDecodeError("empty name")
    if name == "" or name == ".":
        return b"\x00"
    out = bytearray()
    for raw_label in name.rstrip(".").split("."):
        if not raw_label:
            raise DnsDecodeError(f"empty label in name: {name!r}")
        try:
            label = raw_label.encode("idna")
        except UnicodeError:
            label = raw_label.encode("utf-8")
        if len(label) > 63:
            raise DnsDecodeError("label longer than 63 octets")
        out.append(len(label))
        out.extend(label)
    out.append(0)
    if len(out) > 255:
        raise DnsDecodeError("name longer than 255 octets")
    return bytes(out)


def read_name(buf: bytes, pos: int) -> tuple[str, int]:
    """Read a possibly compressed name.

    Returns ``(dotted_name, first_position_after_the_name)``.  When a
    compression pointer is followed, the returned end position is the position
    immediately after the pointer in the *current* stream.
    """
    labels: list[str] = []
    resolved_end: int | None = None
    p = pos
    steps = 0

    while True:
        steps += 1
        if steps > 128:
            raise DnsDecodeError("name too long or compression loop")
        if p >= len(buf):
            raise DnsDecodeError("truncated name")

        length = buf[p]
        if length == 0:
            if resolved_end is None:
                resolved_end = p + 1
            break

        kind = length & 0xC0
        if kind == 0xC0:
            if p + 1 >= len(buf):
                raise DnsDecodeError("truncated compression pointer")
            pointer = ((length & 0x3F) << 8) | buf[p + 1]
            if resolved_end is None:
                resolved_end = p + 2
            if pointer >= p:
                # A compression pointer must reference earlier octets.
                raise DnsDecodeError("invalid forward compression pointer")
            p = pointer
            continue

        if kind != 0:
            raise DnsDecodeError("unsupported label type")

        p += 1
        if p + length > len(buf):
            raise DnsDecodeError("truncated label")
        raw = buf[p : p + length]
        try:
            labels.append(raw.decode("ascii"))
        except UnicodeDecodeError:
            labels.append(raw.decode("utf-8", errors="replace"))
        p += length

    assert resolved_end is not None
    return ".".join(labels), resolved_end


def parse_message(data: bytes) -> DnsMessage:
    """Parse a DNS header, all questions and answer-section records."""
    if not isinstance(data, (bytes, bytearray)):
        raise DnsDecodeError("message must be bytes")
    if len(data) < _HEADER.size:
        raise DnsDecodeError("message shorter than 12-byte header")

    msg_id, flags, qd, an, ns, ar = _HEADER.unpack_from(data, 0)
    msg = DnsMessage(
        msg_id=msg_id,
        flags=flags,
        qdcount=qd,
        ancount=an,
        nscount=ns,
        arcount=ar,
        rcode=flags & 0x000F,
    )

    pos = _HEADER.size
    for _ in range(qd):
        name, pos = read_name(data, pos)
        if pos + 4 > len(data):
            raise DnsDecodeError("truncated question")
        qtype, qclass = struct.unpack_from("!HH", data, pos)
        pos += 4
        msg.questions.append(Question(name=name, qtype=qtype, qclass=qclass))

    for _ in range(an):
        name, pos = read_name(data, pos)
        if pos + 10 > len(data):
            raise DnsDecodeError("truncated resource record header")
        rtype, rclass, ttl, rdlen = struct.unpack_from("!HHIH", data, pos)
        pos += 10
        if pos + rdlen > len(data):
            raise DnsDecodeError("truncated rdata")
        rdata = bytes(data[pos : pos + rdlen])
        ip: str | None = None
        if rtype == TYPE_A and rclass == CLASS_IN and rdlen == 4:
            ip = str(ipaddress.IPv4Address(rdata))
        msg.answers.append(
            ResourceRecord(
                name=name,
                rtype=rtype,
                rclass=rclass,
                ttl=ttl,
                rdata=rdata,
                ip=ip,
            )
        )
        pos += rdlen

    return msg


def build_query(
    name: str,
    qtype: int = TYPE_A,
    qclass: int = CLASS_IN,
    msg_id: int = 0x1234,
    *,
    rd: bool = True,
) -> bytes:
    """Build a minimal standard recursive query."""
    flags = 0x0100 if rd else 0x0000
    header = _HEADER.pack(msg_id & 0xFFFF, flags, 1, 0, 0, 0)
    question = encode_name(name) + struct.pack("!HH", qtype, qclass)
    return header + question


def _flags_for_response(query_flags: int, rcode: int, *, aa: bool, ra: bool) -> int:
    flags = 0x8000  # QR = response
    flags |= (query_flags >> 11 & 0x0F) << 11  # copy OPCODE
    flags |= query_flags & 0x0100  # copy RD
    if aa:
        flags |= 0x0400
    if ra:
        flags |= 0x0080
    flags |= rcode & 0x000F
    return flags


def build_response(
    query: bytes,
    rcode: int = RCODE_NOERROR,
    answers: list[tuple[str, int]] | tuple[tuple[str, int], ...] = (),
    *,
    aa: bool = False,
    ra: bool = True,
) -> bytes:
    """Build a response echoing the query question and appending A records.

    ``answers`` is a sequence of ``(ipv4_address, ttl)`` pairs.  Answer names
    use a compression pointer to offset 12 (the question name).
    """
    if len(query) < _HEADER.size:
        raise DnsDecodeError("query shorter than header")

    msg_id, query_flags, qd, _an, _ns, _ar = _HEADER.unpack_from(query, 0)
    flags = _flags_for_response(query_flags, rcode, aa=aa, ra=ra)

    if qd < 1:
        header = _HEADER.pack(msg_id, flags, 0, len(answers), 0, 0)
        return header

    _, question_end = read_name(query, _HEADER.size)
    if question_end + 4 > len(query):
        raise DnsDecodeError("truncated question in query")
    question_raw = bytes(query[_HEADER.size : question_end + 4])

    body = bytearray()
    for ip, ttl in answers:
        packed_ip = ipaddress.IPv4Address(ip).packed
        ttl_i = max(0, min(int(ttl), 0xFFFFFFFF))
        # name pointer 0xC00C, type A, class IN, TTL, rdlength 4, address
        body += struct.pack(
            "!HHHIH4s",
            0xC00C,
            TYPE_A,
            CLASS_IN,
            ttl_i,
            4,
            packed_ip,
        )

    header = _HEADER.pack(msg_id, flags, 1, len(answers), 0, 0)
    return header + question_raw + bytes(body)


def build_header_error(query: bytes, rcode: int) -> bytes:
    """Build an error response when the question cannot be echoed."""
    if len(query) >= _HEADER.size:
        msg_id, query_flags, _qd, _an, _ns, _ar = _HEADER.unpack_from(query, 0)
    else:
        msg_id, query_flags = 0, 0
    flags = _flags_for_response(query_flags, rcode, aa=False, ra=True)
    return _HEADER.pack(msg_id, flags, 0, 0, 0, 0)


def rcode_name(rcode: int) -> str:
    return {
        0: "NOERROR",
        1: "FORMERR",
        2: "SERVFAIL",
        3: "NXDOMAIN",
        4: "NOTIMP",
        5: "REFUSED",
    }.get(rcode, f"RCODE{rcode}")


def qtype_name(qtype: int) -> str:
    return {
        TYPE_A: "A",
        TYPE_AAAA: "AAAA",
        TYPE_CNAME: "CNAME",
        TYPE_SOA: "SOA",
        TYPE_NS: "NS",
        TYPE_PTR: "PTR",
    }.get(qtype, f"TYPE{qtype}")
