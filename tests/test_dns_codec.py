import struct
import unittest

from app.dns_codec import (
    CLASS_IN,
    TYPE_A,
    build_query,
    build_response,
    canonical_name,
    encode_name,
    parse_message,
    read_name,
)


class NameCodecTests(unittest.TestCase):
    def test_encode_name(self):
        self.assertEqual(encode_name("a.b"), b"\x01a\x01b\x00")
        self.assertEqual(encode_name("a.b."), b"\x01a\x01b\x00")
        self.assertEqual(encode_name("."), b"\x00")

    def test_read_plain_name(self):
        buf = b"\x03www\x07example\x03com\x00"
        name, end = read_name(buf, 0)
        self.assertEqual(name, "www.example.com")
        self.assertEqual(end, len(buf))

    def test_read_compressed_name(self):
        # offset 0: example.com ; then a query-like name with pointer to 0
        head = b"\x07example\x03com\x00"
        tail = b"\x03www\xc0\x00"
        buf = head + tail
        name, end = read_name(buf, len(head))
        self.assertEqual(name, "www.example.com")
        self.assertEqual(end, len(buf))

    def test_forward_pointer_rejected(self):
        buf = b"\x03www\xc0\x10"  # points forward beyond position
        with self.assertRaises(ValueError):
            read_name(buf, 0)

    def test_canonical(self):
        self.assertEqual(canonical_name("Demo.Local."), "demo.local")


class MessageTests(unittest.TestCase):
    def test_query_roundtrip(self):
        packet = build_query("demo.local", msg_id=0x4242)
        msg = parse_message(packet)
        self.assertEqual(msg.msg_id, 0x4242)
        self.assertEqual(msg.qdcount, 1)
        self.assertEqual(msg.questions[0].name, "demo.local")
        self.assertEqual(msg.questions[0].qtype, TYPE_A)
        self.assertEqual(msg.questions[0].qclass, CLASS_IN)
        # RD flag set
        self.assertTrue(msg.flags & 0x0100)

    def test_response_a_records(self):
        query = build_query("demo.local", msg_id=111)
        resp = build_response(query, 0, [("203.0.113.10", 60), ("203.0.113.11", 60)])
        msg = parse_message(resp)
        self.assertEqual(msg.msg_id, 111)
        self.assertEqual(msg.rcode, 0)
        self.assertTrue(msg.flags & 0x8000)  # QR
        self.assertTrue(msg.flags & 0x0080)  # RA
        self.assertEqual(len(msg.answers), 2)
        self.assertEqual([a.ip for a in msg.answers], ["203.0.113.10", "203.0.113.11"])
        self.assertEqual(msg.answers[0].ttl, 60)
        self.assertEqual(msg.answers[0].name, "demo.local")

    def test_error_rcodes(self):
        query = build_query("gone.local")
        for rcode in (2, 3, 5):
            resp = build_response(query, rcode)
            msg = parse_message(resp)
            self.assertEqual(msg.rcode, rcode)
            self.assertEqual(msg.questions[0].name, "gone.local")
            self.assertEqual(msg.ancount, 0)

    def test_truncated_and_garbage(self):
        with self.assertRaises(ValueError):
            parse_message(b"short")
        header_with_one_question = struct.pack("!HHHHHH", 1, 0, 1, 0, 0, 0)
        with self.assertRaises(ValueError):
            parse_message(header_with_one_question + b"\x05abc")  # truncated label

    def test_answer_accepted_by_dnspython_when_available(self):
        try:
            import dns.message
        except ImportError:
            self.skipTest("dnspython not installed")
        query = build_query("demo.local", msg_id=7)
        resp = build_response(query, 0, [("198.51.100.77", 0)])
        decoded = dns.message.from_wire(resp)
        self.assertEqual(decoded.id, 7)
        self.assertEqual(len(decoded.answer), 1)
        self.assertIn("198.51.100.77", str(decoded.answer[0]))

    def test_query_matches_dnspython_parser(self):
        try:
            import dns.message
        except ImportError:
            self.skipTest("dnspython not installed")
        import dns.rdatatype

        packet = build_query("demo.local")
        decoded = dns.message.from_wire(packet)
        self.assertEqual(decoded.question[0].name.to_text(), "demo.local.")
        self.assertEqual(decoded.question[0].rdtype, dns.rdatatype.A)


if __name__ == "__main__":
    unittest.main()
