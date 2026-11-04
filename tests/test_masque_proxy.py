import unittest

from PasarGuardNodeBridge.common import service_pb2
from PasarGuardNodeBridge.common.service_pb2 import Hysteria2, Proxy
from PasarGuardNodeBridge.utils import create_proxy, create_user


class MasqueProxyTest(unittest.TestCase):
    def test_create_proxy_carries_masque_pass(self):
        proxy = create_proxy(masque_pass="s3cret")
        self.assertTrue(proxy.HasField("masque"))
        self.assertEqual(getattr(proxy.masque, "pass"), "s3cret")

    def test_create_proxy_without_masque_pass_sends_an_empty_pass(self):
        proxy = create_proxy(vless_id="11111111-1111-1111-1111-111111111111")
        self.assertEqual(getattr(proxy.masque, "pass"), "")

    def test_masque_rides_proxy_field_12_like_the_fork_node(self):
        field = Proxy.DESCRIPTOR.fields_by_name["masque"]
        self.assertEqual(field.number, 12)
        self.assertEqual(field.message_type.full_name, "service.Masque")
        pass_field = service_pb2.Masque.DESCRIPTOR.fields_by_name["pass"]
        self.assertEqual(pass_field.number, 1)
        self.assertEqual(pass_field.type, pass_field.TYPE_STRING)
        self.assertEqual(list(service_pb2.Masque.DESCRIPTOR.fields_by_name), ["pass"])

    def test_masque_wire_bytes_match_the_fork_node(self):
        data = Proxy(masque=service_pb2.Masque(**{"pass": "p"})).SerializeToString()
        self.assertEqual(data, bytes([0x62, 0x03, 0x0A, 0x01, ord("p")]))
        self.assertFalse(data.startswith(bytes([0x3A])))

    def test_create_user_serializes_masque_on_field_12(self):
        user = create_user("u@example", create_proxy(masque_pass="p"), ["masque-in"])
        decoded = service_pb2.User.FromString(user.SerializeToString())
        self.assertEqual(getattr(decoded.proxies.masque, "pass"), "p")
        self.assertEqual(decoded.proxies.hysteria2.password, "")


class Hysteria2FieldUnchangedTest(unittest.TestCase):
    def test_hysteria2_stays_on_proxy_field_7(self):
        field = Proxy.DESCRIPTOR.fields_by_name["hysteria2"]
        self.assertEqual(field.number, 7)
        self.assertEqual(field.message_type.full_name, "service.Hysteria2")

    def test_field_7_bytes_decode_as_hysteria2_not_masque(self):
        data = bytes([0x3A, 0x03, 0x0A, 0x01, ord("p")])
        decoded = Proxy.FromString(data)
        self.assertEqual(decoded.hysteria2.password, "p")
        self.assertFalse(decoded.HasField("masque"))
        self.assertEqual(Proxy(hysteria2=Hysteria2(password="p")).SerializeToString(), data)

    def test_create_proxy_hysteria2_password_is_unchanged(self):
        proxy = create_proxy(hysteria2_password="h2", masque_pass="mq")
        self.assertEqual(proxy.hysteria2.password, "h2")
        self.assertEqual(getattr(proxy.masque, "pass"), "mq")

    def test_proxy_field_numbers_are_the_fork_node_layout(self):
        numbers = {f.name: f.number for f in Proxy.DESCRIPTOR.fields}
        self.assertEqual(
            numbers,
            {
                "vmess": 1,
                "vless": 2,
                "trojan": 3,
                "shadowsocks": 4,
                "wireguard": 5,
                "hysteria": 6,
                "hysteria2": 7,
                "open_vpn": 8,
                "mtproto": 9,
                "tuic": 10,
                "l2tp": 11,
                "masque": 12,
            },
        )


if __name__ == "__main__":
    unittest.main()
