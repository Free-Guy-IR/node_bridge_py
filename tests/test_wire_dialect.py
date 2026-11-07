import unittest

from PasarGuardNodeBridge.common import service_pb2 as service
from PasarGuardNodeBridge.utils import create_proxy

FORK_BACKEND_TYPES = {"XRAY": 0, "WIREGUARD": 1, "SING_BOX": 2, "OPEN_VPN": 3, "MTPROTO": 4, "L2TP": 5}

FORK_PROXY_FIELDS = {
    "vmess": (1, "Vmess"),
    "vless": (2, "Vless"),
    "trojan": (3, "Trojan"),
    "shadowsocks": (4, "Shadowsocks"),
    "wireguard": (5, "Wireguard"),
    "hysteria": (6, "Hysteria"),
    "hysteria2": (7, "Hysteria2"),
    "open_vpn": (8, "OpenVpnUser"),
    "mtproto": (9, "MtprotoUser"),
    "tuic": (10, "Tuic"),
    "l2tp": (11, "L2tpUser"),
}

UPSTREAM_ONLY_MTPROTO_ARGS = ("mtproto_user_ad_tag", "mtproto_max_tcp_conns", "mtproto_max_unique_ips")


def field_layout(message):
    return {field.name: field.number for field in message.DESCRIPTOR.fields}


def top_level_fields(data):
    fields = {}
    index = 0
    while index < len(data):
        key = 0
        shift = 0
        while True:
            byte = data[index]
            index += 1
            key |= (byte & 0x7F) << shift
            shift += 7
            if not byte & 0x80:
                break
        number, wire_type = key >> 3, key & 7
        if wire_type != 2:
            raise AssertionError(f"unexpected wire type {wire_type} for field {number}")
        length = 0
        shift = 0
        while True:
            byte = data[index]
            index += 1
            length |= (byte & 0x7F) << shift
            shift += 7
            if not byte & 0x80:
                break
        fields[number] = data[index : index + length]
        index += length
    return fields


class BackendTypeDialectTest(unittest.TestCase):
    def test_every_fork_backend_type_keeps_its_number(self):
        for name, number in FORK_BACKEND_TYPES.items():
            with self.subTest(name=name):
                self.assertEqual(service.BackendType.Value(name), number)

    def test_backend_type_two_is_sing_box(self):
        self.assertEqual(service.BackendType.Name(2), "SING_BOX")

    def test_mtproto_backend_is_four_on_the_wire(self):
        data = service.Backend(type=service.BackendType.MTPROTO).SerializeToString()
        self.assertEqual(data, b"\x08\x04")


class ProxyFieldDialectTest(unittest.TestCase):
    def test_every_fork_proxy_field_keeps_its_number_and_message(self):
        fields = {field.name: field for field in service.Proxy.DESCRIPTOR.fields}
        for name, (number, message) in FORK_PROXY_FIELDS.items():
            with self.subTest(field=name):
                self.assertIn(name, fields)
                self.assertEqual(fields[name].number, number)
                self.assertEqual(fields[name].message_type.name, message)

    def test_credential_messages_keep_their_layout(self):
        self.assertEqual(field_layout(service.MtprotoUser), {"username": 1, "secret": 2})
        self.assertEqual(
            field_layout(service.OpenVpnUser), {"username": 1, "password": 2, "max_concurrent_connections": 3}
        )
        self.assertEqual(field_layout(service.Hysteria2), {"password": 1})

    def test_mtproto_credentials_travel_on_field_nine(self):
        secret = "0123456789abcdef0123456789abcdef"
        fields = top_level_fields(create_proxy(mtproto_username="mt", mtproto_secret=secret).SerializeToString())
        self.assertEqual(service.MtprotoUser.FromString(fields[9]), service.MtprotoUser(username="mt", secret=secret))
        self.assertEqual(fields.get(8, b""), b"")

    def test_openvpn_credentials_travel_on_field_eight(self):
        fields = top_level_fields(
            create_proxy(
                openvpn_username="ov", openvpn_password="pw", openvpn_max_concurrent_connections=2
            ).SerializeToString()
        )
        self.assertEqual(
            service.OpenVpnUser.FromString(fields[8]),
            service.OpenVpnUser(username="ov", password="pw", max_concurrent_connections=2),
        )
        self.assertEqual(fields.get(9, b""), b"")

    def test_a_user_with_every_credential_keeps_each_on_its_own_field(self):
        secret = "0123456789abcdef0123456789abcdef"
        fields = top_level_fields(
            create_proxy(
                hysteria2_password="hy2",
                openvpn_username="ovu",
                openvpn_password="ovp",
                openvpn_max_concurrent_connections=4,
                mtproto_username="mtu",
                mtproto_secret=secret,
                tuic_uuid="tuu",
                tuic_password="tup",
                l2tp_username="l2u",
                l2tp_password="l2p",
            ).SerializeToString()
        )
        self.assertEqual(service.Hysteria2.FromString(fields[7]), service.Hysteria2(password="hy2"))
        self.assertEqual(
            service.OpenVpnUser.FromString(fields[8]),
            service.OpenVpnUser(username="ovu", password="ovp", max_concurrent_connections=4),
        )
        self.assertEqual(service.MtprotoUser.FromString(fields[9]), service.MtprotoUser(username="mtu", secret=secret))
        self.assertEqual(service.Tuic.FromString(fields[10]), service.Tuic(uuid="tuu", password="tup"))
        self.assertEqual(service.L2tpUser.FromString(fields[11]), service.L2tpUser(username="l2u", password="l2p"))


class UpstreamMtprotoArgumentsTest(unittest.TestCase):
    def test_upstream_only_mtproto_arguments_are_refused(self):
        for name in UPSTREAM_ONLY_MTPROTO_ARGS:
            with self.subTest(argument=name), self.assertRaises(TypeError):
                create_proxy(mtproto_secret="0123456789abcdef0123456789abcdef", **{name: 1})


if __name__ == "__main__":
    unittest.main()
