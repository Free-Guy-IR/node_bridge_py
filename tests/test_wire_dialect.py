import unittest

from google.protobuf import descriptor_pb2

from PasarGuardNodeBridge.common import service_pb2 as service
from PasarGuardNodeBridge.utils import create_proxy

FORK_BACKEND_TYPES = {"XRAY": 0, "WIREGUARD": 1, "SING_BOX": 2, "OPEN_VPN": 3, "L2TP": 5}

FORK_PROXY_FIELDS = {
    "vmess": (1, "Vmess"),
    "vless": (2, "Vless"),
    "trojan": (3, "Trojan"),
    "shadowsocks": (4, "Shadowsocks"),
    "wireguard": (5, "Wireguard"),
    "hysteria": (6, "Hysteria"),
    "hysteria2": (7, "Hysteria2"),
    "open_vpn": (8, "OpenVpnUser"),
    "tuic": (10, "Tuic"),
    "l2tp": (11, "L2tpUser"),
    "masque": (12, "Masque"),
}

REMOVED_MTPROTO_ARGS = (
    "mtproto_username",
    "mtproto_secret",
    "mtproto_user_ad_tag",
    "mtproto_max_tcp_conns",
    "mtproto_max_unique_ips",
)

VLESS_ID = "11111111-2222-3333-4444-555555555555"

VLESS_PAYLOAD_FROM_0_9_2 = bytes.fromhex(
    "12260a2431313131313131312d323232322d333333332d343434342d3535353535353535353535354a00"
)


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


def every_credential_proxy():
    return create_proxy(
        vmess_id="vm",
        vless_id=VLESS_ID,
        trojan_password="tr",
        shadowsocks_password="ss",
        shadowsocks_method="aes-128-gcm",
        wireguard_public_key="wg-public-key",
        wireguard_peer_ips=["10.0.0.2/32"],
        hysteria_auth="hy",
        hysteria2_password="hy2",
        openvpn_username="ovu",
        openvpn_password="ovp",
        openvpn_max_concurrent_connections=4,
        tuic_uuid="tuu",
        tuic_password="tup",
        l2tp_username="l2u",
        l2tp_password="l2p",
        masque_pass="mq",
    )


class BackendTypeDialectTest(unittest.TestCase):
    def test_every_fork_backend_type_keeps_its_number(self):
        values = {value.name: value.number for value in service.BackendType.DESCRIPTOR.values}
        self.assertEqual(values, FORK_BACKEND_TYPES)

    def test_backend_type_two_is_sing_box(self):
        self.assertEqual(service.BackendType.Name(2), "SING_BOX")

    def test_the_mtproto_backend_number_and_name_stay_reserved(self):
        enum = descriptor_pb2.EnumDescriptorProto()
        service.BackendType.DESCRIPTOR.CopyToProto(enum)
        self.assertEqual([(reserved.start, reserved.end) for reserved in enum.reserved_range], [(4, 4)])
        self.assertEqual(list(enum.reserved_name), ["MTPROTO"])
        with self.assertRaises(ValueError):
            service.BackendType.Name(4)


class ProxyFieldDialectTest(unittest.TestCase):
    def test_every_fork_proxy_field_keeps_its_number_and_message(self):
        fields = {field.name: (field.number, field.message_type.name) for field in service.Proxy.DESCRIPTOR.fields}
        self.assertEqual(fields, FORK_PROXY_FIELDS)

    def test_the_mtproto_field_number_and_name_stay_reserved(self):
        message = descriptor_pb2.DescriptorProto()
        service.Proxy.DESCRIPTOR.CopyToProto(message)
        self.assertEqual([(reserved.start, reserved.end) for reserved in message.reserved_range], [(9, 10)])
        self.assertEqual(list(message.reserved_name), ["mtproto"])
        self.assertFalse(hasattr(service, "MtprotoUser"))

    def test_credential_messages_keep_their_layout(self):
        self.assertEqual(field_layout(service.Wireguard), {"public_key": 1, "peer_ips": 2, "pre_shared_key": 3})
        self.assertEqual(
            field_layout(service.OpenVpnUser), {"username": 1, "password": 2, "max_concurrent_connections": 3}
        )
        self.assertEqual(field_layout(service.Hysteria2), {"password": 1})

    def test_pre_shared_key_travels_on_wireguard_field_three(self):
        proxy = create_proxy(wireguard_public_key="wg-public-key", wireguard_peer_ips=["10.0.0.2/32"])
        proxy.wireguard.pre_shared_key = "wg-psk"
        wireguard = top_level_fields(top_level_fields(proxy.SerializeToString())[5])
        self.assertEqual(wireguard, {1: b"wg-public-key", 2: b"10.0.0.2/32", 3: b"wg-psk"})

    def test_no_payload_carries_field_nine(self):
        for proxy in (create_proxy(), create_proxy(vless_id=VLESS_ID), every_credential_proxy()):
            with self.subTest(fields=sorted(top_level_fields(proxy.SerializeToString()))):
                self.assertNotIn(9, top_level_fields(proxy.SerializeToString()))

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
        self.assertNotIn(9, fields)

    def test_a_user_with_every_credential_keeps_each_on_its_own_field(self):
        fields = top_level_fields(every_credential_proxy().SerializeToString())
        self.assertEqual(sorted(fields), [1, 2, 3, 4, 5, 6, 7, 8, 10, 11, 12])
        self.assertEqual(service.Hysteria2.FromString(fields[7]), service.Hysteria2(password="hy2"))
        self.assertEqual(
            service.OpenVpnUser.FromString(fields[8]),
            service.OpenVpnUser(username="ovu", password="ovp", max_concurrent_connections=4),
        )
        self.assertEqual(service.Tuic.FromString(fields[10]), service.Tuic(uuid="tuu", password="tup"))
        self.assertEqual(service.L2tpUser.FromString(fields[11]), service.L2tpUser(username="l2u", password="l2p"))
        self.assertEqual(service.Masque.FromString(fields[12]), service.Masque(**{"pass": "mq"}))


class PreviousBridgePayloadTest(unittest.TestCase):
    def test_a_payload_bridge_0_9_2_built_still_decodes_without_its_empty_mtproto_section(self):
        proxy = service.Proxy.FromString(VLESS_PAYLOAD_FROM_0_9_2)
        self.assertEqual([field.name for field, _ in proxy.ListFields()], ["vless"])
        self.assertEqual(proxy.vless.id, VLESS_ID)


class RemovedMtprotoArgumentsTest(unittest.TestCase):
    def test_every_mtproto_argument_is_refused(self):
        for name in REMOVED_MTPROTO_ARGS:
            with self.subTest(argument=name), self.assertRaises(TypeError):
                create_proxy(**{name: "0123456789abcdef0123456789abcdef"})


if __name__ == "__main__":
    unittest.main()
