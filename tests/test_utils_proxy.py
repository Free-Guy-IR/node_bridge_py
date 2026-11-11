import unittest

from PasarGuardNodeBridge.common import service_pb2 as service


class WireguardPreSharedKeyTest(unittest.TestCase):
    def test_pre_shared_key_round_trips(self):
        wireguard = service.Wireguard(public_key="wg-public-key", peer_ips=["10.0.0.2/32"], pre_shared_key="wg-psk")

        decoded = service.Wireguard.FromString(wireguard.SerializeToString())

        self.assertEqual(decoded.pre_shared_key, "wg-psk")
        self.assertEqual(decoded, wireguard)
        self.assertEqual(service.Wireguard.DESCRIPTOR.fields_by_name["pre_shared_key"].number, 3)


if __name__ == "__main__":
    unittest.main()
