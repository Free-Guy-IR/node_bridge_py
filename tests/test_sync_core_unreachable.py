import unittest

from PasarGuardNodeBridge.controller import core_unreachable
from tests.sync_fakes import DEAD_CORE_ERROR, HUNG_CORE_ERROR

PREFIX = "failed to update users: "


class CoreUnreachableTests(unittest.TestCase):
    def test_every_line_unavailable_is_a_dead_core(self):
        self.assertTrue(core_unreachable(PREFIX + DEAD_CORE_ERROR))

    def test_every_line_deadline_exceeded_is_a_dead_core(self):
        self.assertTrue(core_unreachable(PREFIX + HUNG_CORE_ERROR))
        self.assertTrue(
            core_unreachable(
                PREFIX + "failed to update users:"
                "\nrpc error: code = DeadlineExceeded desc = context deadline exceeded"
                "\nrpc error: code = DeadlineExceeded desc = context deadline exceeded"
            )
        )

    def test_unavailable_and_deadline_lines_together_are_a_dead_core(self):
        self.assertTrue(
            core_unreachable(
                PREFIX + "failed to update users:"
                "\nrpc error: code = DeadlineExceeded desc = context deadline exceeded"
                '\nrpc error: code = Unavailable desc = connection error: desc = "transport: Error while dialing: '
                'dial tcp 127.0.0.1:8080: connect: connection refused"'
            )
        )

    def test_one_live_rejection_among_dead_lines_is_not_a_dead_core(self):
        self.assertFalse(
            core_unreachable(
                PREFIX + "failed to update users:"
                "\nrpc error: code = DeadlineExceeded desc = context deadline exceeded"
                "\nproxy/vless: User a already exists"
            )
        )

    def test_an_empty_or_prefix_only_detail_is_not_a_dead_core(self):
        self.assertFalse(core_unreachable(""))
        self.assertFalse(core_unreachable("failed to update users:"))

    def test_a_live_rejection_that_mentions_connection_refused_is_not_a_dead_core(self):
        self.assertFalse(core_unreachable(PREFIX + "failed to update users:\ninvalid user a: upstream connection refused by policy"))
        self.assertFalse(core_unreachable(PREFIX + "invalid account a: connection refused"))

    def test_a_live_rejection_that_quotes_a_grpc_code_is_not_a_dead_core(self):
        self.assertFalse(core_unreachable(PREFIX + "invalid user a: rpc error: code = Unavailable desc = x"))
        self.assertFalse(core_unreachable(PREFIX + "invalid user a: code = DeadlineExceeded"))

    def test_other_grpc_codes_are_not_a_dead_core(self):
        self.assertFalse(
            core_unreachable(PREFIX + "rpc error: code = Canceled desc = grpc: the client connection is closing")
        )
        self.assertFalse(core_unreachable(PREFIX + "rpc error: code = unavailable desc = Connection Refused"))

    def test_the_core_api_connect_error_line_is_a_dead_core(self):
        self.assertTrue(core_unreachable(PREFIX + "dial tcp 127.0.0.1:8080: connect: connection refused"))
        self.assertTrue(
            core_unreachable(PREFIX + "failed to update users:\ndial tcp4 127.0.0.1:62789: connect: connection refused")
        )
        self.assertFalse(core_unreachable(PREFIX + "dial tcp 10.0.0.9:8080: connect: connection refused"))

    def test_a_crlf_detail_is_read_line_by_line(self):
        self.assertTrue(core_unreachable(PREFIX + HUNG_CORE_ERROR.replace("\n", "\r\n")))


if __name__ == "__main__":
    unittest.main()
