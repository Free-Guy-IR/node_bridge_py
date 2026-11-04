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


if __name__ == "__main__":
    unittest.main()
