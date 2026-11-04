import asyncio
from unittest.mock import patch

from PasarGuardNodeBridge.common import service_pb2 as service
from PasarGuardNodeBridge.grpclib import Node as GrpcNode
from tests.sync_fakes import FakeNodeCore, SyncFakeTestCase, user


def backoff_before_threshold(threshold):
    return sum(min(2.0**step, 30.0) for step in range(threshold - 1))


class QuietStartedNodeTests(SyncFakeTestCase):
    def answering(self, node, reply):
        async def answer(*args, **kwargs):
            return reply

        name = "_handle_grpc_request" if isinstance(node, GrpcNode) else "_make_request"
        return patch.object(node, name, answer)

    async def started(self, node, core, users):
        for item in users:
            core.accept(item)
        info = service.BaseInfoResponse(started=True, node_version="0.2.0", core_version="1.0.0")

        async def lease(*args, **kwargs):
            return None

        with (
            self.answering(node, info),
            patch.object(node, "_acquire_lifecycle_lease", lease),
            patch.object(node, "_release_lifecycle_lease", lease),
        ):
            await node.start("{}", service.BackendType.XRAY, users)

    async def fully_synced(self, node, core, users):
        for item in users:
            core.accept(item)
        with self.answering(node, service.Empty()):
            await node.sync_users(users)

    def critical_lines(self, node):
        lines = []
        original = node.logger.critical

        def recording(message, *args, **kwargs):
            lines.append(message)
            return original(message, *args, **kwargs)

        node.logger.critical = recording
        return lines

    async def test_a_started_node_is_not_hard_reset_for_a_quiet_burst_of_invalid_users(self):
        threshold = 2
        for make in (self.grpc_node, self.rest_node):
            with self.subTest(transport=make.__name__):
                core = FakeNodeCore(delay=0.01)
                node = await make(core, sync_lease_seconds=1.0)
                node._hard_reset_threshold = threshold
                raised = self.watch_hard_reset(node)
                await self.started(node, core, [user(f"good-{index}") for index in range(3)])
                await node.update_users([service.User(email=f"invalid-{index}", inbounds=["bad"]) for index in range(5)])
                await asyncio.sleep(backoff_before_threshold(threshold) + 3)
                self.assertGreater(core.sends_with("invalid-0"), 0)
                self.assertEqual(raised, [])

    async def test_a_started_node_at_the_production_threshold_is_not_hard_reset_for_five_invalid_users(self):
        for make in (self.grpc_node, self.rest_node):
            with self.subTest(transport=make.__name__):
                core = FakeNodeCore(delay=0.01)
                node = await make(core, sync_lease_seconds=1.0)
                raised = self.watch_hard_reset(node)
                await self.started(node, core, [user(f"good-{index}") for index in range(20)])
                for index in range(5):
                    await node.update_user(service.User(email=f"invalid-{index}", inbounds=["bad"]))
                    await asyncio.sleep(0.2)
                await asyncio.sleep(backoff_before_threshold(node._hard_reset_threshold) + 2)
                self.assertGreater(core.sends_with("invalid-4"), 0)
                self.assertEqual(raised, [])

    async def test_a_fully_synced_node_is_not_hard_reset_for_a_quiet_burst_of_invalid_users(self):
        threshold = 2
        for make in (self.grpc_node, self.rest_node):
            with self.subTest(transport=make.__name__):
                core = FakeNodeCore(delay=0.01)
                node = await make(core, sync_lease_seconds=1.0)
                node._hard_reset_threshold = threshold
                raised = self.watch_hard_reset(node)
                await node.connect("0.2.0", "1.0.0")
                await self.fully_synced(node, core, [user(f"good-{index}") for index in range(3)])
                await node.update_users([service.User(email=f"invalid-{index}", inbounds=["bad"]) for index in range(5)])
                await asyncio.sleep(backoff_before_threshold(threshold) + 3)
                self.assertGreater(core.sends_with("invalid-0"), 0)
                self.assertEqual(raised, [])

    async def test_a_start_racing_the_first_rejected_rounds_leaves_no_systemic_window(self):
        threshold = 2
        for make in (self.grpc_node, self.rest_node):
            for in_flight in (True, False):
                with self.subTest(transport=make.__name__, in_flight=in_flight):
                    core = FakeNodeCore(delay=0.3)
                    node = await make(core, sync_lease_seconds=2.0)
                    node._hard_reset_threshold = threshold
                    raised = self.watch_hard_reset(node)
                    good = [user(f"good-{index}") for index in range(3)]
                    first_send = asyncio.Event()
                    first_answer = asyncio.Event()

                    async def on_send(users):
                        first_send.set()

                    core.on_send = on_send
                    original_count = node._count_systemic_rejection

                    def counting(emails):
                        original_count(emails)
                        first_answer.set()

                    node._count_systemic_rejection = counting
                    await node.connect("0.2.0", "1.0.0")
                    await node.update_users(
                        [service.User(email=f"invalid-{index}", inbounds=["bad"]) for index in range(5)]
                    )
                    await asyncio.wait_for((first_send if in_flight else first_answer).wait(), 10)
                    await self.started(node, core, good)
                    await asyncio.sleep(backoff_before_threshold(threshold) + 3)
                    self.assertGreater(core.sends_with("invalid-0"), 0)
                    self.assertEqual(raised, [])

    async def test_a_node_that_never_accepted_a_user_since_it_connected_is_still_hard_reset(self):
        threshold = 2
        for make in (self.grpc_node, self.rest_node):
            with self.subTest(transport=make.__name__):
                core = FakeNodeCore(delay=0.01)
                core.reject_all = True
                node = await make(core, sync_lease_seconds=1.0)
                node._hard_reset_threshold = threshold
                raised = self.watch_hard_reset(node)
                lines = self.critical_lines(node)
                await self.started(node, core, [])
                await node.update_users([user(f"fresh-{index}") for index in range(3)])

                async def fired():
                    return node.requires_hard_reset()

                await self.until(fired, 10)
                self.assertEqual(len(raised), 1, raised)
                self.assertTrue(any("since it connected" in line for line in lines), lines)

