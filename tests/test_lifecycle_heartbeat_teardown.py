import asyncio
import contextlib
import logging
import unittest
from unittest.mock import patch

from aiohttp import web

from PasarGuardNodeBridge.aiohttp_compat import LazyClientSession, make_timeout
from PasarGuardNodeBridge.controller import NodeAPIError
from PasarGuardNodeBridge.rest import Node as RestNode
from PasarGuardNodeBridge.storage import InMemoryNodeLifecycleCoordinator, InMemoryUserSyncStore

COMMAND_SECONDS = 1.5
LEASE_SECONDS = 1.2
FLAP_LEASE_SECONDS = 0.75
LOGGER_NAME = "lifecycle-heartbeat-teardown"
NODE_ID = "shared-node"


class CancellationToErrorCoordinator(InMemoryNodeLifecycleCoordinator):
    def __init__(self, stall_in):
        super().__init__()
        self.stall_in = stall_in
        self.converting = True
        self.calls = 0

    async def _stall(self):
        try:
            await asyncio.sleep(30)
        except asyncio.CancelledError as e:
            if not self.converting:
                raise
            raise RuntimeError("store call aborted") from e

    async def heartbeat(self, lease):
        self.calls += 1
        if self.stall_in == "heartbeat":
            await self._stall()
        if self.stall_in == "get_state" and self.calls == 3:
            return False
        return await super().heartbeat(lease)

    async def get_state(self, node_id):
        if self.stall_in == "get_state":
            await self._stall()
        return await super().get_state(node_id)


class FlappingCoordinator(InMemoryNodeLifecycleCoordinator):
    PATTERN = ("held-false", "renew", "error", "renew", "unreported", "renew")

    def __init__(self):
        super().__init__()
        self.calls = 0
        self.answers = []

    async def heartbeat(self, lease):
        step = self.PATTERN[self.calls % len(self.PATTERN)]
        self.calls += 1
        self.answers.append(step)
        if step == "error":
            raise RuntimeError("store unavailable")
        if step == "unreported":
            return None
        if step == "held-false":
            async with self._lock:
                current = self._leases.get(lease.node_id)
                if current is None or current[0].token != lease.token:
                    return False
            return False
        return await super().heartbeat(lease)


class LeakedCancelRequestCoordinator(InMemoryNodeLifecycleCoordinator):
    def __init__(self):
        super().__init__()
        self.leaked = False
        self.answers = []

    async def heartbeat(self, lease):
        if not self.leaked:
            self.leaked = True
            self.answers.append("timeout")
            asyncio.current_task().cancel()
            try:
                await asyncio.sleep(1)
            except asyncio.CancelledError:
                raise TimeoutError("store call timed out") from None
        answer = await super().heartbeat(lease)
        self.answers.append(f"renewed={answer}")
        return answer


class RecordingCoordinator(InMemoryNodeLifecycleCoordinator):
    def __init__(self):
        super().__init__()
        self.answers = []

    async def heartbeat(self, lease):
        answer = await super().heartbeat(lease)
        self.answers.append((lease.worker_id, answer))
        return answer


class LifecycleHeartbeatTeardownTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.in_flight = 0
        self.max_in_flight = 0
        self.completed = 0

        async def root(request):
            return web.json_response({"status": "ok"})

        async def command(request):
            self.in_flight += 1
            self.max_in_flight = max(self.max_in_flight, self.in_flight)
            try:
                await asyncio.sleep(COMMAND_SECONDS)
            finally:
                self.in_flight -= 1
            self.completed += 1
            return web.json_response({"status": "ok"})

        app = web.Application()
        app.router.add_get("/", root)
        app.router.add_post("/node/update", command)
        self.runner = web.AppRunner(app)
        await self.runner.setup()
        await web.TCPSite(self.runner, "127.0.0.1", 0).start()
        self.api_port = self.runner.addresses[0][1]
        self.nodes = []

    async def asyncTearDown(self):
        for node in self.nodes:
            for task in list(node._lifecycle_heartbeat_tasks.values()):
                task.cancel()
            await node._json_client.close()
        await self.runner.cleanup()

    def make_node(self, coordinator, worker_id, lease_seconds=LEASE_SECONDS):
        with patch("ssl.SSLContext.load_verify_locations"):
            node = RestNode(
                address="127.0.0.1",
                port=1,
                api_port=self.api_port,
                server_ca="test",
                api_key="00000000-0000-0000-0000-000000000000",
                node_id=NODE_ID,
                user_sync_store=InMemoryUserSyncStore(),
                default_timeout=10,
                logger=logging.getLogger(LOGGER_NAME),
                lifecycle_coordinator=coordinator,
                lifecycle_lease_seconds=lease_seconds,
                worker_id=worker_id,
            )
        node._json_client = LazyClientSession(
            ssl_context=None, headers={}, base_url=f"http://127.0.0.1:{self.api_port}", timeout=make_timeout(30)
        )
        self.nodes.append(node)
        return node

    async def _assert_release_finishes(self, stall_in):
        coordinator = CancellationToErrorCoordinator(stall_in)
        node = self.make_node(coordinator, "worker-a")
        with self.assertLogs(LOGGER_NAME, logging.WARNING):
            update = asyncio.create_task(node.update_node())
            done, _ = await asyncio.wait({update}, timeout=COMMAND_SECONDS + 2.5)
            if not done:
                coordinator.converting = False
                for task in list(node._lifecycle_heartbeat_tasks.values()):
                    task.cancel()
                await asyncio.wait_for(update, 5)
        self.assertTrue(done, f"update_node did not return after its call finished ({stall_in}, {coordinator.calls})")
        update.result()
        self.assertNotIn(NODE_ID, coordinator._leases)
        self.assertEqual(node._lifecycle_heartbeat_tasks, {})

    async def test_release_finishes_when_a_heartbeat_turns_cancellation_into_an_error(self):
        await self._assert_release_finishes("heartbeat")

    async def test_release_finishes_when_a_lease_state_read_turns_cancellation_into_an_error(self):
        await self._assert_release_finishes("get_state")

    async def _contend_during_holder_call(self, coordinator, lease_seconds, expect_warnings=True):
        holder = self.make_node(coordinator, "worker-a", lease_seconds)
        rival = self.make_node(coordinator, "worker-b", lease_seconds)
        conflicts = 0
        stop = asyncio.Event()

        async def contend():
            nonlocal conflicts
            while not stop.is_set():
                try:
                    await rival.update_node()
                except NodeAPIError as e:
                    if e.code != 409:
                        raise
                    conflicts += 1
                await asyncio.sleep(0.02)

        logs = self.assertLogs(LOGGER_NAME, logging.WARNING) if expect_warnings else contextlib.nullcontext()
        with logs:
            first = asyncio.create_task(holder.update_node())
            await asyncio.sleep(0.05)
            contender = asyncio.create_task(contend())
            await first
            stop.set()
            await contender
        return rival, conflicts

    async def test_a_leaked_cancel_request_from_a_coordinator_does_not_end_renewal(self):
        coordinator = LeakedCancelRequestCoordinator()
        _, conflicts = await self._contend_during_holder_call(coordinator, LEASE_SECONDS)
        self.assertEqual(self.max_in_flight, 1, coordinator.answers)
        self.assertGreater(conflicts, 10)
        self.assertEqual(coordinator.answers[0], "timeout")
        self.assertIn("renewed=True", coordinator.answers)

    async def test_renewal_runs_under_an_eager_task_factory(self):
        loop = asyncio.get_running_loop()
        previous = loop.get_task_factory()
        loop.set_task_factory(asyncio.eager_task_factory)
        self.addCleanup(loop.set_task_factory, previous)
        coordinator = RecordingCoordinator()
        _, conflicts = await self._contend_during_holder_call(coordinator, FLAP_LEASE_SECONDS, expect_warnings=False)
        holder_renewals = [answer for worker, answer in coordinator.answers if worker == "worker-a"]
        self.assertEqual(self.max_in_flight, 1, coordinator.answers)
        self.assertGreater(conflicts, 10)
        self.assertGreaterEqual(len(holder_renewals), 2, coordinator.answers)
        self.assertTrue(all(holder_renewals), coordinator.answers)

    async def test_a_contending_worker_never_overlaps_while_renewals_flap(self):
        coordinator = FlappingCoordinator()
        rival, conflicts = await self._contend_during_holder_call(coordinator, FLAP_LEASE_SECONDS)
        self.assertEqual(self.max_in_flight, 1, coordinator.answers)
        self.assertGreater(conflicts, 10)
        self.assertGreaterEqual(self.completed, 1)
        self.assertIn("held-false", coordinator.answers)
        self.assertIn("error", coordinator.answers)
        self.assertIn("unreported", coordinator.answers)
        followup = await rival.update_node()
        self.assertIsNotNone(followup)


if __name__ == "__main__":
    unittest.main()
