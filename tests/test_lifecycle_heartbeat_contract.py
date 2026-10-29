import asyncio
import logging
import unittest
from unittest.mock import patch

from PasarGuardNodeBridge.controller import Controller
from PasarGuardNodeBridge.storage import InMemoryNodeLifecycleCoordinator, LifecycleOperation

LEASE_SECONDS = 3.0
START = 1000.0
LOGGER_NAME = "lifecycle-heartbeat-contract-test"
_real_sleep = asyncio.sleep


class ScriptedCoordinator(InMemoryNodeLifecycleCoordinator):
    def __init__(self, answers, clock):
        super().__init__()
        self.answers = list(answers)
        self.clock = clock
        self.calls = []

    async def heartbeat(self, lease):
        answer = self.answers[len(self.calls)] if len(self.calls) < len(self.answers) else self.answers[-1]
        self.calls.append((self.clock.now - START, answer))
        if answer == "renewed":
            return await super().heartbeat(lease)
        if answer == "unreported":
            return None
        if answer == "error":
            raise RuntimeError("store unavailable")
        if answer == "taken":
            async with self._lock:
                self._leases.pop(lease.node_id, None)
            await self.try_acquire(lease.node_id, "other-worker", LifecycleOperation.START, 30)
        elif answer == "released":
            await self.release(lease)
        return False


class FakeClock:
    def __init__(self):
        self.now = START

    def time(self):
        return self.now

    async def sleep(self, delay, result=None):
        self.now += delay
        await _real_sleep(0)
        return result


class HeartbeatContractTests(unittest.IsolatedAsyncioTestCase):
    async def run_heartbeat(self, answers, max_calls=12):
        clock = FakeClock()
        coordinator = ScriptedCoordinator(answers, clock)
        with patch("ssl.SSLContext.load_verify_locations"):
            controller = Controller(
                server_ca="test",
                api_key="00000000-0000-0000-0000-000000000000",
                service_url="https://127.0.0.1:1/",
                name="contract",
                node_id="contract",
                logger=logging.getLogger(LOGGER_NAME),
                lifecycle_coordinator=coordinator,
                lifecycle_lease_seconds=LEASE_SECONDS,
            )
        self.addAsyncCleanup(controller._json_client.close)
        loop = asyncio.get_running_loop()
        with patch.object(loop, "time", clock.time), patch.object(asyncio, "sleep", clock.sleep):
            lease = await coordinator.try_acquire(
                "contract", controller.worker_id, LifecycleOperation.UPDATE_NODE, LEASE_SECONDS
            )
            task = asyncio.create_task(controller._heartbeat_lifecycle_lease(lease, START))
            while not task.done() and len(coordinator.calls) < max_calls:
                await _real_sleep(0)
            stopped = task.done()
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        return stopped, coordinator.calls

    async def test_unreported_renewals_do_not_extend_the_lease_estimate(self):
        stopped, calls = await self.run_heartbeat(["unreported"] * 3 + ["refused"])
        self.assertTrue(stopped, calls)
        self.assertEqual([at for at, _ in calls], [1.0, 2.0, 3.0, 4.0], calls)

    async def test_an_error_does_not_extend_the_lease_estimate(self):
        stopped, calls = await self.run_heartbeat(["error"] * 3 + ["refused"])
        self.assertTrue(stopped, calls)
        self.assertEqual(len(calls), 4, calls)

    async def test_a_granted_renewal_extends_the_lease_estimate(self):
        stopped, calls = await self.run_heartbeat(["renewed"] * 3 + ["refused"] + ["renewed"] * 8)
        self.assertFalse(stopped, calls)
        self.assertEqual(len(calls), 12, calls)

    async def test_a_refused_renewal_stops_once_the_last_granted_one_may_have_expired(self):
        stopped, calls = await self.run_heartbeat(["renewed"] + ["unreported"] * 2 + ["refused"] * 4)
        self.assertTrue(stopped, calls)
        self.assertEqual([at for at, _ in calls], [1.0, 2.0, 3.0, 4.0], calls)

    async def test_a_refusal_after_another_worker_took_the_lease_stops_renewal(self):
        stopped, calls = await self.run_heartbeat(["renewed", "taken"])
        self.assertTrue(stopped, calls)
        self.assertEqual(len(calls), 2, calls)

    async def test_a_refusal_after_the_lease_was_released_stops_renewal(self):
        stopped, calls = await self.run_heartbeat(["renewed", "released"])
        self.assertTrue(stopped, calls)
        self.assertEqual(len(calls), 2, calls)

    async def test_a_coordinator_that_never_reports_keeps_renewing(self):
        stopped, calls = await self.run_heartbeat(["unreported"], max_calls=30)
        self.assertFalse(stopped, calls)
        self.assertEqual(len(calls), 30, calls)


if __name__ == "__main__":
    unittest.main()
