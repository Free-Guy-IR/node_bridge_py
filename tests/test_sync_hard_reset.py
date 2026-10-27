import asyncio

from PasarGuardNodeBridge.common import service_pb2 as service
from tests.sync_fakes import FakeNodeCore, SyncFakeTestCase, user


def backoff_before_threshold(threshold):
    return sum(min(2.0**step, 30.0) for step in range(threshold - 1))


class SystemicRejectionTests(SyncFakeTestCase):
    def arrivals(self, node, prefix, every, latest, bad_every=0):
        async def arrive():
            index = 0
            while True:
                await asyncio.sleep(every)
                index += 1
                email = f"{prefix}-{index}"
                inbounds = ["bad"] if bad_every and index % bad_every == 0 else [f"{email}-v1"]
                latest[email] = inbounds
                await node.update_user(service.User(email=email, inbounds=inbounds))

        task = asyncio.create_task(arrive())

        async def stop():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

        self.cleanups.append(stop)
        return stop

    async def start(self, node, users):
        await node._user_sync_store.enqueue_users(node.node_id, users)
        started = asyncio.get_running_loop().time()
        await node.connect("0.2.0", "1.0.0")
        return started

    async def until_hard_reset(self, node, timeout):
        async def fired():
            return node.requires_hard_reset()

        await self.until(fired, timeout)

    async def test_r1_f2_a_node_rejecting_every_user_with_steady_arrivals_is_hard_reset(self):
        lease, threshold = 1.0, 2
        for make in (self.grpc_node, self.rest_node):
            with self.subTest(transport=make.__name__):
                core = FakeNodeCore(delay=0.25)
                core.reject_all = True
                node = await make(core, sync_lease_seconds=lease)
                node._hard_reset_threshold = threshold
                raised = self.watch_hard_reset(node)
                started = await self.start(node, [user(f"seed-{index}") for index in range(2)])
                stop = self.arrivals(node, "fresh", 0.15, {})
                await self.until_hard_reset(node, 30)
                await stop()
                self.assertGreaterEqual(raised[0] - started, backoff_before_threshold(threshold))

    async def test_a_few_rejected_users_among_accepted_arrivals_never_hard_reset(self):
        lease, threshold = 1.0, 2
        for make in (self.grpc_node, self.rest_node):
            with self.subTest(transport=make.__name__):
                core = FakeNodeCore(delay=0.05)
                node = await make(core, sync_lease_seconds=lease)
                node._hard_reset_threshold = threshold
                raised = self.watch_hard_reset(node)
                failures = self.count_failures(node)
                initial = [user(f"u-{index}", "bad" if index in (1, 4) else "good") for index in range(6)]
                latest = {item.email: list(item.inbounds) for item in initial}
                await self.start(node, initial)
                stop = self.arrivals(node, "fresh", 0.15, latest, bad_every=5)
                await asyncio.sleep(4 * threshold * lease)
                await stop()
                suspects = {email for email, inbounds in latest.items() if inbounds == ["bad"]}
                await self.until(lambda: self.settled(node, core, latest, suspects), 10)
                self.assertEqual((raised, failures), ([], []))

    async def test_a_slow_node_isolating_one_user_and_retrying_it_never_hard_resets(self):
        lease, threshold = 2.0, 2
        for make in (self.grpc_node, self.rest_node):
            with self.subTest(transport=make.__name__):
                core = FakeNodeCore(delay=0.8)
                node = await make(core, sync_lease_seconds=lease)
                node._hard_reset_threshold = threshold
                raised = self.watch_hard_reset(node)
                users = [user(f"user-{index}", "bad" if index == 0 else "good") for index in range(24)]
                latest = {item.email: list(item.inbounds) for item in users}
                started = await self.start(node, users)
                await self.until(lambda: self.settled(node, core, latest, ["user-0"]), 60)
                await asyncio.sleep(max(0.0, started + 4 * threshold * lease - asyncio.get_running_loop().time()))
                self.assertGreater(core.sends_with("user-0"), 5)
                self.assertEqual(raised, [])

    async def test_a_systemic_rejection_shorter_than_the_bound_never_hard_resets(self):
        lease, threshold = 1.0, 3
        for make in (self.grpc_node, self.rest_node):
            with self.subTest(transport=make.__name__):
                core = FakeNodeCore(delay=0.05)
                core.reject_all = True
                node = await make(core, sync_lease_seconds=lease)
                node._hard_reset_threshold = threshold
                raised = self.watch_hard_reset(node)
                initial = [user(f"u-{index}") for index in range(4)]
                latest = {item.email: list(item.inbounds) for item in initial}
                await self.start(node, initial)
                stop = self.arrivals(node, "fresh", 0.15, latest)
                await asyncio.sleep(backoff_before_threshold(threshold) / 2)
                core.reject_all = False
                await asyncio.sleep(2 * backoff_before_threshold(threshold))
                await stop()
                await self.until(lambda: self.settled(node, core, latest, []), 20)
                self.assertEqual(raised, [])

    async def test_a_systemic_rejection_with_pushes_hard_resets_once_and_recovers_without_losing_a_push(self):
        lease, threshold = 1.0, 2
        for make in (self.grpc_node, self.rest_node):
            with self.subTest(transport=make.__name__):
                core = FakeNodeCore(delay=0.05)
                core.reject_all = True
                node = await make(core, sync_lease_seconds=lease)
                node._hard_reset_threshold = threshold
                raised = self.watch_hard_reset(node)
                acked = self.record_acks(node)
                loop = asyncio.get_running_loop()
                initial = [user(f"u-{index}", f"u-{index}-v1") for index in range(4)]
                latest = {item.email: list(item.inbounds) for item in initial}
                written = {email: [inbounds] for email, inbounds in latest.items()}
                await self.start(node, initial)
                stop = self.arrivals(node, "fresh", 0.2, latest)

                async def u1_held():
                    return "u-1" in self.held(node)

                await self.until(u1_held, 10)
                latest["u-1"] = ["u-1-v2"]
                written["u-1"].append(["u-1-v2"])
                await node.update_user(service.User(email="u-1", inbounds=["u-1-v2"]))
                await self.until_hard_reset(node, 30)
                await asyncio.sleep(0.5)
                await stop()
                for email, inbounds in latest.items():
                    written.setdefault(email, [inbounds])
                recovered = loop.time()
                core.reject_all = False
                await self.until(lambda: self.settled(node, core, latest, []), 90)
                self.assertFalse(node.requires_hard_reset())
                self.assertEqual([at for at in raised if at >= recovered], [])
                self.assertEqual(len(raised), 1, raised)
                self.assert_never_went_back(core, written)
                self.assert_no_batch_repeats_a_user(core)
                self.assert_nothing_lost(node, core, acked, latest)

    async def test_a_reconnect_restarts_the_systemic_bound(self):
        lease, threshold = 1.0, 2
        for make in (self.grpc_node, self.rest_node):
            with self.subTest(transport=make.__name__):
                core = FakeNodeCore(delay=0.05)
                core.reject_all = True
                node = await make(core, sync_lease_seconds=lease)
                node._hard_reset_threshold = threshold
                raised = self.watch_hard_reset(node)
                loop = asyncio.get_running_loop()
                await self.start(node, [user(f"seed-{index}") for index in range(2)])
                stop = self.arrivals(node, "fresh", 0.15, {})
                await self.until_hard_reset(node, 30)
                await node.connect("0.2.0", "1.0.0")
                reconnected = loop.time()
                self.assertFalse(node.requires_hard_reset())
                await self.until_hard_reset(node, 30)
                await stop()
                self.assertEqual(len(raised), 2, raised)
                self.assertGreaterEqual(raised[1] - reconnected, backoff_before_threshold(threshold))
