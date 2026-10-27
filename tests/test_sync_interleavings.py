import asyncio

from PasarGuardNodeBridge.common import service_pb2 as service
from tests.sync_fakes import POLL_INTERVAL, FakeNodeCore, SyncFakeTestCase, is_bad

STALL_LEASE = 2.0


class InterleavingTests(SyncFakeTestCase):
    async def test_d2_forced_interleavings_lose_nothing_and_never_swallow_a_valid_user(self):
        for make in (self.grpc_node, self.rest_node):
            with self.subTest(transport=make.__name__):
                await self.forced_interleavings(make)

    async def forced_interleavings(self, make):
        core = FakeNodeCore()
        node = await make(core, sync_lease_seconds=STALL_LEASE)
        node._sync_retry_delays = (30.0, 60.0, 300.0)
        acked = self.record_acks(node)
        raised = self.watch_hard_reset(node)
        loop = asyncio.get_running_loop()
        events = {}
        latest = {}
        written = {}

        def record(email, inbounds):
            latest[email] = list(inbounds)
            written.setdefault(email, []).append(list(inbounds))

        async def write(email, inbounds):
            record(email, inbounds)
            await node.update_user(service.User(email=email, inbounds=inbounds))

        async def hook(users):
            emails = [item.email for item in users]
            if "reconnect_mid_bisection" not in events and 1 < len(emails) < 10 and any(map(is_bad, users)):
                events["reconnect_mid_bisection"] = len(emails)
                await node.connect("0.2.0", "1.0.0")
            if "valid_in_flight" not in events and "s3" in emails:
                events["valid_in_flight"] = emails
                await write("s3", ["s3-valid"])
            if "push_during_retry" not in events and emails == ["s4"] and "s4" in self.held(node):
                events["push_during_retry"] = True
                await write("n1", ["n1-v1"])
                await write("g1", ["g1-v2"])

        def retry_at(email):
            return node._sync_retries[email].next_attempt

        async def applied(email, inbounds):
            async with asyncio.timeout(STALL_LEASE):
                while core.applied.get(email) != inbounds:
                    await asyncio.sleep(POLL_INTERVAL)

        core.on_send = hook
        initial = [service.User(email=f"s{index}", inbounds=["bad"]) for index in range(1, 5)]
        initial += [service.User(email=f"g{index}", inbounds=[f"g{index}-v1"]) for index in range(1, 7)]
        for item in initial:
            record(item.email, item.inbounds)
        await node._user_sync_store.enqueue_users(node.node_id, initial)
        await node.connect("0.2.0", "1.0.0")
        async with asyncio.timeout(20):
            while self.held(node) != {"s1", "s2", "s4"} or any(
                core.applied.get(email) != inbounds for email, inbounds in latest.items() if inbounds != ["bad"]
            ):
                await asyncio.sleep(POLL_INTERVAL)

        self.assertGreater(retry_at("s1"), loop.time())
        started = loop.time()
        await write("s1", ["s1-fixed"])
        await applied("s1", ["s1-fixed"])
        events["update_in_backoff"] = loop.time() - started

        self.assertGreater(retry_at("s2"), loop.time())
        await write("s2", [])
        await applied("s2", [])
        events["removal_of_held_user"] = True

        self.assertGreater(retry_at("s4"), loop.time())
        before = core.sends_with("s4")
        await node.connect("0.2.0", "1.0.0")
        await write("n0", ["n0-v1"])
        await applied("n0", ["n0-v1"])
        await asyncio.sleep(STALL_LEASE / 2)
        events["hard_reset_reconnect_in_backoff"] = core.sends_with("s4") - before
        self.assertEqual(events["hard_reset_reconnect_in_backoff"], 0)

        node._sync_retries["s4"].next_attempt = loop.time()
        await node.wake_sync_worker()
        async with asyncio.timeout(4 * STALL_LEASE):
            while (
                "push_during_retry" not in events
                or core.applied.get("n1") != ["n1-v1"]
                or core.applied.get("g1") != ["g1-v2"]
                or await self.pending(node) | set(node._sync_retries) != {"s4"}
            ):
                await asyncio.sleep(POLL_INTERVAL)

        self.assertEqual(
            sorted(events),
            sorted(
                [
                    "reconnect_mid_bisection",
                    "valid_in_flight",
                    "update_in_backoff",
                    "removal_of_held_user",
                    "hard_reset_reconnect_in_backoff",
                    "push_during_retry",
                ]
            ),
        )
        good = {email: inbounds for email, inbounds in latest.items() if inbounds != ["bad"]}
        self.assertEqual({email: core.applied.get(email) for email in good}, good)
        self.assertEqual(self.held(node), {"s4"})
        self.assertNotIn(["bad"], [inbounds for history in core.history.values() for inbounds in history])
        self.assert_never_went_back(core, written)
        self.assert_no_batch_repeats_a_user(core)
        self.assert_nothing_lost(node, core, acked, latest)
        self.assertEqual(raised, [])
        self.assertFalse(node.requires_hard_reset())
