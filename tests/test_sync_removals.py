import asyncio

from grpclib.const import Status

from PasarGuardNodeBridge.common import service_pb2 as service
from tests.sync_fakes import FakeNodeCore, SyncFakeTestCase, user


class FlakyRemovalCore(FakeNodeCore):
    def __init__(self, flaky, **kwargs):
        super().__init__(**kwargs)
        self.flaky = dict(flaky)

    async def apply(self, users):
        refused = [item.email for item in users if not item.inbounds and self.flaky.get(item.email, 0) > 0]
        if refused:
            self.batches.append([item.email for item in users])
            for email in refused:
                self.flaky[email] -= 1
            return Status.INTERNAL, f"failed to update users: cannot remove {refused[0]}"
        return await super().apply(users)


def removal(email):
    return service.User(email=email)


class RemovalTests(SyncFakeTestCase):
    async def activate(self, node, core, emails, timeout):
        await node._user_sync_store.enqueue_users(node.node_id, [user(email, f"{email}-v1") for email in emails])
        await node.connect("0.2.0", "1.0.0")

        async def active():
            return all(core.applied.get(email) == [f"{email}-v1"] for email in emails)

        await self.until(active, timeout)

    def assert_removed_for_good(self, core, emails):
        for email in emails:
            history = core.history.get(email, [])
            self.assertIn([], history, f"{email} never removed: {history}")
            after = history[history.index([]) :]
            self.assertEqual(after, [[]] * len(after), f"{email} came back after its removal: {history}")

    async def test_removals_queued_behind_a_rejected_user_reach_the_node(self):
        for lease, delay, bound in ((1.0, 0.02, 3.0), (2.0, 0.8, 10.0)):
            for make in (self.grpc_node, self.rest_node):
                with self.subTest(lease=lease, transport=make.__name__):
                    await self.removals_behind_a_rejected_user(make, lease, delay, bound)

    async def removals_behind_a_rejected_user(self, make, lease, delay, bound):
        core = FakeNodeCore(delay=delay)
        node = await make(core, sync_lease_seconds=lease)
        loop = asyncio.get_running_loop()
        active = [f"r-{index}" for index in range(1, 9)]
        await self.activate(node, core, active, 30)
        removed_at = {}
        events = {}

        async def remove(email):
            removed_at[email] = loop.time()
            await node.update_user(removal(email))

        async def hook(sent):
            emails = [item.email for item in sent]
            if "removal_while_rejected_send_in_flight" not in events and "bad-1" in emails and len(emails) > 1:
                events["removal_while_rejected_send_in_flight"] = emails
                await remove("r-3")
            if "removal_while_rejected_user_retried" not in events and emails == ["bad-1"]:
                if sum(1 for batch in core.batches if batch == ["bad-1"]) >= 2:
                    events["removal_while_rejected_user_retried"] = True
                    await remove("r-4")
                    await remove("bad-1")

        core.on_send = hook
        events["removal_in_the_claim_of_a_rejected_user"] = True
        for email in ("r-1", "r-2"):
            removed_at[email] = loop.time()
        await node.update_users([user("bad-1", "bad"), removal("r-1"), removal("r-2"), user("g-1", "g-1-v1")])
        removed = ["r-1", "r-2", "r-3", "r-4", "bad-1"]

        async def all_removed():
            return len(events) == 3 and all(core.applied.get(email) == [] for email in removed)

        await self.until(all_removed, 3 * bound)
        await node.disconnect()
        self.assertEqual(
            sorted(events),
            sorted(
                [
                    "removal_in_the_claim_of_a_rejected_user",
                    "removal_while_rejected_send_in_flight",
                    "removal_while_rejected_user_retried",
                ]
            ),
        )
        late = {
            email: round(core.delivered_at(email, []) - removed_at[email], 2)
            for email in removed
            if core.delivered_at(email, []) - removed_at[email] > bound
        }
        self.assertEqual(late, {})
        self.assert_removed_for_good(core, removed)
        self.assertEqual(core.applied.get("g-1"), ["g-1-v1"])
        self.assertEqual({email: core.applied[email] for email in active[4:]}, {e: [f"{e}-v1"] for e in active[4:]})
        self.assertNotIn(["bad"], [inbounds for history in core.history.values() for inbounds in history])
        self.assert_no_batch_repeats_a_user(core)

    async def test_a_removal_the_node_rejects_is_retried_until_it_lands_without_holding_back_others(self):
        for make in (self.grpc_node, self.rest_node):
            with self.subTest(transport=make.__name__):
                core = FlakyRemovalCore({"r-1": 3}, delay=0.02)
                node = await make(core, sync_lease_seconds=1.0)
                await self.activate(node, core, ["r-1"], 10)
                await node.update_users([removal("r-1"), user("g-1", "g-1-v1")])

                async def g1_landed():
                    return core.applied.get("g-1") == ["g-1-v1"]

                async def r1_removed():
                    return core.applied.get("r-1") == []

                await self.until(g1_landed, 3)
                await self.until(r1_removed, 30)
                await node.disconnect()
                self.assertGreaterEqual(core.sends_with("r-1"), 4)
                self.assert_removed_for_good(core, ["r-1"])
