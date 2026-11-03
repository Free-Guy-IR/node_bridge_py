import asyncio
import math

from PasarGuardNodeBridge.common import service_pb2 as service
from tests.sync_fakes import FakeNodeCore, SyncFakeTestCase, is_bad, round_bound, single_reject_send_bound, user


class RoundBudgetTests(SyncFakeTestCase):
    def counted(self, failures, raised):
        def reason():
            if failures or raised:
                return f"failure counted {failures} or HARD RESET raised {raised}"

        return reason

    async def start(self, node, users):
        await node._user_sync_store.enqueue_users(node.node_id, users)
        await node.connect("0.2.0", "1.0.0")

    async def test_a_slow_node_delivers_every_healthy_user_without_restarting_the_split(self):
        for make in (self.grpc_node, self.rest_node):
            with self.subTest(transport=make.__name__):
                core = FakeNodeCore(delay=0.8)
                node = await make(core, sync_lease_seconds=2.0)
                failures = self.count_failures(node)
                raised = self.watch_hard_reset(node)
                users = [user(f"user-{index}", "bad" if index == 0 else "good") for index in range(24)]
                latest = {item.email: list(item.inbounds) for item in users}
                counted = self.counted(failures, raised)

                def restarted_or_counted():
                    whole = sum(1 for batch in core.batches if len(batch) == len(users))
                    if whole > 1:
                        return f"split restarted: send sizes {[len(batch) for batch in core.batches]}"
                    return counted()

                await self.start(node, users)
                await self.until(lambda: self.settled(node, core, latest, ["user-0"]), 60, restarted_or_counted)
                isolating = [batch for batch in core.batches if batch != ["user-0"]]
                self.assertLessEqual(len(isolating) + 1, single_reject_send_bound(len(users)), core.batches)
                self.assertEqual((failures, raised), ([], []))

    async def test_r2_f3_a_slow_node_that_fits_one_send_per_claim_must_isolate_without_a_hard_reset(self):
        lease = 2.0
        delay = 0.8
        for make in (self.grpc_node, self.rest_node):
            with self.subTest(transport=make.__name__):
                core = FakeNodeCore(delay=delay)
                node = await make(core, sync_lease_seconds=lease)
                failures = self.count_failures(node)
                raised = self.watch_hard_reset(node)
                users = [user(f"user-{index}", "bad" if index == 0 else "good") for index in range(24)]
                latest = {item.email: list(item.inbounds) for item in users}
                await self.start(node, users)
                await self.until(lambda: self.settled(node, core, latest, ["user-0"]), 60)
                self.assertEqual(raised, [], f"failure counts {failures}")
                self.assertLess(max(failures, default=0), node._hard_reset_threshold)

    async def test_a_send_that_overruns_its_estimate_is_carried_without_counting(self):
        for make in (self.grpc_node, self.rest_node):
            with self.subTest(transport=make.__name__):
                core = FakeNodeCore(delay=0.2, slow_sends={2: 5.0})
                node = await make(core, sync_lease_seconds=2.0)
                failures = self.count_failures(node)
                raised = self.watch_hard_reset(node)
                users = [user(f"user-{index}", "bad" if index == 3 else "good") for index in range(16)]
                latest = {item.email: list(item.inbounds) for item in users}
                await self.start(node, users)
                await self.until(
                    lambda: self.settled(node, core, latest, ["user-3"]), 30, self.counted(failures, raised)
                )
                self.assertEqual(set(core.batches[2]), set(core.batches[1]), core.batches)
                self.assertEqual(sum(1 for batch in core.batches if len(batch) == len(users)), 1, core.batches)
                self.assertEqual((failures, raised), ([], []))

    async def test_a_first_send_that_overruns_the_round_is_still_a_counted_failure(self):
        for make in (self.grpc_node, self.rest_node):
            with self.subTest(transport=make.__name__):
                core = FakeNodeCore(delay=0.05, slow_sends={1: 5.0})
                node = await make(core, sync_lease_seconds=2.0)
                failures = self.count_failures(node)
                users = [user(f"user-{index}") for index in range(4)]
                latest = {item.email: list(item.inbounds) for item in users}
                await self.start(node, users)
                await self.until(lambda: self.settled(node, core, latest, []), 30)
                self.assertEqual(failures, [1])
                self.assertEqual(core.batches[1], core.batches[0])

    async def test_a_round_without_a_deadline_isolates_in_one_call(self):
        for make in (self.grpc_node, self.rest_node):
            with self.subTest(transport=make.__name__):
                core = FakeNodeCore(delay=0.05)
                node = await make(core)
                node._sync_round_end = math.inf
                users = [user(f"user-{index}", "bad" if index == 5 else "good") for index in range(8)]
                delivery = await node._sync_isolating_rejections(users)
                self.assertEqual([item.email for item, *_ in delivery.isolated], ["user-5"])
                self.assertEqual((list(delivery), list(delivery.deferred)), ([], []))
                self.assertEqual(sorted(core.applied), sorted(f"user-{index}" for index in range(8) if index != 5))

    async def test_rounds_that_only_carry_users_neither_count_nor_reset(self):
        for make in (self.grpc_node, self.rest_node):
            with self.subTest(transport=make.__name__):
                core = FakeNodeCore(delay=0.8)
                node = await make(core, sync_lease_seconds=2.0)
                failures = self.count_failures(node)
                resets = self.record_resets(node)
                resolved_at = []

                async def first_resolution(sent):
                    if not resolved_at and (len(sent) == 1 or not any(is_bad(item) for item in sent)):
                        resolved_at.append(asyncio.get_running_loop().time())

                core.on_send = first_resolution
                users = [user(f"user-{index}", "bad" if index == 0 else "good") for index in range(24)]
                latest = {item.email: list(item.inbounds) for item in users}
                await self.start(node, users)
                node._user_sync_failure_count = 2
                await self.until(
                    lambda: self.settled(node, core, latest, ["user-0"]), 60, self.counted(failures, [])
                )
                self.assertEqual(failures, [])
                self.assertTrue(resolved_at)
                self.assertEqual([at for at, _ in resets if at < resolved_at[0]], [])
                self.assertEqual(resets[0][1], 2)

    async def test_b1_pushes_during_a_slow_split_are_delivered_by_the_next_round_and_nothing_is_lost(self):
        for make in (self.grpc_node, self.rest_node):
            with self.subTest(transport=make.__name__):
                core = FakeNodeCore(delay=0.6)
                node = await make(core, sync_lease_seconds=4.0)
                node._internal_timeout = 1.0
                failures = self.count_failures(node)
                raised = self.watch_hard_reset(node)
                acked = self.record_acks(node)
                rounds = self.record_rounds(node)
                loop = asyncio.get_running_loop()
                events = {}
                latest = {}
                written = {}
                pushed = {}

                def record(email, inbounds):
                    latest[email] = list(inbounds)
                    written.setdefault(email, []).append(list(inbounds))

                async def push(email, inbounds):
                    record(email, inbounds)
                    pushed[email] = loop.time()
                    await node.update_user(service.User(email=email, inbounds=inbounds))

                async def hook(sent):
                    emails = [item.email for item in sent]
                    carried = self.carried(node)
                    if "split_send_in_flight" not in events and 1 < len(emails) < 32 and any(map(is_bad, sent)):
                        events["split_send_in_flight"] = emails
                        await push("p-2", ["p-2-v1"])
                    if "update_of_a_user_in_flight" not in events and "u-5" in emails and len(emails) < 32:
                        events["update_of_a_user_in_flight"] = emails
                        await push("u-5", ["u-5-v2"])
                    if "new_user_while_split_carried" not in events and carried:
                        events["new_user_while_split_carried"] = sorted(carried)
                        await push("p-1", ["p-1-v1"])
                    if "newer_version_of_a_carried_user" not in events and "u-20" in carried and "u-20" not in emails:
                        events["newer_version_of_a_carried_user"] = True
                        await push("u-20", ["u-20-v2"])
                    if "push_during_the_lone_retry" not in events and emails == ["u-13"] and "u-13" in self.held(node):
                        events["push_during_the_lone_retry"] = True
                        await push("p-3", ["p-3-v1"])

                core.on_send = hook
                initial = [
                    user(f"u-{index}", "bad") if index == 13 else user(f"u-{index}", f"u-{index}-v1")
                    for index in range(32)
                ]
                for item in initial:
                    record(item.email, item.inbounds)
                counted = self.counted(failures, raised)

                async def done():
                    return len(events) == 5 and await self.settled(node, core, latest, ["u-13"])

                await self.start(node, initial)
                await self.until(done, 45, counted)

                self.assertEqual(
                    sorted(events),
                    sorted(
                        [
                            "split_send_in_flight",
                            "update_of_a_user_in_flight",
                            "new_user_while_split_carried",
                            "newer_version_of_a_carried_user",
                            "push_during_the_lone_retry",
                        ]
                    ),
                )
                for email in ("p-1", "p-2", "p-3"):
                    at = core.delivered_at(email, latest[email])
                    self.assertLessEqual(at, round_bound(rounds, pushed[email], 1), f"{email} late: {rounds}")
                good = {email: inbounds for email, inbounds in latest.items() if email != "u-13"}
                self.assertEqual({email: core.applied.get(email) for email in good}, good)
                self.assertNotIn(["bad"], [inbounds for history in core.history.values() for inbounds in history])
                depth = math.ceil(math.log2(len(initial))) + 3
                looping = {email: core.sends_with(email) for email in good if core.sends_with(email) > depth}
                self.assertEqual(looping, {})
                self.assert_never_went_back(core, written)
                self.assert_no_batch_repeats_a_user(core)
                self.assert_nothing_lost(node, core, acked, latest)
                self.assertEqual((failures, raised), ([], []))
                self.assertEqual(self.held(node), {"u-13"})

    async def test_pushes_every_round_cannot_starve_a_split_on_a_node_that_fits_one_send(self):
        for make in (self.grpc_node, self.rest_node):
            with self.subTest(transport=make.__name__):
                core = FakeNodeCore(delay=1.1)
                node = await make(core, sync_lease_seconds=3.0)
                failures = self.count_failures(node)
                raised = self.watch_hard_reset(node)
                loop = asyncio.get_running_loop()
                latest = {}
                pushed = {}
                deliver = node._deliver_claimed_users

                async def pushing(claimed, node=node, deliver=deliver, latest=latest, pushed=pushed):
                    if len(pushed) < 12:
                        email = f"f-{len(pushed)}"
                        latest[email] = [f"{email}-v1"]
                        pushed[email] = loop.time()
                        await node.update_user(service.User(email=email, inbounds=latest[email]))
                    return await deliver(claimed)

                node._deliver_claimed_users = pushing
                rounds = self.record_rounds(node)
                initial = [user(f"u-{index}", "bad" if index == 5 else "good") for index in range(8)]
                latest.update({item.email: list(item.inbounds) for item in initial})

                async def done():
                    return len(pushed) == 12 and await self.settled(node, core, latest, ["u-5"])

                threshold = node._hard_reset_threshold

                def starved():
                    if raised or max(failures, default=0) >= threshold - 1:
                        return f"split starved: failures counted {failures}, HARD RESET raised {raised}"

                await self.start(node, initial)
                await self.until(done, 90, starved)
                late = {
                    email: (core.delivered_at(email, latest[email]), round_bound(rounds, at, 2))
                    for email, at in pushed.items()
                    if core.delivered_at(email, latest[email]) > round_bound(rounds, at, 2)
                }
                self.assertEqual(late, {})
                self.assertEqual(raised, [])
                self.assertLess(max(failures, default=0), threshold - 1, failures)
                self.assertEqual(self.held(node), {"u-5"})
