import asyncio
import random
import time
import unittest

try:
    import uvloop
except ImportError:
    uvloop = None

from PasarGuardNodeBridge.common.service_pb2 import Empty, User
from PasarGuardNodeBridge.controller import FailedUsers, NodeAPIError
from PasarGuardNodeBridge.storage import InMemoryUserSyncStore
from tests.test_user_sync import RecordingNode, eventually


class LeaseOnlyStore(InMemoryUserSyncStore):
    hold_users = None
    enqueue_users_if_absent = None


class RefreshStore(LeaseOnlyStore):
    def __init__(self):
        super().__init__()
        self.source = {}

    async def enqueue_users(self, node_id, users):
        for user in users:
            self.source[user.email] = list(user.inbounds)
        await super().enqueue_users(node_id, users)

    async def request_refresh(self, node_id, emails):
        await super().enqueue_users(
            node_id, [User(email=email, inbounds=self.source.get(email, [])) for email in emails]
        )


STORES = {"held": InMemoryUserSyncStore, "refresh": RefreshStore, "lease": LeaseOnlyStore}
KEEPER_WAKE = 7.0
HELD_DELAY = 1.0


def never_back(payloads):
    for email in {email for email, _ in payloads}:
        versions = [inbounds for other, inbounds in payloads if other == email]
        if any(version != versions[-1] for version in versions[versions.index(versions[-1]) :]):
            return False
    return True


class Fleet:
    def __init__(self, store, nodes, retry_delays=(30.0, 60.0, 300.0), lease=30.0):
        self.store = store
        self.nodes = nodes
        self.retry_delays = retry_delays
        self.lease = lease
        self.on_node = {}
        self.sends = []
        self.refuse = set()
        self.snapshots = []

    def worker(self, name):
        node = RecordingNode(user_sync_store=self.store, worker_id=name, sync_lease_seconds=self.lease)
        node._sync_retry_delays = self.retry_delays
        self.nodes.append(node)

        async def send(users, **kwargs):
            batch = [(user.email, list(user.inbounds)) for user in users]
            self.sends.extend((name, email, inbounds) for email, inbounds in batch)
            if any(item in self.refuse for item in ((email, tuple(inbounds)) for email, inbounds in batch)):
                return FailedUsers(users, rejected=True, detail="proxy is not a UserManager")
            for email, inbounds in batch:
                self.apply(email, inbounds)
            return []

        async def snapshot(users, **kwargs):
            self.snapshots.append((name, [(user.email, list(user.inbounds)) for user in users]))
            self.on_node = {}
            for user in users:
                self.apply(user.email, list(user.inbounds))
            return Empty()

        node.sync_users_chunked = send
        node.sync_users = snapshot
        return node

    def apply(self, email, inbounds):
        if inbounds:
            self.on_node[email] = inbounds
        else:
            self.on_node.pop(email, None)

    def payloads(self):
        return [(email, inbounds) for _, email, inbounds in self.sends]


class SnapshotSupersedesHeldRetryTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.nodes = []

    async def asyncTearDown(self):
        for node in self.nodes:
            await node.disconnect()
            await node._json_client.close()

    async def reject_on_first_worker(self, kind, inbounds=("v1",)):
        fleet = Fleet(STORES[kind](), self.nodes, retry_delays=(HELD_DELAY, 60.0, 300.0))
        fleet.refuse.add(("x", tuple(inbounds)))
        held = fleet.worker("A")
        await held.update_user(User(email="x", inbounds=list(inbounds)))
        await held.connect("0.2.0", "1.0.0")
        await eventually(lambda: "x" in held._sync_retries and held._sync_retries["x"].attempts == 1)
        fleet.refuse.clear()
        return fleet, held

    async def make_due(self, node):
        await asyncio.sleep(HELD_DELAY + 0.3)
        await node.wake_sync_worker()

    async def settle(self, store):
        async with asyncio.timeout(4):
            while await store.has_pending("node"):
                await asyncio.sleep(0.005)
        await asyncio.sleep(0.2)

    async def test_a_full_sync_on_another_worker_supersedes_a_held_retry(self):
        for kind in STORES:
            with self.subTest(store=kind):
                fleet, held = await self.reject_on_first_worker(kind)
                other = fleet.worker("B")

                async def removed():
                    if kind == "refresh":
                        fleet.store.source["x"] = []
                    return [User(email="x")]

                async with asyncio.timeout(KEEPER_WAKE):
                    await other.sync_users_from_source(removed)
                self.assertEqual(fleet.on_node, {})

                await self.make_due(held)
                await eventually(
                    lambda: "x" not in held._sync_retries or held._sync_retries["x"].queued_at,
                    timeout=9,
                )
                await self.settle(fleet.store)

                self.assertEqual(fleet.on_node, {}, fleet.sends)
                self.assertEqual(fleet.payloads().count(("x", ["v1"])), 1, fleet.sends)
                self.assertEqual(await fleet.store.capture_queued("node"), ({}, 0))
                await self.asyncTearDown()
                self.nodes.clear()

    async def test_a_failed_or_cancelled_full_sync_keeps_the_held_retry(self):
        for kind in STORES:
            for outcome in ("failed", "cancelled"):
                with self.subTest(store=kind, outcome=outcome):
                    fleet, held = await self.reject_on_first_worker(kind)
                    other = fleet.worker("B")
                    loading = asyncio.Event()

                    async def broken():
                        loading.set()
                        if outcome == "failed":
                            raise RuntimeError("database is gone")
                        await asyncio.sleep(10)

                    task = asyncio.create_task(other.sync_users_from_source(broken))
                    await eventually(loading.is_set, timeout=KEEPER_WAKE)
                    if outcome == "cancelled":
                        task.cancel()
                    with self.assertRaises((RuntimeError, asyncio.CancelledError)):
                        await task
                    self.assertEqual(fleet.snapshots, [])

                    await self.make_due(held)
                    await eventually(lambda: fleet.on_node.get("x") == ["v1"])
                    await self.settle(fleet.store)

                    self.assertEqual(fleet.on_node, {"x": ["v1"]}, fleet.sends)
                    self.assertEqual(fleet.payloads(), [("x", ["v1"]), ("x", ["v1"])], fleet.sends)
                    self.assertEqual(held._sync_retries, {})
                    await self.asyncTearDown()
                    self.nodes.clear()

    async def test_a_full_sync_does_not_wait_out_the_lease_of_a_kept_retry(self):
        for kind in STORES:
            with self.subTest(store=kind):
                fleet, _ = await self.reject_on_first_worker(kind)
                other = fleet.worker("B")

                async def current():
                    return [User(email="x", inbounds=["v1"])]

                async with asyncio.timeout(KEEPER_WAKE):
                    await other.sync_users_from_source(current)
                await self.settle(fleet.store)
                self.assertEqual(fleet.on_node, {"x": ["v1"]}, fleet.sends)
                await self.asyncTearDown()
                self.nodes.clear()


class StoreKindRetryTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.nodes = []

    async def asyncTearDown(self):
        for node in self.nodes:
            await node.disconnect()
            await node._json_client.close()
        self.nodes = []

    async def test_a_rejected_removal_survives_the_death_of_its_worker(self):
        for kind in ("held", "lease"):
            for inbounds in ([], ["v1"]):
                with self.subTest(store=kind, inbounds=inbounds):
                    fleet = Fleet(STORES[kind](), self.nodes, retry_delays=(0.3, 60.0, 300.0), lease=1.0)
                    fleet.on_node = {"x": ["v0"]}
                    fleet.refuse.add(("x", tuple(inbounds)))
                    dying = fleet.worker("A")
                    await dying.update_user(User(email="x", inbounds=inbounds))
                    await dying.connect("0.2.0", "1.0.0")
                    await eventually(lambda: "x" in dying._sync_retries)
                    await asyncio.sleep(0.05)
                    await dying.disconnect()
                    dying._sync_retries.clear()
                    fleet.refuse.clear()

                    survivor = fleet.worker("B")
                    await survivor.connect("0.2.0", "1.0.0")
                    expected = {"x": inbounds} if inbounds else {}
                    async with asyncio.timeout(5):
                        while fleet.on_node != expected:
                            await survivor.wake_sync_worker()
                            await asyncio.sleep(0.02)
                    self.assertEqual(fleet.payloads(), [("x", inbounds), ("x", inbounds)], fleet.sends)
                    await self.asyncTearDown()

    async def test_a_due_retry_never_overwrites_a_newer_change_from_another_worker(self):
        for kind in STORES:
            with self.subTest(store=kind):
                fleet = Fleet(STORES[kind](), self.nodes, retry_delays=(0.3, 60.0, 300.0), lease=1.0)
                fleet.refuse.add(("x", ("v1",)))
                held = fleet.worker("A")
                await held.update_user(User(email="x", inbounds=["v1"]))
                await held.connect("0.2.0", "1.0.0")
                await eventually(lambda: "x" in held._sync_retries)
                other = fleet.worker("B")
                await other.update_user(User(email="x", inbounds=["v2"]))
                await other.connect("0.2.0", "1.0.0")
                async with asyncio.timeout(5):
                    while fleet.on_node.get("x") != ["v2"]:
                        await held.wake_sync_worker()
                        await other.wake_sync_worker()
                        await asyncio.sleep(0.02)
                await asyncio.sleep(1.5)
                for node in (held, other):
                    await node.wake_sync_worker()
                await asyncio.sleep(0.3)

                self.assertEqual(fleet.on_node, {"x": ["v2"]}, fleet.sends)
                self.assertEqual(fleet.payloads()[0], ("x", ["v1"]), fleet.sends)
                self.assertEqual(fleet.payloads().count(("x", ["v1"])), 1, fleet.sends)
                await self.asyncTearDown()

    async def test_a_newer_local_change_is_not_held_behind_a_rejected_send(self):
        for kind in STORES:
            for in_flight in (False, True):
                with self.subTest(store=kind, in_flight=in_flight):
                    fleet = Fleet(STORES[kind](), self.nodes)
                    fleet.refuse.add(("x", ("v1",)))
                    node = fleet.worker("A")
                    send = node.sync_users_chunked
                    sending, release = asyncio.Event(), asyncio.Event()

                    async def slow(users, **kwargs):
                        if in_flight and any(user.inbounds == ["v1"] for user in users):
                            sending.set()
                            await release.wait()
                        return await send(users, **kwargs)

                    node.sync_users_chunked = slow
                    await node.update_user(User(email="x", inbounds=["v1"]))
                    await node.connect("0.2.0", "1.0.0")
                    if in_flight:
                        await eventually(sending.is_set)
                    else:
                        await eventually(lambda: "x" in node._sync_retries)
                    await node.update_user(User(email="x", inbounds=["v2"]))
                    release.set()
                    async with asyncio.timeout(2):
                        while fleet.on_node.get("x") != ["v2"]:
                            await asyncio.sleep(0.01)
                    self.assertEqual(fleet.payloads(), [("x", ["v1"]), ("x", ["v2"])], fleet.sends)
                    await eventually(lambda: "x" not in node._sync_retries)
                    await self.asyncTearDown()

    async def test_a_kept_retry_does_not_poll_the_store(self):
        for kind in STORES:
            with self.subTest(store=kind):
                fleet = Fleet(STORES[kind](), self.nodes)
                fleet.refuse.add(("x", ("v1",)))
                node = fleet.worker("A")
                node._sync_poll_interval = 0.01
                claims = []
                claim = fleet.store.claim_users

                async def counting(*args, **kwargs):
                    claims.append(args)
                    return await claim(*args, **kwargs)

                fleet.store.claim_users = counting
                await node.update_user(User(email="x", inbounds=["v1"]))
                await node.connect("0.2.0", "1.0.0")
                await eventually(lambda: "x" in node._sync_retries)
                await asyncio.sleep(0.05)
                before = len(claims)
                await asyncio.sleep(1.0)
                self.assertLessEqual(len(claims) - before, 1, kind)
                await self.asyncTearDown()

    async def test_a_full_sync_releases_a_kept_claim_for_a_newer_change_from_another_worker(self):
        fleet = Fleet(LeaseOnlyStore(), self.nodes)
        fleet.refuse.add(("x", ("v1",)))
        held = fleet.worker("A")
        await held.update_user(User(email="x", inbounds=["v1"]))
        await held.connect("0.2.0", "1.0.0")
        await eventually(lambda: "x" in held._sync_retries)
        other = fleet.worker("B")
        await other.update_user(User(email="x", inbounds=["v2"]))
        await other.connect("0.2.0", "1.0.0")
        await asyncio.sleep(0.1)
        self.assertEqual(fleet.on_node, {})

        await held.sync_users([], flush_pending=False)
        await held._clear_rejected_users()
        async with asyncio.timeout(2):
            while fleet.on_node.get("x") != ["v2"]:
                await other.wake_sync_worker()
                await asyncio.sleep(0.01)
        self.assertEqual(held._sync_retries, {})


class LateHoldStore(InMemoryUserSyncStore):
    async def hold_users(self, node_id, tokens, retry_after):
        await super().hold_users(node_id, tokens, retry_after + 0.05)


class GatedHoldStore(InMemoryUserSyncStore):
    def __init__(self):
        super().__init__()
        self.gate = None
        self.entered = asyncio.Event()

    async def hold_users(self, node_id, tokens, retry_after):
        if self.gate is not None:
            self.entered.set()
            await self.gate.wait()
        await super().hold_users(node_id, tokens, retry_after)


class GatedEnqueueLeaseStore(LeaseOnlyStore):
    def __init__(self):
        super().__init__()
        self.gate = None
        self.entered = asyncio.Event()

    async def enqueue_users(self, node_id, users):
        gate, self.gate = self.gate, None
        if gate is not None:
            self.entered.set()
            await gate.wait()
        await super().enqueue_users(node_id, users)


async def close_all(nodes):
    for node in nodes:
        await node.disconnect()
        await node._json_client.close()
    nodes.clear()


class HeldRetryDueTimeTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.nodes = []

    async def asyncTearDown(self):
        await close_all(self.nodes)

    async def test_a_hold_that_ends_after_the_controller_due_time_is_still_claimed(self):
        fleet = Fleet(LateHoldStore(), self.nodes, retry_delays=(0.1, 0.2, 0.2))
        fleet.refuse.add(("x", ("v1",)))
        node = fleet.worker("A")
        await node.update_user(User(email="x", inbounds=["v1"]))
        await node.connect("0.2.0", "1.0.0")
        await eventually(lambda: "x" in node._sync_retries)
        fleet.refuse.clear()
        await eventually(lambda: fleet.on_node.get("x") == ["v1"], timeout=4)
        self.assertEqual(fleet.payloads(), [("x", ["v1"]), ("x", ["v1"])], fleet.sends)


@unittest.skipUnless(uvloop is not None, "uvloop is not installed")
class UvloopHeldRetryTests(unittest.TestCase):
    async def rejected_many_times(self):
        nodes = []
        fleet = Fleet(InMemoryUserSyncStore(), nodes, retry_delays=(0.02, 0.05, 0.05))
        node = fleet.worker("A")
        send = node.sync_users_chunked
        attempts = []

        async def flaky(users, **kwargs):
            attempts.append(len(users))
            if len(attempts) <= 20:
                return FailedUsers(users, rejected=True, detail="core restarting")
            return await send(users, **kwargs)

        node.sync_users_chunked = flaky
        try:
            await node.update_user(User(email="x", inbounds=["v1"]))
            await node.connect("0.2.0", "1.0.0")
            await eventually(lambda: fleet.on_node.get("x") == ["v1"], timeout=8)
        finally:
            await close_all(nodes)
        return len(attempts)

    def test_repeated_rejections_under_uvloop_end_in_a_delivery(self):
        for run in range(3):
            with self.subTest(run=run):
                with asyncio.Runner(loop_factory=uvloop.new_event_loop) as runner:
                    self.assertEqual(runner.run(self.rejected_many_times()), 21)


class DirectSnapshotRetiresRetriesTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.nodes = []

    async def asyncTearDown(self):
        await close_all(self.nodes)

    async def direct_snapshot(self, node, users):
        await node.sync_users(users, flush_pending=False)
        await node._clear_rejected_users()

    async def unrelated_update_settles(self, fleet, node):
        await node.update_user(User(email="y", inbounds=["y1"]))
        await eventually(lambda: fleet.on_node.get("y") == ["y1"])
        await asyncio.sleep(0.3)

    async def test_a_direct_snapshot_retires_the_retries_it_supersedes(self):
        for kind in STORES:
            with self.subTest(store=kind):
                fleet = Fleet(STORES[kind](), self.nodes, retry_delays=(0.3, 60.0, 300.0))
                fleet.refuse.add(("x", ("v1",)))
                node = fleet.worker("A")
                await node.update_user(User(email="x", inbounds=["v1"]))
                await node.connect("0.2.0", "1.0.0")
                await eventually(lambda: "x" in node._sync_retries)
                await asyncio.sleep(0.05)
                fleet.refuse.clear()
                await self.direct_snapshot(node, [User(email="x", inbounds=["v2"])])
                await asyncio.sleep(0.6)
                await node.wake_sync_worker()
                await self.unrelated_update_settles(fleet, node)

                self.assertEqual(fleet.on_node, {"x": ["v2"], "y": ["y1"]}, fleet.sends)
                self.assertEqual(fleet.payloads().count(("x", ["v1"])), 1, fleet.sends)
                self.assertEqual(await fleet.store.capture_queued("node"), ({}, 0))
                await close_all(self.nodes)

    async def test_a_direct_snapshot_during_the_hold_call_retires_the_retry(self):
        store = GatedHoldStore()
        fleet = Fleet(store, self.nodes, retry_delays=(0.3, 60.0, 300.0))
        fleet.refuse.add(("x", ("v1",)))
        node = fleet.worker("A")
        store.gate = asyncio.Event()
        await node.update_user(User(email="x", inbounds=["v1"]))
        await node.connect("0.2.0", "1.0.0")
        await eventually(store.entered.is_set)
        fleet.refuse.clear()
        await self.direct_snapshot(node, [User(email="x", inbounds=["v2"])])
        store.gate.set()
        store.gate = None
        await asyncio.sleep(0.6)
        await node.wake_sync_worker()
        await self.unrelated_update_settles(fleet, node)

        self.assertEqual(fleet.on_node, {"x": ["v2"], "y": ["y1"]}, fleet.sends)
        self.assertEqual(fleet.payloads().count(("x", ["v1"])), 1, fleet.sends)
        self.assertEqual(await store.capture_queued("node"), ({}, 0))


class LeaseModeRaceTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.nodes = []

    async def asyncTearDown(self):
        await close_all(self.nodes)

    async def test_a_round_started_during_a_slow_enqueue_does_not_hold_back_the_newer_change(self):
        for trigger in ("connect", "wake"):
            with self.subTest(trigger=trigger):
                store = GatedEnqueueLeaseStore()
                fleet = Fleet(store, self.nodes)
                fleet.refuse.add(("x", ("v1",)))
                node = fleet.worker("A")
                if trigger == "connect":
                    await node.update_user(User(email="x", inbounds=["v1"]))
                else:
                    await node.connect("0.2.0", "1.0.0")
                    await store.enqueue_users("node", [User(email="x", inbounds=["v1"])])
                store.gate = gate = asyncio.Event()
                update = asyncio.create_task(node.update_user(User(email="x", inbounds=["v2"])))
                await eventually(store.entered.is_set)
                if trigger == "connect":
                    await node.connect("0.2.0", "1.0.0")
                else:
                    await node.wake_sync_worker()
                await eventually(lambda: ("x", ["v1"]) in fleet.payloads())
                await asyncio.sleep(0.05)
                gate.set()
                await update
                await eventually(lambda: fleet.on_node.get("x") == ["v2"], timeout=3)
                await close_all(self.nodes)

    async def test_a_fence_gets_every_kept_claim_back_in_one_wake(self):
        store = LeaseOnlyStore()
        refuse = {f"u{i}" for i in range(12)}
        on_node = {}

        def worker(name, batch):
            node = RecordingNode(user_sync_store=store, worker_id=name, sync_lease_seconds=60.0, sync_batch_size=batch)
            node._sync_poll_interval = 1.0
            node._sync_retry_delays = (30.0, 60.0, 300.0)
            self.nodes.append(node)

            async def send(users, **kwargs):
                if any(user.email in refuse for user in users):
                    return FailedUsers(users, rejected=True, detail="proxy is not a UserManager")
                for user in users:
                    on_node[user.email] = list(user.inbounds)
                return []

            async def snapshot(users, **kwargs):
                on_node.clear()
                for user in users:
                    on_node[user.email] = list(user.inbounds)
                return Empty()

            node.sync_users_chunked = send
            node.sync_users = snapshot
            return node

        keeper = worker("A", 2)
        await keeper.update_users([User(email=email, inbounds=["v1"]) for email in sorted(refuse)])
        await keeper.connect("0.2.0", "1.0.0")
        await eventually(
            lambda: len([e for e in keeper._sync_retries.values() if e.queued_at is None and e.attempts]) == 12,
            timeout=10,
        )
        other = worker("B", 100)
        refuse.clear()

        async def load():
            return [User(email=f"u{i}", inbounds=["v1"]) for i in range(12)]

        started = time.monotonic()
        await other.sync_users_from_source(load)
        self.assertLessEqual(time.monotonic() - started, 6.5)

    async def test_a_fence_returns_kept_claims_a_batch_per_store_call_within_the_internal_timeout(self):
        calls = []

        class PerClaimCostStore(LeaseOnlyStore):
            async def requeue_users(self, node_id, claimed_users):
                calls.append(len(claimed_users))
                await asyncio.sleep(0.1 * len(claimed_users))
                await super().requeue_users(node_id, claimed_users)

        store = PerClaimCostStore()
        refuse = {f"u{i:02d}" for i in range(12)}

        def worker(name):
            node = RecordingNode(
                user_sync_store=store, worker_id=name, sync_lease_seconds=60.0, sync_batch_size=4, internal_timeout=1
            )
            node._sync_poll_interval = 0.2
            node._sync_retry_delays = (30.0, 60.0, 300.0)
            self.nodes.append(node)

            async def send(users, **kwargs):
                if any(user.email in refuse for user in users):
                    return FailedUsers(users, rejected=True, detail="proxy is not a UserManager")
                return []

            async def snapshot(users, **kwargs):
                return Empty()

            node.sync_users_chunked = send
            node.sync_users = snapshot
            return node

        keeper = worker("A")
        await keeper.update_users([User(email=email, inbounds=["v1"]) for email in sorted(refuse)])
        await keeper.connect("0.2.0", "1.0.0")
        await eventually(
            lambda: len([e for e in keeper._sync_retries.values() if e.queued_at is None and e.attempts]) == 12,
            timeout=10,
        )
        failures = keeper._user_sync_failure_count
        other = worker("B")
        refuse.clear()

        async def load():
            return [User(email=f"u{i:02d}", inbounds=["v1"]) for i in range(12)]

        async with asyncio.timeout(10):
            await other.sync_users_from_source(load)
        self.assertEqual(sum(calls), 12)
        self.assertLessEqual(max(calls), 4)
        self.assertEqual(keeper._user_sync_failure_count, failures)


class FailingRetireStores:
    class Held(InMemoryUserSyncStore):
        def __init__(self):
            super().__init__()
            self.fail = 0
            self.gate = None
            self.entered = asyncio.Event()

        async def retire_held(self, node_id, tokens):
            if self.gate is not None:
                self.entered.set()
                await self.gate.wait()
            if self.fail:
                self.fail -= 1
                raise ConnectionError("store briefly unavailable")
            await super().retire_held(node_id, tokens)

    class Lease(LeaseOnlyStore):
        def __init__(self):
            super().__init__()
            self.fail = 0

        async def ack_users(self, node_id, tokens):
            if self.fail:
                self.fail -= 1
                raise ConnectionError("store briefly unavailable")
            return await super().ack_users(node_id, tokens)


class SnapshotRetireFailureTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.nodes = []

    async def asyncTearDown(self):
        await close_all(self.nodes)

    async def test_a_snapshot_whose_retire_fails_or_is_cancelled_never_lets_the_old_payload_back(self):
        for case in ("held_fails", "held_cancelled", "lease_fails"):
            with self.subTest(case=case):
                lease = case == "lease_fails"
                store = FailingRetireStores.Lease() if lease else FailingRetireStores.Held()
                fleet = Fleet(store, self.nodes, retry_delays=(0.3, 60.0, 300.0), lease=1.5 if lease else 60.0)
                fleet.refuse.add(("x", ("v1",)))
                node = fleet.worker("A")
                await node.update_user(User(email="x", inbounds=["v1"]))
                await node.connect("0.2.0", "1.0.0")
                await eventually(lambda: "x" in node._sync_retries)
                await asyncio.sleep(0.05)
                fleet.refuse.clear()
                await node.sync_users([User(email="x", inbounds=["v2"])], flush_pending=False)
                if case == "held_cancelled":
                    store.gate = asyncio.Event()
                    clearing = asyncio.create_task(node._clear_rejected_users())
                    await eventually(store.entered.is_set)
                    clearing.cancel()
                    with self.assertRaises(asyncio.CancelledError):
                        await clearing
                    store.gate = None
                else:
                    store.fail = 1
                    await node._clear_rejected_users()
                await asyncio.sleep(2.0 if lease else 0.6)
                await node.update_user(User(email="y", inbounds=["y1"]))
                await eventually(lambda: fleet.on_node.get("y") == ["y1"], timeout=4)
                await asyncio.sleep(0.3)
                self.assertEqual(fleet.on_node.get("x"), ["v2"], fleet.sends)
                await close_all(self.nodes)


class HoldCallsPerRoundTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.nodes = []

    async def asyncTearDown(self):
        await close_all(self.nodes)

    async def test_the_users_one_round_rejects_are_held_in_one_store_call(self):
        calls = []

        class PerCallCostStore(InMemoryUserSyncStore):
            async def hold_users(self, node_id, tokens, retry_after):
                calls.append(len(tokens))
                await asyncio.sleep(0.2)
                await super().hold_users(node_id, tokens, retry_after)

        store = PerCallCostStore()
        node = RecordingNode(
            user_sync_store=store, worker_id="A", sync_lease_seconds=30.0, sync_batch_size=8, internal_timeout=1
        )
        node._sync_retry_delays = (30.0, 60.0, 300.0)
        self.nodes.append(node)
        sends = []

        async def send(users, **kwargs):
            sends.append([user.email for user in users])
            return FailedUsers(users, rejected=True, detail="proxy is not a UserManager")

        node.sync_users_chunked = send
        await node.update_users([User(email=f"u{i}", inbounds=["v1"]) for i in range(8)])
        await node.connect("0.2.0", "1.0.0")
        await eventually(lambda: len(calls) >= 1, timeout=5)
        await asyncio.sleep(1.5)
        self.assertEqual(calls, [8])
        self.assertLessEqual(len(sends), 15, sends)
        self.assertEqual(node._user_sync_failure_count, 0)


def slow_enqueue_reply(base):
    class SlowEnqueueReplyStore(base):
        def __init__(self):
            super().__init__()
            self.gate = None
            self.committed = asyncio.Event()

        async def enqueue_users(self, node_id, users):
            await super().enqueue_users(node_id, users)
            gate, self.gate = self.gate, None
            if gate is not None:
                self.committed.set()
                await gate.wait()

        async def fence_state(self, node_id):
            await asyncio.sleep(0.01)
            return await super().fence_state(node_id)

    return SlowEnqueueReplyStore


class EnqueueReplyRaceTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.nodes = []

    async def asyncTearDown(self):
        await close_all(self.nodes)

    async def test_a_rejection_of_the_new_revision_during_a_slow_enqueue_reply_is_retried(self):
        for kind in STORES:
            for resume in ("after_send", "after_hold"):
                if kind != "held" and resume == "after_hold":
                    continue
                with self.subTest(store=kind, resume=resume):
                    store = slow_enqueue_reply(STORES[kind])()
                    fleet = Fleet(store, self.nodes, retry_delays=(0.3, 0.6, 0.6))
                    node = fleet.worker("A")
                    send = node.sync_users_chunked
                    gate = asyncio.Event()

                    async def rejecting(users, **kwargs):
                        result = await send(users, **kwargs)
                        if result and resume == "after_send":
                            gate.set()
                        return result

                    node.sync_users_chunked = rejecting
                    if resume == "after_hold":
                        hold = store.hold_users

                        async def holding(node_id, tokens, retry_after):
                            await hold(node_id, tokens, retry_after)
                            gate.set()

                        store.hold_users = holding
                    await node.connect("0.2.0", "1.0.0")
                    fleet.refuse.add(("x", ("v2",)))
                    store.gate = gate
                    update = asyncio.create_task(node.update_user(User(email="x", inbounds=["v2"])))
                    await eventually(store.committed.is_set)
                    await node.wake_sync_worker()
                    await update
                    fleet.refuse.clear()
                    await eventually(lambda: fleet.on_node.get("x") == ["v2"], timeout=4)
                    await close_all(self.nodes)


class SharedStoreSnapshotHammerTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.nodes = []

    async def asyncTearDown(self):
        for node in self.nodes:
            await node.disconnect()
            await node._json_client.close()
        self.nodes = []

    async def run_hammer(self, kind, seed):
        rng = random.Random(seed)
        store = STORES[kind]()
        emails = ["a", "b", "c", "d"]
        latest, history, refusals, on_node = {}, {email: [] for email in emails}, {}, {}
        source = asyncio.Lock()

        def worker(name):
            node = RecordingNode(user_sync_store=store, worker_id=name, sync_lease_seconds=0.6)
            node._sync_retry_delays = (0.01, 0.04, 0.08)
            node._sync_retry_window = 1.0
            self.nodes.append(node)

            async def send(users, **kwargs):
                await asyncio.sleep(rng.choice((0, 0, 0.002, 0.01)))
                payloads = [(user.email, int(user.inbounds[0][1:])) for user in users]
                if any(refusals.get(payload, 0) for payload in payloads):
                    for payload in payloads:
                        if refusals.get(payload, 0):
                            refusals[payload] -= 1
                    return FailedUsers(users, rejected=True, detail="proxy is not a UserManager")
                for email, version in payloads:
                    on_node[email] = version
                    history[email].append(version)
                return []

            async def snapshot(users, **kwargs):
                for user in users:
                    version = int(user.inbounds[0][1:])
                    on_node[user.email] = version
                    history[user.email].append(version)
                return Empty()

            node.sync_users_chunked = send
            node.sync_users = snapshot
            return node

        workers = [worker(name) for name in ("A", "B", "C")]
        for node in workers:
            await node.connect("0.2.0", "1.0.0")

        async def load():
            async with source:
                return [User(email=email, inbounds=[f"v{version}"]) for email, version in latest.items()]

        snapshots = []
        for version in range(1, 61):
            email = rng.choice(emails)
            async with source:
                latest[email] = version
                refusals[(email, version)] = rng.choice((0, 0, 1, 1, 2))
                await rng.choice(workers).update_user(User(email=email, inbounds=[f"v{version}"]))
            if rng.random() < 0.1:
                snapshots.append(asyncio.create_task(rng.choice(workers).sync_users_from_source(load)))
            await asyncio.sleep(rng.choice((0, 0.001, 0.005, 0.02)))
        for task in snapshots:
            try:
                await task
            except NodeAPIError as exc:
                self.assertEqual(exc.code, 409)

        async def settled():
            while on_node != latest or await store.has_pending("node"):
                for node in workers:
                    await node.wake_sync_worker()
                await asyncio.sleep(0.01)

        async with asyncio.timeout(10):
            await settled()
        await asyncio.sleep(0.3)
        return latest, history, on_node

    async def test_several_workers_with_snapshots_never_move_a_user_back(self):
        for kind in STORES:
            for seed in range(8):
                with self.subTest(store=kind, seed=seed):
                    latest, history, on_node = await self.run_hammer(kind, seed)
                    self.assertEqual(on_node, latest, history)
                    for email, applied in history.items():
                        self.assertEqual(applied, sorted(applied), f"{email} went back: {applied}")
                    await self.asyncTearDown()


if __name__ == "__main__":
    unittest.main()
