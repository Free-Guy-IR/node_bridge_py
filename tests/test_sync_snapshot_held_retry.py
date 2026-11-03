import asyncio
import random
import unittest

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
        fleet = Fleet(STORES[kind](), self.nodes)
        fleet.refuse.add(("x", tuple(inbounds)))
        held = fleet.worker("A")
        await held.update_user(User(email="x", inbounds=list(inbounds)))
        await held.connect("0.2.0", "1.0.0")
        await eventually(lambda: "x" in held._sync_retries and held._sync_retries["x"].attempts == 1)
        fleet.refuse.clear()
        return fleet, held

    async def make_due(self, node):
        node._sync_retries["x"].next_attempt = 0
        held = node._user_sync_store._held.get("node", {})
        if "x" in held:
            held["x"] = 0.0
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
                await eventually(lambda: "x" not in held._sync_retries or held._sync_retries["x"].queued_at)
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
