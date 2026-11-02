import asyncio
import unittest

from PasarGuardNodeBridge.common.service_pb2 import User
from PasarGuardNodeBridge.controller import FailedUsers
from PasarGuardNodeBridge.storage import InMemoryUserSyncStore
from tests.test_user_sync import RecordingNode, eventually


class SharedStoreRetryRaceTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.nodes = []

    async def asyncTearDown(self):
        for node in self.nodes:
            await node.disconnect()
            await node._json_client.close()

    async def test_held_retry_racing_a_newer_change_on_another_worker_never_lands_stale(self):
        store = InMemoryUserSyncStore()
        on_node, sends = {}, []
        newer_in_flight, release = asyncio.Event(), asyncio.Event()

        def worker(name):
            node = RecordingNode(user_sync_store=store, worker_id=name)
            node._sync_retry_delays = (30.0, 60.0, 300.0)
            self.nodes.append(node)

            async def send(users, **kwargs):
                batch = [(user.email, list(user.inbounds)) for user in users]
                sends.extend((name, email, inbounds) for email, inbounds in batch)
                if ("x", ["v1"]) in batch and len(sends) == 1:
                    return FailedUsers(users, rejected=True, detail="proxy is not a UserManager")
                if ("x", ["v2"]) in batch and not release.is_set():
                    newer_in_flight.set()
                    await release.wait()
                for email, inbounds in batch:
                    on_node[email] = inbounds
                return []

            node.sync_users_chunked = send
            return node

        held, first, second = worker("A"), worker("B"), worker("C")
        await held.update_user(User(email="x", inbounds=["v1"]))
        await held.connect("0.2.0", "1.0.0")
        await eventually(lambda: "x" in held._sync_retries)

        await first.update_user(User(email="x", inbounds=["v2"]))
        await first.connect("0.2.0", "1.0.0")
        await second.connect("0.2.0", "1.0.0")
        await eventually(newer_in_flight.is_set)

        held._sync_retries["x"].next_attempt = 0
        await held.wake_sync_worker()
        await eventually(lambda: "x" not in held._sync_retries or held._sync_retries["x"].queued_at is not None)
        await asyncio.sleep(0.05)
        release.set()

        async def settled():
            while await store.has_pending("node"):
                await asyncio.sleep(0.005)

        async with asyncio.timeout(4):
            await settled()
        await asyncio.sleep(0.2)

        self.assertEqual(on_node, {"x": ["v2"]}, sends)
        last_newer = max(index for index, (_, _, inbounds) in enumerate(sends) if inbounds == ["v2"])
        self.assertNotIn(["v1"], [inbounds for _, _, inbounds in sends[last_newer:]], sends)
        self.assertEqual([inbounds for _, _, inbounds in sends], [["v1"], ["v2"]], sends)
        self.assertFalse(await store.has_pending("node"))

    def stale_race_workers(self, store, reject_newer):
        on_node, sends = {}, []
        rejected_once = set()

        def worker(name):
            node = RecordingNode(user_sync_store=store, worker_id=name)
            node._sync_retry_delays = (30.0, 60.0, 300.0)
            self.nodes.append(node)

            async def send(users, **kwargs):
                batch = [(user.email, list(user.inbounds)) for user in users]
                sends.extend((name, email, inbounds) for email, inbounds in batch)
                if ("x", ["v1"]) in batch and "v1" not in rejected_once:
                    rejected_once.add("v1")
                    return FailedUsers(users, rejected=True, detail="proxy is not a UserManager")
                if ("x", ["v2"]) in batch and reject_newer:
                    return FailedUsers(users, rejected=True, detail="proxy is not a UserManager")
                for email, inbounds in batch:
                    on_node[email] = inbounds
                return []

            node.sync_users_chunked = send
            return node

        return worker, on_node, sends

    async def run_held_retry_after_newer_change(self, reject_newer):
        store = InMemoryUserSyncStore()
        worker, on_node, sends = self.stale_race_workers(store, reject_newer)
        held, other = worker("A"), worker("B")
        await held.update_user(User(email="x", inbounds=["v1"]))
        await held.connect("0.2.0", "1.0.0")
        await eventually(lambda: "x" in held._sync_retries and held._sync_retries["x"].attempts == 1)

        await other.update_user(User(email="x", inbounds=["v2"]))
        await other.connect("0.2.0", "1.0.0")
        if reject_newer:
            await eventually(lambda: "x" in other._sync_retries and other._sync_retries["x"].attempts == 1)
        else:
            await eventually(lambda: on_node.get("x") == ["v2"])
        async with asyncio.timeout(4):
            while await store.has_pending("node"):
                await asyncio.sleep(0.005)

        held._sync_retries["x"].next_attempt = 0
        await held.wake_sync_worker()
        await eventually(lambda: "x" not in held._sync_retries or held._sync_retries["x"].queued_at is not None)
        async with asyncio.timeout(4):
            while await store.has_pending("node"):
                await asyncio.sleep(0.005)
        await asyncio.sleep(0.2)
        return on_node, sends

    async def test_held_retry_due_after_another_worker_delivered_a_newer_change_never_lands_stale(self):
        on_node, sends = await self.run_held_retry_after_newer_change(reject_newer=False)

        self.assertEqual(on_node, {"x": ["v2"]}, sends)
        self.assertEqual([inbounds for _, _, inbounds in sends], [["v1"], ["v2"]], sends)

    async def test_held_retry_due_while_a_newer_change_waits_in_another_backoff_never_lands_stale(self):
        on_node, sends = await self.run_held_retry_after_newer_change(reject_newer=True)

        self.assertEqual(on_node, {}, sends)
        self.assertEqual([inbounds for _, _, inbounds in sends], [["v1"], ["v2"]], sends)
