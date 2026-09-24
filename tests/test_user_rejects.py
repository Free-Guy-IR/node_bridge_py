import asyncio
import logging
import random
import unittest
from unittest.mock import patch

from aiohttp import web
from grpclib.client import Channel
from grpclib.const import Cardinality, Handler, Status
from grpclib.exceptions import GRPCError
from grpclib.server import Server

from PasarGuardNodeBridge.aiohttp_compat import LazyClientSession, make_timeout
from PasarGuardNodeBridge.common import service_grpc
from PasarGuardNodeBridge.common import service_pb2 as service
from PasarGuardNodeBridge.grpclib import Node as GrpcNode
from PasarGuardNodeBridge.rest import Node as RestNode
from PasarGuardNodeBridge.storage import InMemoryUserSyncStore


def user(email, inbound="good"):
    return service.User(email=email, inbounds=[inbound])


def is_bad(item):
    return "bad" in item.inbounds


class FakeNodeCore:
    def __init__(self, mode="reject", delay=0.0):
        self.mode = mode
        self.delay = delay
        self.applied = {}
        self.batches = []
        self.on_reject = None

    async def apply(self, users):
        self.batches.append([item.email for item in users])
        if self.delay:
            await asyncio.sleep(self.delay)
        if self.mode == "unavailable":
            return Status.UNAVAILABLE, "connection to the core was lost"
        if self.mode == "systemic":
            return Status.INTERNAL, "failed to update users: core api unreachable"
        bad = [item.email for item in users if is_bad(item)]
        if bad:
            if self.on_reject is not None:
                await self.on_reject(bad)
            return Status.INTERNAL, f"failed to update users: invalid account {bad[0]}"
        for item in users:
            self.applied[item.email] = list(item.inbounds)
        return None, ""

    async def apply_one_by_one(self, users):
        self.batches.append([item.email for item in users])
        for item in users:
            if is_bad(item):
                if self.on_reject is not None:
                    await self.on_reject([item.email])
                return Status.INTERNAL, f"failed to update user: invalid account {item.email}"
            self.applied[item.email] = list(item.inbounds)
        return None, ""


class GrpcFakeNode:
    def __init__(self, core):
        self.core = core

    async def chunked(self, stream):
        users = [item for chunk in [chunk async for chunk in stream] for item in chunk.users]
        status, message = await self.core.apply(users)
        if status is not None:
            raise GRPCError(status, message)
        await stream.send_message(service.Empty())

    async def per_user(self, stream):
        users = [item async for item in stream]
        status, message = await self.core.apply_one_by_one(users)
        if status is not None:
            raise GRPCError(status, message)
        await stream.send_message(service.Empty())

    def __mapping__(self):
        return {
            "/service.NodeService/SyncUsersChunked": Handler(
                self.chunked, Cardinality.STREAM_UNARY, service.UsersChunk, service.Empty
            ),
            "/service.NodeService/SyncUser": Handler(
                self.per_user, Cardinality.STREAM_UNARY, service.User, service.Empty
            ),
        }


def decode_chunks(body):
    chunks, offset = [], 0
    while offset < len(body):
        length = shift = 0
        while True:
            byte = body[offset]
            offset += 1
            length |= (byte & 127) << shift
            if byte < 128:
                break
            shift += 7
        chunks.append(service.UsersChunk.FromString(body[offset : offset + length]))
        offset += length
    return chunks


class RejectIsolationTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.cleanups = []

    async def asyncTearDown(self):
        for cleanup in reversed(self.cleanups):
            await cleanup()

    def make_node(self, node_class, port, **kwargs):
        options = {"sync_poll_interval": 0.01, "sync_chunk_size": 100}
        options.update(kwargs)
        with patch("ssl.SSLContext.load_verify_locations"):
            return node_class(
                address="127.0.0.1",
                port=port,
                api_port=1,
                server_ca="test",
                api_key="00000000-0000-0000-0000-000000000000",
                node_id="rejects",
                user_sync_store=InMemoryUserSyncStore(),
                logger=logging.getLogger("rejects-test"),
                **options,
            )

    async def grpc_node(self, core, **kwargs):
        server = Server([GrpcFakeNode(core)])
        await server.start("127.0.0.1", 0)
        port = server._server.sockets[0].getsockname()[1]
        node = self.make_node(GrpcNode, port, **kwargs)
        node.channel.close()
        node.channel = Channel("127.0.0.1", port, ssl=False)
        node._client = service_grpc.NodeServiceStub(node.channel)

        async def cleanup():
            await node.disconnect()
            node.channel.close()
            await node._json_client.close()
            server.close()
            await server.wait_closed()

        self.cleanups.append(cleanup)
        return node

    async def rest_node(self, core, **kwargs):
        async def chunked(request):
            users = [item for chunk in decode_chunks(await request.read()) for item in chunk.users]
            status, message = await core.apply(users)
            if status is Status.UNAVAILABLE:
                return web.Response(status=503, text=message)
            if status is not None:
                return web.Response(status=500, text=message)
            return web.Response(body=service.Empty().SerializeToString())

        app = web.Application()
        app.router.add_put("/users/sync/chunked", chunked)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        port = site._server.sockets[0].getsockname()[1]
        node = self.make_node(RestNode, port, **kwargs)
        await node._client.close()
        node._client = LazyClientSession(
            ssl_context=None,
            headers={"Content-Type": "application/x-protobuf"},
            base_url=f"http://127.0.0.1:{port}/",
            timeout=make_timeout(None),
        )

        async def cleanup():
            await node.disconnect()
            await node._client.close()
            await node._json_client.close()
            await runner.cleanup()

        self.cleanups.append(cleanup)
        return node

    async def drain(self, node, users, version="0.2.0", timeout=10):
        await node._user_sync_store.enqueue_users(node.node_id, users)
        await node.connect(version, "1.0.0")
        await self.until_drained(node, timeout)

    async def until_drained(self, node, timeout=10):
        async with asyncio.timeout(timeout):
            while await node._user_sync_store.has_pending(node.node_id):
                await asyncio.sleep(0.01)

    def assert_isolated(self, node, core, users, rejected):
        good = {item.email: list(item.inbounds) for item in users if item.email not in rejected}
        self.assertEqual({email: core.applied[email] for email in good if email in core.applied}, good)
        self.assertTrue(set(rejected).isdisjoint(core.applied))
        self.assertEqual(sorted(item.email for item in node.quarantined_users()), sorted(rejected))
        self.assertEqual(node.rejected_users_total, len(rejected))
        self.assertEqual(node._user_sync_failure_count, 0)
        self.assertFalse(node.requires_hard_reset())

    async def test_grpc_one_rejected_user_is_quarantined_and_everyone_else_is_delivered(self):
        core = FakeNodeCore()
        node = await self.grpc_node(core)
        users = [user(f"user-{index}", "bad" if index == 17 else "good") for index in range(40)]
        await self.drain(node, users)
        self.assert_isolated(node, core, users, ["user-17"])
        quarantined = node.quarantined_users()[0]
        self.assertIn("invalid account user-17", quarantined.reason)

    async def test_grpc_two_rejected_users_in_different_halves_are_both_isolated(self):
        core = FakeNodeCore()
        node = await self.grpc_node(core)
        users = [user(f"user-{index}", "bad" if index in (3, 33) else "good") for index in range(40)]
        await self.drain(node, users)
        self.assert_isolated(node, core, users, ["user-3", "user-33"])

    async def test_grpc_legacy_per_user_stream_isolates_the_rejected_user(self):
        core = FakeNodeCore()
        node = await self.grpc_node(core)
        users = [user(f"user-{index}", "bad" if index == 5 else "good") for index in range(12)]
        await self.drain(node, users, version="0.1.9")
        self.assert_isolated(node, core, users, ["user-5"])

    async def test_rest_one_rejected_user_is_quarantined_and_everyone_else_is_delivered(self):
        core = FakeNodeCore()
        node = await self.rest_node(core)
        users = [user(f"user-{index}", "bad" if index == 29 else "good") for index in range(40)]
        await self.drain(node, users)
        self.assert_isolated(node, core, users, ["user-29"])

    async def test_a_systemic_rejection_quarantines_nobody_and_keeps_the_queue(self):
        for make in (self.grpc_node, self.rest_node):
            with self.subTest(transport=make.__name__):
                core = FakeNodeCore(mode="systemic")
                node = await make(core)
                users = [user(f"user-{index}") for index in range(8)]
                await node._user_sync_store.enqueue_users(node.node_id, users)
                await node.connect("0.2.0", "1.0.0")
                async with asyncio.timeout(10):
                    while node._user_sync_failure_count < 2:
                        await asyncio.sleep(0.01)
                await node.disconnect()
                self.assertEqual(node.quarantined_users(), [])
                self.assertEqual(node.rejected_users_total, 0)
                captured, _ = await node._user_sync_store.capture_queued(node.node_id)
                self.assertEqual(sorted(captured), sorted(item.email for item in users))

    async def test_a_transport_failure_is_retried_whole_and_never_bisected(self):
        for make in (self.grpc_node, self.rest_node):
            with self.subTest(transport=make.__name__):
                core = FakeNodeCore(mode="unavailable")
                node = await make(core)
                users = [user(f"user-{index}") for index in range(8)]
                await node._user_sync_store.enqueue_users(node.node_id, users)
                await node.connect("0.2.0", "1.0.0")
                async with asyncio.timeout(10):
                    while node._user_sync_failure_count < 2:
                        await asyncio.sleep(0.01)
                await node.disconnect()
                self.assertTrue(all(len(batch) == 8 for batch in core.batches))
                self.assertEqual(node.quarantined_users(), [])
                self.assertEqual(node.rejected_users_total, 0)

    async def test_a_fix_queued_while_the_user_is_isolated_is_delivered_and_lifts_the_quarantine(self):
        core = FakeNodeCore()
        node = await self.grpc_node(core)
        fixed = asyncio.Event()

        async def fix_on_first_reject(bad):
            if not fixed.is_set():
                fixed.set()
                await node.update_user(user("user-9", "fixed"))

        core.on_reject = fix_on_first_reject
        users = [user(f"user-{index}", "bad" if index == 9 else "good") for index in range(20)]
        await self.drain(node, users)
        async with asyncio.timeout(10):
            while core.applied.get("user-9") != ["fixed"]:
                await asyncio.sleep(0.01)
        await self.until_drained(node)
        self.assertEqual(core.applied["user-9"], ["fixed"])
        self.assertEqual(node.quarantined_users(), [])
        self.assertEqual(node._user_sync_failure_count, 0)

    async def test_isolation_that_outlasts_one_claim_lease_still_makes_progress(self):
        core = FakeNodeCore(delay=0.1)
        node = await self.grpc_node(core, sync_lease_seconds=1.0)
        users = [user(f"user-{index}", "bad" if index == 41 else "good") for index in range(64)]
        await self.drain(node, users, timeout=30)
        self.assert_isolated(node, core, users, ["user-41"])

    async def test_interleaved_writers_during_isolation_converge_to_the_latest_state(self):
        for round_number in range(25):
            rng = random.Random(round_number)
            core = FakeNodeCore(delay=rng.choice((0.0, 0.002)))
            node = await self.grpc_node(core, sync_batch_size=rng.choice((5, 16, 50)))
            latest = {}

            async def write(email, inbound, node=node, latest=latest):
                latest[email] = inbound
                await node.update_user(user(email, inbound))

            emails = [f"user-{index}" for index in range(30)]
            for email in emails:
                await write(email, "bad" if rng.random() < 0.1 else "v0")
            await node.connect("0.2.0", "1.0.0")

            async def writer(seed, write=write, emails=emails):
                local = random.Random(seed)
                for step in range(15):
                    email = local.choice(emails)
                    await write(email, "bad" if local.random() < 0.15 else f"v{seed}-{step}")
                    await asyncio.sleep(local.choice((0, 0.001, 0.003)))

            await asyncio.gather(*(writer(round_number * 10 + index) for index in range(4)))
            bad = {email for email, inbound in latest.items() if inbound == "bad"}
            async with asyncio.timeout(20):
                while not set((await node._user_sync_store.capture_queued(node.node_id))[0]) <= bad:
                    await asyncio.sleep(0.01)
            for index in range(12):
                await write(f"tail-{index}", "tail")
            await self.until_drained(node, timeout=20)
            expected_good = {email: [inbound] for email, inbound in latest.items() if inbound != "bad"}
            self.assertEqual(
                {email: core.applied.get(email) for email in expected_good}, expected_good, f"round {round_number}"
            )
            self.assertTrue(bad.isdisjoint(email for email, inbounds in core.applied.items() if inbounds == ["bad"]))
            self.assertEqual(sorted(item.email for item in node.quarantined_users()), sorted(bad), f"round {round_number}")
            self.assertFalse(node.requires_hard_reset())
            cleanup = self.cleanups.pop()
            await cleanup()

if __name__ == "__main__":
    unittest.main()
