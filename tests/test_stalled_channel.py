import asyncio
import logging
import tempfile
import time
import unittest
from unittest.mock import patch
from uuid import uuid4

from grpclib.config import Configuration
from grpclib.const import Cardinality, Handler
from grpclib.server import Server

import PasarGuardNodeBridge.grpclib as grpc_transport
from PasarGuardNodeBridge import NodeAPIError
from PasarGuardNodeBridge.common import service_pb2 as service
from PasarGuardNodeBridge.grpclib import Node as GrpcNode
from PasarGuardNodeBridge.storage import InMemoryNodeLifecycleCoordinator, InMemoryUserSyncStore
from tests.test_tls import certificate

PING_POLICY = "keepalive-policy-v1"
NODE_VERSION = "0.6.22"
CORE = "1.0"
SETTLE = 3.0
SHORT_PING_WINDOW = 0.5
SLOW_REPLY_BYTES = 256 * 1024
SLOW_LINK_BYTES_PER_SECOND = 64 * 1024
GRPC_GO_INITIAL_WINDOW = 65535

logger = logging.getLogger("stalled-channel-test")
logger.setLevel(logging.CRITICAL)


class NodeService:
    def __init__(self):
        self.advertises_pings = False
        self.release = asyncio.Event()
        self.held_arrived = asyncio.Event()
        self.synced_users = []

    async def base_info(self, stream):
        await stream.recv_message()
        if self.advertises_pings:
            await stream.send_initial_metadata(metadata={"pg-capabilities": PING_POLICY})
        await stream.send_message(service.BaseInfoResponse(started=True, node_version=NODE_VERSION, core_version=CORE))

    async def backend_stats(self, stream):
        await stream.recv_message()
        await stream.send_message(service.BackendStatsResponse())

    async def held(self, stream):
        await stream.recv_message()
        self.held_arrived.set()
        await self.release.wait()
        await stream.send_message(service.SystemStatsResponse())

    async def sized(self, stream):
        request = await stream.recv_message()
        await stream.send_message(service.StatResponse(stats=[service.Stat(name="x" * int(request.name), value=1)]))

    async def sync_users(self, stream):
        request = await stream.recv_message()
        self.synced_users.append([user.email for user in request.users])
        await stream.send_message(service.Empty())

    async def sync_users_chunked(self, stream):
        emails = [user.email async for chunk in stream for user in chunk.users]
        self.synced_users.append(emails)
        await stream.send_message(service.Empty())

    def __mapping__(self):
        return {
            "/service.NodeService/GetBaseInfo": Handler(
                self.base_info, Cardinality.UNARY_UNARY, service.Empty, service.BaseInfoResponse
            ),
            "/service.NodeService/GetBackendStats": Handler(
                self.backend_stats, Cardinality.UNARY_UNARY, service.Empty, service.BackendStatsResponse
            ),
            "/service.NodeService/GetSystemStats": Handler(
                self.held, Cardinality.UNARY_UNARY, service.Empty, service.SystemStatsResponse
            ),
            "/service.NodeService/GetStats": Handler(
                self.sized, Cardinality.UNARY_UNARY, service.StatRequest, service.StatResponse
            ),
            "/service.NodeService/SyncUsers": Handler(
                self.sync_users, Cardinality.UNARY_UNARY, service.Users, service.Empty
            ),
            "/service.NodeService/SyncUsersChunked": Handler(
                self.sync_users_chunked, Cardinality.STREAM_UNARY, service.UsersChunk, service.Empty
            ),
        }


class _Pair:
    def __init__(self, client_writer):
        self.stalled = False
        self.writers = [client_writer]


class StallingRelay:
    def __init__(self, upstream_port: int):
        self.upstream_port = upstream_port
        self.accepted = 0
        self.open = 0
        self.pairs: list[_Pair] = []
        self.down_bytes_per_second: float | None = None
        self.up_bytes_per_second: float | None = None
        self.server = None

    async def start(self) -> int:
        self.server = await asyncio.start_server(self._relay, "127.0.0.1", 0)
        return self.server.sockets[0].getsockname()[1]

    def stall(self):
        for pair in self.pairs:
            pair.stalled = True

    async def _pump(self, pair: _Pair, reader, writer, downstream: bool):
        try:
            while data := await reader.read(16384):
                rate = self.down_bytes_per_second if downstream else self.up_bytes_per_second
                if rate:
                    await asyncio.sleep(len(data) / rate)
                if pair.stalled:
                    continue
                writer.write(data)
                await writer.drain()
        except (ConnectionError, OSError):
            return
        finally:
            writer.close()

    async def _relay(self, client_reader, client_writer):
        self.accepted += 1
        self.open += 1
        pair = _Pair(client_writer)
        self.pairs.append(pair)
        try:
            upstream_reader, upstream_writer = await asyncio.open_connection("127.0.0.1", self.upstream_port)
            pair.writers.append(upstream_writer)
            await asyncio.gather(
                self._pump(pair, client_reader, upstream_writer, False),
                self._pump(pair, upstream_reader, client_writer, True),
            )
        finally:
            self.open -= 1

    async def settle_to(self, expected: int, within: float = SETTLE) -> int:
        deadline = asyncio.get_running_loop().time() + within
        while self.open != expected and asyncio.get_running_loop().time() < deadline:
            await asyncio.sleep(0.005)
        return self.open

    def close(self):
        for pair in self.pairs:
            for writer in pair.writers:
                writer.close()
        self.server.close()


def make_node(port: int, name: str, server_ca: str, timeout: int = 5, node_logger=logger) -> GrpcNode:
    return GrpcNode(
        address="127.0.0.1",
        port=port,
        api_port=port,
        server_ca=server_ca,
        api_key=str(uuid4()),
        name=name,
        node_id=f"{name}-{uuid4()}",
        user_sync_store=InMemoryUserSyncStore(),
        lifecycle_coordinator=InMemoryNodeLifecycleCoordinator(),
        logger=node_logger,
        default_timeout=timeout,
        internal_timeout=timeout,
    )


class StalledNodeCase(unittest.IsolatedAsyncioTestCase):
    server_window: int | None = None

    async def asyncSetUp(self):
        with tempfile.TemporaryDirectory() as directory:
            server_ssl, self.server_ca, _ = certificate(directory)
        self.service = NodeService()
        config = None
        if self.server_window is not None:
            config = Configuration(
                http2_connection_window_size=self.server_window, http2_stream_window_size=self.server_window
            )
        self.server = Server([self.service], config=config)
        await self.server.start("127.0.0.1", 0, ssl=server_ssl)
        self.relay = StallingRelay(self.server._server.sockets[0].getsockname()[1])
        self.port = await self.relay.start()
        self.nodes = []
        self.pending = []

    async def asyncTearDown(self):
        self.service.release.set()
        for task in self.pending:
            task.cancel()
        await asyncio.gather(*self.pending, return_exceptions=True)
        for node in self.nodes:
            node._close_chan()
            await node.disconnect()
            await node._json_client.close()
        self.relay.close()
        self.server.close()
        try:
            await asyncio.wait_for(self.server.wait_closed(), 2)
        except TimeoutError:
            pass

    async def connected_node(self, name: str, timeout: int = 5, pings: bool = False, node_logger=logger) -> GrpcNode:
        node = make_node(self.port, name, self.server_ca, timeout, node_logger)
        self.nodes.append(node)
        self.service.advertises_pings = pings
        await node.connect(NODE_VERSION, CORE)
        self.assertIsNotNone(await node.get_backend_stats(timeout=5))
        self.assertEqual(await self.relay.settle_to(1), 1)
        return node

    def keep(self, coroutine) -> asyncio.Task:
        task = asyncio.create_task(coroutine)
        self.pending.append(task)
        return task


class StalledConnectionTests(StalledNodeCase):
    async def test_a_node_whose_connection_stalls_answers_again_through_a_new_connection(self):
        node = await self.connected_node("stalled")
        self.relay.stall()
        outcomes = []
        for _ in range(4):
            try:
                await node.get_backend_stats(timeout=1)
                outcomes.append("answered")
            except NodeAPIError as error:
                outcomes.append(error.detail)
        self.assertEqual(outcomes, ["Request timed out", "answered", "answered", "answered"])
        self.assertEqual(self.relay.accepted, 2)
        self.assertEqual(await self.relay.settle_to(1), 1)

    async def test_a_chunked_sync_that_times_out_on_a_stalled_connection_replaces_it(self):
        node = await self.connected_node("stalled-chunked", timeout=1)
        self.relay.stall()
        failed = await node.sync_users_chunked([service.User(email="1")], timeout=1)
        self.assertEqual([user.email for user in failed], ["1"])
        self.assertFalse(failed.rejected)
        self.assertIsNotNone(await node.get_backend_stats(timeout=2))
        self.assertEqual(await node.sync_users_chunked([service.User(email="2")], timeout=2), [])
        self.assertEqual(self.service.synced_users, [["2"]])
        self.assertEqual(self.relay.accepted, 2)
        self.assertEqual(await self.relay.settle_to(1), 1)

    async def test_calls_left_on_a_stalled_connection_end_once_it_misses_a_ping(self):
        node = await self.connected_node("stalled-in-flight")
        with patch.object(grpc_transport, "SILENT_CONNECTION_PING_TIMEOUT", SHORT_PING_WINDOW, create=True):
            self.relay.stall()
            stuck = self.keep(node.sync_users([service.User(email="stuck")], timeout=30))
            await asyncio.sleep(0.2)
            self.assertTrue(node._node_lock.locked())
            with self.assertRaises(NodeAPIError):
                await node.get_backend_stats(timeout=1)
            recycled = time.monotonic()
            with self.assertRaises(NodeAPIError):
                await asyncio.wait_for(stuck, 10)
            self.assertLess(time.monotonic() - recycled, SHORT_PING_WINDOW + 1.5)
            self.assertFalse(node._node_lock.locked())
            await node.sync_users([service.User(email="after")], timeout=5)
        self.assertEqual(self.service.synced_users, [["after"]])
        self.assertEqual(self.relay.accepted, 2)
        self.assertEqual(await self.relay.settle_to(1), 1)


class ReplacementSafetyTests(StalledNodeCase):
    async def test_a_replaced_connection_that_answers_the_ping_keeps_its_slow_calls(self):
        node = await self.connected_node("slow-node", timeout=1)
        first = node.channel
        with patch.object(grpc_transport, "SILENT_CONNECTION_PING_TIMEOUT", SHORT_PING_WINDOW, create=True):
            held = self.keep(node.get_system_stats(timeout=30))
            await asyncio.wait_for(self.service.held_arrived.wait(), 2)
            with self.assertRaises(NodeAPIError):
                await node.get_system_stats(timeout=1)
            self.assertIsNot(node.channel, first)
            await asyncio.sleep(SHORT_PING_WINDOW + 2)
            self.assertFalse(held.done())
            self.assertEqual(self.relay.open, 1)
            self.service.release.set()
            self.assertIsInstance(await asyncio.wait_for(held, 5), service.SystemStatsResponse)
        self.assertEqual(await self.relay.settle_to(0), 0)
        self.assertIsNone(first._protocol)
        self.assertIsNotNone(await node.get_backend_stats(timeout=5))
        self.assertEqual(self.relay.accepted, 2)
        self.assertEqual(await self.relay.settle_to(1), 1)

    async def test_a_connection_still_delivering_a_slow_reply_is_not_replaced(self):
        node = await self.connected_node("slow-download")
        first = node.channel
        self.relay.down_bytes_per_second = SLOW_LINK_BYTES_PER_SECOND
        slow = self.keep(
            node.get_stats(service.StatType.UsersStat, reset=False, name=str(SLOW_REPLY_BYTES), timeout=30)
        )
        await asyncio.sleep(0.3)
        started = time.monotonic()
        with self.assertRaises(NodeAPIError):
            await node.get_system_stats(timeout=1)
        reply = await asyncio.wait_for(slow, 20)
        self.assertGreater(time.monotonic() - started, 1.5)
        self.assertEqual(len(reply.stats[0].name), SLOW_REPLY_BYTES)
        self.assertIs(node.channel, first)
        self.assertEqual(self.relay.accepted, 1)
        self.assertEqual(self.relay.open, 1)


class SlowUploadSafetyTests(StalledNodeCase):
    server_window = GRPC_GO_INITIAL_WINDOW

    async def test_a_connection_still_taking_a_slow_upload_is_not_replaced(self):
        node = await self.connected_node("slow-upload")
        first = node.channel
        users = [service.User(email=f"{index:06d}", inbounds=["i" * 480]) for index in range(512)]
        self.relay.up_bytes_per_second = SLOW_LINK_BYTES_PER_SECOND
        upload = self.keep(node.sync_users(users, timeout=30))
        await asyncio.sleep(0.3)
        started = time.monotonic()
        with self.assertRaises(NodeAPIError):
            await node.get_system_stats(timeout=1)
        await asyncio.wait_for(upload, 20)
        self.assertGreater(time.monotonic() - started, 1.5)
        self.assertEqual(len(self.service.synced_users[-1]), len(users))
        self.assertIs(node.channel, first)
        self.assertEqual(self.relay.accepted, 1)
        self.assertEqual(self.relay.open, 1)


if __name__ == "__main__":
    unittest.main()
