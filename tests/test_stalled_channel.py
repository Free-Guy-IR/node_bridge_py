import asyncio
import logging
import random
import tempfile
import time
import unittest
from collections import Counter
from dataclasses import dataclass
from unittest.mock import patch
from uuid import uuid4

from grpclib.client import Channel
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
INTERLEAVING_ROUNDS = 12
SWAPS = ("none", "tls", "connect", "both")

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


@dataclass
class CallRecord:
    started_on_stalled: bool
    tls_reconfigured_during_call: bool
    succeeded: bool
    error: str


class CountingHandler(logging.Handler):
    def __init__(self):
        super().__init__(logging.WARNING)
        self.recycles = 0

    def emit(self, record):
        if "the next calls use a new connection" in record.getMessage():
            self.recycles += 1


class StalledChannelInterleavingTests(StalledNodeCase):
    async def _round(self, round_index: int, connects: Counter) -> tuple[list[CallRecord], int, str]:
        rng = random.Random(round_index)
        swaps = SWAPS[round_index % len(SWAPS)]
        counter = CountingHandler()
        round_logger = logging.getLogger(f"stalled-channel-test.round-{round_index}")
        round_logger.setLevel(logging.WARNING)
        round_logger.propagate = False
        round_logger.addHandler(counter)
        self.service.held_arrived = asyncio.Event()
        node = await self.connected_node(
            f"interleave-{round_index}", timeout=1, pings=rng.random() < 0.5, node_logger=round_logger
        )
        stalled = node.channel
        state = {"tls": 0}
        records: list[CallRecord] = []

        async def call(kind: str, timeout: float, delay: float):
            await asyncio.sleep(delay)
            started_on = node.channel
            tls_before = state["tls"]
            error = ""
            try:
                if kind == "held":
                    await node.get_system_stats(timeout=timeout)
                else:
                    await node.get_backend_stats(timeout=timeout)
                succeeded = True
            except NodeAPIError as exc:
                succeeded, error = False, exc.detail
            records.append(CallRecord(started_on is stalled, state["tls"] != tls_before, succeeded, error))

        async def reconfigure(delay: float):
            await asyncio.sleep(delay)
            state["tls"] += 1
            await node.configure_tls(server_ca=self.server_ca)

        async def reconnect(delay: float, pings: bool):
            await asyncio.sleep(delay)
            self.service.advertises_pings = pings
            await node.connect(NODE_VERSION, CORE)

        tasks = [asyncio.create_task(call("held", 2, 0)) for _ in range(rng.randint(1, 3))]
        await asyncio.wait_for(self.service.held_arrived.wait(), 2)
        self.relay.stall()
        tasks += [asyncio.create_task(call("unary", rng.uniform(0.2, 0.6), 0)) for _ in range(rng.randint(2, 5))]
        tasks += [asyncio.create_task(call("unary", 2, rng.uniform(0, 1.0))) for _ in range(rng.randint(3, 8))]
        if swaps in ("tls", "both"):
            tasks.append(asyncio.create_task(reconfigure(rng.uniform(0.1, 0.8))))
        if swaps in ("connect", "both"):
            tasks.append(asyncio.create_task(reconnect(rng.uniform(0.1, 0.8), rng.random() < 0.5)))
        await asyncio.wait_for(asyncio.gather(*tasks), 15)
        after = await asyncio.wait_for(
            asyncio.gather(*(node.get_backend_stats(timeout=2) for _ in range(3)), return_exceptions=True), 10
        )
        self.assertEqual(
            [type(result).__name__ for result in after], ["BackendStatsResponse"] * 3, f"round {round_index}"
        )
        self.assertEqual(await self.relay.settle_to(1), 1, f"round {round_index}: connections left open")
        reconnected = [channel for channel, count in connects.items() if count > 1]
        self.assertEqual(reconnected, [], f"round {round_index}: a channel opened a second connection")
        left_open = [channel for channel in connects if channel is not node.channel and channel._protocol is not None]
        self.assertEqual(left_open, [], f"round {round_index}: a replaced channel kept its connection")
        self.assertIsNotNone(node.channel._protocol)
        self.assertLessEqual(counter.recycles, 1, f"round {round_index}")
        node._close_chan()
        self.assertEqual(await self.relay.settle_to(0), 0, f"round {round_index}: connections left after teardown")
        connects.clear()
        round_logger.removeHandler(counter)
        return records, counter.recycles, swaps

    async def test_recycles_racing_calls_tls_reconfiguration_and_the_keepalive_swap(self):
        connects: Counter = Counter()
        original = Channel._create_connection

        async def counted(channel):
            protocol = await original(channel)
            connects[channel] += 1
            return protocol

        recycled_with = Counter()
        totals = Counter()
        with (
            patch.object(Channel, "_create_connection", counted),
            patch.object(grpc_transport, "SILENT_CONNECTION_PING_TIMEOUT", 0.3, create=True),
        ):
            for round_index in range(INTERLEAVING_ROUNDS):
                records, recycles, swaps = await self._round(round_index, connects)
                recycled_with[swaps] += recycles
                for record in records:
                    totals["calls"] += 1
                    totals["failed_on_stalled"] += record.started_on_stalled and not record.succeeded
                    totals["answered_after_swap"] += not record.started_on_stalled and record.succeeded
                    allowed = record.started_on_stalled or record.tls_reconfigured_during_call
                    self.assertTrue(
                        record.succeeded or allowed, f"round {round_index}: a call on a healthy channel failed {record}"
                    )
        self.assertEqual(sorted(recycled_with), sorted(SWAPS))
        self.assertGreaterEqual(recycled_with["none"], INTERLEAVING_ROUNDS // len(SWAPS))
        self.assertGreater(recycled_with["tls"] + recycled_with["both"], 0)
        self.assertGreater(recycled_with["connect"] + recycled_with["both"], 0)
        self.assertGreater(totals["failed_on_stalled"], INTERLEAVING_ROUNDS)
        self.assertGreater(totals["answered_after_swap"], INTERLEAVING_ROUNDS)


if __name__ == "__main__":
    unittest.main()
