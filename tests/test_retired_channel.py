import asyncio
import logging
import random
import unittest
from dataclasses import dataclass
from unittest.mock import patch
from uuid import uuid4

from grpclib.const import Cardinality, Handler
from grpclib.server import Server

from PasarGuardNodeBridge import NodeAPIError
from PasarGuardNodeBridge.common import service_grpc
from PasarGuardNodeBridge.common import service_pb2 as service
from PasarGuardNodeBridge.grpclib import Node as GrpcNode
from PasarGuardNodeBridge.storage import InMemoryNodeLifecycleCoordinator, InMemoryUserSyncStore

PING_POLICY = "keepalive-policy-v1"
NODE_VERSION = "0.6.22"
CORE = "1.0"
SETTLE = 2.0
INTERLEAVING_ROUNDS = 60
TEARDOWN_SETTLE = 8.0
HELD_CALL_TIMEOUT = 20

logger = logging.getLogger("retired-channel-test")
logger.setLevel(logging.CRITICAL)


class FakeNodeService:
    def __init__(self):
        self.advertises_pings = True
        self.held_arrived = asyncio.Event()
        self.release_held = asyncio.Event()

    async def start(self, stream):
        await stream.recv_message()
        await stream.send_message(service.BaseInfoResponse(started=True, node_version=NODE_VERSION, core_version=CORE))

    async def base_info(self, stream):
        await stream.recv_message()
        if self.advertises_pings:
            await stream.send_initial_metadata(metadata={"pg-capabilities": PING_POLICY})
        await stream.send_message(service.BaseInfoResponse(started=True, node_version=NODE_VERSION, core_version=CORE))

    async def stop(self, stream):
        await stream.recv_message()
        await stream.send_message(service.Empty())

    async def backend_stats(self, stream):
        await stream.recv_message()
        await stream.send_message(service.BackendStatsResponse())

    async def held(self, stream):
        await stream.recv_message()
        self.held_arrived.set()
        await self.release_held.wait()
        await stream.send_message(service.SystemStatsResponse())

    async def delayed(self, stream):
        request = await stream.recv_message()
        await asyncio.sleep(float(request.name))
        await stream.send_message(service.StatResponse())

    async def chunked(self, stream):
        chunks = [chunk async for chunk in stream]
        await asyncio.sleep(float(chunks[0].users[0].email))
        await stream.send_message(service.Empty())

    def __mapping__(self):
        return {
            "/service.NodeService/Start": Handler(
                self.start, Cardinality.UNARY_UNARY, service.Backend, service.BaseInfoResponse
            ),
            "/service.NodeService/Stop": Handler(self.stop, Cardinality.UNARY_UNARY, service.Empty, service.Empty),
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
                self.delayed, Cardinality.UNARY_UNARY, service.StatRequest, service.StatResponse
            ),
            "/service.NodeService/SyncUsersChunked": Handler(
                self.chunked, Cardinality.STREAM_UNARY, service.UsersChunk, service.Empty
            ),
        }


class ConnectionCountingRelay:
    def __init__(self, upstream_port: int):
        self.upstream_port = upstream_port
        self.accepted = 0
        self.open = 0
        self.writers = []
        self.server = None

    async def start(self) -> int:
        self.server = await asyncio.start_server(self._relay, "127.0.0.1", 0)
        return self.server.sockets[0].getsockname()[1]

    async def _pump(self, reader, writer):
        try:
            while data := await reader.read(65536):
                writer.write(data)
                await writer.drain()
        except Exception:
            return
        finally:
            writer.close()

    async def _relay(self, client_reader, client_writer):
        self.accepted += 1
        self.open += 1
        try:
            upstream_reader, upstream_writer = await asyncio.open_connection("127.0.0.1", self.upstream_port)
            self.writers += [client_writer, upstream_writer]
            await asyncio.gather(
                self._pump(client_reader, upstream_writer),
                self._pump(upstream_reader, client_writer),
            )
        finally:
            self.open -= 1

    async def settle_to(self, expected: int, within: float = SETTLE) -> int:
        deadline = asyncio.get_running_loop().time() + within
        while self.open != expected and asyncio.get_running_loop().time() < deadline:
            await asyncio.sleep(0.005)
        return self.open

    def close(self):
        for writer in self.writers:
            writer.close()
        self.server.close()


def make_node(port: int, name: str) -> GrpcNode:
    with patch("ssl.SSLContext.load_verify_locations"):
        node = GrpcNode(
            address="127.0.0.1",
            port=port,
            api_port=1,
            server_ca="test",
            api_key=str(uuid4()),
            name=name,
            node_id=f"{name}-{uuid4()}",
            user_sync_store=InMemoryUserSyncStore(),
            lifecycle_coordinator=InMemoryNodeLifecycleCoordinator(),
            logger=logger,
            default_timeout=5,
            internal_timeout=5,
        )
    node.channel.close()
    node.ctx = None
    node.channel = node._make_channel(keepalive=False)
    node._client = service_grpc.NodeServiceStub(node.channel)
    return node


@dataclass
class CallOutcome:
    kind: str
    delay: float
    generation_at_start: int
    generation_at_end: int
    succeeded: bool
    ended_after_teardown_began: bool
    error: str


class RetiredChannelTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.service = FakeNodeService()
        self.server = Server([self.service])
        await self.server.start("127.0.0.1", 0)
        self.relay = ConnectionCountingRelay(self.server._server.sockets[0].getsockname()[1])
        self.port = await self.relay.start()
        self.nodes = []
        self.pending = []

    async def asyncTearDown(self):
        for task in self.pending:
            task.cancel()
        await asyncio.gather(*self.pending, return_exceptions=True)
        for node in self.nodes:
            node._close_chan()
            await node._json_client.close()
        self.relay.close()
        self.server.close()
        try:
            await asyncio.wait_for(self.server.wait_closed(), 2)
        except TimeoutError:
            pass

    def node(self, name: str) -> GrpcNode:
        node = make_node(self.port, name)
        self.nodes.append(node)
        return node

    async def assert_server_shuts_down_promptly(self):
        self.server.close()
        try:
            await asyncio.wait_for(self.server.wait_closed(), 2)
        except TimeoutError:
            self.fail(f"server.wait_closed() still blocked after 2 s with {self.relay.open} connection(s) open")

    async def reconnect(self, node: GrpcNode, pings: bool):
        self.service.advertises_pings = pings
        await node.connect(NODE_VERSION, CORE)
        self.assertEqual(node.channel._config._keepalive_time is not None, pings)

    async def hold_a_call_on_the_current_channel(self, node: GrpcNode) -> asyncio.Task:
        call = asyncio.create_task(node.get_system_stats(timeout=10))
        self.pending.append(call)
        await asyncio.wait_for(self.service.held_arrived.wait(), 2)
        return call

    async def test_a_full_teardown_after_a_ping_swap_leaves_no_connection_open(self):
        node = self.node("teardown")
        await node.start("{}", service.BackendType.XRAY, [])
        await node.stop()
        node._close_chan()
        self.assertEqual(await self.relay.settle_to(0), 0)
        await self.assert_server_shuts_down_promptly()

    async def test_a_teardown_closes_a_retired_channel_even_while_a_call_is_still_on_it(self):
        node = self.node("teardown-busy")
        await self.reconnect(node, pings=False)
        held = await self.hold_a_call_on_the_current_channel(node)
        await self.reconnect(node, pings=True)
        self.assertIsNotNone(await node.get_backend_stats(timeout=5))
        await node.stop()
        node._close_chan()
        self.assertEqual(await self.relay.settle_to(0), 0)
        with self.assertRaises(NodeAPIError):
            await asyncio.wait_for(held, 2)
        await self.assert_server_shuts_down_promptly()

    async def test_a_call_in_flight_on_the_old_channel_during_a_swap_completes(self):
        node = self.node("in-flight")
        await self.reconnect(node, pings=False)
        held = await self.hold_a_call_on_the_current_channel(node)
        await self.reconnect(node, pings=True)
        self.assertIsNotNone(await node.get_backend_stats(timeout=5))
        await asyncio.sleep(0.2)
        self.assertFalse(held.done())
        self.service.release_held.set()
        self.assertIsInstance(await asyncio.wait_for(held, 5), service.SystemStatsResponse)
        await node.stop()
        node._close_chan()

    async def test_a_retired_channel_closes_as_soon_as_its_last_call_finishes(self):
        node = self.node("prompt-close")
        await self.reconnect(node, pings=False)
        held = await self.hold_a_call_on_the_current_channel(node)
        await self.reconnect(node, pings=True)
        self.assertIsNotNone(await node.get_backend_stats(timeout=5))
        self.assertEqual(self.relay.open, 2)
        self.service.release_held.set()
        await asyncio.wait_for(held, 5)
        self.assertEqual(await self.relay.settle_to(1), 1)
        accepted = self.relay.accepted
        self.assertIsNotNone(await node.get_backend_stats(timeout=5))
        self.assertEqual(self.relay.accepted, accepted)
        await node.stop()
        node._close_chan()
        self.assertEqual(await self.relay.settle_to(0), 0)

    async def test_a_retired_channel_without_calls_closes_at_the_swap(self):
        node = self.node("idle-close")
        await self.reconnect(node, pings=False)
        self.assertIsNotNone(await node.get_backend_stats(timeout=5))
        self.assertEqual(self.relay.open, 1)
        await self.reconnect(node, pings=True)
        self.assertEqual(await self.relay.settle_to(0), 0)
        await node.stop()
        node._close_chan()
        self.assertEqual(await self.relay.settle_to(0), 0)

    async def test_a_call_that_reopens_a_closed_channel_does_not_keep_the_connection(self):
        node = self.node("reopened")
        await self.reconnect(node, pings=False)
        self.assertIsNotNone(await node.get_backend_stats(timeout=5))
        await node.stop()
        node._close_chan()
        self.assertEqual(await self.relay.settle_to(0), 0)
        self.assertIsNotNone(await node.get_backend_stats(timeout=5))
        self.assertEqual(await self.relay.settle_to(0), 0)

    async def test_a_teardown_racing_a_call_that_is_still_connecting_leaves_nothing_open(self):
        node = self.node("connecting")
        call = asyncio.create_task(node.get_backend_stats(timeout=5))
        self.pending.append(call)
        await asyncio.sleep(0)
        self.assertEqual(self.relay.accepted, 0)
        node._close_chan()
        await asyncio.gather(call, return_exceptions=True)
        self.assertEqual(self.relay.accepted, 1)
        self.assertEqual(await self.relay.settle_to(0), 0)

    async def _interleaving_round(self, round_index: int) -> tuple[list[CallOutcome], int]:
        rng = random.Random(round_index)
        node = self.node(f"interleave-{round_index}")
        state = {"generation": 0, "teardown": False}
        outcomes: list[CallOutcome] = []
        calls: list[asyncio.Task] = []

        async def call(kind: str, delay: float):
            generation = state["generation"]
            error = ""
            try:
                if kind == "held":
                    succeeded = await node.get_system_stats(timeout=HELD_CALL_TIMEOUT) is not None
                elif kind == "chunked":
                    failed = await node.sync_users_chunked([service.User(email=str(delay))], timeout=10)
                    succeeded = not failed
                else:
                    reply = await node.get_stats(service.StatType.UsersStat, name=str(delay), timeout=10)
                    succeeded = reply is not None
            except Exception as exc:
                succeeded, error = False, f"{type(exc).__name__}: {exc}"
            outcomes.append(
                CallOutcome(kind, delay, generation, state["generation"], succeeded, state["teardown"], error)
            )

        def launch(count: int):
            for _ in range(count):
                kind = "chunked" if rng.random() < 0.2 else "unary"
                calls.append(asyncio.create_task(call(kind, round(rng.uniform(0.0, 0.04), 4))))

        calls.append(asyncio.create_task(call("held", 0.0)))
        launch(rng.randint(2, 5))
        for pings in (True, False, True):
            await asyncio.sleep(rng.uniform(0.0, 0.015))
            launch(rng.randint(1, 4))
            await asyncio.sleep(rng.uniform(0.0, 0.01))
            await self.reconnect(node, pings=pings)
            state["generation"] += 1
            launch(rng.randint(0, 3))
        await asyncio.sleep(0.0 if rng.random() < 0.3 else rng.uniform(0.0, 0.03))
        state["teardown"] = True
        if rng.random() < 0.5:
            await node.stop()
            node._close_chan()
        else:
            node._close_chan()
            await node.stop()
        await asyncio.wait_for(asyncio.gather(*calls), TEARDOWN_SETTLE)
        return outcomes, await self.relay.settle_to(0)

    async def test_calls_straddling_repeated_swaps_and_a_racing_teardown(self):
        straddled = ended_during_teardown = total = 0
        for round_index in range(INTERLEAVING_ROUNDS):
            outcomes, left_open = await self._interleaving_round(round_index)
            cut_by_swap = [o for o in outcomes if not o.succeeded and not o.ended_after_teardown_began]
            self.assertEqual(cut_by_swap, [], f"round {round_index}: calls failed before any teardown began")
            self.assertEqual(left_open, 0, f"round {round_index}: connections left open after teardown")
            finite = [o for o in outcomes if o.kind != "held"]
            total += len(finite)
            straddled += sum(o.generation_at_end > o.generation_at_start for o in finite)
            ended_during_teardown += sum(o.ended_after_teardown_began for o in finite)
        self.assertGreater(straddled, INTERLEAVING_ROUNDS)
        self.assertGreater(ended_during_teardown, INTERLEAVING_ROUNDS // 2)
        self.assertGreater(total, INTERLEAVING_ROUNDS * 5)
        await self.assert_server_shuts_down_promptly()


if __name__ == "__main__":
    unittest.main()
