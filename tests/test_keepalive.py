import asyncio
import time
import unittest
from contextlib import suppress
from unittest.mock import patch
from uuid import uuid4

from grpclib.server import Server

import PasarGuardNodeBridge.grpclib as grpc_transport
from PasarGuardNodeBridge import NodeAPIError
from PasarGuardNodeBridge.common import service_pb2
from PasarGuardNodeBridge.common.service_grpc import NodeServiceBase, NodeServiceStub
from PasarGuardNodeBridge.grpclib import Node as GrpcNode


async def _backend_stats(self, stream):
    await stream.recv_message()
    await stream.send_message(service_pb2.BackendStatsResponse())


async def _base_info(self, stream):
    await stream.recv_message()
    if self.capabilities is not None:
        await stream.send_initial_metadata(metadata={"pg-capabilities": self.capabilities})
    await stream.send_message(service_pb2.BaseInfoResponse(started=True, node_version="0.6.21"))


async def _unused(self, stream):
    return None


def _service(capabilities=None):
    methods = {name: _unused for name in NodeServiceBase.__abstractmethods__}
    methods["GetBackendStats"] = _backend_stats
    methods["GetBaseInfo"] = _base_info
    service = type("KeepaliveNodeService", (NodeServiceBase,), methods)()
    service.capabilities = capabilities
    return service


class SilentProxy:
    def __init__(self, upstream_port: int):
        self.upstream_port = upstream_port
        self.silent = False
        self.server = None
        self.writers = []

    async def _pump(self, reader, writer):
        try:
            while data := await reader.read(65536):
                if not self.silent:
                    writer.write(data)
                    await writer.drain()
        except Exception:
            return

    async def _handle(self, client_reader, client_writer):
        upstream_reader, upstream_writer = await asyncio.open_connection("127.0.0.1", self.upstream_port)
        self.writers += [client_writer, upstream_writer]
        await asyncio.gather(
            self._pump(client_reader, upstream_writer),
            self._pump(upstream_reader, client_writer),
        )

    async def start(self) -> int:
        self.server = await asyncio.start_server(self._handle, "127.0.0.1", 0)
        return self.server.sockets[0].getsockname()[1]

    def close(self):
        for writer in self.writers:
            writer.close()
        self.server.close()


def _node(port: int = 1, **kwargs) -> GrpcNode:
    with patch("ssl.SSLContext.load_verify_locations"):
        node = GrpcNode(
            address="127.0.0.1",
            port=port,
            api_port=port,
            server_ca="test",
            api_key=str(uuid4()),
            name="keepalive",
            **kwargs,
        )
    node.ctx = None
    node.channel.close()
    node.channel = node._make_channel(keepalive=False)
    node._client = NodeServiceStub(node.channel)
    return node


class KeepaliveGateTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.service = _service()
        self.server = Server([self.service])
        await self.server.start("127.0.0.1", 0)
        self.port = self.server._server.sockets[0].getsockname()[1]

    async def asyncTearDown(self):
        self.server.close()
        with suppress(TimeoutError):
            await asyncio.wait_for(self.server.wait_closed(), 1)

    def advertise(self, capabilities):
        self.service.capabilities = capabilities

    def pings(self, node):
        return node.channel._config._keepalive_time is not None

    async def test_a_node_that_reports_the_policy_version_but_does_not_advertise_it_gets_no_pings(self):
        node = _node(self.port)
        await node.connect("0.6.21", "26.3.27")
        self.assertFalse(self.pings(node))
        node._close_chan()

    async def test_a_node_that_advertises_the_ping_policy_gets_pings_whatever_its_version(self):
        self.advertise("keepalive-policy-v1")
        for version in ("0.6.20", "0.6.22", "dev-build"):
            node = _node(self.port)
            await node.connect(version, "26.3.27")
            config = node.channel._config
            self.assertEqual(config._keepalive_time, grpc_transport.KEEPALIVE_TIME, version)
            self.assertGreaterEqual(config._keepalive_time, 5)
            self.assertTrue(config._keepalive_permit_without_calls)
            self.assertEqual(config._http2_max_pings_without_data, 0)
            self.assertGreaterEqual(config._http2_min_sent_ping_interval_without_data, 5)
            node._close_chan()

    async def test_the_capability_is_found_among_other_and_unknown_tokens(self):
        for header, expected in (
            ("rejected-users-v1,keepalive-policy-v1", True),
            ("future-thing-v9,keepalive-policy-v1,rejected-users-v1", True),
            ("keepalive-policy-v2", False),
            ("rejected-users-v1", False),
            (",", False),
        ):
            self.advertise(header)
            node = _node(self.port)
            await node.connect("0.6.21", "26.3.27")
            self.assertEqual(self.pings(node), expected, header)
            node._close_chan()

    async def test_a_node_that_cannot_be_probed_gets_no_pings(self):
        node = _node(1)
        await node.connect("0.6.21", "26.3.27")
        self.assertFalse(self.pings(node))
        node._close_chan()

    async def test_a_rotated_channel_stays_open_long_enough_for_calls_already_on_it(self):
        self.advertise("keepalive-policy-v1")
        node = _node(self.port)
        retired = node.channel
        loop = asyncio.get_running_loop()
        with patch.object(loop, "call_later") as scheduled:
            await node.connect("0.6.21", "26.3.27")
        delay, callback = scheduled.call_args.args
        self.assertIs(callback.__self__, retired)
        self.assertGreaterEqual(delay, grpc_transport.RETIRED_CHANNEL_GRACE)
        self.assertGreater(grpc_transport.RETIRED_CHANNEL_GRACE, node._default_timeout + node._internal_timeout * 20)
        retired.close()
        node._close_chan()

    async def test_a_node_that_reconnects_after_an_upgrade_starts_getting_pings(self):
        node = _node(self.port)
        await node.connect("0.6.21", "26.3.27")
        self.assertFalse(self.pings(node))
        self.advertise("keepalive-policy-v1")
        await node.connect("0.6.22", "26.3.27")
        self.assertTrue(self.pings(node))
        self.advertise(None)
        await node.connect("0.6.21", "26.3.27")
        self.assertFalse(self.pings(node))
        node._close_chan()


class SilentPeerTests(unittest.IsolatedAsyncioTestCase):
    async def test_a_node_whose_network_goes_silent_is_detected_before_the_rpc_timeout(self):
        server = Server([_service("keepalive-policy-v1")])
        await server.start("127.0.0.1", 0)
        proxy = SilentProxy(server._server.sockets[0].getsockname()[1])
        port = await proxy.start()
        with (
            patch.object(grpc_transport, "KEEPALIVE_TIME", 0.3),
            patch.object(grpc_transport, "KEEPALIVE_TIMEOUT", 0.5),
        ):
            node = _node(port, default_timeout=5, internal_timeout=5)
            try:
                await node.connect("0.6.21", "26.3.27")
                self.assertIsNotNone(await node.get_backend_stats(timeout=5))
                proxy.silent = True
                started = time.monotonic()
                with self.assertRaises(NodeAPIError) as caught:
                    await node.get_backend_stats(timeout=10)
                elapsed = time.monotonic() - started
                self.assertLess(elapsed, 5, f"code={caught.exception.code} detail={caught.exception.detail!r}")
            finally:
                node._close_chan()
                proxy.close()
                server.close()
                await server.wait_closed()


if __name__ == "__main__":
    unittest.main()
