import asyncio
import time
import unittest
from unittest.mock import patch
from uuid import uuid4

from grpclib.server import Server

import PasarGuardNodeBridge.grpclib as grpc_transport
from PasarGuardNodeBridge import NodeAPIError
from PasarGuardNodeBridge.common import service_pb2
from PasarGuardNodeBridge.common.service_grpc import NodeServiceBase
from PasarGuardNodeBridge.grpclib import Node as GrpcNode


async def _backend_stats(self, stream):
    await stream.recv_message()
    await stream.send_message(service_pb2.BackendStatsResponse())


async def _unused(self, stream):
    return None


def _service():
    methods = {name: _unused for name in NodeServiceBase.__abstractmethods__}
    methods["GetBackendStats"] = _backend_stats
    return type("KeepaliveNodeService", (NodeServiceBase,), methods)()


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
    return node


class KeepaliveGateTests(unittest.IsolatedAsyncioTestCase):
    async def asyncTearDown(self):
        await asyncio.sleep(0)

    async def test_a_node_older_than_the_ping_policy_gets_no_pings(self):
        node = _node()
        await node.connect("0.6.20", "26.3.27")
        self.assertIsNone(node.channel._config._keepalive_time)
        node._close_chan()

    async def test_a_node_with_the_ping_policy_gets_pings(self):
        node = _node()
        await node.connect("0.6.21", "26.3.27")
        config = node.channel._config
        self.assertEqual(config._keepalive_time, grpc_transport.KEEPALIVE_TIME)
        self.assertGreaterEqual(config._keepalive_time, 5)
        self.assertTrue(config._keepalive_permit_without_calls)
        self.assertEqual(config._http2_max_pings_without_data, 0)
        self.assertGreaterEqual(config._http2_min_sent_ping_interval_without_data, 5)
        node._close_chan()

    async def test_versions_with_suffixes_are_tolerated(self):
        for version, expected in (("0.6.21+build.7", True), ("0.6.22-rc1", True), ("0.6.20+build.9", False)):
            node = _node()
            await node.connect(version, "26.3.27")
            self.assertEqual(node.channel._config._keepalive_time is not None, expected, version)
            node._close_chan()

    async def test_an_unparseable_version_gets_no_pings(self):
        node = _node()
        await node.connect("dev-build", "26.3.27")
        self.assertIsNone(node.channel._config._keepalive_time)
        node._close_chan()

    async def test_a_node_that_reconnects_after_an_upgrade_starts_getting_pings(self):
        node = _node()
        await node.connect("0.6.20", "26.3.27")
        self.assertIsNone(node.channel._config._keepalive_time)
        await node.connect("0.6.21", "26.3.27")
        self.assertIsNotNone(node.channel._config._keepalive_time)
        await node.connect("0.6.20", "26.3.27")
        self.assertIsNone(node.channel._config._keepalive_time)
        node._close_chan()


class SilentPeerTests(unittest.IsolatedAsyncioTestCase):
    async def test_a_node_whose_network_goes_silent_is_detected_before_the_rpc_timeout(self):
        server = Server([_service()])
        await server.start("127.0.0.1", 0)
        proxy = SilentProxy(server._server.sockets[0].getsockname()[1])
        port = await proxy.start()
        with patch.object(grpc_transport, "KEEPALIVE_TIME", 0.3), patch.object(grpc_transport, "KEEPALIVE_TIMEOUT", 0.5):
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
