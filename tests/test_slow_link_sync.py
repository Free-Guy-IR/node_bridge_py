import asyncio
import logging
import tempfile
import time
import unittest
import uuid

from aiohttp import web
from grpclib.server import Server

from PasarGuardNodeBridge.common import service_pb2 as service
from PasarGuardNodeBridge.grpclib import Node as GrpcNode
from PasarGuardNodeBridge.rest import Node as RestNode
from PasarGuardNodeBridge.storage import InMemoryNodeLifecycleCoordinator, InMemoryUserSyncStore
from PasarGuardNodeBridge.utils import create_proxy, create_user
from tests.test_stalled_channel import NodeService, StallingRelay, UVLoopCase, uvloop
from tests.test_tls import certificate

USERS = 640
UPLINK_BYTES_PER_SECOND = 64 * 1024
NODE_TIMEOUT = 2

logger = logging.getLogger("slow-link-sync-test")
logger.setLevel(logging.CRITICAL)


def realistic_users(count: int) -> list[service.User]:
    inbounds = [f"VLESS TCP REALITY {index}" for index in range(12)]
    return [
        create_user(
            str(100000 + index),
            create_proxy(
                vmess_id=str(uuid.uuid4()),
                vless_id=str(uuid.uuid4()),
                vless_flow="xtls-rprx-vision",
                trojan_password=uuid.uuid4().hex[:22],
                shadowsocks_password=uuid.uuid4().hex[:22],
                shadowsocks_method="chacha20-ietf-poly1305",
                hysteria2_password=uuid.uuid4().hex[:22],
            ),
            inbounds,
        )
        for index in range(count)
    ]


def node_arguments(port: int, server_ca: str, name: str) -> dict:
    return {
        "address": "127.0.0.1",
        "port": port,
        "api_port": port,
        "server_ca": server_ca,
        "api_key": str(uuid.uuid4()),
        "name": name,
        "node_id": f"{name}-{uuid.uuid4()}",
        "user_sync_store": InMemoryUserSyncStore(),
        "lifecycle_coordinator": InMemoryNodeLifecycleCoordinator(),
        "logger": logger,
        "default_timeout": NODE_TIMEOUT,
        "internal_timeout": NODE_TIMEOUT,
    }


class SlowLinkCase(unittest.IsolatedAsyncioTestCase):
    def assert_slower_than_the_default_timeout(self, users: list[service.User]):
        seconds = len(service.Users(users=users).SerializeToString()) / UPLINK_BYTES_PER_SECOND
        self.assertGreater(seconds, NODE_TIMEOUT * 2)


class GrpcSlowLinkSyncTests(SlowLinkCase):
    async def asyncSetUp(self):
        with tempfile.TemporaryDirectory() as directory:
            server_ssl, server_ca, _ = certificate(directory)
        self.service = NodeService()
        self.server = Server([self.service])
        await self.server.start("127.0.0.1", 0, ssl=server_ssl)
        self.relay = StallingRelay(self.server._server.sockets[0].getsockname()[1])
        port = await self.relay.start()
        self.node = GrpcNode(**node_arguments(port, server_ca, "slow-link-grpc"))
        self.assertIsNotNone(await self.node.get_backend_stats(timeout=5))
        self.relay.up_bytes_per_second = UPLINK_BYTES_PER_SECOND
        self.users = realistic_users(USERS)
        self.assert_slower_than_the_default_timeout(self.users)

    async def asyncTearDown(self):
        self.node._close_chan()
        await self.node._json_client.close()
        self.relay.close()
        self.server.close()
        try:
            await asyncio.wait_for(self.server.wait_closed(), 2)
        except TimeoutError:
            pass

    async def test_a_full_sync_with_the_default_timeout_reaches_a_live_node_on_a_slow_link(self):
        started = time.monotonic()
        await self.node.sync_users(self.users)
        self.assertGreater(time.monotonic() - started, NODE_TIMEOUT)
        self.assertEqual(len(self.service.synced_users[-1]), USERS)

    async def test_a_chunked_sync_with_the_default_timeout_reaches_a_live_node_on_a_slow_link(self):
        started = time.monotonic()
        failed = await self.node.sync_users_chunked(self.users, chunk_size=100)
        self.assertEqual(len(failed), 0)
        self.assertGreater(time.monotonic() - started, NODE_TIMEOUT)
        self.assertEqual(len(self.service.synced_users[-1]), USERS)


class RestSlowLinkSyncTests(SlowLinkCase):
    async def asyncSetUp(self):
        with tempfile.TemporaryDirectory() as directory:
            server_ssl, server_ca, _ = certificate(directory)
        self.synced = []

        async def sync(request):
            self.synced.append(len(service.Users.FromString(await request.read()).users))
            return web.Response(body=service.Empty().SerializeToString())

        async def backend_stats(request):
            return web.Response(body=service.BackendStatsResponse().SerializeToString())

        app = web.Application()
        app.router.add_put("/users/sync", sync)
        app.router.add_get("/stats/backend", backend_stats)
        self.runner = web.AppRunner(app)
        await self.runner.setup()
        site = web.TCPSite(self.runner, "127.0.0.1", 0, ssl_context=server_ssl)
        await site.start()
        self.relay = StallingRelay(site._server.sockets[0].getsockname()[1])
        port = await self.relay.start()
        self.node = RestNode(**node_arguments(port, server_ca, "slow-link-rest"))
        self.assertIsNotNone(await self.node.get_backend_stats(timeout=5))
        self.relay.up_bytes_per_second = UPLINK_BYTES_PER_SECOND
        self.users = realistic_users(USERS)
        self.assert_slower_than_the_default_timeout(self.users)

    async def asyncTearDown(self):
        await self.node._client.close()
        await self.node._json_client.close()
        self.relay.close()
        await self.runner.cleanup()

    async def test_a_full_sync_with_the_default_timeout_reaches_a_live_rest_node_on_a_slow_link(self):
        started = time.monotonic()
        await self.node.sync_users(self.users)
        self.assertGreater(time.monotonic() - started, NODE_TIMEOUT)
        self.assertEqual(self.synced, [USERS])


@unittest.skipIf(uvloop is None, "uvloop is unavailable on this platform")
class UVLoopGrpcSlowLinkSyncTests(UVLoopCase, GrpcSlowLinkSyncTests):
    pass


@unittest.skipIf(uvloop is None, "uvloop is unavailable on this platform")
class UVLoopRestSlowLinkSyncTests(UVLoopCase, RestSlowLinkSyncTests):
    pass


if __name__ == "__main__":
    unittest.main()
