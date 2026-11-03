import asyncio
import logging
import math
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

POLL_INTERVAL = 0.01


def single_reject_send_bound(users):
    return 2 * math.ceil(math.log2(users)) + 1


def user(email, inbound="good"):
    return service.User(email=email, inbounds=[inbound])


def is_bad(item):
    return "bad" in item.inbounds


class FakeNodeCore:
    def __init__(self, delay=0.0, slow_sends=None):
        self.delay = delay
        self.slow_sends = dict(slow_sends or {})
        self.applied = {}
        self.history = {}
        self.accepted_at = {}
        self.batches = []
        self.on_send = None
        self.reject_all = False

    def accept(self, item):
        self.applied[item.email] = list(item.inbounds)
        self.history.setdefault(item.email, []).append(list(item.inbounds))
        self.accepted_at.setdefault(item.email, []).append((list(item.inbounds), asyncio.get_running_loop().time()))

    async def apply(self, users):
        self.batches.append([item.email for item in users])
        if self.on_send is not None:
            await self.on_send(users)
        delay = self.slow_sends.get(len(self.batches), self.delay)
        if delay:
            await asyncio.sleep(delay)
        if self.reject_all:
            return Status.INTERNAL, "failed to update users: core api unreachable"
        bad = [item.email for item in users if is_bad(item)]
        if bad:
            return Status.INTERNAL, f"failed to update users: invalid account {bad[0]}"
        for item in users:
            self.accept(item)
        return None, ""

    def delivered_at(self, email, inbounds):
        return next((at for applied, at in self.accepted_at.get(email, []) if applied == inbounds), None)

    def sends_with(self, email):
        return sum(1 for batch in self.batches if email in batch)


class GrpcFakeNode:
    def __init__(self, core):
        self.core = core

    async def chunked(self, stream):
        users = [item for chunk in [chunk async for chunk in stream] for item in chunk.users]
        status, message = await self.core.apply(users)
        if status is not None:
            raise GRPCError(status, message)
        await stream.send_message(service.Empty())

    def __mapping__(self):
        return {
            "/service.NodeService/SyncUsersChunked": Handler(
                self.chunked, Cardinality.STREAM_UNARY, service.UsersChunk, service.Empty
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


class ClosedOnceSession(LazyClientSession):
    def __init__(self, late_sends, **kwargs):
        super().__init__(**kwargs)
        self.late_sends = late_sends
        self.retired = False

    async def _get_session(self):
        if self.retired:
            task = asyncio.current_task()
            self.late_sends.append(task.get_coro().__qualname__ if task is not None else None)
        return await super()._get_session()

    async def retire(self):
        self.retired = True
        await self.close()

    def is_open(self):
        return self._session is not None and not self._session.closed


class SyncFakeTestCase(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.cleanups = []
        self.late_sends = []
        self.rest_clients = []

    async def asyncTearDown(self):
        for cleanup in reversed(self.cleanups):
            await cleanup()
        await asyncio.sleep(0)
        self.assertEqual(self.late_sends, [], "a REST send started after disconnect() closed the client")
        self.assertEqual([client for client in self.rest_clients if client.is_open()], [])

    def make_node(self, node_class, port, **kwargs):
        options = {"sync_poll_interval": POLL_INTERVAL, "sync_chunk_size": 100}
        options.update(kwargs)
        with patch("ssl.SSLContext.load_verify_locations"):
            return node_class(
                address="127.0.0.1",
                port=port,
                api_port=1,
                server_ca="test",
                api_key="00000000-0000-0000-0000-000000000000",
                node_id="round-budget",
                user_sync_store=InMemoryUserSyncStore(),
                logger=logging.getLogger("round-budget-test"),
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
        node._client = ClosedOnceSession(
            self.late_sends,
            ssl_context=None,
            headers={"Content-Type": "application/x-protobuf"},
            base_url=f"http://127.0.0.1:{port}/",
            timeout=make_timeout(None),
        )
        self.rest_clients.append(node._client)

        async def cleanup():
            await node.disconnect()
            await node._client.retire()
            await node._json_client.close()
            await runner.cleanup()

        self.cleanups.append(cleanup)
        return node

    def held(self, node):
        return {email for email, entry in node._sync_retries.items() if entry.attempts > 0}

    def carried(self, node):
        return {email for email, entry in node._sync_retries.items() if entry.attempts == 0}

    async def pending(self, node):
        return set((await node._user_sync_store.capture_queued(node.node_id))[0])

    async def settled(self, node, core, latest, suspects):
        good = {email: inbounds for email, inbounds in latest.items() if email not in suspects}
        delivered = all(core.applied.get(email) == inbounds for email, inbounds in good.items())
        if not delivered or self.held(node) != set(suspects):
            return False
        return await self.pending(node) | set(node._sync_retries) == set(suspects)

    async def until(self, predicate, timeout, abort=None):
        async with asyncio.timeout(timeout):
            while not await predicate():
                if abort is not None:
                    reason = abort()
                    if reason:
                        self.fail(reason)
                await asyncio.sleep(POLL_INTERVAL)

    def count_failures(self, node):
        counted = []
        increment = node._increment_user_sync_failure

        async def counting_increment():
            counted.append(node._user_sync_failure_count + 1)
            await increment()

        node._increment_user_sync_failure = counting_increment
        return counted

    def record_resets(self, node):
        resets = []
        reset = node._reset_user_sync_failure_count

        async def recording_reset():
            resets.append((asyncio.get_running_loop().time(), node._user_sync_failure_count))
            await reset()

        node._reset_user_sync_failure_count = recording_reset
        return resets

    def watch_hard_reset(self, node):
        raised = []
        original = node._hard_reset_event.set

        def recording_set():
            raised.append(asyncio.get_running_loop().time())
            original()

        node._hard_reset_event.set = recording_set
        return raised

    def record_rounds(self, node):
        rounds = []
        deliver = node._deliver_claimed_users

        async def recorded(claimed):
            loop = asyncio.get_running_loop()
            entry = [loop.time(), math.inf, [item.user.email for item in claimed]]
            rounds.append(entry)
            try:
                return await deliver(claimed)
            finally:
                entry[1] = loop.time()

        node._deliver_claimed_users = recorded
        return rounds

    def record_acks(self, node):
        acked = []
        ack, hold = node._ack_claimed_users, node._hold_claimed_users

        async def recording_ack(claimed_users):
            acked.extend((item.user.email, list(item.user.inbounds)) for item in claimed_users)
            await ack(claimed_users)

        async def recording_hold(claimed_users):
            acked.extend((item.user.email, list(item.user.inbounds)) for item in claimed_users)
            await hold(claimed_users)

        node._ack_claimed_users = recording_ack
        node._hold_claimed_users = recording_hold
        return acked

    def assert_nothing_lost(self, node, core, acked, latest):
        for email, inbounds in acked:
            if inbounds in core.history.get(email, []):
                continue
            entry = node._sync_retries.get(email)
            held = entry is not None and entry.attempts > 0 and list(entry.user.inbounds) == inbounds
            superseded = latest.get(email) != inbounds and core.applied.get(email) == latest.get(email)
            self.assertTrue(held or superseded, f"{email} {inbounds} acknowledged, not delivered, held or superseded")

    def assert_never_went_back(self, core, written):
        for email, applied in core.history.items():
            order = [written[email].index(inbounds) for inbounds in applied]
            self.assertEqual(order, sorted(order), f"{email} went back: {applied}")

    def assert_no_batch_repeats_a_user(self, core):
        for batch in core.batches:
            self.assertEqual(len(batch), len(set(batch)), batch)


def round_bound(rounds, pushed_at, extra_rounds):
    later = [end for start, end, _ in rounds if start > pushed_at]
    if len(later) < extra_rounds:
        return math.inf
    return later[extra_rounds - 1]

