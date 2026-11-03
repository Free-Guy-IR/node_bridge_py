import asyncio

from tests.sync_fakes import FakeNodeCore, SyncFakeTestCase, user


class DisconnectRaceTests(SyncFakeTestCase):
    async def race(self, core, users, trigger, pause):
        node = await self.rest_node(core, sync_lease_seconds=3.0)
        requests = []
        reached = asyncio.Event()
        make_request = node._make_request
        get_session = node._client._get_session

        async def recorded_make_request(*args, **kwargs):
            requests.append((kwargs.get("endpoint"), node.is_shutting_down()))
            if trigger(kwargs.get("endpoint")):
                reached.set()
                await asyncio.sleep(pause)
            return await make_request(*args, **kwargs)

        async def recorded_get_session():
            requests.append(("session", node.is_shutting_down()))
            if trigger("session"):
                reached.set()
                await asyncio.sleep(pause)
            return await get_session()

        node._make_request = recorded_make_request
        node._client._get_session = recorded_get_session
        await node._user_sync_store.enqueue_users(node.node_id, users)
        await node.connect("0.2.0", "1.0.0")
        async with asyncio.timeout(30):
            await reached.wait()
        await node.disconnect()
        await node._client.retire()
        seen = len(requests)
        await asyncio.sleep(1.5)
        self.assertEqual(requests[seen:], [])
        self.assertFalse(node._client.is_open())
        self.assertIsNone(node._sync_worker_task)
        self.assertEqual(self.late_sends, [])

    async def test_disconnect_during_an_in_flight_chunked_round_starts_no_later_send(self):
        for pause in (0.0, 0.05, 0.3):
            with self.subTest(pause=pause):
                core = FakeNodeCore(delay=0.4)
                users = [user(f"u-{index}", "good") for index in range(6)]
                await self.race(core, users, lambda endpoint: endpoint == "session", pause)

    async def test_disconnect_during_a_split_of_a_rejected_round_starts_no_later_send(self):
        for pause, nth in ((0.0, 2), (0.05, 3), (0.3, 4)):
            with self.subTest(pause=pause, nth=nth):
                core = FakeNodeCore(delay=0.05)
                users = [user(f"u-{index}", "bad" if index == 1 else "good") for index in range(4)]
                calls = []

                def nth_send(endpoint, calls=calls, nth=nth):
                    calls.append(endpoint)
                    return calls.count("session") >= nth

                await self.race(core, users, nth_send, pause)
                self.assertGreaterEqual(len(core.batches), nth - 1)
