import asyncio
import logging
import math
import weakref
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from grpclib.client import Channel, Stream
from grpclib.config import Configuration
from grpclib.const import Status
from grpclib.exceptions import GRPCError, StreamTerminatedError
from python_socks.async_.asyncio import Proxy

from PasarGuardNodeBridge.abstract_node import PasarGuardNode
from PasarGuardNodeBridge.common import service_grpc
from PasarGuardNodeBridge.common import service_pb2 as service
from PasarGuardNodeBridge.controller import (
    CAPABILITIES_HEADER,
    KEEPALIVE_CAPABILITY,
    Health,
    NodeAPIError,
    parse_capabilities,
    record_user_sync_failure,
)
from PasarGuardNodeBridge.storage import LifecycleOperation, LifecycleStatus
from PasarGuardNodeBridge.utils import format_host_for_url, grpc_to_http_status


class _CountedStream:
    def __init__(self, channel: "RetirableChannel", stream: Stream):
        self._channel = channel
        self._stream = stream

    def __getattr__(self, name):
        return getattr(self._stream, name)

    async def __aenter__(self):
        self._channel._call_opened()
        try:
            return await self._stream.__aenter__()
        except BaseException:
            self._channel._call_closed()
            raise

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        try:
            return await self._stream.__aexit__(exc_type, exc_val, exc_tb)
        finally:
            self._channel._call_closed()


class RetirableChannel(Channel):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._open_calls = 0
        self._retired = False
        self._grace_timer: asyncio.TimerHandle | None = None

    def request(self, *args, **kwargs):
        return _CountedStream(self, super().request(*args, **kwargs))

    def retire(self, grace: float) -> None:
        self._retired = True
        self._grace_timer = asyncio.get_running_loop().call_later(grace, self.close)
        if not self._open_calls:
            self.close()

    def close(self) -> None:
        self._retired = True
        if self._grace_timer is not None:
            self._grace_timer.cancel()
            self._grace_timer = None
        super().close()

    def _call_opened(self) -> None:
        self._open_calls += 1

    def _call_closed(self) -> None:
        self._open_calls -= 1
        if self._retired and not self._open_calls:
            self.close()


class ProxiedChannel(RetirableChannel):
    def __init__(self, *args, proxy_url: str, **kwargs):
        super().__init__(*args, **kwargs)
        self._proxy_url = proxy_url

    async def _create_connection(self):
        proxy = Proxy.from_url(self._proxy_url)
        sock = await proxy.connect(dest_host=self._host, dest_port=self._port)
        server_hostname = self._config.ssl_target_name_override or self._host
        _, protocol = await self._loop.create_connection(
            self._protocol_factory,
            sock=sock,
            ssl=self._ssl,
            server_hostname=(server_hostname if self._ssl is not None else None),
        )
        return protocol


CAPABILITY_PROBE_TIMEOUT = 5.0
KEEPALIVE_TIME = 10.0
KEEPALIVE_TIMEOUT = 10.0
RETIRED_CHANNEL_GRACE = 600.0
USER_REJECTION_STATUSES = frozenset({Status.INTERNAL, Status.UNKNOWN})


def _user_rejection(error: BaseException) -> str | None:
    if isinstance(error, GRPCError) and error.status in USER_REJECTION_STATUSES:
        return error.message or error.status.name
    return None


class Node(PasarGuardNode):
    def __init__(
        self,
        address: str,
        port: int,
        api_port: int,
        server_ca: str,
        api_key: str,
        name: str = "default",
        extra: dict | None = None,
        logger: logging.Logger | None = None,
        default_timeout: int = 10,
        internal_timeout: int = 15,
        max_message_size: int | None = None,
        proxy: str | None = None,
        **kwargs,
    ):
        self._retired_channels: weakref.WeakSet[RetirableChannel] = weakref.WeakSet()
        host_for_url = format_host_for_url(address)
        service_url = f"https://{host_for_url}:{api_port}/"
        super().__init__(
            server_ca,
            api_key,
            service_url,
            name,
            extra,
            logger,
            default_timeout,
            internal_timeout,
            proxy,
            **kwargs,
        )

        try:
            # Set HTTP/2 window sizes to 64MB to handle large node configurations
            # Default is 4MB which can be exceeded with many users or large configs
            # http2_connection_window_size and http2_stream_window_size control max message size
            if max_message_size is None:
                max_message_size = 64 * 1024 * 1024  # 64MB
            self._max_message_size = max_message_size
            if self._proxy is not None and not self._proxy.grpc_supported:
                raise NodeAPIError(-7, f"{self._proxy.scheme.upper()} proxies are not supported for gRPC transport")
            self._address = address
            self._port = port
            self._keepalive = False
            self.channel = self._make_channel(keepalive=False)
            self._client = service_grpc.NodeServiceStub(self.channel)
            self._metadata = {"x-api-key": api_key}
        except NodeAPIError:
            raise
        except Exception as e:
            raise NodeAPIError(-1, f"Channel initialization failed: {e!s}")

        self._node_lock = asyncio.Lock()

    def _make_channel(self, keepalive: bool) -> RetirableChannel:
        channel_class = RetirableChannel if self._proxy is None else ProxiedChannel
        channel_kwargs = {} if self._proxy is None else {"proxy_url": self._proxy.url}
        keepalive_settings = (
            {
                "_keepalive_time": KEEPALIVE_TIME,
                "_keepalive_timeout": KEEPALIVE_TIMEOUT,
                "_keepalive_permit_without_calls": True,
                "_http2_max_pings_without_data": 0,
                "_http2_min_sent_ping_interval_without_data": KEEPALIVE_TIME,
            }
            if keepalive
            else {"_keepalive_timeout": 10}
        )
        return channel_class(
            host=self._address,
            port=self._port,
            ssl=self.ctx,
            config=Configuration(
                http2_connection_window_size=self._max_message_size,
                http2_stream_window_size=self._max_message_size,
                **keepalive_settings,
            ),
            **channel_kwargs,
        )

    def _use_keepalive(self, wanted: bool) -> None:
        if wanted == self._keepalive:
            return
        retired = self.channel
        self.channel = self._make_channel(keepalive=wanted)
        self._client = service_grpc.NodeServiceStub(self.channel)
        self._keepalive = wanted
        self._retired_channels.add(retired)
        retired.retire(RETIRED_CHANNEL_GRACE)

    async def _probe_capabilities(self) -> frozenset[str]:
        try:
            async with (
                asyncio.timeout(min(CAPABILITY_PROBE_TIMEOUT, self._internal_timeout)),
                self._client.GetBaseInfo.open(metadata=self._metadata) as stream,
            ):
                await stream.send_message(service.Empty(), end=True)
                await stream.recv_message()
                await stream.recv_trailing_metadata()
                header = stream.initial_metadata.get(CAPABILITIES_HEADER) if stream.initial_metadata else None
                return parse_capabilities(header)
        except Exception as e:
            self.logger.debug(
                f"[{self.name}] Capability probe failed; assuming none | Error: {type(e).__name__} - {e!s}"
            )
            return frozenset()

    async def connect(self, node_version: str, core_version: str, tasks: list | None = None):
        self._capabilities = await self._probe_capabilities()
        self._use_keepalive(KEEPALIVE_CAPABILITY in self._capabilities)
        await super().connect(node_version, core_version, tasks)

    def _close_chan(self):
        """Close gRPC channel"""
        channels = list(getattr(self, "_retired_channels", ()))
        if hasattr(self, "channel"):
            channels.append(self.channel)
        for channel in channels:
            try:
                channel.close()
            except Exception as e:
                self.logger.debug(f"[{self.name}] Failed to close gRPC channel | Error: {type(e).__name__} - {e!s}")

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.stop()
        self._close_chan()

    def __del__(self):
        self._close_chan()

    def _handle_error(self, error: BaseException):
        """Convert gRPC errors to NodeAPIError with HTTP status codes."""
        if isinstance(error, asyncio.TimeoutError):
            raise NodeAPIError(-1, "Request timed out")
        elif isinstance(error, GRPCError):
            http_status = grpc_to_http_status(error.status)
            message = error.message or "Unknown gRPC error"
            raise NodeAPIError(http_status, message)
        elif isinstance(error, StreamTerminatedError):
            raise NodeAPIError(-1, f"Stream terminated: {error!s}")
        else:
            raise NodeAPIError(0, str(error))

    async def _handle_grpc_request(self, method, request, timeout: int | None = None):
        """Handle a gRPC request and convert errors to NodeAPIError."""
        timeout = timeout or self._internal_timeout
        try:
            return await asyncio.wait_for(method(request, metadata=self._metadata), timeout=timeout)
        except Exception as e:
            self._handle_error(e)

    async def start(
        self,
        config: str,
        backend_type: service.BackendType,
        users: list[service.User],
        keep_alive: int = 0,
        exclude_inbounds: list[str] | None = None,
        timeout: int | None = None,
    ) -> service.BaseInfoResponse | None:
        """Start the node with proper task management"""
        exclude_inbounds = exclude_inbounds or []
        timeout = timeout or self._default_timeout
        health = await self.get_health()
        if health is Health.INVALID:
            raise NodeAPIError(code=-4, detail="Invalid node")

        req = service.Backend(
            type=backend_type, config=config, users=users, keep_alive=keep_alive, exclude_inbounds=exclude_inbounds
        )

        lease = await self._acquire_lifecycle_lease(LifecycleOperation.START)
        try:
            async with self._node_lock:
                info: service.BaseInfoResponse = await self._handle_grpc_request(
                    method=self._client.Start,
                    request=req,
                    timeout=timeout,
                )

                if not info.started:
                    raise NodeAPIError(500, "Failed to start the node")

                try:
                    await self.connect(info.node_version, info.core_version)
                except Exception as e:
                    await self.disconnect()
                    self._handle_error(e)

                await self._release_lifecycle_lease(
                    lease,
                    LifecycleStatus.HEALTHY,
                    desired=LifecycleStatus.HEALTHY,
                    node_version=info.node_version,
                    core_version=info.core_version,
                )
                return info
        except BaseException:
            await self._release_lifecycle_lease(lease, LifecycleStatus.BROKEN, desired=LifecycleStatus.HEALTHY)
            raise

    async def stop(self, timeout: int | None = None) -> None:
        """Stop the node with proper cleanup"""
        timeout = timeout or self._default_timeout
        try:
            if await self.get_health() is Health.NOT_CONNECTED:
                return

            lease = await self._acquire_lifecycle_lease(LifecycleOperation.STOP)
            try:
                async with self._node_lock:
                    await self.disconnect()

                    try:
                        await self._handle_grpc_request(
                            method=self._client.Stop,
                            request=service.Empty(),
                            timeout=timeout,
                        )
                    except Exception as e:
                        self.logger.debug(
                            f"[{self.name}] Best-effort Stop request failed | Error: {type(e).__name__} - {e!s}"
                        )
                    await self._release_lifecycle_lease(
                        lease, LifecycleStatus.STOPPED, desired=LifecycleStatus.STOPPED
                    )
            except BaseException:
                await self._release_lifecycle_lease(lease, LifecycleStatus.BROKEN, desired=LifecycleStatus.STOPPED)
                raise
        finally:
            await self._json_client.close()

    async def info(self, timeout: int | None = None) -> service.BaseInfoResponse | None:
        timeout = timeout or self._default_timeout
        return await self._handle_grpc_request(
            method=self._client.GetBaseInfo,
            request=service.Empty(),
            timeout=timeout,
        )

    async def get_system_stats(self, timeout: int | None = None) -> service.SystemStatsResponse | None:
        timeout = timeout or self._default_timeout
        return await self._handle_grpc_request(
            method=self._client.GetSystemStats,
            request=service.Empty(),
            timeout=timeout,
        )

    async def get_backend_stats(self, timeout: int | None = None) -> service.BackendStatsResponse | None:
        timeout = timeout or self._default_timeout
        return await self._handle_grpc_request(
            method=self._client.GetBackendStats,
            request=service.Empty(),
            timeout=timeout,
        )

    async def get_stats(
        self, stat_type: service.StatType, reset: bool = True, name: str = "", timeout: int | None = None
    ) -> service.StatResponse | None:
        timeout = timeout or self._default_timeout
        return await self._handle_grpc_request(
            method=self._client.GetStats,
            request=service.StatRequest(reset=reset, name=name, type=stat_type),
            timeout=timeout,
        )

    async def get_outbounds_latency(self, name: str = "", timeout: int | None = None) -> service.LatencyResponse | None:
        timeout = timeout or self._default_timeout
        return await self._handle_grpc_request(
            method=self._client.GetOutboundsLatency,
            request=service.LatencyRequest(name=name),
            timeout=timeout,
        )

    async def get_user_online_stats(self, email: str, timeout: int | None = None) -> service.OnlineStatResponse | None:
        timeout = timeout or self._default_timeout
        return await self._handle_grpc_request(
            method=self._client.GetUserOnlineStats,
            request=service.StatRequest(name=email),
            timeout=timeout,
        )

    async def get_user_online_ip_list(
        self, email: str, timeout: int | None = None
    ) -> service.StatsOnlineIpListResponse | None:
        timeout = timeout or self._default_timeout
        return await self._handle_grpc_request(
            method=self._client.GetUserOnlineIpListStats,
            request=service.StatRequest(name=email),
            timeout=timeout,
        )

    async def sync_users(
        self, users: list[service.User], flush_pending: bool = False, timeout: int | None = None
    ) -> service.Empty | None:
        timeout = timeout or self._default_timeout
        if flush_pending:
            await self.flush_pending_users()

        async with self._node_lock:
            return await self._handle_grpc_request(
                method=self._client.SyncUsers,
                request=service.Users(users=users),
                timeout=timeout,
            )

    async def sync_users_chunked(
        self,
        users: list[service.User],
        chunk_size: int = 100,
        flush_pending: bool = False,
        timeout: int | None = None,
    ) -> list[service.User]:
        """Send users via the client-streaming SyncUsersChunked RPC. Returns failed users."""
        if chunk_size <= 0:
            raise NodeAPIError(code=-2, detail="chunk_size must be positive")

        timeout = timeout or self._default_timeout
        if flush_pending:
            await self.flush_pending_users()

        chunk_count = max(1, math.ceil(len(users) / chunk_size))
        budget = timeout + self._internal_timeout * (chunk_count + 2)
        async with self._node_lock:
            try:
                async with (
                    asyncio.timeout(budget),
                    self._client.SyncUsersChunked.open(metadata=self._metadata) as stream,
                ):
                    if not users:
                        await asyncio.wait_for(
                            stream.send_message(service.UsersChunk(index=0, last=True)),
                            timeout=self._internal_timeout,
                        )
                    else:
                        total_users = len(users)
                        for index, start in enumerate(range(0, total_users, chunk_size)):
                            chunk_users = users[start : start + chunk_size]
                            is_last = start + chunk_size >= total_users
                            await asyncio.wait_for(
                                stream.send_message(service.UsersChunk(users=chunk_users, index=index, last=is_last)),
                                timeout=self._internal_timeout,
                            )

                    await asyncio.wait_for(stream.end(), timeout=self._internal_timeout)
                    await asyncio.wait_for(stream.recv_message(), timeout=timeout)
                    return []
            except Exception as e:
                record_user_sync_failure(_user_rejection(e))
                error_type = type(e).__name__
                self.logger.warning(
                    f"[{self.name}] Chunked gRPC sync failed for {len(users)} user(s) | Error: {error_type} - {e!s}"
                )
                return users

    async def list_routing_rules(self, timeout: int | None = None) -> service.RoutingRulesResponse | None:
        timeout = timeout or self._default_timeout
        return await self._handle_grpc_request(
            method=self._client.ListRoutingRules,
            request=service.Empty(),
            timeout=timeout,
        )

    async def get_balancer_info(self, tag: str, timeout: int | None = None) -> service.BalancerInfoResponse | None:
        timeout = timeout or self._default_timeout
        return await self._handle_grpc_request(
            method=self._client.GetBalancerInfo,
            request=service.BalancerInfoRequest(tag=tag),
            timeout=timeout,
        )

    async def test_route(
        self,
        inbound_tag: str = "",
        network: str = "",
        target_ip: str = "",
        target_domain: str = "",
        target_port: int = 0,
        protocol: str = "",
        user: str = "",
        attributes: dict[str, str] | None = None,
        field_selectors: list[str] | None = None,
        publish_result: bool = False,
        timeout: int | None = None,
    ) -> service.RouteResult | None:
        timeout = timeout or self._default_timeout
        return await self._handle_grpc_request(
            method=self._client.TestRoute,
            request=service.TestRouteRequest(
                inbound_tag=inbound_tag,
                network=network,
                target_ip=target_ip,
                target_domain=target_domain,
                target_port=target_port,
                protocol=protocol,
                user=user,
                attributes=attributes or {},
                field_selectors=field_selectors or [],
                publish_result=publish_result,
            ),
            timeout=timeout,
        )

    async def add_backend(
        self,
        config: str,
        backend_type: service.BackendType,
        users: list[service.User],
        exclude_inbounds: list[str] = [],
        timeout: int | None = None,
    ) -> service.Empty | None:
        timeout = timeout or self._default_timeout
        if await self.get_health() is Health.INVALID:
            raise NodeAPIError(code=-4, detail="Invalid node")

        async with self._node_lock:
            return await self._handle_grpc_request(
                method=self._client.AddBackend,
                request=service.Backend(
                    type=backend_type,
                    config=config,
                    users=users,
                    exclude_inbounds=exclude_inbounds,
                ),
                timeout=timeout,
            )

    async def remove_backend(
        self, backend_type: service.BackendType, timeout: int | None = None
    ) -> service.Empty | None:
        timeout = timeout or self._default_timeout
        async with self._node_lock:
            return await self._handle_grpc_request(
                method=self._client.RemoveBackend,
                request=service.RemoveBackendRequest(type=backend_type),
                timeout=timeout,
            )

    async def list_backends(self, timeout: int | None = None) -> service.BackendList | None:
        timeout = timeout or self._default_timeout
        return await self._handle_grpc_request(
            method=self._client.ListBackends,
            request=service.Empty(),
            timeout=timeout,
        )

    async def add_routing_rule(
        self, rule: str, should_reset: bool = False, timeout: int | None = None
    ) -> service.Empty | None:
        timeout = timeout or self._default_timeout
        # Serialize state-changing routing calls under the node lock, like the other
        # mutating ops (start/stop/sync_users), so a rule change can't race a
        # concurrent core restart (read-only routing methods stay lock-free).
        async with self._node_lock:
            return await self._handle_grpc_request(
                method=self._client.AddRoutingRule,
                request=service.AddRoutingRuleRequest(rule=rule, should_reset=should_reset),
                timeout=timeout,
            )

    async def remove_routing_rule(self, rule_tag: str, timeout: int | None = None) -> service.Empty | None:
        timeout = timeout or self._default_timeout
        async with self._node_lock:
            return await self._handle_grpc_request(
                method=self._client.RemoveRoutingRule,
                request=service.RemoveRoutingRuleRequest(rule_tag=rule_tag),
                timeout=timeout,
            )

    async def override_balancer_target(
        self, balancer_tag: str, target: str, timeout: int | None = None
    ) -> service.Empty | None:
        timeout = timeout or self._default_timeout
        async with self._node_lock:
            return await self._handle_grpc_request(
                method=self._client.OverrideBalancerTarget,
                request=service.OverrideBalancerTargetRequest(balancer_tag=balancer_tag, target=target),
                timeout=timeout,
            )

    async def _sync_batch_users(self, users: list[service.User]) -> list[service.User]:
        """Sync users via gRPC SyncUser stream. Returns failed users."""
        failed = []
        budget = self._internal_timeout * (len(users) + 2)
        try:
            async with asyncio.timeout(budget), self._client.SyncUser.open(metadata=self._metadata) as stream:
                for user in users:
                    try:
                        await asyncio.wait_for(stream.send_message(user), timeout=self._internal_timeout)
                    except Exception as e:
                        record_user_sync_failure(None)
                        error_type = type(e).__name__
                        self.logger.warning(
                            f"[{self.name}] Failed to sync user {user.email} | Error: {error_type} - {e!s}"
                        )
                        failed.append(user)
                await asyncio.wait_for(stream.end(), timeout=self._internal_timeout)
        except Exception as e:
            # Stream-level failure - all users failed
            record_user_sync_failure(_user_rejection(e))
            error_type = type(e).__name__
            self.logger.error(f"[{self.name}] Stream failed | Error: {error_type} - {e!s}")
            return users
        return failed

    async def _check_node_health(self):
        """Health check task with proper cancellation handling"""
        health_check_interval = 10
        max_retries = 3
        retry_delay = 2
        retries = 0
        self.logger.debug(f"[{self.name}] Health check task started")

        try:
            while not self.is_shutting_down():
                last_health = await self.get_health()

                if last_health in (Health.NOT_CONNECTED, Health.INVALID):
                    self.logger.debug(f"[{self.name}] Health check task stopped due to node state: {last_health.name}")
                    return

                try:
                    await asyncio.wait_for(self.get_backend_stats(), timeout=10)
                    # Only update to HEALTHY if we were BROKEN or NOT_CONNECTED
                    if last_health in (Health.BROKEN, Health.NOT_CONNECTED):
                        self.logger.debug(f"[{self.name}] Node health is HEALTHY")
                        await self.set_health(Health.HEALTHY)
                    retries = 0
                except Exception as e:
                    retries += 1
                    error_type = type(e).__name__
                    if retries >= max_retries:
                        if last_health != Health.BROKEN:
                            self.logger.error(
                                f"[{self.name}] Health check failed after {max_retries} retries, setting health to BROKEN | "
                                f"Error: {error_type} - {e!s}"
                            )
                            await self.set_health(Health.BROKEN)
                    else:
                        self.logger.warning(
                            f"[{self.name}] Health check failed, retry {retries}/{max_retries} in {retry_delay}s | "
                            f"Error: {error_type} - {e!s}"
                        )
                        await asyncio.sleep(retry_delay)
                        continue

                try:
                    await asyncio.wait_for(asyncio.sleep(health_check_interval), timeout=health_check_interval + 1)
                except TimeoutError:
                    continue

        except asyncio.CancelledError:
            self.logger.debug(f"[{self.name}] Health check task cancelled")
        except Exception as e:
            error_type = type(e).__name__
            self.logger.exception(f"[{self.name}] Unexpected error in health check task | Error: {error_type}")
            try:
                await self.set_health(Health.BROKEN)
            except Exception as e_set_health:
                error_type_set = type(e_set_health).__name__
                self.logger.exception(f"[{self.name}] Failed to set health to BROKEN | Error: {error_type_set}")
        finally:
            self.logger.debug(f"[{self.name}] Health check task finished")

    @asynccontextmanager
    async def stream_logs(self, max_queue_size: int = 1000) -> AsyncGenerator[asyncio.Queue[str | NodeAPIError], None]:
        """Context manager for streaming logs on-demand.

        Yields a queue that receives log messages in real-time.
        The stream is automatically closed when the context exits.

        IMPORTANT: When an error occurs during log streaming, a NodeAPIError instance
        is placed in the queue. You must check the type of each item received from
        the queue and raise it if it's an error.

        Args:
            max_queue_size: Maximum size of the log queue

        Yields:
            asyncio.Queue containing log messages (str) or NodeAPIError on failure

        Raises:
            NodeAPIError: If the stream fails to open or encounters errors during operation

        Example:
            try:
                async with node.stream_logs() as log_queue:
                    while True:
                        item = await log_queue.get()
                        # Check if we received an error
                        if isinstance(item, NodeAPIError):
                            raise item
                        # Process the log message
                        print(f"LOG: {item}")
            except NodeAPIError as e:
                print(f"Log stream failed: {e.code} - {e.detail}")
                # Reconnect or handle error
        """
        log_queue: asyncio.Queue[str | NodeAPIError] = asyncio.Queue(maxsize=max_queue_size)
        stream_task = None

        async def _receive_logs(stream: Stream[service.Empty, service.Log]):
            """Receive log messages and put them in the queue."""
            try:
                while True:
                    log = await stream.recv_message()
                    if log is None:
                        break

                    try:
                        await log_queue.put(log.detail)
                    except asyncio.QueueFull:
                        # Drop oldest log if queue is full
                        try:
                            log_queue.get_nowait()
                            await log_queue.put(log.detail)
                        except (asyncio.QueueEmpty, asyncio.QueueFull):
                            pass
            except asyncio.CancelledError:
                self.logger.debug(f"[{self.name}] Log stream receive task cancelled")
                raise
            except StreamTerminatedError as e:
                # Stream was cancelled intentionally, this is expected during cleanup
                self.logger.debug(f"[{self.name}] Log stream terminated: {e!s}")
            except Exception as e:
                error_type = type(e).__name__
                self.logger.error(f"[{self.name}] Error receiving logs | Error: {error_type} - {e!s}")
                # Convert exception to NodeAPIError and put directly into log queue
                # so user gets immediate notification when reading
                try:
                    self._handle_error(e)
                except NodeAPIError as api_error:
                    try:
                        # Put error into log queue for immediate detection
                        log_queue.put_nowait(api_error)
                    except asyncio.QueueFull:
                        pass

        try:
            self.logger.debug(f"[{self.name}] Opening on-demand log stream")
            async with self._client.GetLogs.open(metadata=self._metadata) as stream:
                await stream.send_message(service.Empty())
                self.logger.debug(f"[{self.name}] On-demand log stream opened successfully")

                # Start background task to receive logs
                stream_task = asyncio.create_task(_receive_logs(stream))

                try:
                    # Yield the queue to the caller
                    yield log_queue

                    # After context exits, check if background task failed
                    if stream_task.done():
                        exc = stream_task.exception()
                        if exc and not isinstance(exc, asyncio.CancelledError):
                            self._handle_error(exc)
                finally:
                    # Cleanup: cancel the gRPC stream to unblock recv_message()
                    try:
                        await stream.cancel()
                    except Exception as e:
                        self.logger.debug(
                            f"[{self.name}] Failed to cancel gRPC stream | Error: {type(e).__name__} - {e!s}"
                        )

                    # Then cancel and wait for background task to finish
                    if stream_task and not stream_task.done():
                        stream_task.cancel()
                        try:
                            await asyncio.wait_for(stream_task, timeout=1.0)
                        except (TimeoutError, asyncio.CancelledError):
                            pass

        except NodeAPIError:
            # Already a NodeAPIError, re-raise as-is
            raise
        except Exception as e:
            error_type = type(e).__name__
            self.logger.error(f"[{self.name}] Failed to open log stream | Error: {error_type} - {e!s}")
            # Convert to NodeAPIError
            self._handle_error(e)
        finally:
            self.logger.debug(f"[{self.name}] On-demand log stream closed")
