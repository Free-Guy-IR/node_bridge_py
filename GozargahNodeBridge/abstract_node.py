from abc import ABC, abstractmethod
from typing import List, Optional
import asyncio

from GozargahNodeBridge.common import service_pb2 as service
from GozargahNodeBridge.controller import Controller, Health


class GozargahNode(Controller, ABC):
    @abstractmethod
    async def start(
        self, config: str, backend_type: service.BackendType, users: List[service.User]
    ) -> service.BaseInfoResponse | None:
        raise NotImplementedError

    @abstractmethod
    async def stop(self) -> None:
        raise NotImplementedError

    @abstractmethod
    async def info(self) -> service.BaseInfoResponse | None:
        raise NotImplementedError

    @abstractmethod
    async def get_system_stats(self) -> service.SystemStatsResponse | None:
        raise NotImplementedError

    @abstractmethod
    async def get_backend_stats(self) -> service.BackendStatsResponse | None:
        raise NotImplementedError

    @abstractmethod
    async def get_outbounds_stats(self, reset: bool = True) -> service.StatResponse | None:
        raise NotImplementedError

    @abstractmethod
    async def get_outbound_stats(self, tag: str, reset: bool = True) -> service.StatResponse | None:
        raise NotImplementedError

    @abstractmethod
    async def get_inbounds_stats(self, reset: bool = True) -> service.StatResponse | None:
        raise NotImplementedError

    @abstractmethod
    async def get_inbound_stats(self, tag: str, reset: bool = True) -> service.StatResponse | None:
        raise NotImplementedError

    @abstractmethod
    async def get_users_stats(self, reset: bool = True) -> service.StatResponse | None:
        raise NotImplementedError

    @abstractmethod
    async def get_user_stats(self, email: str, reset: bool = True) -> service.StatResponse | None:
        raise NotImplementedError

    @abstractmethod
    async def sync_users(self, users: List[service.User]) -> service.Empty | None:
        raise NotImplementedError

    @abstractmethod
    async def _check_node_health(self):
        raise NotImplementedError

    @abstractmethod
    async def _fetch_logs(self):
        raise NotImplementedError

    @abstractmethod
    async def _sync_user(self):
        raise NotImplementedError
