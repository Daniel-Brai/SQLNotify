import logging
from abc import ABC, abstractmethod
from collections.abc import Callable
from typing import Any

from sqlalchemy.engine import Engine
from sqlalchemy.ext.asyncio import AsyncEngine

from ..watcher import Watcher


class BaseDialect(ABC):
    """
    Abstract base class for database dialect implementations
    """

    def __init__(
        self,
        async_engine: AsyncEngine | None,
        sync_engine: Engine | None,
        logger: logging.Logger | None = None,
        revoke_on_model_change: bool = True,
    ):
        self._async_engine = async_engine
        self._sync_engine = sync_engine
        self._logger = logger
        self._listen_conn: Any | None = None
        self.revoke_on_model_change = revoke_on_model_change

    @property
    @abstractmethod
    def name(self) -> str:
        """
        Return the dialect name (e.g., 'postgresql', 'mysql', 'sqlite')
        """
        pass

    @abstractmethod
    async def table_exists_async(self, schema: str, table: str) -> bool:
        """
        Check if a table exists in the database asynchronously
        """
        pass

    @abstractmethod
    def table_exists_sync(self, schema: str, table: str) -> bool:
        """
        Check if a table exists in the database synchronously
        """
        pass

    @abstractmethod
    async def create_trigger_async(self, watcher: Watcher) -> None:
        """
        Create database trigger asynchronously
        """
        pass

    @abstractmethod
    def create_trigger_sync(self, watcher: Watcher) -> None:
        """
        Create database trigger synchronously
        """
        pass

    @abstractmethod
    async def listen_async(
        self,
        watchers: list[Watcher],
        running_check: Callable[[], bool],
        handle_notification: Callable[[str, str], Any],
        database_url: str,
        listener_ready: Any | None = None,
    ) -> None:
        """
        Listen for database notifications asynchronously
        """
        pass

    @abstractmethod
    async def cleanup_async(self, watchers: list[Watcher]) -> None:
        """
        Remove all triggers and functions asynchronously
        """
        pass

    @abstractmethod
    def cleanup_sync(self, watchers: list[Watcher]) -> None:
        """
        Remove all triggers and functions synchronously
        """
        pass

    @abstractmethod
    async def notify_async(
        self,
        watcher: Watcher,
        payload: dict[str, Any],
        use_overflow_table: bool = False,
    ) -> None:
        """
        Send a notification asynchronously

        Args:
            watcher (Watcher): The watcher configuration
            payload (dict[str, Any]): The payload dict to send
            use_overflow_table (bool): If True, use overflow table for large payloads
        """
        pass

    @abstractmethod
    def notify_sync(
        self,
        watcher: Watcher,
        payload: dict[str, Any],
        use_overflow_table: bool = False,
    ) -> None:
        """
        Send a notification synchronously

        Args:
            watcher (Watcher): The watcher configuration
            payload (dict[str, Any]): The payload dict to send
            use_overflow_table (bool): If True, use overflow table for large payloads
        """
        pass

    @abstractmethod
    async def store_overflow_async(self, watcher: Watcher, payload_str: str) -> str:
        """
        Store large payload in overflow table asynchronously
        """
        pass

    @abstractmethod
    def store_overflow_sync(self, watcher: Watcher, payload_str: str) -> str:
        """
        Store large payload in overflow table synchronously
        """
        pass

    @abstractmethod
    async def fetch_overflow_async(self, watcher: Watcher, overflow_id: int) -> dict[str, Any] | None:
        """
        Fetch and consume overflow payload asynchronously
        """
        pass

    @abstractmethod
    def fetch_overflow_sync(self, watcher: Watcher, overflow_id: int) -> dict[str, Any] | None:
        """
        Fetch and consume overflow payload synchronously
        """
        pass

    @abstractmethod
    def build_watcher_sql(self, watcher: Watcher) -> tuple[str, str]:
        """
        Build SQL for trigger and function creation
        """
        pass

    @abstractmethod
    def create_overflow_table_sql(self, schema: str) -> str:
        """
        Generate SQL to create overflow table
        """
        pass

    async def stop_listening(self) -> None:
        """
        Clean up listening connection
        """

        if self._listen_conn is not None:
            try:
                await self._listen_conn.close()
                self._listen_conn = None
                if self._logger:
                    self._logger.debug("Closed listen connection")
            except Exception as e:
                if self._logger:
                    self._logger.debug(f"Error closing listen connection: {str(e)}")
