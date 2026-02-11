import asyncio
import sys

if sys.version_info < (3, 13):

    class QueueShutDown(Exception):
        """Placeholder for asyncio.QueueShutDown in <3.13"""

        pass

else:
    from asyncio import QueueShutDown

import json
from collections import defaultdict
from collections.abc import Callable
from typing import Any

from sqlalchemy import text

from ..constants import MAX_SQLNOTIFY_EVENT_RETRIES, MAX_SQLNOTIFY_PAYLOAD_BYTES, PACKAGE_NAME
from ..exceptions import SQLNotifyConfigurationError
from ..watcher import Watcher
from .base import BaseDialect


class SQLiteDialect(BaseDialect):
    """
    SQLite dialect for the `Notifier` using a polling and trigger based approach with an in-memory queue

    Since SQLite doesn't have native LISTEN/NOTIFY like PostgreSQL, this implementation uses:

    - It triggers an insert into a persistent queue table on data changes
    - It uses fast polling (10ms) to detect new notifications with a max polling interval of 250ms when idle to reduce overhead
    - Also, it uses a in-memory asyncio.Queue for instant fan out to multiple subscribers
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._base_poll_interval = 0.01
        self._max_poll_interval = 0.25
        self._current_poll_interval = self._base_poll_interval
        self._backoff_multiplier = 1.5

        self._message_queues: dict[str, asyncio.Queue[Any]] = defaultdict(asyncio.Queue)
        self._notification_queue_table = f"{PACKAGE_NAME}_notification_queue"
        self._running = False
        self._poll_task: asyncio.Task | None = None

    @property
    def name(self) -> str:
        return "sqlite"

    def _get_table_reference(self, watcher: Watcher) -> str:
        """
        Get table reference for SQLite

        Args:
            watcher (Watcher): The watcher configuration

        Returns:
            str: Table name without schema prefix
        """

        return watcher.table_name

    async def table_exists_async(self, schema: str, table: str) -> bool:  # noqa: ARG002
        """
        Check if a table exists in the database asynchronously

        Args:
            schema (str): The schema name (unused in SQLite)
            table (str): The table name

        Returns:
            bool: True if the table exists, False otherwise
        """

        if not self._async_engine:
            raise SQLNotifyConfigurationError("Async engine is required for async operations")

        query = """
            SELECT EXISTS (
                SELECT 1 FROM sqlite_master
                WHERE type = 'table'
                AND name = :table
            )
        """

        async with self._async_engine.begin() as conn:
            result = await conn.execute(text(query), {"table": table})
            return bool(result.scalar())

    def table_exists_sync(self, schema: str, table: str) -> bool:  # noqa: ARG002
        """
        Check if a table exists in the database synchronously

        Args:
            schema (str): The schema name (unused in SQLite)
            table (str): The table name

        Returns:
            bool: True if the table exists, False otherwise
        """

        if not self._sync_engine:
            raise SQLNotifyConfigurationError("Sync engine is required for sync operations")

        query = """
            SELECT EXISTS (
                SELECT 1 FROM sqlite_master
                WHERE type = 'table'
                AND name = :table
            )
        """

        with self._sync_engine.begin() as conn:
            result = conn.execute(text(query), {"table": table})
            return bool(result.scalar())

    async def _ensure_notification_queue_async(self) -> None:
        """
        Ensure the notification queue table exists asynchronously
        """

        if not self._async_engine:
            raise SQLNotifyConfigurationError("Async engine is required for async operations")

        create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {self._notification_queue_table} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                channel_name TEXT NOT NULL,
                payload TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """

        async with self._async_engine.begin() as conn:
            await conn.execute(text(create_table_sql))

        if self._logger:
            self._logger.debug(f"Ensured notification queue table '{self._notification_queue_table}' exists")

    def _ensure_notification_queue_sync(self) -> None:
        """
        Ensure the notification queue table exists synchronously
        """

        if not self._sync_engine:
            raise SQLNotifyConfigurationError("Sync engine is required for sync operations")

        create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {self._notification_queue_table} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                channel_name TEXT NOT NULL,
                payload TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """

        with self._sync_engine.begin() as conn:
            conn.execute(text(create_table_sql))

        if self._logger:
            self._logger.debug(f"Ensured notification queue table '{self._notification_queue_table}' exists")

    async def create_trigger_async(self, watcher: Watcher) -> None:
        """
        Create SQLite trigger asynchronously

        Args:
            watcher (Watcher): The Watcher configuration object

        Raises:
            SQLNotifyConfigurationError: If async engine is not available
        """

        if not self._async_engine:
            raise SQLNotifyConfigurationError("Async engine is required for async operations")

        await self._ensure_notification_queue_async()

        if watcher.use_overflow_table:
            # For SQLite, ignore schema and use table name directly
            overflow_table_exists = await self.table_exists_async("", watcher.overflow_table_name)
            if not overflow_table_exists:
                create_overflow_sql = self.create_overflow_table_sql("")
                async with self._async_engine.begin() as conn:
                    await conn.execute(text(create_overflow_sql))

        try:
            _, trigger_sql = self.build_watcher_sql(watcher)

            async with self._async_engine.begin() as conn:
                if self.revoke_on_model_change:
                    drop_trigger_sql = f"DROP TRIGGER IF EXISTS {watcher.trigger_name}"
                    await conn.execute(text(drop_trigger_sql))

                await conn.execute(text(trigger_sql))

            if self._logger:
                self._logger.info(
                    f"Created trigger '{watcher.trigger_name}' for table '{watcher.table_name}' "
                    f"on {watcher.operation.value.upper()} operation"
                )

        except Exception as e:
            if self._logger:
                self._logger.error(
                    f"Failed to create trigger for {watcher.table_name}: {str(e)}",
                    exc_info=True,
                )
            raise SQLNotifyConfigurationError("Failed to create SQLNotify trigger") from e

    def create_trigger_sync(self, watcher: Watcher) -> None:
        """
        Create SQLite trigger synchronously

        Args:
            watcher (Watcher): The Watcher configuration object
        """

        if not self._sync_engine:
            raise SQLNotifyConfigurationError("Sync engine is required for sync operations")

        self._ensure_notification_queue_sync()

        if watcher.use_overflow_table:
            overflow_table_exists = self.table_exists_sync("", watcher.overflow_table_name)
            if not overflow_table_exists:
                create_overflow_sql = self.create_overflow_table_sql("")
                with self._sync_engine.begin() as conn:
                    conn.execute(text(create_overflow_sql))

        try:
            _, trigger_sql = self.build_watcher_sql(watcher)

            with self._sync_engine.begin() as conn:
                if self.revoke_on_model_change:
                    drop_trigger_sql = f"DROP TRIGGER IF EXISTS {watcher.trigger_name}"
                    conn.execute(text(drop_trigger_sql))

                conn.execute(text(trigger_sql))

            if self._logger:
                self._logger.info(
                    f"Created trigger '{watcher.trigger_name}' for table '{watcher.table_name}' "
                    f"on {watcher.operation.value.upper()} operation"
                )
        except Exception as e:
            if self._logger:
                self._logger.error(
                    f"Failed to create trigger for {watcher.table_name}: {str(e)}",
                    exc_info=True,
                )
            raise SQLNotifyConfigurationError("Failed to create SQLNotify trigger") from e

    async def listen_async(
        self,
        watchers: list[Watcher],
        running_check: Callable[[], bool],
        handle_notification: Callable[[str, str], Any],
        database_url: str,  # noqa: ARG002
        listener_ready: asyncio.Event | None = None,
    ) -> None:
        """
        Listen for notifications on the specified channels asynchronously

        Args:
            watchers (List[Watcher]): List of watchers
            running_check (Callable[[], bool]): Function to check if still running
            handle_notification (Callable[[str, str], Any]): Callback for handling notifications
            database_url (str): Database URL (unused for SQLite polling)
            listener_ready (Optional[asyncio.Event]): Event to signal when listener is ready

        Raises:
            SQLNotifyConfigurationError: If async engine is not available
        """

        if not self._async_engine:
            raise SQLNotifyConfigurationError("Async engine is required for async operations")

        channel_names = [w.channel_name for w in watchers]

        self._running = True

        dispatcher_tasks: list[asyncio.Task] = []
        for channel in channel_names:
            task = asyncio.create_task(self._dispatch_channel(channel, handle_notification))
            dispatcher_tasks.append(task)

        if self._logger:
            self._logger.info(f"Started pub/sub dispatcher for channels: {channel_names}")

        if listener_ready is not None:
            listener_ready.set()

        try:
            empty_polls = 0

            while running_check() and self._running:
                try:
                    async with self._async_engine.begin() as conn:
                        placeholders = ",".join([f":chan_{i}" for i in range(len(channel_names))])
                        query = f"""
                            SELECT id, channel_name, payload
                            FROM {self._notification_queue_table}
                            WHERE channel_name IN ({placeholders})
                            ORDER BY id ASC
                            LIMIT 100
                        """

                        params = {f"chan_{i}": cn for i, cn in enumerate(channel_names)}
                        result = await conn.execute(text(query), params)
                        notifications = result.all()

                        if notifications:
                            notification_ids = [n[0] for n in notifications]
                            id_placeholders = ",".join([f":id_{i}" for i in range(len(notification_ids))])

                            delete_query = f"""
                                DELETE FROM {self._notification_queue_table}
                                WHERE id IN ({id_placeholders})
                            """

                            id_params = {f"id_{i}": nid for i, nid in enumerate(notification_ids)}

                            await conn.execute(text(delete_query), id_params)

                            for notification in notifications:
                                channel_name = notification[1]
                                payload = notification[2]

                                max_retries = MAX_SQLNOTIFY_EVENT_RETRIES
                                retry_delay = 0.01

                                for attempt in range(max_retries):
                                    try:
                                        await self._message_queues[channel_name].put(payload)
                                        break
                                    except QueueShutDown:
                                        if self._logger:
                                            self._logger.error(
                                                f"Queue for channel '{channel_name}' has been shut down. "
                                                f"Notification lost: {json.dumps(payload)[:100] if len(json.dumps(payload)) > 100 else json.dumps(payload)}"
                                            )
                                        break
                                    except asyncio.QueueFull:
                                        if attempt < max_retries - 1:
                                            if self._logger:
                                                self._logger.warning(
                                                    f"Queue full for channel '{channel_name}', "
                                                    f"retrying in {retry_delay}s (attempt {attempt + 1}/{max_retries})"
                                                )
                                            await asyncio.sleep(retry_delay)
                                            retry_delay *= 2
                                        else:
                                            if self._logger:
                                                self._logger.error(
                                                    f"Failed to add notification to queue after {max_retries} attempts. "
                                                    f"Channel: '{channel_name}', Payload: {json.dumps(payload)[:100] if len(json.dumps(payload)) > 100 else json.dumps(payload)}"
                                                )
                                    except Exception as e:
                                        if attempt < max_retries - 1:
                                            if self._logger:
                                                self._logger.warning(
                                                    f"Error adding to queue for channel '{channel_name}': {str(e)}. "
                                                    f"Retrying in {retry_delay}s (attempt {attempt + 1}/{max_retries})"
                                                )
                                            await asyncio.sleep(retry_delay)
                                            retry_delay *= 2
                                        else:
                                            if self._logger:
                                                self._logger.error(
                                                    f"Failed to add notification to queue after {max_retries} attempts. "
                                                    f"Channel: '{channel_name}', Error: {str(e)}, "
                                                    f"Payload: {json.dumps(payload)[:100] if len(json.dumps(payload)) > 100 else json.dumps(payload)}",
                                                    exc_info=True,
                                                )

                            empty_polls = 0
                            self._current_poll_interval = self._base_poll_interval

                            if self._logger:
                                self._logger.debug(f"Dispatched {len(notifications)} notifications to queues")
                        else:
                            empty_polls += 1
                            if empty_polls > 3:
                                self._current_poll_interval = min(
                                    self._current_poll_interval * self._backoff_multiplier,
                                    self._max_poll_interval,
                                )

                    await asyncio.sleep(self._current_poll_interval)

                except Exception as e:
                    if self._logger:
                        self._logger.error(f"Error during polling: {str(e)}", exc_info=True)
                    await asyncio.sleep(self._current_poll_interval)

        finally:
            for task in dispatcher_tasks:
                task.cancel()

            await asyncio.gather(*dispatcher_tasks, return_exceptions=True)

            if self._logger:
                self._logger.info("Stopped pub/sub dispatcher")

    async def _dispatch_channel(self, channel: str, handler: Callable[[str, str], Any]) -> None:
        """
        Dispatches the function that fans out messages from queue to handler

        This runs continuously and processes messages as they arrive in the queue
        Multiple subscribers to the same channel all get notified

        Args:
            channel (str): Channel name to dispatch
            handler (Callable): Handler function to call with (channel, payload)
        """

        queue = self._message_queues[channel]

        while self._running:
            try:
                payload = await queue.get()

                asyncio.create_task(handler(channel, payload))

            except asyncio.CancelledError:
                break
            except Exception as e:
                if self._logger:
                    self._logger.error(
                        f"Error in dispatcher for channel '{channel}': {str(e)}",
                        exc_info=True,
                    )

    async def cleanup_async(self, watchers: list[Watcher]) -> None:
        """
        Remove all triggers asynchronously

        Args:
            watchers (List[Watcher]): List of watchers to clean up
        """

        if not self._async_engine:
            raise SQLNotifyConfigurationError("Async engine is required for async operations")

        async with self._async_engine.begin() as conn:
            for watcher in watchers:
                try:
                    drop_trigger_sql = f"DROP TRIGGER IF EXISTS {watcher.trigger_name}"
                    await conn.execute(text(drop_trigger_sql))

                    if self._logger:
                        self._logger.info(f"Dropped trigger '{watcher.trigger_name}'")

                except Exception as e:
                    if self._logger:
                        self._logger.error(f"Failed to drop trigger '{watcher.trigger_name}': {str(e)}")

    def cleanup_sync(self, watchers: list[Watcher]) -> None:
        """
        Remove all triggers synchronously

        Note: This internally calls the async version using asyncio.run()

        Args:
            watchers (List[Watcher]): List of watchers to clean up
        """

        if not self._sync_engine:
            raise SQLNotifyConfigurationError("Sync engine is required for sync operations")

        with self._sync_engine.begin() as conn:
            for watcher in watchers:
                try:
                    drop_trigger_sql = f"DROP TRIGGER IF EXISTS {watcher.trigger_name}"
                    conn.execute(text(drop_trigger_sql))

                    if self._logger:
                        self._logger.info(f"Dropped trigger '{watcher.trigger_name}'")

                except Exception as e:
                    if self._logger:
                        self._logger.error(f"Failed to drop trigger '{watcher.trigger_name}': {str(e)}")

    async def notify_async(
        self,
        watcher: Watcher,
        payload: dict[str, Any],
        use_overflow_table: bool = False,
    ) -> None:
        """
        Send a notification by inserting into the queue table asynchronously

        Args:
            watcher (Watcher): The watcher configuration
            payload (dict[str, Any]): The payload dict to send
            use_overflow_table (bool): If True, use overflow table for large payloads
        """

        if not self._async_engine:
            raise SQLNotifyConfigurationError("Async engine is required for async operations")

        payload_str = json.dumps(payload)

        # NOTE: For SQLite, we don't have a hard size limit like PostgreSQL but we are just going to hard limit that is why overflow handling is available in this dialect as well to avoid issues with very large payloads
        if use_overflow_table and len(payload_str) > MAX_SQLNOTIFY_PAYLOAD_BYTES:
            overflow_id = await self.store_overflow_async(watcher, payload_str)
            payload_str = json.dumps({"overflow_id": overflow_id})

        insert_sql = f"""
            INSERT INTO {self._notification_queue_table} (channel_name, payload)
            VALUES (:channel_name, :payload)
        """

        async with self._async_engine.begin() as conn:
            await conn.execute(
                text(insert_sql),
                {"channel_name": watcher.channel_name, "payload": payload_str},
            )

    def notify_sync(
        self,
        watcher: Watcher,
        payload: dict[str, Any],
        use_overflow_table: bool = False,
    ) -> None:
        """
        Send a notification by inserting into the queue table synchronously

        Note: This internally calls the async version using asyncio.run()

        Args:
            watcher (Watcher): The watcher configuration
            payload (dict[str, Any]): The payload dict to send
            use_overflow_table (bool): If True, use overflow table for large payloads
        """

        if not self._sync_engine:
            raise SQLNotifyConfigurationError("Sync engine is required for sync operations")

        payload_str = json.dumps(payload)

        if use_overflow_table and len(payload_str) > MAX_SQLNOTIFY_PAYLOAD_BYTES:
            overflow_id = asyncio.run(self.store_overflow_async(watcher, payload_str))
            payload_str = json.dumps({"overflow_id": overflow_id})

        insert_sql = f"""
            INSERT INTO {self._notification_queue_table} (channel_name, payload)
            VALUES (:channel_name, :payload)
        """

        with self._sync_engine.begin() as conn:
            conn.execute(
                text(insert_sql),
                {"channel_name": watcher.channel_name, "payload": payload_str},
            )

    async def store_overflow_async(self, watcher: Watcher, payload_str: str) -> str:
        """
        Store large payload in overflow table asynchronously

        Args:
            watcher (Watcher): The watcher configuration
            payload_str (str): The payload string to store

        Returns:
            str: The overflow ID
        """

        if not self._async_engine:
            raise SQLNotifyConfigurationError("Async engine is required for async operations")

        overflow_table_exists = await self.table_exists_async("", watcher.overflow_table_name)

        if not overflow_table_exists:
            create_overflow_sql = self.create_overflow_table_sql("")
            async with self._async_engine.begin() as conn:
                await conn.execute(text(create_overflow_sql))

        insert_sql = f"""
            INSERT INTO {watcher.overflow_table_name} (channel_name, payload, created_at)
            VALUES (:channel_name, :payload, datetime('now'))
        """

        async with self._async_engine.begin() as conn:
            result = await conn.execute(
                text(insert_sql),
                {"channel_name": watcher.channel_name, "payload": payload_str},
            )
            overflow_id = result.lastrowid

        return str(overflow_id)

    def store_overflow_sync(self, watcher: Watcher, payload_str: str) -> str:
        """
        Store large payload in overflow table synchronously

        Note: This internally calls the async version using asyncio.run()

        Args:
            watcher (Watcher): The watcher configuration
            payload_str (str): The payload string to store

        Returns:
            str: The overflow ID
        """

        if not self._sync_engine:
            raise SQLNotifyConfigurationError("Sync engine is required for sync operations")

        overflow_table_exists = self.table_exists_sync("", watcher.overflow_table_name)
        if not overflow_table_exists:
            create_overflow_sql = self.create_overflow_table_sql("")
            with self._sync_engine.begin() as conn:
                conn.execute(text(create_overflow_sql))

        insert_sql = f"""
            INSERT INTO {watcher.overflow_table_name} (channel_name, payload, created_at)
            VALUES (:channel_name, :payload, datetime('now'))
        """

        with self._sync_engine.begin() as conn:
            result = conn.execute(
                text(insert_sql),
                {"channel_name": watcher.channel_name, "payload": payload_str},
            )
            overflow_id = result.lastrowid

        return str(overflow_id)

    async def fetch_overflow_async(self, watcher: Watcher, overflow_id: int) -> dict[str, Any] | None:
        """
        Fetch and consume overflow payload asynchronously

        Args:
            watcher (Watcher): The watcher configuration
            overflow_id (int): The overflow ID to fetch

        Returns:
            Optional[dict[str, Any]]: The payload dict or None if not found
        """

        if not self._async_engine:
            raise SQLNotifyConfigurationError("Async engine is required for async operations")

        select_sql = f"""
            SELECT payload FROM {watcher.overflow_table_name}
            WHERE id = :overflow_id
        """

        delete_sql = f"""
            DELETE FROM {watcher.overflow_table_name}
            WHERE id = :overflow_id
        """

        async with self._async_engine.begin() as conn:
            result = await conn.execute(text(select_sql), {"overflow_id": overflow_id})
            row = result.one()

            if row:
                payload_str = row[0]
                await conn.execute(text(delete_sql), {"overflow_id": overflow_id})
                return json.loads(payload_str)

        return None

    def fetch_overflow_sync(self, watcher: Watcher, overflow_id: int) -> dict[str, Any] | None:
        """
        Fetch and consume overflow payload synchronously

        Args:
            watcher (Watcher): The watcher configuration
            overflow_id (int): The overflow ID to fetch

        Returns:
            Optional[dict[str, Any]]: The payload dict or None if not found
        """

        if not self._sync_engine:
            raise SQLNotifyConfigurationError("Sync engine is required for sync operations")

        select_sql = f"""
            SELECT payload FROM {watcher.overflow_table_name}
            WHERE id = :overflow_id
        """

        delete_sql = f"""
            DELETE FROM {watcher.overflow_table_name}
            WHERE id = :overflow_id
        """

        with self._sync_engine.begin() as conn:
            result = conn.execute(text(select_sql), {"overflow_id": overflow_id})
            row = result.one_or_none()

            if row:
                payload_str = row[0]
                conn.execute(text(delete_sql), {"overflow_id": overflow_id})
                return json.loads(payload_str)

        return None

    def build_watcher_sql(self, watcher: Watcher) -> tuple[str, str]:
        """
        Build SQL for SQLite trigger creation

        Args:
            watcher (Watcher): The watcher configuration

        Returns:
            tuple[str, str]: Trigger SQL and empty string (SQLite doesn't need separate function)
        """

        operation_prefix = {
            "insert": "AFTER INSERT",
            "update": "AFTER UPDATE",
            "delete": "AFTER DELETE",
        }

        op_prefix = operation_prefix.get(watcher.operation.value)

        row_alias = "OLD" if watcher.operation.value == "delete" else "NEW"

        json_parts = []

        for pk in watcher.primary_keys:
            json_parts.append(f"'{pk}', {row_alias}.{pk}")

        for col in watcher.extra_columns:
            json_parts.append(f"'{col}', {row_alias}.{col}")

        json_columns = ", ".join(json_parts) if json_parts else "'__empty__', null"

        where_clause = ""
        if watcher.operation.value == "update" and watcher.trigger_columns:
            conditions = [f"OLD.{col} IS NOT NEW.{col}" for col in watcher.trigger_columns]
            where_clause = f"WHEN {' OR '.join(conditions)}"

        payload_json = f"json_object({json_columns})"

        trigger_sql = f"""
            CREATE TRIGGER {watcher.trigger_name}
            {op_prefix} ON {self._get_table_reference(watcher)}
            FOR EACH ROW
            {where_clause}
            BEGIN
                INSERT INTO {self._notification_queue_table} (channel_name, payload)
                VALUES ('{watcher.channel_name}', {payload_json});
            END
        """

        return "", trigger_sql.strip()

    def _build_pk_value(self, watcher: Watcher, row_alias: str) -> str:
        """
        Build the primary key value expression for SQLite

        I create a new json array with the primary key values

        Args:
            watcher (Watcher): The watcher configuration
            row_alias (str): Either 'NEW' or 'OLD'

        Returns:
            str: SQL expression for the primary key value
        """

        if len(watcher.primary_keys) == 1:
            return f"{row_alias}.{watcher.primary_keys[0]}"
        else:
            pk_values = [f"{row_alias}.{pk}" for pk in watcher.primary_keys]
            return f"json_array({', '.join(pk_values)})"

    def create_overflow_table_sql(self, schema: str) -> str:  # noqa: ARG002
        """
        Generate SQL to create overflow table for SQLite

        Args:
            schema (str): Schema name (unused in SQLite)

        Returns:
            str: SQL statement to create overflow table
        """

        return f"""
            CREATE TABLE IF NOT EXISTS {PACKAGE_NAME}_overflow (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                channel_name TEXT NOT NULL,
                payload TEXT NOT NULL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """
