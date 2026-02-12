import asyncio
import inspect
import json
from typing import Any

from sqlalchemy.engine import Engine
from sqlalchemy.ext.asyncio import AsyncEngine

from ..dialects import BaseDialect, get_dialect_for_engine
from ..exceptions import SQLNotifyConfigurationError
from ..logger import get_logger
from ..types import ChangeEvent, Operation
from ..utils import extract_database_url, strip_database_query_params, wrap_unhandled_error
from ..watcher import Watcher
from .base import BaseNotifier


class Notifier(BaseNotifier):
    """
    Standalone notifier for watching database changes and sending notifications to subscribers

    It supports multiple database dialects through a pluggable dialect system

    Currently supported database: PostgreSQL, SQLite

    Examples:

        #### Using an existing async engine ####
        from sqlalchemy.ext.asyncio import create_async_engine

        engine = create_async_engine("postgresql+asyncpg://user:pass@localhost/db")

        notifier = Notifier(db_engine=engine)

        # Register watchers for models and operations
        # By default `extra_columns` is None which means only primary key(s) are included in events
        notifier.watch(User, Operation.INSERT)
        notifier.watch(User, Operation.UPDATE, extra_columns=["email"])

        # With custom primary key
        notifier.watch(Account, Operation.INSERT, extra_columns=["balance"], primary_keys=["account_id"])

        #### Note: subscriber callbacks can be async or sync regardless of engine type
        #### we can use User or "User" as the model identifier in the subscribe decorator

        @notifier.subscribe(User, Operation.INSERT)
        async def on_user_insert(event: ChangeEvent):
            print(f"User {event.id} inserted")

        # In async context if  we are not using the lifespan helper
        await notifier.astart()
        await notifier.astop()


        #### Using an existing sync engine ####
        from sqlalchemy import create_engine

        engine = create_engine("postgresql+psycopg2://user:pass@localhost/db")

        notifier = Notifier(db_engine=engine)

        # Register watchers
        notifier.watch(User, Operation.UPDATE, extra_columns=["email", "name"])

        # No extra columns which is the default (omit `extra_columns`)
        # Used when you only care about the primary key (event id)
        notifier.watch(Post, Operation.INSERT)

        @notifier.subscribe(User, Operation.UPDATE)
        def on_user_update(event: ChangeEvent):
            print(f"User {event.id} updated")

        # In sync context if we are not using the lifespan helper
        notifier.start()
        notifier.stop()
    """

    def __init__(
        self,
        db_engine: AsyncEngine | Engine,
        revoke_on_model_change: bool = True,
        cleanup_on_start: bool = False,
        use_logger: bool = True,
    ) -> None:
        """
        Initialize the Notifier

        Args:
            db_engine (Union[AsyncEngine, Engine]): SQLAlchemy Engine or AsyncEngine to use for database interactions
            revoke_on_model_change (bool): If True, drops existing triggers and functions when a model changes. Defaults to True.
            cleanup_on_start (bool): If True, will remove existing triggers/functions on startup to ensure a clean state. Defaults to False.
            use_logger (bool): If True, enables logging of important events and errors. Defaults to True.

        Raises:
            SQLNotifyConfigurationError: If there is a configuration issue with the Notifier setup
            SQLNotifyUnSupportedDatabaseProviderError: If the database dialect is not supported.

        Returns:
            Notifier: An instance of the Notifier class
        """

        super().__init__()  # Raises a SQLNotifyConfigurationError if not valid

        self._async_engine: AsyncEngine | None = None
        self._sync_engine: Engine | None = None
        self._logger = get_logger(enabled=use_logger)
        self.cleanup_on_start = cleanup_on_start

        if isinstance(db_engine, AsyncEngine):
            self._async_engine = db_engine
            self.async_mode = True
        else:
            self._sync_engine = db_engine
            self.async_mode = False

        self._database_url = extract_database_url(db_engine)

        self._dialect: BaseDialect = get_dialect_for_engine(
            engine=db_engine,
            async_engine=self._async_engine,
            sync_engine=self._sync_engine,
            logger=self._logger,
            revoke_on_model_change=revoke_on_model_change,
        )

    @property
    def dialect(self) -> BaseDialect:
        return self._dialect

    @property
    def dialect_name(self) -> str:
        return self._dialect.name

    @property
    def is_running(self) -> bool:
        return self._running

    @wrap_unhandled_error(lambda self: self._logger)
    def start(self) -> None:
        """
        Start watching for database changes synchronously.

        Use this method when working with a sync engine.
        For async engines, use astart() instead.

        Raises:
            SQLNotifyConfigurationError: If called with an async engine
        """

        if self.async_mode:
            raise SQLNotifyConfigurationError("Cannot use start() with async engine. Use astart() instead.")

        self._start_sync()

    @wrap_unhandled_error(lambda self: self._logger)
    async def astart(self) -> None:
        """
        Start watching for database changes asynchronously.

        Use this method when working with an async engine.
        For sync engines, use start() instead.

        Raises:
            SQLNotifyConfigurationError: If called with a sync engine
        """

        if not self.async_mode:
            raise SQLNotifyConfigurationError("Cannot use astart() with sync engine. Use start() instead.")

        await self._start_async()

    async def _start_async(self) -> None:

        if self._running:
            if self._logger:
                self._logger.warning("SQLNotify Notifier is already running")
            return

        self._running = True
        self._listener_ready = asyncio.Event()

        for watcher in self.watchers:
            await self._dialect.create_trigger_async(watcher)

        self._listening_task = asyncio.create_task(self._listen_async())

        await asyncio.wait_for(self._listener_ready.wait(), timeout=5.0)

        await asyncio.sleep(0.1)

        if self._logger:
            self._logger.info(f"SQLNotify Notifier started (async mode, dialect: {self.dialect_name})")

    def _start_sync(self) -> None:
        if self._running:
            if self._logger:
                self._logger.warning("SQLNotify Notifier is already running")
            return

        self._running = True

        for watcher in self.watchers:
            self._dialect.create_trigger_sync(watcher)

        if self._logger:
            self._logger.info(f"SQLNotify Notifier started (sync mode, dialect: {self.dialect_name})")

    @wrap_unhandled_error(lambda self: self._logger)
    def stop(self) -> None:
        """
        Stop watching for database changes synchronously

        Use this method when working with a sync engine.
        For async engines, use astop() instead.

        Raises:
            SQLNotifyConfigurationError: If called with an async engine
        """

        if self.async_mode:
            raise SQLNotifyConfigurationError("Cannot use stop() with async engine. Use astop() instead.")

        self._stop_sync()

    @wrap_unhandled_error(lambda self: self._logger)
    async def astop(self) -> None:
        """
        Stop watching for database changes asynchronously.

        Use this method when working with an async engine.
        For sync engines, use stop() instead.

        Raises:
            SQLNotifyConfigurationError: If called with a sync engine
        """

        if not self.async_mode:
            raise SQLNotifyConfigurationError("Cannot use astop() with sync engine. Use stop() instead.")

        await self._stop_async()

    async def _stop_async(self) -> None:

        self._running = False

        await asyncio.sleep(0.1)

        if self._listening_task is not None:
            self._listening_task.cancel()

            try:
                await self._listening_task
            except asyncio.CancelledError:
                pass

        await self._dialect.stop_listening()

        if self._logger:
            self._logger.info("SQLNotify Notifier stopped")

    def _stop_sync(self) -> None:

        self._running = False

        if self._logger:
            self._logger.info("SQLNotify Notifier stopped (sync mode)")

    @wrap_unhandled_error(lambda self: self._logger, reraise=False)
    def cleanup(self) -> None:
        """
        Remove all triggers and functions from the database synchronously

        Use this method when working with a sync engine.
        For async engines, use acleanup() instead.

        Raises:
            SQLNotifyConfigurationError: If called with an async engine
        """

        if self.async_mode:
            raise SQLNotifyConfigurationError("Cannot use cleanup() with async engine. Use acleanup() instead.")

        self._cleanup_sync()

    @wrap_unhandled_error(lambda self: self._logger, reraise=False)
    async def acleanup(self) -> None:
        """
        Remove all triggers and functions from the database asynchronously.

        Use this method when working with an async engine.
        For sync engines, use cleanup() instead.

        Raises:
            SQLNotifyConfigurationError: If called with a sync engine

        Examples:

            await notifier.acleanup()
        """

        if not self.async_mode:
            raise SQLNotifyConfigurationError("Cannot use acleanup() with sync engine. Use cleanup() instead.")

        await self._cleanup_async()

    async def _cleanup_async(self) -> None:

        await self._dialect.cleanup_async(self.watchers)

        if self._logger:
            self._logger.info("SQLNotify Notifier cleaned up all triggers and functions")

    def _cleanup_sync(self) -> None:

        self._dialect.cleanup_sync(self.watchers)

        if self._logger:
            self._logger.info("SQLNotify Notifier cleaned up all triggers and functions (sync mode)")

    @wrap_unhandled_error(lambda self: self._logger)
    def notify(
        self,
        model: type | str,
        operation: Operation,
        payload: dict[str, Any],
        channel_label: str | None = None,
        use_overflow_table: bool = False,
    ) -> None:
        """
        Send a notification to a database channel for a registered watcher synchronously.

        Use this method when working with a sync engine.
        For async engines, use anotify() instead.

        Args:
            model (Union[Type, str]): The model class or class name as string to notify for (must have a registered watcher)
            operation (Operation): The operation type (INSERT, UPDATE, DELETE)
            payload (dict[str, Any]): The payload to send (will be JSON serialized)
            channel_label (Optional[str]): Custom channel label if watcher was registered with one
            use_overflow_table (bool): If True, large payloads are stored in overflow table. Defaults to False.

        Raises:
            SQLNotifyConfigurationError: If called with an async engine
            SQLNotifyConfigurationError: If no watcher is registered for the model/operation
            SQLNotifyPayloadSizeError: If payload exceeds 7999 bytes and use_overflow_table is False
        """

        if self.async_mode:
            raise SQLNotifyConfigurationError("Cannot use notify() with async engine. Use anotify() instead.")

        watcher = self._get_watcher(model, operation, channel_label)

        self._notify_sync(watcher, payload, use_overflow_table)

    @wrap_unhandled_error(lambda self: self._logger)
    async def anotify(
        self,
        model: type | str,
        operation: Operation,
        payload: dict[str, Any],
        channel_label: str | None = None,
        use_overflow_table: bool = False,
    ) -> None:
        """
        Send a notification to a database channel for a registered watcher asynchronously.

        Use this method when working with an async engine.
        For sync engines, use notify() instead.

        Args:
            model (Union[Type, str]): The model class or class name as string to notify for (must have a registered watcher)
            operation (Operation): The operation type (INSERT, UPDATE, DELETE)
            payload (dict[str, Any]): The payload to send (will be JSON serialized)
            channel_label (Optional[str]): Custom channel label if watcher was registered with one
            use_overflow_table (bool): If True, large payloads are stored in overflow table. Defaults to False.

        Raises:
            SQLNotifyConfigurationError: If called with a sync engine
            SQLNotifyConfigurationError: If no watcher is registered for the model/operation
            SQLNotifyPayloadSizeError: If payload exceeds 7999 bytes and use_overflow_table is False
        """

        if not self.async_mode:
            raise SQLNotifyConfigurationError("Cannot use anotify() with sync engine. Use notify() instead.")

        watcher = self._get_watcher(model, operation, channel_label)

        await self._notify_async(watcher, payload, use_overflow_table)

    def _get_watcher(
        self,
        model: type | str,
        operation: Operation,
        channel_label: str | None = None,
    ) -> Watcher:

        model_name = model if isinstance(model, str) else model.__name__

        for watcher in self.watchers:
            watcher_matches = False

            if isinstance(model, str):
                watcher_matches = (
                    watcher.model.__name__ == model
                    and watcher.operation == operation
                    and watcher.channel_label == channel_label
                )
            else:
                watcher_matches = (
                    watcher.model == model and watcher.operation == operation and watcher.channel_label == channel_label
                )

            if watcher_matches:
                return watcher

        label_msg = f" with channel_label='{channel_label}'" if channel_label else ""

        raise SQLNotifyConfigurationError(
            f"No watcher registered for {model_name} with operation {operation}{label_msg}. "
            f"Call .watch({model_name}, {operation}) first."
        )

    async def _notify_async(self, watcher: Watcher, payload: dict[str, Any], use_overflow_table: bool) -> None:

        await self._dialect.notify_async(watcher, payload, use_overflow_table)

    def _notify_sync(self, watcher: Watcher, payload: dict[str, Any], use_overflow_table: bool) -> None:

        self._dialect.notify_sync(watcher, payload, use_overflow_table)

    async def _listen_async(self) -> None:

        cleaned_db_url = strip_database_query_params(self._database_url)

        await self._dialect.listen_async(
            watchers=self.watchers,
            running_check=lambda: self._running,
            handle_notification=self._handle_notification_payload,
            database_url=cleaned_db_url,
            listener_ready=self._listener_ready,
        )

    async def _handle_notification_payload(self, channel: str, payload_str: str) -> None:
        """
        Handle a notification payload from the database asynchronously

        Args:
            channel (str): The channel name the notification was received on
            payload_str (str): The JSON payload string from the database
        """

        if self._logger:
            self._logger.info(f"Handling notification on channel '{channel}': {payload_str[:200]}...")

        payload: dict[str, Any] = json.loads(payload_str)

        watcher: Watcher | None = None
        for w in self.watchers:
            if w.channel_name == channel:
                watcher = w
                break

        if not watcher:
            if self._logger:
                self._logger.warning(f"No watcher matched for channel {channel}")

            return

        if self._logger:
            self._logger.info(f"Matched watcher {watcher.model.__name__}.{watcher.operation} for channel {channel}")

        if "overflow_id" in payload and watcher.use_overflow_table:
            overflow_id = payload["overflow_id"]
            if self._logger:
                self._logger.info(f"Fetching overflow payload id={overflow_id}")

            overflow_payload = await self._dialect.fetch_overflow_async(watcher, overflow_id)
            if overflow_payload:
                payload = overflow_payload
            else:
                if self._logger:
                    self._logger.error(f"Overflow record {overflow_id} not found")
                return

        await self._dispatch_event(watcher, payload)

    def _handle_notification_payload_sync(self, channel: str, payload_str: str) -> None:
        """
        Handle a notification payload from the database synchronously

        Args:
            channel (str): The channel name the notification was received on
            payload_str (str): The JSON payload string from the database
        """

        payload: dict[str, Any] = json.loads(payload_str)

        watcher: Watcher | None = None
        for w in self.watchers:
            if w.channel_name == channel:
                watcher = w
                break

        if not watcher:
            return

        if "overflow_id" in payload and watcher.use_overflow_table:
            overflow_id = payload["overflow_id"]
            overflow_payload = self._dialect.fetch_overflow_sync(watcher, overflow_id)

            if overflow_payload:
                payload = overflow_payload
            else:
                if self._logger:
                    self._logger.error(f"Overflow record {overflow_id} not found")
                return

        self._dispatch_event_sync(watcher, payload)

    def _dispatch_event_sync(self, watcher: Watcher, payload: dict[str, Any]) -> None:
        """
        Create event and dispatch to subscribers synchronously

        Args:
            watcher (Watcher): The watcher that triggered
            payload (dict): The notification payload
        """

        if len(watcher.primary_keys) == 1:
            pk_value = payload.get(watcher.primary_keys[0])
        else:
            # For composite primary keys just return a tuple of values in the order specified in primary_keys
            pk_value = tuple(payload.get(pk) for pk in watcher.primary_keys)

        extra_cols = {k: v for k, v in payload.items() if k not in watcher.primary_keys}

        event = ChangeEvent(
            schema=watcher.schema_name,
            table=watcher.table_name,
            operation=watcher.operation,
            id=pk_value,
            extra_columns=extra_cols,
            timestamp=str(asyncio.get_event_loop().time()),
        )

        all_payload = {
            **{
                watcher.primary_keys[i]: (pk_value[i] if isinstance(pk_value, tuple) else pk_value)
                for i in range(len(watcher.primary_keys))
            },
            **extra_cols,
        }

        channels_to_check = [watcher.channel_name]

        for ch in list(self.subscribers.keys()):
            if ch.startswith(watcher.channel_name + ":"):
                filter_part = ch[len(watcher.channel_name) + 1 :]
                filter_pairs = filter_part.split("|")

                all_match = True
                for pair in filter_pairs:
                    if ":" in pair:
                        col, val = pair.split(":", 1)
                        event_val = str(all_payload.get(col, ""))
                        if event_val != val:
                            all_match = False
                            break

                if all_match:
                    channels_to_check.append(ch)

        for ch in channels_to_check:
            if ch in self.subscribers:
                for callback in self.subscribers[ch]:
                    try:
                        if inspect.iscoroutinefunction(callback):
                            asyncio.run(callback(event))
                        else:
                            callback(event)
                    except Exception as e:
                        if self._logger:
                            self._logger.error(f"SQLNotify error in subscriber callback: {str(e)}")

    async def _dispatch_event(self, watcher: Watcher, payload: dict[str, Any]) -> None:
        """
        Create event and dispatch to subscribers

        Args:
            watcher (Watcher): The watcher that triggered
            payload (dict[str, Any]): The notification payload
        """

        if len(watcher.primary_keys) == 1:
            pk_value = payload.get(watcher.primary_keys[0])
        else:
            pk_value = tuple(payload.get(pk) for pk in watcher.primary_keys)

        extra_cols = {k: v for k, v in payload.items() if k not in watcher.primary_keys}

        event = ChangeEvent(
            schema=watcher.schema_name,
            table=watcher.table_name,
            operation=watcher.operation,
            id=pk_value,
            extra_columns=extra_cols,
            timestamp=str(asyncio.get_event_loop().time()),
        )

        all_payload = {
            **{
                watcher.primary_keys[i]: (pk_value[i] if isinstance(pk_value, tuple) else pk_value)
                for i in range(len(watcher.primary_keys))
            },
            **extra_cols,
        }

        channels_to_check = [watcher.channel_name]

        for ch in list(self.subscribers.keys()):
            if ch.startswith(watcher.channel_name + ":"):
                filter_part = ch[len(watcher.channel_name) + 1 :]
                filter_pairs = filter_part.split("|")

                all_match = True
                for pair in filter_pairs:
                    if ":" in pair:
                        col, val = pair.split(":", 1)
                        event_val = str(all_payload.get(col, ""))
                        if event_val != val:
                            all_match = False
                            break

                if all_match:
                    channels_to_check.append(ch)

        for ch in channels_to_check:
            if ch in self.subscribers:
                for callback in self.subscribers[ch]:
                    try:
                        if inspect.iscoroutinefunction(callback):
                            await callback(event)
                        else:
                            callback(event)
                    except Exception as e:
                        if self._logger:
                            self._logger.error(f"SQLNotify error in subscriber callback: {str(e)}")
