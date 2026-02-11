import asyncio
import json
from collections.abc import Callable
from typing import Any

from sqlalchemy import text

from ..constants import MAX_SQLNOTIFY_EVENT_RETRIES, PACKAGE_NAME
from ..exceptions import SQLNotifyConfigurationError
from ..types import Operation
from ..utils import validate_payload_size
from ..watcher import Watcher
from .base import BaseDialect


class PostgreSQLDialect(BaseDialect):
    """
    PostgreSQL dialect for the `Notifier` using LISTEN/NOTIFY
    """

    @property
    def name(self) -> str:
        return "postgresql"

    def _split_sql_statements(self, sql: str) -> list[str]:
        """
        Split SQL into individual statements, respecting $$ blocks for functions.

        **NOTE**: asyncpg cannot execute multiple statements in a single call that is why I do this.

        See: https://stackoverflow.com/questions/76569945/how-to-run-asyncpg-postgresql-multiple-queries-simultaneously

        Args:
            sql (str): SQL string potentially containing multiple statements

        Returns:
            List[str]: Individual SQL statements
        """
        if not sql or not sql.strip():
            return []

        statements = []
        current = []
        in_dollar_quote = False

        for line in sql.splitlines():
            if "$$" in line:
                for _ in range(line.count("$$")):
                    in_dollar_quote = not in_dollar_quote

            current.append(line)

            if ";" in line and not in_dollar_quote:
                stmt = "\n".join(current).strip()
                if stmt and not stmt.startswith("--"):
                    statements.append(stmt)
                current = []

        if current:
            stmt = "\n".join(current).strip()
            if stmt and not stmt.startswith("--"):
                statements.append(stmt)

        return statements

    async def table_exists_async(self, schema: str, table: str) -> bool:
        if not self._async_engine:
            raise SQLNotifyConfigurationError("Async engine not available")

        query = """
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_schema = :schema
                AND table_name = :table
            )
        """

        async with self._async_engine.connect() as conn:
            result = await conn.execute(text(query), {"schema": schema, "table": table})
            row = result.one()
            return bool(row[0])

    def table_exists_sync(self, schema: str, table: str) -> bool:
        if not self._sync_engine:
            raise SQLNotifyConfigurationError("Sync engine not available")

        query = """
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_schema = :schema
                AND table_name = :table
            )
        """

        with self._sync_engine.connect() as conn:
            result = conn.execute(text(query), {"schema": schema, "table": table})
            row = result.one()
            return bool(row[0])

    async def create_trigger_async(self, watcher: Watcher) -> None:
        """
        Create PostgreSQL trigger and function asynchronously

        Args:
            watcher (Watcher): The Watcher configuration object

        Raises:
            SQLNotifyConfigurationError: If async engine is not available
        """

        if not self._async_engine:
            raise SQLNotifyConfigurationError("Async engine not available")

        try:
            async with self._async_engine.begin() as conn:
                table_check_query = """
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables
                        WHERE table_schema = :schema
                        AND table_name = :table
                    )
                """

                result = await conn.execute(
                    text(table_check_query),
                    {"schema": watcher.schema_name, "table": watcher.table_name},
                )
                row = result.one()
                table_exists = bool(row[0])

                if not table_exists:
                    if self._logger:
                        self._logger.warning(
                            f"Table {watcher.schema_name}.{watcher.table_name} does not exist. "
                            f"Skipping trigger creation for {watcher.model.__name__}.{watcher.operation}"
                        )
                    return

                if self.revoke_on_model_change:
                    try:
                        await conn.execute(
                            text(
                                f"DROP TRIGGER IF EXISTS {watcher.trigger_name} "
                                f"ON {watcher.schema_name}.{watcher.table_name}"
                            )
                        )
                        await conn.execute(
                            text(f"DROP FUNCTION IF EXISTS " f"{watcher.schema_name}.{watcher.function_name}()")
                        )
                        if self._logger:
                            self._logger.debug(f"Dropped existing trigger/function for {watcher.trigger_name}")
                    except Exception as e:
                        if self._logger:
                            self._logger.debug(
                                f"No existing trigger/function to drop for {watcher.trigger_name}: {str(e)}"
                            )

                if watcher.use_overflow_table:
                    overflow_sql = self.create_overflow_table_sql(watcher.schema_name)
                    for stmt in self._split_sql_statements(overflow_sql):
                        await conn.execute(text(stmt))

                    if self._logger:
                        self._logger.info(f"Created overflow table in schema {watcher.schema_name}")

                function_sql, trigger_sql = self.build_watcher_sql(watcher)

                await conn.execute(text(function_sql))
                for stmt in self._split_sql_statements(trigger_sql):
                    await conn.execute(text(stmt))

                if self._logger:
                    self._logger.info(f"Created trigger: {watcher.trigger_name}")
        except Exception as e:
            if self._logger:
                self._logger.error(
                    f"Error creating trigger for {watcher.model.__name__}.{watcher.operation}: {str(e)}",
                    exc_info=True,
                )

            raise SQLNotifyConfigurationError(
                f"Failed to create trigger for {watcher.model.__name__}.{watcher.operation}. Error: {str(e)}"
            ) from e

    def create_trigger_sync(self, watcher: Watcher) -> None:
        """
        Create PostgreSQL trigger and function synchronously

        Args:
            watcher (Watcher): The Watcher configuration object
        """

        if not self._sync_engine:
            raise SQLNotifyConfigurationError("Sync engine not available")

        try:
            with self._sync_engine.begin() as conn:
                table_check_query = """
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables
                        WHERE table_schema = :schema
                        AND table_name = :table
                    )
                """

                result = conn.execute(
                    text(table_check_query),
                    {"schema": watcher.schema_name, "table": watcher.table_name},
                )
                row = result.one()
                table_exists = bool(row[0])

                if not table_exists:
                    if self._logger:
                        self._logger.warning(
                            f"Table {watcher.schema_name}.{watcher.table_name} does not exist. "
                            f"Skipping trigger creation for {watcher.model.__name__}.{watcher.operation}"
                        )
                    return

                if self.revoke_on_model_change:
                    try:
                        conn.execute(
                            text(
                                f"DROP TRIGGER IF EXISTS {watcher.trigger_name} "
                                f"ON {watcher.schema_name}.{watcher.table_name}"
                            )
                        )
                        conn.execute(
                            text(f"DROP FUNCTION IF EXISTS " f"{watcher.schema_name}.{watcher.function_name}()")
                        )
                        if self._logger:
                            self._logger.debug(f"Dropped existing trigger/function for {watcher.trigger_name}")
                    except Exception as e:
                        if self._logger:
                            self._logger.debug(
                                f"No existing trigger/function to drop for {watcher.trigger_name}: {str(e)}"
                            )

                if watcher.use_overflow_table:
                    overflow_sql = self.create_overflow_table_sql(watcher.schema_name)
                    conn.execute(text(overflow_sql))

                    if self._logger:
                        self._logger.info(f"Created overflow table in schema {watcher.schema_name}")

                function_sql, trigger_sql = self.build_watcher_sql(watcher)

                conn.execute(text(function_sql))
                conn.execute(text(trigger_sql))

                if self._logger:
                    self._logger.info(f"Created trigger: {watcher.trigger_name}")

        except Exception as e:
            if self._logger:
                self._logger.error(
                    f"Error creating trigger for {watcher.model.__name__}.{watcher.operation}: {str(e)}",
                    exc_info=True,
                )

            raise SQLNotifyConfigurationError(
                f"Failed to create trigger for {watcher.model.__name__}.{watcher.operation}. Error: {str(e)}"
            ) from e

    async def listen_async(
        self,
        watchers: list[Watcher],
        running_check: Callable[[], bool],
        handle_notification: Callable[[str, str], Any],
        database_url: str,
        listener_ready: asyncio.Event | None = None,
    ) -> None:
        """
        Listen for PostgreSQL notifications asynchronously using asyncpg's add_listener API

        Args:
            watchers (List[Watcher]): List of watchers to listen for
            running_check (Callable[[], bool]): Callable that returns True while listening should continue
            handle_notification (Callable[[str, str], Any]): Callback to handle notification payloads
            database_url (str): PostgreSQL connection URL
            listener_ready (Optional[asyncio.Event]): Event to signal when listener is ready

        Raises:
            SQLNotifyConfigurationError: If asyncpg connection cannot be established after retries
        """

        import asyncpg

        conn: asyncpg.Connection | None = None

        if self._listen_conn is not None:
            conn = self._listen_conn
        else:
            last_error: Exception | None = None
            for attempt in range(1, MAX_SQLNOTIFY_EVENT_RETRIES + 1):
                try:
                    if self._logger:
                        self._logger.debug(
                            f"Attempting asyncpg connection (attempt {attempt}/{MAX_SQLNOTIFY_EVENT_RETRIES})"
                        )
                    conn = await asyncpg.connect(database_url)
                    self._listen_conn = conn
                    if self._logger:
                        self._logger.info(f"Successfully established asyncpg connection on attempt {attempt}")
                    break
                except Exception as e:
                    last_error = e

                    if attempt < MAX_SQLNOTIFY_EVENT_RETRIES:
                        wait_time = 2 ** (attempt - 1)

                        if self._logger:
                            self._logger.warning(
                                f"Failed to connect with asyncpg (attempt {attempt}/{MAX_SQLNOTIFY_EVENT_RETRIES}): {str(e)}. "
                                f"Retrying in {wait_time}s..."
                            )

                        await asyncio.sleep(wait_time)
                    else:
                        if self._logger:
                            self._logger.error(
                                f"Failed to connect with asyncpg after {MAX_SQLNOTIFY_EVENT_RETRIES} attempts: {str(e)}",
                                exc_info=True,
                            )

            if conn is None:
                error_msg = f"Failed to establish asyncpg connection after {MAX_SQLNOTIFY_EVENT_RETRIES} attempts"
                if last_error:
                    error_msg += f": {str(last_error)}"

                raise SQLNotifyConfigurationError(error_msg)

        assert conn is not None

        async def notification_handler(
            _conn: asyncpg.Connection,
            _pid: int,
            channel: str,
            payload: str,
        ) -> None:

            if self._logger:
                self._logger.info(f"Received notification on channel '{channel}' with payload: {payload[:200]}...")

            try:
                await handle_notification(channel, payload)
            except Exception as e:
                if self._logger:
                    self._logger.error(f"Error processing notification: {str(e)}", exc_info=True)

        # Track pending notification tasks to ensure they complete
        pending_tasks: set[asyncio.Task] = set()

        def sync_tracked_notification_handler(
            conn: asyncpg.Connection,
            pid: int,
            channel: str,
            payload: str,
        ) -> None:
            """Synchronous wrapper that creates and tracks async tasks"""
            task = asyncio.create_task(notification_handler(conn, pid, channel, payload))
            pending_tasks.add(task)
            task.add_done_callback(pending_tasks.discard)

        try:
            for watcher in watchers:
                await conn.add_listener(watcher.channel_name, sync_tracked_notification_handler)
                if self._logger:
                    self._logger.info(f"Listening on channel: {watcher.channel_name}")

            if listener_ready is not None:
                listener_ready.set()

            while running_check():
                try:
                    await conn.fetchval("SELECT 1")
                    await asyncio.sleep(0.1)
                except Exception as e:
                    if self._logger:
                        self._logger.error(f"Error in listen loop: {e}")

                    await asyncio.sleep(0.1)

        finally:
            if pending_tasks:
                await asyncio.gather(*pending_tasks, return_exceptions=True)

            for watcher in watchers:
                try:
                    await conn.remove_listener(watcher.channel_name, sync_tracked_notification_handler)
                except Exception as e:
                    if self._logger:
                        self._logger.warning(f"Error removing listener for channel {watcher.channel_name}: {str(e)}")
                    pass

            await conn.close()
            self._listen_conn = None

    async def cleanup_async(self, watchers: list[Watcher]) -> None:
        """
        Remove all triggers and functions asynchronously
        """

        if not self._async_engine:
            raise SQLNotifyConfigurationError("Async engine not available")

        async with self._async_engine.begin() as conn:
            for watcher in watchers:
                await conn.execute(
                    text(
                        f"DROP TRIGGER IF EXISTS {watcher.trigger_name} "
                        f"ON {watcher.schema_name}.{watcher.table_name}"
                    )
                )
                await conn.execute(text(f"DROP FUNCTION IF EXISTS " f"{watcher.schema_name}.{watcher.function_name}()"))

        if self._logger:
            self._logger.info("Cleaned up all triggers and functions")

    def cleanup_sync(self, watchers: list[Watcher]) -> None:
        """
        Remove all triggers and functions synchronously.
        """

        if not self._sync_engine:
            raise SQLNotifyConfigurationError("Sync engine not available")

        with self._sync_engine.begin() as conn:
            for watcher in watchers:
                conn.execute(
                    text(
                        f"DROP TRIGGER IF EXISTS {watcher.trigger_name} "
                        f"ON {watcher.schema_name}.{watcher.table_name}"
                    )
                )
                conn.execute(text(f"DROP FUNCTION IF EXISTS " f"{watcher.schema_name}.{watcher.function_name}()"))

        if self._logger:
            self._logger.info("Cleaned up all triggers and functions (sync mode)")

    async def notify_async(
        self,
        watcher: Watcher,
        payload: dict[str, Any],
        use_overflow_table: bool = False,
    ) -> None:
        """
        Send a notification asynchronously using pg_notify

        Args:
            watcher (Watcher): The watcher configuration
            payload (dict[str, Any]): The payload dict to send
            use_overflow_table (bool): If True, use overflow table for large payloads
        """
        if not self._async_engine:
            raise SQLNotifyConfigurationError("Async engine not available")

        payload_str = json.dumps(payload)
        channel = watcher.channel_name

        is_valid = validate_payload_size(payload_str, allow_overflow=use_overflow_table)

        if not is_valid and use_overflow_table:
            overflow_id = await self.store_overflow_async(watcher, payload_str)
            payload_str = json.dumps({"overflow_id": overflow_id})

            if self._logger:
                self._logger.debug(f"Stored large payload in overflow table with ID {overflow_id}")

        async with self._async_engine.begin() as conn:
            await conn.execute(
                text("SELECT pg_notify(:channel, :payload)"),
                {"channel": channel, "payload": payload_str},
            )

        if self._logger:
            self._logger.debug(f"Sent notification to channel {channel}")

    def notify_sync(
        self,
        watcher: Watcher,
        payload: dict[str, Any],
        use_overflow_table: bool = False,
    ) -> None:
        """
        Send a notification synchronously using pg_notify

        Args:
            watcher (Watcher): The watcher configuration
            payload (dict[str, Any]): The payload dict to send
            use_overflow_table (bool): If True, use overflow table for large payloads
        """

        if not self._sync_engine:
            raise SQLNotifyConfigurationError("Sync engine not available")

        payload_str = json.dumps(payload)
        channel = watcher.channel_name

        is_valid = validate_payload_size(payload_str, allow_overflow=use_overflow_table)

        if not is_valid and use_overflow_table:
            overflow_id = self.store_overflow_sync(watcher, payload_str)
            payload_str = json.dumps({"overflow_id": overflow_id})

            if self._logger:
                self._logger.debug(f"Stored large payload in overflow table with ID {overflow_id}")

        with self._sync_engine.begin() as conn:
            conn.execute(
                text("SELECT pg_notify(:channel, :payload)"),
                {"channel": channel, "payload": payload_str},
            )

        if self._logger:
            self._logger.debug(f"Sent notification to channel {channel} (sync mode)")

    async def store_overflow_async(self, watcher: Watcher, payload_str: str) -> str:
        """
        Store large payload in overflow table asynchronously
        """

        if not self._async_engine:
            raise SQLNotifyConfigurationError("Async engine not available")

        overflow_table = f"{watcher.schema_name}.{PACKAGE_NAME}_overflow"

        async with self._async_engine.begin() as conn:
            result = await conn.execute(
                text(
                    f"INSERT INTO {overflow_table} (channel, payload, created_at) "
                    "VALUES (:channel, :payload, NOW()) RETURNING id"
                ),
                {"channel": watcher.channel_name, "payload": payload_str},
            )
            (overflow_id,) = result.one()

        return str(overflow_id)

    def store_overflow_sync(self, watcher: Watcher, payload_str: str) -> str:
        """
        Store large payload in overflow table synchronously
        """

        if not self._sync_engine:
            raise SQLNotifyConfigurationError("Sync engine not available")

        overflow_table = f"{watcher.schema_name}.{PACKAGE_NAME}_overflow"

        with self._sync_engine.begin() as conn:
            result = conn.execute(
                text(
                    f"INSERT INTO {overflow_table} (channel, payload, created_at) "
                    "VALUES (:channel, :payload, NOW()) RETURNING id"
                ),
                {"channel": watcher.channel_name, "payload": payload_str},
            )
            (overflow_id,) = result.one()

        return str(overflow_id)

    async def fetch_overflow_async(self, watcher: Watcher, overflow_id: int) -> dict[str, Any] | None:
        """
        Fetch and consume overflow payload asynchronously
        """

        if not self._async_engine:
            raise SQLNotifyConfigurationError("Async engine not available")

        async with self._async_engine.begin() as conn:
            result = await conn.execute(
                text(
                    f"""
                    UPDATE {watcher.schema_name}.{PACKAGE_NAME}_overflow
                    SET consumed = TRUE, consumed_at = NOW()
                    WHERE id = :overflow_id
                    RETURNING payload
                """
                ),
                {"overflow_id": overflow_id},
            )
            row = result.one()

            if row:
                return json.loads(row[0]) if isinstance(row[0], str) else row[0]

            return None

    def fetch_overflow_sync(self, watcher: Watcher, overflow_id: int) -> dict[str, Any] | None:
        """
        Fetch and consume overflow payload synchronously
        """

        if not self._sync_engine:
            raise SQLNotifyConfigurationError("Sync engine not available")

        with self._sync_engine.begin() as conn:
            overflow_table = f"{watcher.schema_name}.{PACKAGE_NAME}_overflow"

            select_result = conn.execute(
                text(f"SELECT payload FROM {overflow_table} WHERE id = :overflow_id"),
                {"overflow_id": overflow_id},
            )
            row = select_result.one_or_none()

            if row:
                payload_str = row[0]

                conn.execute(
                    text(f"UPDATE {overflow_table} SET consumed = TRUE, consumed_at = NOW() WHERE id = :overflow_id"),
                    {"overflow_id": overflow_id},
                )

                return json.loads(payload_str)

        return None

    def build_watcher_sql(self, watcher: Watcher) -> tuple[str, str]:
        """
        Build SQL for PostgreSQL trigger and function creation

        Args:
            watcher (Watcher): Watcher configuration

        Returns:
            tuple[str, str]: (function_sql, trigger_sql)
        """

        all_columns = watcher.primary_keys + watcher.extra_columns
        columns_sql = ", ".join(f"'{col}', row.{col}" for col in all_columns)

        if watcher.operation == Operation.INSERT:
            trigger_event = "INSERT"
            row_ref = "NEW"
        elif watcher.operation == Operation.UPDATE:
            trigger_event = "UPDATE"
            row_ref = "NEW"
            if watcher.trigger_columns:
                cols = ", ".join(watcher.trigger_columns)
                trigger_event = f"UPDATE OF {cols}"
        else:
            trigger_event = "DELETE"
            row_ref = "OLD"

        if watcher.use_overflow_table:
            function_sql = f"""
                CREATE OR REPLACE FUNCTION {watcher.schema_name}.{watcher.function_name}()
                RETURNS trigger AS $$
                DECLARE
                    payload json;
                    payload_text text;
                    payload_size integer;
                    overflow_id bigint;
                    row RECORD;
                BEGIN
                    row := {row_ref};
                    payload := json_build_object(
                        {columns_sql}
                    );

                    payload_text := payload::text;
                    payload_size := octet_length(payload_text);

                    -- Check if payload exceeds limit (7999 bytes)
                    IF payload_size > 7999 THEN
                        -- Store in overflow table and send only reference
                        INSERT INTO {watcher.schema_name}.{watcher.overflow_table_name}
                            (channel, payload, created_at)
                        VALUES
                            ('{watcher.channel_name}', payload, NOW())
                        RETURNING id INTO overflow_id;

                        -- Send minimal notification with overflow reference
                        -- Include all primary keys in the notification
                        PERFORM pg_notify(
                            '{watcher.channel_name}',
                            json_build_object(
                                'overflow_id', overflow_id,
                                {", ".join(f"'{pk}', row.{pk}" for pk in watcher.primary_keys)}
                            )::text
                        );
                    ELSE
                        -- Send normal notification
                        PERFORM pg_notify(
                            '{watcher.channel_name}',
                            payload_text
                        );
                    END IF;

                    RETURN row;
                END;
                $$ LANGUAGE plpgsql;
            """
        else:
            function_sql = f"""
                CREATE OR REPLACE FUNCTION {watcher.schema_name}.{watcher.function_name}()
                RETURNS trigger AS $$
                DECLARE
                    payload json;
                    payload_text text;
                    payload_size integer;
                    row RECORD;
                BEGIN
                    row := {row_ref};
                    payload := json_build_object(
                        {columns_sql}
                    );

                    payload_text := payload::text;
                    payload_size := octet_length(payload_text);

                    -- Validate payload size (PostgreSQL NOTIFY limit is 7999 bytes)
                    IF payload_size > 7999 THEN
                        RAISE EXCEPTION 'sqlnotify: Payload size % bytes exceeds limit of 7999 bytes for channel {watcher.channel_name}. Enable use_overflow_table or reduce extra_columns.', payload_size
                            USING HINT = 'Consider using use_overflow_table=True or reducing extra_columns';
                    END IF;

                    PERFORM pg_notify(
                        '{watcher.channel_name}',
                        payload_text
                    );

                    RETURN row;
                END;
                $$ LANGUAGE plpgsql;
            """

        trigger_sql = f"""
            DROP TRIGGER IF EXISTS {watcher.trigger_name}
            ON {watcher.schema_name}.{watcher.table_name};

            CREATE TRIGGER {watcher.trigger_name}
            AFTER {trigger_event} ON {watcher.schema_name}.{watcher.table_name}
            FOR EACH ROW
            EXECUTE FUNCTION {watcher.schema_name}.{watcher.function_name}();
        """

        return function_sql, trigger_sql

    def create_overflow_table_sql(self, schema: str) -> str:
        """
        Create SQL for overflow table for large payloads

        Args:
            schema (str): Database schema name

        Returns:
            str: SQL statement to create overflow table
        """

        create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {schema}.{PACKAGE_NAME}_overflow (
                id BIGSERIAL PRIMARY KEY,
                channel VARCHAR(63) NOT NULL,
                payload JSONB NOT NULL,
                created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                consumed BOOLEAN DEFAULT FALSE,
                consumed_at TIMESTAMP
            );

            CREATE INDEX IF NOT EXISTS idx_overflow_channel_created
            ON {schema}.{PACKAGE_NAME}_overflow(channel, created_at DESC)
            WHERE NOT consumed;

            -- Auto-cleanup old consumed records (older than 1 hour)
            CREATE OR REPLACE FUNCTION {schema}.cleanup_overflow()
            RETURNS void AS $$
            BEGIN
                DELETE FROM {schema}.{PACKAGE_NAME}_overflow
                WHERE consumed = TRUE
                AND consumed_at < NOW() - INTERVAL '1 hour';
            END;
            $$ LANGUAGE plpgsql;
        """

        return create_table_sql
