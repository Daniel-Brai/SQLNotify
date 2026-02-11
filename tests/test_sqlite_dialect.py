import json

import pytest
from sqlalchemy import text

from sqlnotify.dialects import SQLiteDialect
from sqlnotify.dialects.utils import detect_dialect_name
from sqlnotify.types import Operation
from sqlnotify.watcher import Watcher
from tests.models import User, UserPostLink


class TestSQLiteDialect:

    def test_detect_sqlite_dialect_sync(self, sync_engine):
        dialect_name = detect_dialect_name(sync_engine)
        assert dialect_name == "sqlite"

    def test_detect_sqlite_dialect_async(self, async_engine):
        dialect_name = detect_dialect_name(async_engine)
        assert dialect_name == "sqlite"

    @pytest.fixture(autouse=True)
    def skip_if_not_sqlite(self, db_type):

        if db_type != "sqlite":
            pytest.skip("SQLite functionality tests require a sqlite test run")

    @pytest.fixture
    def watcher(self):

        return Watcher(
            model=User,
            operation=Operation.INSERT,
            extra_columns=["email", "name"],
            trigger_columns=None,
            primary_keys=["id"],
        )

    @pytest.fixture
    def composite_key_watcher(self):

        return Watcher(
            model=UserPostLink,
            operation=Operation.UPDATE,
            extra_columns=None,
            trigger_columns=None,
            primary_keys=["user_id", "post_id"],
        )

    @pytest.fixture
    def watcher_with_overflow(self):

        return Watcher(
            model=User,
            operation=Operation.INSERT,
            extra_columns=["email", "name"],
            trigger_columns=None,
            primary_keys=["id"],
            use_overflow_table=True,
        )

    @pytest.fixture
    async def sqlite_dialect_async(self, async_engine):

        return SQLiteDialect(
            async_engine=async_engine,
            sync_engine=None,
        )

    @pytest.fixture
    def sqlite_dialect_sync(self, sync_engine):

        return SQLiteDialect(
            async_engine=None,
            sync_engine=sync_engine,
        )

    @pytest.mark.asyncio
    async def test_table_exists_async(self, sqlite_dialect_async):
        exists = await sqlite_dialect_async.table_exists_async("public", "users")
        assert exists is True

        exists = await sqlite_dialect_async.table_exists_async(
            "public", "nonexistent_table"
        )
        assert exists is False

    def test_table_exists_sync(self, sqlite_dialect_sync):
        exists = sqlite_dialect_sync.table_exists_sync("public", "users")
        assert exists is True

        exists = sqlite_dialect_sync.table_exists_sync("public", "nonexistent_table")
        assert exists is False

    @pytest.mark.asyncio
    async def test_notification_queue_is_created_async(self, sqlite_dialect_async):
        await sqlite_dialect_async._ensure_notification_queue_async()

        queue_table_exists = await sqlite_dialect_async.table_exists_async(
            "public", sqlite_dialect_async._notification_queue_table
        )
        assert queue_table_exists is True

    async def test_notification_queue_is_created_sync(self, sqlite_dialect_sync):
        sqlite_dialect_sync._ensure_notification_queue_sync()

        queue_table_exists = sqlite_dialect_sync.table_exists_sync(
            "public", sqlite_dialect_sync._notification_queue_table
        )
        assert queue_table_exists is True

    @pytest.mark.asyncio
    async def test_create_trigger_async(self, sqlite_dialect_async, watcher):
        await sqlite_dialect_async.create_trigger_async(watcher)

        async with sqlite_dialect_async._async_engine.begin() as conn:
            result = await conn.execute(
                text(
                    """
                    SELECT name FROM sqlite_master
                    WHERE type = 'trigger' AND name = :trigger_name
                """
                ),
                {"trigger_name": watcher.trigger_name},
            )
            trigger_name = result.one_or_none()
            assert trigger_name is not None

    def test_create_trigger_sync(self, sqlite_dialect_sync, watcher):
        sqlite_dialect_sync.create_trigger_sync(watcher)

        with sqlite_dialect_sync._sync_engine.begin() as conn:
            result = conn.execute(
                text(
                    """
                    SELECT name FROM sqlite_master
                    WHERE type = 'trigger' AND name = :trigger_name
                """
                ),
                {"trigger_name": watcher.trigger_name},
            )
            trigger_name = result.one_or_none()
            assert trigger_name is not None

    @pytest.mark.asyncio
    async def test_notify_async(self, sqlite_dialect_async, watcher):
        await sqlite_dialect_async._ensure_notification_queue_async()

        payload = {"id": 1, "email": "test@example.com", "name": "Test User"}

        await sqlite_dialect_async.notify_async(watcher, payload)

        async with sqlite_dialect_async._async_engine.begin() as conn:
            result = await conn.execute(
                text(
                    f"""
                    SELECT COUNT(*) FROM {sqlite_dialect_async._notification_queue_table}
                    WHERE channel_name = :channel
                """
                ),
                {"channel": watcher.channel_name},
            )
            (count,) = result.one()
            assert count == 1

    def test_notify_sync(self, sqlite_dialect_sync, watcher):
        payload = {"id": 1, "email": "test@example.com", "name": "Test User"}

        sqlite_dialect_sync.notify_sync(watcher, payload)

    @pytest.mark.asyncio
    async def test_store_and_fetch_overflow(
        self, sqlite_dialect_async, watcher_with_overflow
    ):

        large_payload = {"data": "x" * 10000}
        payload_str = json.dumps(large_payload)

        overflow_id = await sqlite_dialect_async.store_overflow_async(
            watcher_with_overflow, payload_str
        )
        assert overflow_id.isdigit()

        fetched_payload = await sqlite_dialect_async.fetch_overflow_async(
            watcher_with_overflow, int(overflow_id)
        )
        assert fetched_payload is not None
        assert "data" in fetched_payload
        assert fetched_payload["data"] == "x" * 10000

    def test_store_and_fetch_overflow_sync(
        self, sqlite_dialect_sync, watcher_with_overflow
    ):

        large_payload = {"data": "x" * 10000}
        payload_str = json.dumps(large_payload)

        overflow_id = sqlite_dialect_sync.store_overflow_sync(
            watcher_with_overflow, payload_str
        )
        assert overflow_id.isdigit()

        fetched_payload = sqlite_dialect_sync.fetch_overflow_sync(
            watcher_with_overflow, int(overflow_id)
        )
        assert fetched_payload is not None
        assert "data" in fetched_payload
        assert fetched_payload["data"] == "x" * 10000

    @pytest.mark.asyncio
    async def test_cleanup_async(self, sqlite_dialect_async, watcher):

        await sqlite_dialect_async.create_trigger_async(watcher)

        async with sqlite_dialect_async._async_engine.begin() as conn:
            result = await conn.execute(
                text(
                    """
                    SELECT name FROM sqlite_master
                    WHERE type = 'trigger' AND name = :trigger_name
                """
                ),
                {"trigger_name": watcher.trigger_name},
            )
            trigger_name = result.one_or_none()
            assert trigger_name is not None

        await sqlite_dialect_async.cleanup_async([watcher])

        async with sqlite_dialect_async._async_engine.begin() as conn:
            result = await conn.execute(
                text(
                    """
                    SELECT name FROM sqlite_master
                    WHERE type = 'trigger' AND name = :trigger_name
                """
                ),
                {"trigger_name": watcher.trigger_name},
            )
            trigger_name = result.one_or_none()
            assert trigger_name is None

    def test_cleanup_sync(self, sqlite_dialect_sync, watcher):

        sqlite_dialect_sync.create_trigger_sync(watcher)

        with sqlite_dialect_sync._sync_engine.begin() as conn:
            result = conn.execute(
                text(
                    """
                    SELECT name FROM sqlite_master
                    WHERE type = 'trigger' AND name = :trigger_name
                """
                ),
                {"trigger_name": watcher.trigger_name},
            )
            trigger_name = result.one_or_none()
            assert trigger_name is not None

        sqlite_dialect_sync.cleanup_sync([watcher])

        with sqlite_dialect_sync._sync_engine.begin() as conn:
            result = conn.execute(
                text(
                    """
                    SELECT name FROM sqlite_master
                    WHERE type = 'trigger' AND name = :trigger_name
                """
                ),
                {"trigger_name": watcher.trigger_name},
            )
            trigger_name = result.one_or_none()
            assert trigger_name is None

    def test_build_watcher_sql(
        self, sqlite_dialect_sync, watcher, composite_key_watcher
    ):

        function_sql, trigger_sql = sqlite_dialect_sync.build_watcher_sql(watcher)

        assert function_sql == ""
        assert watcher.trigger_name in trigger_sql
        assert watcher.table_name in trigger_sql
        assert "INSERT" in trigger_sql

        function_sql, trigger_sql = sqlite_dialect_sync.build_watcher_sql(
            composite_key_watcher
        )

        assert function_sql == ""
        assert composite_key_watcher.trigger_name in trigger_sql
        assert "UPDATE" in trigger_sql
        assert "name" in trigger_sql

    def test_create_overflow_table_sql(self, sqlite_dialect_sync):
        sql = sqlite_dialect_sync.create_overflow_table_sql("public")

        assert "CREATE TABLE" in sql
        assert "sqlnotify_overflow" in sql
        assert "id INTEGER PRIMARY KEY AUTOINCREMENT" in sql
        assert "payload TEXT NOT NULL" in sql
        assert "created_at DATETIME DEFAULT CURRENT_TIMESTAMP" in sql
        assert "public" not in sql
