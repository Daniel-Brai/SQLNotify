import json

import pytest
from sqlalchemy import text

from sqlnotify.dialects import PostgreSQLDialect
from sqlnotify.dialects.utils import detect_dialect_name, get_dialect_for_engine
from sqlnotify.types import Operation
from sqlnotify.watcher import Watcher
from tests.models import SessionLog, User


class TestPostgreSQLDialect:

    def test_detect_postgres_dialect_sync(self, sync_engine):
        dialect_name = detect_dialect_name(sync_engine)
        assert dialect_name == "postgresql"

    def test_detect_postgres_dialect_async(self, async_engine):
        dialect_name = detect_dialect_name(async_engine)
        assert dialect_name == "postgresql"

    def test_get_postgres_dialect_sync(self, sync_engine):

        if "postgresql" not in str(sync_engine.url):
            pytest.skip("Test requires PostgreSQL engine")

        dialect = get_dialect_for_engine(sync_engine, sync_engine=sync_engine)
        assert isinstance(dialect, PostgreSQLDialect)
        assert dialect.name == "postgresql"

    @pytest.fixture(autouse=True)
    def skip_if_not_postgres(self, db_type):

        if db_type != "postgresql":
            pytest.skip("PostgreSQL functionality tests require a postgresql test run")

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
    def update_watcher(self):

        return Watcher(
            model=User,
            operation=Operation.UPDATE,
            extra_columns=["email"],
            trigger_columns=["email", "name"],
            primary_keys=["id"],
        )

    @pytest.fixture
    def delete_watcher(self):

        return Watcher(
            model=User,
            operation=Operation.DELETE,
            extra_columns=None,
            trigger_columns=None,
            primary_keys=["id"],
        )

    @pytest.fixture
    def custom_schema_watcher(self):

        return Watcher(
            model=SessionLog,
            operation=Operation.UPDATE,
            extra_columns=["actions_count"],
            trigger_columns=["actions_count"],
            primary_keys=["id"],
        )

    @pytest.fixture
    async def postgres_dialect_async(self, async_engine, sync_engine):
        dialect = PostgreSQLDialect(
            async_engine=async_engine,
            sync_engine=sync_engine,
        )
        yield dialect

        if dialect._listen_conn:
            await dialect._listen_conn.close()
            dialect._listen_conn = None

    @pytest.fixture
    def postgres_dialect_sync(self, sync_engine):

        return PostgreSQLDialect(
            async_engine=None,
            sync_engine=sync_engine,
        )

    @pytest.mark.asyncio
    async def test_table_exists_async(self, postgres_dialect_async):

        exists = await postgres_dialect_async.table_exists_async("public", "users")
        assert exists is True

        exists = await postgres_dialect_async.table_exists_async(
            "public", "nonexistent_table"
        )
        assert exists is False

        exists = await postgres_dialect_async.table_exists_async(
            "test", "custom_schema_model"
        )
        assert exists is False

    def test_table_exists_sync(self, postgres_dialect_sync):

        exists = postgres_dialect_sync.table_exists_sync("public", "users")
        assert exists is True

        exists = postgres_dialect_sync.table_exists_sync("public", "nonexistent_table")
        assert exists is False

    @pytest.mark.asyncio
    async def test_create_trigger_async(self, postgres_dialect_async, watcher):

        try:
            await postgres_dialect_async.cleanup_async([watcher])
        except:
            pass

        await postgres_dialect_async.create_trigger_async(watcher)

        async with postgres_dialect_async._async_engine.begin() as conn:
            result = await conn.execute(
                text(
                    """
                    SELECT EXISTS (
                        SELECT 1 FROM information_schema.triggers 
                        WHERE trigger_name = :trigger_name
                        AND event_object_table = :table_name
                    )
                """
                ),
                {
                    "trigger_name": watcher.trigger_name,
                    "table_name": watcher.table_name,
                },
            )
            trigger_exists = result.scalar()
            assert trigger_exists is True

        async with postgres_dialect_async._async_engine.begin() as conn:
            result = await conn.execute(
                text(
                    """
                    SELECT EXISTS (
                        SELECT 1 FROM information_schema.routines 
                        WHERE routine_name = :function_name
                        AND routine_type = 'FUNCTION'
                    )
                """
                ),
                {"function_name": watcher.function_name},
            )
            function_exists = result.scalar()
            assert function_exists is True

    def test_create_trigger_sync(self, postgres_dialect_sync, watcher):

        try:
            postgres_dialect_sync.cleanup_sync([watcher])
        except:
            pass

        postgres_dialect_sync.create_trigger_sync(watcher)

        with postgres_dialect_sync._sync_engine.begin() as conn:
            result = conn.execute(
                text(
                    """
                    SELECT EXISTS (
                        SELECT 1 FROM information_schema.triggers 
                        WHERE trigger_name = :trigger_name
                        AND event_object_table = :table_name
                    )
                """
                ),
                {
                    "trigger_name": watcher.trigger_name,
                    "table_name": watcher.table_name,
                },
            )
            trigger_exists = result.one_or_none()
            assert trigger_exists is not None

    @pytest.mark.asyncio
    async def test_notify_async(self, postgres_dialect_async, watcher):
        payload = {"id": 1, "email": "test@example.com", "name": "Test User"}

        await postgres_dialect_async.notify_async(watcher, payload)

    def test_notify_sync(self, postgres_dialect_sync, watcher):

        payload = {"id": 1, "email": "test@example.com", "name": "Test User"}

        postgres_dialect_sync.notify_sync(watcher, payload)

    @pytest.mark.asyncio
    async def test_store_and_fetch_overflow(self, postgres_dialect_async, watcher):

        overflow_sql = postgres_dialect_async.create_overflow_table_sql("public")
        async with postgres_dialect_async._async_engine.begin() as conn:
            # Split and execute statements separately for asyncpg
            for stmt in postgres_dialect_async._split_sql_statements(overflow_sql):
                await conn.execute(text(stmt))

        large_payload = {"data": "x" * 10000}  # Large payload
        payload_str = json.dumps(large_payload)

        overflow_id = await postgres_dialect_async.store_overflow_async(
            watcher, payload_str
        )
        assert overflow_id.isdigit()

        fetched_payload = await postgres_dialect_async.fetch_overflow_async(
            watcher, int(overflow_id)
        )

        assert fetched_payload is not None
        assert "data" in fetched_payload
        assert fetched_payload["data"] == "x" * 10000

    @pytest.mark.asyncio
    async def test_cleanup_async(self, postgres_dialect_async, watcher):

        await postgres_dialect_async.create_trigger_async(watcher)

        async with postgres_dialect_async._async_engine.begin() as conn:
            result = await conn.execute(
                text(
                    """
                    SELECT EXISTS (
                        SELECT 1 FROM information_schema.triggers 
                        WHERE trigger_name = :trigger_name
                    )
                """
                ),
                {"trigger_name": watcher.trigger_name},
            )
            exists = result.scalar()
            assert exists is True

        await postgres_dialect_async.cleanup_async([watcher])

        async with postgres_dialect_async._async_engine.begin() as conn:
            result = await conn.execute(
                text(
                    """
                    SELECT EXISTS (
                        SELECT 1 FROM information_schema.triggers 
                        WHERE trigger_name = :trigger_name
                    )
                """
                ),
                {"trigger_name": watcher.trigger_name},
            )
            trigger_exists = result.scalar()
            assert trigger_exists is False

        async with postgres_dialect_async._async_engine.begin() as conn:
            result = await conn.execute(
                text(
                    """
                    SELECT EXISTS (
                        SELECT 1 FROM information_schema.routines 
                        WHERE routine_name = :function_name
                        AND routine_type = 'FUNCTION'
                    )
                """
                ),
                {"function_name": watcher.function_name},
            )
            function_exists = result.scalar()
            assert function_exists is False

    def test_cleanup_sync(self, postgres_dialect_sync, watcher):

        postgres_dialect_sync.create_trigger_sync(watcher)

        postgres_dialect_sync.cleanup_sync([watcher])

        with postgres_dialect_sync._sync_engine.begin() as conn:
            result = conn.execute(
                text(
                    """
                    SELECT EXISTS (
                        SELECT 1 FROM information_schema.triggers 
                        WHERE trigger_name = :trigger_name
                    )
                """
                ),
                {"trigger_name": watcher.trigger_name},
            )
            row = result.one()
            assert not row[0]

    def test_build_watcher_sql_insert(self, postgres_dialect_sync, watcher):

        function_sql, trigger_sql = postgres_dialect_sync.build_watcher_sql(watcher)

        assert (
            f"CREATE OR REPLACE FUNCTION {watcher.schema_name}.{watcher.function_name}()"
            in function_sql
        )
        assert "RETURNS trigger" in function_sql
        assert "pg_notify" in function_sql
        assert watcher.channel_name in function_sql
        assert "row := NEW" in function_sql  # INSERT uses NEW row

        assert f"CREATE TRIGGER {watcher.trigger_name}" in trigger_sql
        assert f"ON {watcher.schema_name}.{watcher.table_name}" in trigger_sql
        assert "AFTER INSERT" in trigger_sql
        assert (
            f"EXECUTE FUNCTION {watcher.schema_name}.{watcher.function_name}()"
            in trigger_sql
        )

    def test_build_watcher_sql_update(self, postgres_dialect_sync, update_watcher):

        function_sql, trigger_sql = postgres_dialect_sync.build_watcher_sql(
            update_watcher
        )

        assert (
            f"CREATE OR REPLACE FUNCTION {update_watcher.schema_name}.{update_watcher.function_name}()"
            in function_sql
        )
        assert "row := NEW" in function_sql  # UPDATE uses NEW row

        assert "AFTER UPDATE" in trigger_sql
        assert "OF email, name" in trigger_sql  # Should include trigger columns

    def test_build_watcher_sql_delete(self, postgres_dialect_sync, delete_watcher):
        function_sql, trigger_sql = postgres_dialect_sync.build_watcher_sql(
            delete_watcher
        )

        assert "row := OLD" in function_sql  # DELETE uses OLD row
        assert "row := NEW" not in function_sql  # DELETE doesn't use NEW row

        assert "AFTER DELETE" in trigger_sql

    def test_build_watcher_sql_custom_schema(
        self, postgres_dialect_sync, custom_schema_watcher
    ):

        _, trigger_sql = postgres_dialect_sync.build_watcher_sql(custom_schema_watcher)

        assert (
            f"ON {custom_schema_watcher.schema_name}.{custom_schema_watcher.table_name}"
            in trigger_sql
        )

    def test_create_overflow_table_sql(self, postgres_dialect_sync):

        sql = postgres_dialect_sync.create_overflow_table_sql("public")

        assert "CREATE TABLE IF NOT EXISTS public.sqlnotify_overflow" in sql
        assert "id BIGSERIAL PRIMARY KEY" in sql
        assert "payload JSONB NOT NULL" in sql
        assert "created_at TIMESTAMP NOT NULL DEFAULT NOW()" in sql

    @pytest.mark.asyncio
    async def test_multiple_watchers_cleanup(
        self, postgres_dialect_async, watcher, update_watcher
    ):

        await postgres_dialect_async.create_trigger_async(watcher)
        await postgres_dialect_async.create_trigger_async(update_watcher)

        watchers = [watcher, update_watcher]
        for w in watchers:
            async with postgres_dialect_async._async_engine.begin() as conn:
                result = await conn.execute(
                    text(
                        """
                        SELECT EXISTS (
                            SELECT 1 FROM information_schema.triggers 
                            WHERE trigger_name = :trigger_name
                        )
                    """
                    ),
                    {"trigger_name": w.trigger_name},
                )
                exists = result.scalar()
                assert exists is True

        await postgres_dialect_async.cleanup_async(watchers)

        for w in watchers:
            async with postgres_dialect_async._async_engine.begin() as conn:
                result = await conn.execute(
                    text(
                        """
                        SELECT EXISTS (
                            SELECT 1 FROM information_schema.triggers 
                            WHERE trigger_name = :trigger_name
                        )
                    """
                    ),
                    {"trigger_name": w.trigger_name},
                )
                trigger_exists = result.scalar()
                assert trigger_exists is False
