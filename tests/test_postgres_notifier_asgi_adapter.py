import asyncio
from contextlib import asynccontextmanager

import pytest

from sqlnotify import ChangeEvent, Notifier, Operation
from sqlnotify.adapters.asgi import sqlnotify_lifespan
from tests.models import User


class TestPostgreSQLNotifierASGIAdapter:

    @pytest.fixture(autouse=True)
    def skip_if_not_postgresql(self, db_type):
        if db_type != "postgresql":
            pytest.skip("Skipping PostgreSQL-specific tests")

    @pytest.mark.asyncio
    async def test_asgi_lifespan_async_engine(
        self,
        async_engine,
        async_session,
        async_cleanup_tables,
    ):

        notifier = Notifier(db_engine=async_engine, cleanup_on_start=True)
        notifier.watch(User, Operation.INSERT, extra_columns=["email", "name"])

        received_events: list[ChangeEvent] = []

        @notifier.subscribe(User, Operation.INSERT)
        async def on_user_insert(event: ChangeEvent):
            received_events.append(event)

        async with sqlnotify_lifespan(notifier):
            assert notifier.is_running is True
            await asyncio.sleep(0.2)

            user = User(email="asgi@example.com", name="ASGI User")
            async_session.add(user)
            await async_session.commit()
            await async_session.refresh(user)

            await asyncio.sleep(0.5)

        assert notifier.is_running is False

        assert len(received_events) == 1

        event = received_events[0]

        assert event["operation"] == Operation.INSERT
        assert event["table"] == "users"
        assert event["id"] == user.id
        assert event["extra_columns"]["email"] == "asgi@example.com"

    @pytest.mark.asyncio
    async def test_asgi_lifespan_multiple_operations(
        self,
        async_engine,
        async_session,
        async_cleanup_tables,
    ):

        notifier = Notifier(db_engine=async_engine, cleanup_on_start=True)
        notifier.watch(User, Operation.INSERT, extra_columns=["email"])
        notifier.watch(User, Operation.UPDATE, extra_columns=["email"])
        notifier.watch(User, Operation.DELETE)

        insert_events: list[ChangeEvent] = []
        update_events: list[ChangeEvent] = []
        delete_events: list[ChangeEvent] = []

        @notifier.subscribe(User, Operation.INSERT)
        async def on_insert(event: ChangeEvent):
            insert_events.append(event)

        @notifier.subscribe(User, Operation.UPDATE)
        async def on_update(event: ChangeEvent):
            update_events.append(event)

        @notifier.subscribe(User, Operation.DELETE)
        async def on_delete(event: ChangeEvent):
            delete_events.append(event)

        async with sqlnotify_lifespan(notifier):
            await asyncio.sleep(0.2)

            user = User(email="multi@example.com", name="Multi User")
            async_session.add(user)
            await async_session.commit()
            await async_session.refresh(user)
            await asyncio.sleep(0.5)

            user.email = "updated@example.com"
            await async_session.commit()
            await asyncio.sleep(0.5)

            user_id = user.id
            await async_session.delete(user)
            await async_session.commit()
            await asyncio.sleep(0.5)

        assert len(insert_events) == 1
        assert insert_events[0]["extra_columns"]["email"] == "multi@example.com"

        assert len(update_events) == 1
        assert update_events[0]["extra_columns"]["email"] == "updated@example.com"

        assert len(delete_events) == 1
        assert delete_events[0]["id"] == user_id

    @pytest.mark.asyncio
    async def test_asgi_lifespan_exception_handling(
        self,
        async_engine,
        async_cleanup_tables,
    ):

        notifier = Notifier(db_engine=async_engine, cleanup_on_start=True)
        notifier.watch(User, Operation.INSERT, extra_columns=["email"])

        try:
            async with sqlnotify_lifespan(notifier):
                assert notifier.is_running is True
                raise Exception("Test exception")
        except Exception:
            pass

        await asyncio.sleep(0.3)

        assert notifier.is_running is False

    @pytest.mark.asyncio
    async def test_asgi_lifespan_with_fastapi_like_pattern(
        self,
        async_engine,
        async_session,
        async_cleanup_tables,
    ):

        notifier = Notifier(db_engine=async_engine, cleanup_on_start=True)
        notifier.watch(User, Operation.INSERT, extra_columns=["name"])

        received_events: list[ChangeEvent] = []

        @notifier.subscribe(User, Operation.INSERT)
        async def on_user_insert(event: ChangeEvent):
            received_events.append(event)

        @asynccontextmanager
        async def app_lifespan(app=None):
            async with sqlnotify_lifespan(notifier):
                yield

        async with app_lifespan():
            await asyncio.sleep(0.2)

            user = User(email="fastapi@example.com", name="FastAPI User")
            async_session.add(user)
            await async_session.commit()

            await asyncio.sleep(0.2)

        assert len(received_events) == 1
        assert received_events[0]["extra_columns"]["name"] == "FastAPI User"

    @pytest.mark.asyncio
    async def test_asgi_lifespan_with_nested_context(
        self,
        async_engine,
        async_session,
        async_cleanup_tables,
    ):

        notifier = Notifier(db_engine=async_engine, cleanup_on_start=True)
        notifier.watch(User, Operation.INSERT, extra_columns=["email"])

        received_events: list[ChangeEvent] = []

        @notifier.subscribe(User, Operation.INSERT)
        async def on_user_insert(event: ChangeEvent):
            received_events.append(event)

        @asynccontextmanager
        async def outer_context():
            async with sqlnotify_lifespan(notifier):
                yield

        async with outer_context():
            await asyncio.sleep(0.2)

            user = User(email="nested@example.com", name="Nested User")
            async_session.add(user)
            await async_session.commit()
            await asyncio.sleep(0.2)

        assert len(received_events) == 1
        assert received_events[0]["extra_columns"]["email"] == "nested@example.com"

    @pytest.mark.asyncio
    async def test_asgi_lifespan_with_multiple_events(
        self,
        async_engine,
        async_session,
        async_cleanup_tables,
    ):

        notifier = Notifier(db_engine=async_engine, cleanup_on_start=True)
        notifier.watch(User, Operation.INSERT, extra_columns=["email"])

        received_events: list[ChangeEvent] = []

        @notifier.subscribe(User, Operation.INSERT)
        async def on_user_insert(event: ChangeEvent):
            received_events.append(event)

        async with sqlnotify_lifespan(notifier):
            await asyncio.sleep(0.2)

            for i in range(10):
                user = User(email=f"user{i}@example.com", name=f"User {i}")
                async_session.add(user)
                await async_session.commit()

            await asyncio.sleep(0.5)

        assert len(received_events) == 10

        received_emails = {event["extra_columns"]["email"] for event in received_events}
        expected_emails = {f"user{i}@example.com" for i in range(10)}

        assert received_emails == expected_emails
