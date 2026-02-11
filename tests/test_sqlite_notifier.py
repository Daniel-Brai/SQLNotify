import asyncio

import pytest
from sqlalchemy import text

from sqlnotify import ChangeEvent, Notifier, Operation
from sqlnotify.constants import PACKAGE_NAME
from tests.models import Post, User, UserPostLink


class TestSQLiteNotifier:

    @pytest.fixture(autouse=True)
    async def cleanup_notification_queue(self, async_engine):
        async with async_engine.begin() as conn:
            try:
                await conn.execute(
                    text(f"DELETE FROM {PACKAGE_NAME}_notification_queue")
                )
            except Exception:
                pass

    @pytest.fixture(autouse=True)
    def skip_if_not_sqlite(self, db_type):
        if db_type != "sqlite":
            pytest.skip("Skipping SQLite-specific tests")

    @pytest.mark.asyncio
    async def test_notifier_async_insert(
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

        await notifier.astart()

        user = User(email="test@example.com", name="Test User")
        async_session.add(user)
        await async_session.commit()
        await async_session.refresh(user)

        await asyncio.sleep(0.5)

        await notifier.astop()

        assert len(received_events) == 1

        event = received_events[0]

        assert event["operation"] == Operation.INSERT
        assert event["table"] == "users"
        assert event["id"] == user.id
        assert event["extra_columns"]["email"] == "test@example.com"
        assert event["extra_columns"]["name"] == "Test User"

    @pytest.mark.asyncio
    async def test_notifier_async_update(
        self,
        async_engine,
        async_session,
        async_cleanup_tables,
    ):

        notifier = Notifier(db_engine=async_engine, cleanup_on_start=True)
        notifier.watch(User, Operation.UPDATE, extra_columns=["email", "name"])

        received_events: list[ChangeEvent] = []

        @notifier.subscribe(User, Operation.UPDATE)
        async def on_user_update(event: ChangeEvent):
            received_events.append(event)

        user = User(email="old@example.com", name="Old Name")
        async_session.add(user)
        await async_session.commit()
        await async_session.refresh(user)

        await notifier.astart()

        user.email = "new@example.com"
        user.name = "New Name"
        await async_session.commit()

        await asyncio.sleep(0.5)

        await notifier.astop()

        assert len(received_events) == 1
        event = received_events[0]

        assert event["operation"] == Operation.UPDATE
        assert event["table"] == "users"
        assert event["id"] == user.id
        assert event["extra_columns"]["email"] == "new@example.com"
        assert event["extra_columns"]["name"] == "New Name"

    @pytest.mark.asyncio
    async def test_notifier_async_delete(
        self,
        async_engine,
        async_session,
        async_cleanup_tables,
    ):

        notifier = Notifier(db_engine=async_engine, cleanup_on_start=True)
        notifier.watch(User, Operation.DELETE)

        received_events: list[ChangeEvent] = []

        @notifier.subscribe(User, Operation.DELETE)
        async def on_user_delete(event: ChangeEvent):
            received_events.append(event)

        user = User(email="delete@example.com", name="Delete User")
        async_session.add(user)
        await async_session.commit()
        await async_session.refresh(user)

        user_id = user.id

        await notifier.astart()

        await async_session.delete(user)
        await async_session.commit()

        await asyncio.sleep(0.5)

        await notifier.astop()

        assert len(received_events) == 1
        event = received_events[0]

        assert event["operation"] == Operation.DELETE
        assert event["table"] == "users"
        assert event["id"] == user_id
        assert event["extra_columns"] == {}

    @pytest.mark.asyncio
    async def test_notifier_async_multiple_watchers(
        self,
        async_engine,
        async_session,
        async_cleanup_tables,
    ):

        notifier = Notifier(db_engine=async_engine, cleanup_on_start=True)
        notifier.watch(User, Operation.INSERT, extra_columns=["email"])
        notifier.watch(Post, Operation.INSERT, extra_columns=["title"])

        user_events: list[ChangeEvent] = []
        post_events: list[ChangeEvent] = []

        @notifier.subscribe(User, Operation.INSERT)
        async def on_user_insert(event: ChangeEvent):
            user_events.append(event)

        @notifier.subscribe(Post, Operation.INSERT)
        async def on_post_insert(event: ChangeEvent):
            post_events.append(event)

        await notifier.astart()

        user = User(email="test@example.com", name="Test User")
        async_session.add(user)
        await async_session.commit()

        post = Post(title="Test Post", content="Test Content")
        async_session.add(post)
        await async_session.commit()

        await asyncio.sleep(0.5)

        await notifier.astop()

        assert len(user_events) == 1
        assert user_events[0]["table"] == "users"
        assert user_events[0]["extra_columns"]["email"] == "test@example.com"

        assert len(post_events) == 1
        assert post_events[0]["table"] == "posts"
        assert post_events[0]["extra_columns"]["title"] == "Test Post"

    @pytest.mark.asyncio
    async def test_notifier_async_with_filters(
        self,
        async_engine,
        async_session,
        async_cleanup_tables,
    ):

        notifier = Notifier(db_engine=async_engine, cleanup_on_start=True)
        notifier.watch(User, Operation.INSERT, extra_columns=["email", "name"])

        filtered_events: list[ChangeEvent] = []

        @notifier.subscribe(
            User,
            Operation.INSERT,
            filters=[{"column": "email", "value": "filtered@example.com"}],
        )
        async def on_filtered_user_insert(event: ChangeEvent):
            filtered_events.append(event)

        await notifier.astart()

        user1 = User(email="other@example.com", name="Other User")
        async_session.add(user1)
        await async_session.commit()

        user2 = User(email="filtered@example.com", name="Filtered User")
        async_session.add(user2)
        await async_session.commit()

        await asyncio.sleep(0.5)

        await notifier.astop()

        assert len(filtered_events) == 1
        assert filtered_events[0]["extra_columns"]["email"] == "filtered@example.com"

    @pytest.mark.asyncio
    async def test_notifier_async_composite_primary_key(
        self,
        async_engine,
        async_session,
        async_cleanup_tables,
    ):

        notifier = Notifier(db_engine=async_engine, cleanup_on_start=True)
        notifier.watch(
            UserPostLink,
            Operation.INSERT,
            primary_keys=["user_id", "post_id"],
        )

        received_events: list[ChangeEvent] = []

        @notifier.subscribe(UserPostLink, Operation.INSERT)
        async def on_link_insert(event: ChangeEvent):
            received_events.append(event)

        user = User(email="user@example.com", name="Test User")
        async_session.add(user)
        await async_session.commit()
        await async_session.refresh(user)

        post = Post(title="Test Post", content="Test Content")
        async_session.add(post)
        await async_session.commit()
        await async_session.refresh(post)

        await notifier.astart()

        link = UserPostLink(user_id=user.id, post_id=post.id)  # type: ignore
        async_session.add(link)
        await async_session.commit()

        await asyncio.sleep(0.5)

        await notifier.astop()

        assert len(received_events) == 1

        event = received_events[0]

        assert event["operation"] == Operation.INSERT
        assert event["table"] == "user_posts"
        assert event["id"] == (user.id, post.id)

    @pytest.mark.asyncio
    async def test_notifier_async_no_extra_columns(
        self,
        async_engine,
        async_session,
        async_cleanup_tables,
    ):

        notifier = Notifier(db_engine=async_engine, cleanup_on_start=True)
        # extra_columns omitted -> default None -> only primary key(s) included in events
        notifier.watch(User, Operation.INSERT)

        received_events: list[ChangeEvent] = []

        @notifier.subscribe(User, Operation.INSERT)
        async def on_user_insert(event: ChangeEvent):
            received_events.append(event)

        await notifier.astart()

        user = User(email="test@example.com", name="Test User")
        async_session.add(user)
        await async_session.commit()
        await async_session.refresh(user)

        await asyncio.sleep(0.5)
        await notifier.astop()

        assert len(received_events) == 1
        event = received_events[0]
        assert event["id"] == user.id
        assert event["extra_columns"] == {}

    @pytest.mark.asyncio
    async def test_notifier_async_multiple_subscribers_same_event(
        self,
        async_engine,
        async_session,
        async_cleanup_tables,
    ):

        notifier = Notifier(db_engine=async_engine, cleanup_on_start=True)
        notifier.watch(User, Operation.INSERT, extra_columns=["email"])

        events_1: list[ChangeEvent] = []
        events_2: list[ChangeEvent] = []

        @notifier.subscribe(User, Operation.INSERT)
        async def subscriber_1(event: ChangeEvent):
            events_1.append(event)

        @notifier.subscribe(User, Operation.INSERT)
        async def subscriber_2(event: ChangeEvent):
            events_2.append(event)

        await notifier.astart()

        user = User(email="multi@example.com", name="Multi User")
        async_session.add(user)
        await async_session.commit()
        await async_session.refresh(user)

        await asyncio.sleep(0.5)
        await notifier.astop()

        assert len(events_1) == 1
        assert len(events_2) == 1
        assert events_1[0]["id"] == user.id
        assert events_2[0]["id"] == user.id

    @pytest.mark.asyncio
    async def test_notifier_cleanup_on_start(
        self,
        async_engine,
        async_cleanup_tables,
    ):

        notifier1 = Notifier(db_engine=async_engine, cleanup_on_start=False)
        notifier1.watch(User, Operation.INSERT, extra_columns=["email"])

        await notifier1.astart()
        await notifier1.astop()

        notifier2 = Notifier(db_engine=async_engine, cleanup_on_start=True)
        notifier2.watch(User, Operation.INSERT, extra_columns=["name"])

        await notifier2.astart()
        await notifier2.astop()

        assert True
