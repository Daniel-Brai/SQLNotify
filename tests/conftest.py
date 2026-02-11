import os
from typing import AsyncGenerator, Generator
from unittest.mock import patch

import pytest
from sqlalchemy.engine import Engine
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import Session, scoped_session, sessionmaker

from tests.database import DatabaseManager

patch.dict(
    os.environ,
    {"POSTGRES_HOST": "postgres"},
).start()  # noqa


def pytest_collection_modifyitems(config, items):
    required = set()

    for item in items:
        fname = os.path.basename(str(item.fspath)).lower()
        if fname.startswith("test_sqlite"):
            required.add("sqlite")
        if fname.startswith("test_postgres") or fname.startswith("test_postgresql"):
            required.add("postgresql")

    if not required:
        required.add(os.getenv("TEST_DB", "postgresql"))

    config._required_db_types = required


@pytest.fixture
def db_type(request) -> str:
    fname = os.path.basename(str(request.node.fspath)).lower()
    if fname.startswith("test_sqlite"):
        return "sqlite"
    if fname.startswith("test_postgres") or fname.startswith("test_postgresql"):
        return "postgresql"
    return os.getenv("TEST_DB", "postgresql")


@pytest.fixture(scope="session")
def database_manager() -> DatabaseManager:
    return DatabaseManager()


@pytest.fixture(scope="session", autouse=True)
def manage_test_database(request, database_manager: DatabaseManager):
    required = getattr(request.config, "_required_db_types", None)
    if not required:
        required = {os.getenv("TEST_DB", "postgresql")}
    for db in required:
        database_manager.create_test_database(db_type=db)

    yield

    for db in required:
        database_manager.drop_test_database(db_type=db)


@pytest.fixture
async def async_engine(
    database_manager: DatabaseManager,
    db_type: str,
) -> AsyncGenerator[AsyncEngine, None]:
    engine = database_manager.create_async_engine(db_type=db_type)

    await database_manager.create_tables_async(engine, db_type=db_type)

    yield engine

    await engine.dispose()


@pytest.fixture
async def async_session(
    async_engine: AsyncEngine,
) -> AsyncGenerator[AsyncSession, None]:
    async with AsyncSession(
        bind=async_engine,
        autocommit=False,
        autoflush=False,
        expire_on_commit=False,
    ) as session:
        yield session
        await session.rollback()


@pytest.fixture
async def async_session_factory(async_engine: AsyncEngine):
    return async_sessionmaker(
        bind=async_engine,
        class_=AsyncSession,
        autocommit=False,
        autoflush=False,
        expire_on_commit=False,
    )


@pytest.fixture(scope="function")
def sync_engine(
    database_manager: DatabaseManager, db_type: str
) -> Generator[Engine, None, None]:
    engine = database_manager.create_sync_engine(db_type=db_type)

    database_manager.create_tables_sync(engine, db_type=db_type)

    yield engine

    engine.dispose()


@pytest.fixture
def sync_session(sync_engine: Engine) -> Generator[Session, None, None]:
    session = Session(bind=sync_engine, autocommit=False, autoflush=False)

    yield session

    session.rollback()
    session.close()


@pytest.fixture(scope="session")
def sync_session_factory(sync_engine: Engine):
    return scoped_session(
        sessionmaker(
            bind=sync_engine,
            class_=Session,
            autocommit=False,
            autoflush=False,
        )
    )


@pytest.fixture
def cleanup_tables(sync_engine: Engine, db_type: str):
    yield

    with sync_engine.begin() as conn:
        from sqlmodel import SQLModel

        if db_type == "sqlite":
            tables_to_clean = [
                table
                for table in reversed(SQLModel.metadata.sorted_tables)
                if table.schema is None
            ]
        else:
            tables_to_clean = reversed(SQLModel.metadata.sorted_tables)

        for table in tables_to_clean:
            conn.execute(table.delete())


@pytest.fixture
async def async_cleanup_tables(async_engine: AsyncEngine, db_type: str):
    yield

    async with async_engine.begin() as conn:
        from sqlmodel import SQLModel

        if db_type == "sqlite":
            tables_to_clean = [
                table
                for table in reversed(SQLModel.metadata.sorted_tables)
                if table.schema is None
            ]
        else:
            tables_to_clean = reversed(SQLModel.metadata.sorted_tables)

        for table in tables_to_clean:
            await conn.execute(table.delete())
