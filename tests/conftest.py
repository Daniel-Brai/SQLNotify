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
    {
        "POSTGRES_HOST": "postgres"
    },
).start()  # noqa


def pytest_addoption(parser):
    parser.addoption(
        "--db",
        action="store",
        default="postgresql",
        help="Database type to use for tests: postgresql or sqlite",
    )


@pytest.fixture(scope="session")
def db_type(request) -> str:
    return request.config.getoption("--db")


@pytest.fixture(scope="session")
def database_manager(db_type: str) -> DatabaseManager:
    return DatabaseManager(db_type=db_type)


@pytest.fixture(scope="session", autouse=True)
def manage_test_database(database_manager: DatabaseManager):
    database_manager.create_test_database()

    yield

    database_manager.drop_test_database()


@pytest.fixture(scope="function")
async def async_engine(
    database_manager: DatabaseManager,
) -> AsyncGenerator[AsyncEngine, None]:
    engine = database_manager.create_async_engine()

    await database_manager.create_tables_async(engine)

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


@pytest.fixture(scope="session")
def sync_engine(database_manager: DatabaseManager) -> Generator[Engine, None, None]:
    engine = database_manager.create_sync_engine()

    database_manager.create_tables_sync(engine)

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
