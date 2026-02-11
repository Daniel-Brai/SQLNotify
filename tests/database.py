import os
from typing import Generator

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import Session, sessionmaker
from sqlmodel import SQLModel


class DatabaseManager:

    def __init__(self, db_type: str | None = None):
        self.db_type = db_type
        self._engine: Engine | AsyncEngine | None = None
        self._async_session_factory = None
        self._sync_session_factory = None

    def get_database_url(
        self, async_driver: bool = False, db_type: str | None = None
    ) -> str:
        db_type = db_type or self.db_type or os.getenv("TEST_DB", "postgresql")

        if db_type == "sqlite":
            if async_driver:
                return (
                    f"sqlite+aiosqlite:///{os.getenv('SQLITE_DB', 'test_sqlnotify.db')}"
                )

            return f"sqlite:///{os.getenv('SQLITE_DB', 'test_sqlnotify.db')}"
        else:
            worker_id = os.environ.get("PYTEST_XDIST_WORKER", "master")
            db_name = f"{os.getenv('POSTGRES_DB', 'sqlnotify_test')}_{worker_id}"

            user = os.getenv("POSTGRES_USER", "postgres")
            password = os.getenv("POSTGRES_PASSWORD", "postgres")
            host = os.getenv("POSTGRES_HOST", "localhost")
            port = os.getenv("POSTGRES_PORT", "5432")

            if async_driver:
                return f"postgresql+asyncpg://{user}:{password}@{host}:{port}/{db_name}"

            return f"postgresql+psycopg://{user}:{password}@{host}:{port}/{db_name}"

    def get_base_connection_url(self, db_type: str | None = None) -> str:
        db_type = db_type or self.db_type or os.getenv("TEST_DB", "postgresql")
        if db_type == "sqlite":
            return ""  # SQLite doesn't need base connection

        user = os.getenv("POSTGRES_USER", "postgres")
        password = os.getenv("POSTGRES_PASSWORD", "postgres")
        host = os.getenv("POSTGRES_HOST", "localhost")
        port = os.getenv("POSTGRES_PORT", "5432")

        return f"postgresql+psycopg://{user}:{password}@{host}:{port}/postgres"

    def create_test_database(self, db_type: str | None = None) -> None:
        db_type = db_type or self.db_type or os.getenv("TEST_DB", "postgresql")

        if db_type == "sqlite":
            return

        worker_id = os.environ.get("PYTEST_XDIST_WORKER", "master")
        db_name = f"{os.getenv('POSTGRES_DB', 'sqlnotify_test')}_{worker_id}"

        base_url = self.get_base_connection_url(db_type=db_type)
        admin_engine = create_engine(base_url, isolation_level="AUTOCOMMIT")

        with admin_engine.connect() as conn:
            conn.execute(text(f"DROP DATABASE IF EXISTS {db_name} WITH (FORCE)"))
            conn.execute(text(f"CREATE DATABASE {db_name}"))

        admin_engine.dispose()

    def drop_test_database(self, db_type: str | None = None) -> None:
        db_type = db_type or self.db_type or os.getenv("TEST_DB", "postgresql")

        if db_type == "sqlite":
            db_file = os.getenv("SQLITE_DB", "test_sqlnotify.db")

            if os.path.exists(db_file):
                os.remove(db_file)

            return

        worker_id = os.environ.get("PYTEST_XDIST_WORKER", "master")
        db_name = f"{os.getenv('POSTGRES_DB', 'sqlnotify_test')}_{worker_id}"

        base_url = self.get_base_connection_url(db_type=db_type)
        admin_engine = create_engine(base_url, isolation_level="AUTOCOMMIT")

        with admin_engine.connect() as conn:
            conn.execute(text(f"DROP DATABASE IF EXISTS {db_name} WITH (FORCE)"))

        admin_engine.dispose()

    def create_async_engine(self, db_type: str | None = None) -> AsyncEngine:
        url = self.get_database_url(async_driver=True, db_type=db_type)
        return create_async_engine(url, echo=False)

    def create_sync_engine(self, db_type: str | None = None) -> Engine:
        url = self.get_database_url(async_driver=False, db_type=db_type)
        return create_engine(url, echo=False)

    async def create_tables_async(
        self, engine: AsyncEngine, db_type: str | None = None
    ) -> None:
        db_type = db_type or self.db_type or os.getenv("TEST_DB", "postgresql")

        async with engine.begin() as conn:
            if db_type == "sqlite":

                def create_filtered_tables(connection):
                    tables_to_create = [
                        table
                        for table in SQLModel.metadata.sorted_tables
                        if table.schema is None
                    ]
                    SQLModel.metadata.create_all(connection, tables=tables_to_create)

                await conn.run_sync(create_filtered_tables)
            else:
                await conn.execute(text("CREATE SCHEMA IF NOT EXISTS analytics"))
                await conn.run_sync(SQLModel.metadata.create_all)

    def create_tables_sync(self, engine: Engine, db_type: str | None = None) -> None:
        db_type = db_type or self.db_type or os.getenv("TEST_DB", "postgresql")
        if db_type == "sqlite":
            tables_to_create = [
                table
                for table in SQLModel.metadata.sorted_tables
                if table.schema is None
            ]
            SQLModel.metadata.create_all(engine, tables=tables_to_create)
        else:
            with engine.begin() as conn:
                conn.execute(text("CREATE SCHEMA IF NOT EXISTS analytics"))

            SQLModel.metadata.create_all(engine)

    async def drop_tables_async(
        self, engine: AsyncEngine, db_type: str | None = None
    ) -> None:
        db_type = db_type or self.db_type or os.getenv("TEST_DB", "postgresql")

        async with engine.begin() as conn:
            if db_type == "sqlite":

                def drop_filtered_tables(connection):
                    tables_to_drop = [
                        table
                        for table in SQLModel.metadata.sorted_tables
                        if table.schema is None
                    ]
                    SQLModel.metadata.drop_all(connection, tables=tables_to_drop)

                await conn.run_sync(drop_filtered_tables)
            else:
                await conn.run_sync(SQLModel.metadata.drop_all)

    def drop_tables_sync(self, engine: Engine, db_type: str | None = None) -> None:
        db_type = db_type or self.db_type or os.getenv("TEST_DB", "postgresql")
        if db_type == "sqlite":
            tables_to_drop = [
                table
                for table in SQLModel.metadata.sorted_tables
                if table.schema is None
            ]
            SQLModel.metadata.drop_all(engine, tables=tables_to_drop)
        else:
            SQLModel.metadata.drop_all(engine)

    def get_sync_session_factory(self, engine: Engine):

        if self._sync_session_factory is None:
            self._sync_session_factory = sessionmaker(
                bind=engine,
                class_=Session,
                autocommit=False,
                autoflush=False,
            )

        return self._sync_session_factory

    def get_async_session_factory(self, engine: AsyncEngine):

        if self._async_session_factory is None:
            self._async_session_factory = async_sessionmaker(
                bind=engine,
                class_=AsyncSession,
                autocommit=False,
                autoflush=False,
                expire_on_commit=False,
            )

        return self._async_session_factory

    def get_sync_session(self, engine: Engine) -> Generator[Session, None, None]:

        session_factory = self.get_sync_session_factory(engine)
        session = session_factory()

        try:
            yield session
        finally:
            session.close()

    async def get_async_session(self, engine: AsyncEngine):

        session_factory = self.get_async_session_factory(engine)
        session = session_factory()

        try:
            yield session
        finally:
            await session.close()
