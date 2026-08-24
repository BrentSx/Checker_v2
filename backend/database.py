"""SQLAlchemy async engine and session factory."""

from sqlalchemy import event, text
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.pool import NullPool

from backend.config import DATABASE_URL


engine = create_async_engine(
    DATABASE_URL,
    echo=False,
    future=True,
    # NullPool: each session gets its own connection, disposed on close.
    # StaticPool (single shared connection) caused "database is locked"
    # when multiple async coroutines (download progress callbacks, watchdog
    # health checks, API requests) tried to write concurrently — SQLite
    # can't multiplex transactions on a single connection.
    # With NullPool + WAL mode + busy_timeout, SQLite handles concurrency
    # properly: one writer + multiple readers, with automatic retry on lock.
    poolclass=NullPool,
    connect_args={
        "timeout": 60,  # wait up to 60s for the lock instead of failing instantly
    },
)


@event.listens_for(engine.sync_engine, "connect")
def _set_sqlite_pragmas(dbapi_conn, connection_record):
    """Enable WAL mode and busy timeout on every new SQLite connection.

    WAL (Write-Ahead Logging) lets readers not block writers and vice versa.
    busy_timeout tells SQLite to retry for N ms instead of erroring immediately.
    """
    cursor = dbapi_conn.cursor()
    cursor.execute("PRAGMA journal_mode=WAL")
    cursor.execute("PRAGMA busy_timeout=60000")
    cursor.execute("PRAGMA synchronous=NORMAL")
    cursor.close()


async_session = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


class Base(DeclarativeBase):
    pass


async def init_db():
    """Create all tables and run lightweight migrations for new columns."""
    from backend.models import User, Job, Setting, LogEntry, Statistic  # noqa: F401
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    # ── Mini-migrations: add columns that create_all can't add to existing tables ──
    _MIGRATIONS = [
        ("jobs", "archive_password", "ALTER TABLE jobs ADD COLUMN archive_password VARCHAR(256)"),
    ]
    async with engine.begin() as conn:
        for table, column, ddl in _MIGRATIONS:
            exists = await conn.run_sync(
                lambda sync_conn, t=table, c=column: c in [
                    row[1] for row in sync_conn.execute(text(f"PRAGMA table_info({t})"))
                ]
            )
            if not exists:
                await conn.execute(text(ddl))


async def get_db() -> AsyncSession:
    """Dependency that yields a database session."""
    async with async_session() as session:
        yield session
