"""SQLAlchemy async engine and session factory."""

from sqlalchemy import event
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy.orm import DeclarativeBase

from backend.config import DATABASE_URL


engine = create_async_engine(
    DATABASE_URL,
    echo=False,
    future=True,
    # SQLite only allows one writer — serialize all connections through one
    pool_size=1,
    max_overflow=0,
    pool_pre_ping=True,
    connect_args={
        "timeout": 30,  # wait up to 30s for the lock instead of failing instantly
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
    cursor.execute("PRAGMA busy_timeout=30000")
    cursor.execute("PRAGMA synchronous=NORMAL")
    cursor.close()


async_session = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


class Base(DeclarativeBase):
    pass


async def init_db():
    """Create all tables."""
    from backend.models import User, Job, Setting, LogEntry, Statistic  # noqa: F401
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


async def get_db() -> AsyncSession:
    """Dependency that yields a database session."""
    async with async_session() as session:
        yield session
