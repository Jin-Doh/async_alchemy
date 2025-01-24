import os
from pathlib import Path

import pytest
import pytest_asyncio
from dotenv import load_dotenv
from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy.orm import DeclarativeBase

from app.main import Database
from app.modules.configs import AsyncPostgresDsn, DatabaseConfig
from app.modules.session import SessionManager

load_dotenv(
    dotenv_path=Path(__file__).parent / ".tests.env",
)


class TestBase(DeclarativeBase):
    __test__ = False

    pass


@pytest.fixture
def dsn_config():
    return AsyncPostgresDsn(
        host=os.environ.get("POSTGRES_HOST"),
        port=os.environ.get("POSTGRES_PORT"),
        user=os.environ.get("POSTGRES_USER"),
        password=os.environ.get("POSTGRES_PASSWORD"),
        db=os.environ.get("POSTGRES_DB"),
    ).async_dsn


@pytest.fixture
def db_config(dsn_config):
    return DatabaseConfig(
        dsn=dsn_config,
        echo=False,
        pool_size=5,
        max_overflow=10,
        pool_timeout=30,
        pool_pre_ping=True,
    )


@pytest_asyncio.fixture
async def database(db_config):
    db = Database(config=db_config, base=TestBase)
    await db.initialize()
    yield db
    await db.dispose()


@pytest.mark.asyncio
async def test_database_initialization(database):
    assert isinstance(database.engine, AsyncEngine)
    assert isinstance(database.session, SessionManager)


@pytest.mark.asyncio
async def test_database_singleton_instances(db_config):
    db = Database(config=db_config)

    # 엔진 싱글톤 테스트
    engine1 = db.engine
    engine2 = db.engine
    assert engine1 is engine2

    # 세션 매니저 싱글톤 테스트
    session1 = db.session
    session2 = db.session
    assert session1 is session2


@pytest.mark.asyncio
async def test_database_dispose(db_config):
    db = Database(config=db_config)

    # 엔진과 세션 매니저 생성
    _ = db.engine
    _ = db.session

    # dispose 호출
    await db.dispose()

    # 내부 상태 확인
    assert db._engine is None
    assert db._session_manager is None
