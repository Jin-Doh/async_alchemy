from contextlib import asynccontextmanager
from typing import AsyncIterator

import sqlalchemy.ext.asyncio as sql_asyncio


class SessionManager:
    """비동기 세션 관리자"""

    def __init__(self, engine: sql_asyncio.AsyncEngine):
        self.engine = engine
        self.session_factory = sql_asyncio.async_sessionmaker(
            engine, class_=sql_asyncio.AsyncSession, expire_on_commit=False
        )

    @asynccontextmanager
    async def session(self) -> AsyncIterator[sql_asyncio.AsyncSession]:
        """일반 세션 컨텍스트"""
        async with self.session_factory() as session:
            try:
                yield session
            except Exception:
                await session.rollback()
                raise
            finally:
                await session.close()

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[sql_asyncio.AsyncSession]:
        """트랜잭션 세션 컨텍스트"""
        async with self.session_factory() as session:
            try:
                async with session.begin():
                    yield session
            except Exception:
                await session.rollback()
                raise
