from typing import Optional

from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from sqlalchemy.orm import DeclarativeBase

from app.modules import DatabaseConfig, SessionManager


class Database:
    """비동기 데이터베이스 관리자"""

    def __init__(
        self,
        config: DatabaseConfig,
        base: Optional[DeclarativeBase] = None,
    ):
        self.config = config
        self.Base = base
        self._engine: Optional[AsyncEngine] = None
        self._session_manager: Optional[SessionManager] = None

    @property
    def engine(self) -> AsyncEngine:
        """엔진 싱글톤 인스턴스"""
        if self._engine is None:
            self._engine = create_async_engine(
                self.config.dsn,
                echo=self.config.echo,
                pool_size=self.config.pool_size,
                max_overflow=self.config.max_overflow,
                pool_timeout=self.config.pool_timeout,
                pool_pre_ping=self.config.pool_pre_ping,
            )
        return self._engine

    @property
    def session(self) -> SessionManager:
        """세션 관리자 싱글톤 인스턴스"""
        if self._session_manager is None:
            self._session_manager = SessionManager(self.engine)
        return self._session_manager

    async def initialize(self) -> None:
        """데이터베이스 초기화 (테이블 생성)"""
        if self.Base is not None:
            async with self.engine.begin() as conn:
                await conn.run_sync(self.Base.metadata.create_all)

    async def dispose(self) -> None:
        """리소스 정리"""
        if self._engine:
            await self._engine.dispose()
            self._engine = None
            self._session_manager = None
