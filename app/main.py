import typing
from contextlib import asynccontextmanager

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
from utils.logger import Logger

logger = Logger(name="DATABASE")

T = typing.TypeVar("T")
ItemT = typing.TypeVar("ItemT")


class Database:
    def __init__(
        self,
        base: typing.Any,
        user: str,
        password: str,
        host: str,
        port: int,
        db: str,
        echo: bool = False,
        pool_size: int = 5,
        max_overflow: int = 10,
        pool_timeout: int = 300,
        pool_pre_ping: bool = True,
    ):
        prefix = "postgresql+asyncpg://"
        self.Base = base
        self.user = user
        self.wd = password
        self.host = host
        self.port = port
        self.db = db

        self.echo = echo
        self.pool_size = pool_size
        self.max_overflow = max_overflow
        self.pool_timeout = pool_timeout
        self.pool_pre_ping = pool_pre_ping

        conn_1 = f"{prefix}{self.user}:{self.wd}"
        conn_2 = f"@{self.host}:{self.port}/{self.db}"
        self.url = f"{conn_1}{conn_2}"
        self.engine = None
        self.async_session = None
        self._initialize_engine()

    def _initialize_engine(self):
        """엔진과 세션 팩토리를 초기화합니다."""
        if self.engine is None:
            self.engine = create_async_engine(
                self.url,
                echo=self.echo,
                pool_size=self.pool_size,
                max_overflow=self.max_overflow,
                pool_timeout=self.pool_timeout,
                pool_pre_ping=self.pool_pre_ping,
            )
        if self.async_session is None:
            self.async_session = sessionmaker(
                self.engine, class_=AsyncSession, expire_on_commit=False
            )

    async def create_database(self) -> None:
        async with self.engine.begin() as conn:
            await conn.run_sync(self.Base.metadata.create_all)

    @asynccontextmanager
    async def session(self) -> typing.AsyncGenerator[
        AsyncSession,
        None,
    ]:
        """기본 세션 컨텍스트 매니저"""
        await self.ensure_connection()
        async with self.async_session() as session:
            try:
                yield session
            except Exception:
                await session.rollback()
                raise
            finally:
                await session.close()

    @asynccontextmanager
    async def begin_session(self) -> typing.AsyncGenerator[
        AsyncSession,
        None,
    ]:
        """트랜잭션이 포함된 세션 컨텍스트 매니저"""
        async with self.async_session() as session:
            try:
                async with session.begin():
                    yield session
            except Exception:
                await session.rollback()
                raise

    @asynccontextmanager
    async def managed_transaction(
        self, session: AsyncSession
    ) -> typing.AsyncGenerator[AsyncSession, None]:
        """기존 세션에 대한 트랜잭션 컨텍스트 매니저"""
        if session.in_transaction():
            async with session.begin_nested() as nested:
                try:
                    yield session
                except Exception:
                    await nested.rollback()
                    raise
        else:
            async with session.begin():
                try:
                    yield session
                except Exception:
                    await session.rollback()
                    raise

    async def execute_in_transaction(
        self,
        callback: typing.Callable[
            [AsyncSession],
            typing.Awaitable[T],
        ],
        session: AsyncSession | None = None,
    ) -> T:
        """트랜잭션 내에서 콜백 함수를 실행합니다.

        Args:
            callback: 실행할 비동기 콜백 함수
            session: 기존 세션 (옵션)

        Returns:
            콜백 함수의 반환값

        Example:
            async def my_callback(session: AsyncSession) -> int:
                result = await session.execute(query)
                return result.scalar()

            result = await db.execute_in_transaction(my_callback)
        """
        if session is None:
            async with self.begin_session() as new_session:
                return await callback(new_session)
        else:
            async with self.managed_transaction(session):
                return await callback(session)

    async def execute_many(
        self,
        items: typing.Sequence[ItemT],
        callback: typing.Callable[
            [AsyncSession, ItemT],
            typing.Awaitable[typing.Any],
        ],
        batch_size: int = 100,
    ) -> typing.Tuple[int, int]:
        """배치 단위로 아이템들을 처리합니다.

        Args:
            items: 처리할 아이템 시퀀스
            callback: 각 아이템을 처리할 콜백 함수
            batch_size: 배치 크기

        Returns:
            (성공 건수, 실패 건수) 튜플

        Example:
            async def process_item(session: AsyncSession, item: Dict) -> None:
                model = Model(**item)
                session.add(model)

            success, failed = await db.execute_many(items, process_item)
        """
        total_success = 0
        total_failed = 0

        for i in range(0, len(items), batch_size):
            batch = items[i : i + batch_size]
            async with self.async_session() as session:
                async with session.begin():
                    success = 0
                    failed = 0

                    for item in batch:
                        try:
                            await callback(session, item)
                            success += 1
                        except Exception as e:
                            failed += 1
                            logger.error(f"Item processing failed: {e}")
                            await session.rollback()
                            continue

                    if success > 0:
                        try:
                            await session.commit()
                        except Exception as e:
                            logger.error(f"Batch commit failed: {e}")
                            success = 0
                            failed = len(batch)

                total_success += success
                total_failed += failed

        return total_success, total_failed

    async def close(self) -> None:
        """데이터베이스 연결을 종료합니다.

        Note:
            이 메서드는 커넥션 풀의 모든 연결을 반환합니다.
            엔진 자체는 여전히 유효하며, 새로운 연결 요청 시 새로운 커넥션을 생성할 수 있습니다.
            완전한 종료를 원한다면 dispose() 후 engine과 async_session을 None으로 설정해야 합니다.
        """
        if self.engine is not None:
            await self.engine.dispose()

    async def dispose(self) -> None:
        """데이터베이스 엔진을 완전히 종료합니다.

        Note:
            이 메서드는 엔진을 완전히 종료하고 모든 리소스를 정리합니다.
            이후 데이터베이스 사용을 위해서는 _initialize_engine()을 통한 재초기화가 필요합니다.
        """
        if self.engine is not None:
            await self.engine.dispose()
            self.engine = None
            self.async_session = None

    async def ensure_connection(self) -> None:
        """데이터베이스 연결을 확인하고 필요시 재연결합니다."""
        if self.engine is None:
            self._initialize_engine()
        else:
            try:
                # 연결 상태 확인
                async with self.async_session() as session:
                    await session.execute(text("SELECT 1"))
            except Exception:
                # 연결에 문제가 있으면 재초기화
                await self.close()
                self._initialize_engine()


db = Database()
