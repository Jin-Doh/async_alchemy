from typing import Any, Awaitable, Callable, Iterable, Iterator, TypeVar
from warnings import warn

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.main import Database

T = TypeVar("T")


def chunked(iterable: Iterable[T], size: int) -> Iterator[list[T]]:
    """이터러블을 지정된 크기의 청크로 분할

    Args:
        iterable: 분할할 이터러블
        size: 청크 크기

    Yields:
        size 크기의 청크 리스트
    """
    chunk = []
    for item in iterable:
        chunk.append(item)
        if len(chunk) >= size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


class DatabaseExtensions(Database):
    """확장 데이터베이스 기능"""

    async def health_check(self) -> bool:
        """데이터베이스 연결 상태 확인"""
        try:
            async with self.session.transaction() as session:
                await session.execute(text("SELECT 1"))
            return True
        except Exception as e:
            warn(f"Database health check failed: {e}")
            return False

    async def bulk_operation(
        self,
        operation: Callable[[AsyncSession, Any], Awaitable],
        data: Iterable[Any],
        batch_size: int = 100,
    ) -> tuple[int, int]:
        """대량 데이터 배치 처리"""
        success = failed = 0
        for batch in chunked(data, batch_size):
            try:
                async with self.session.transaction() as session:
                    for item in batch:
                        await operation(session, item)
                    success += len(batch)
            except Exception as e:
                failed += len(batch)
                warn(f"Batch operation failed: {e}")
        return success, failed
