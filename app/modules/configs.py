from urllib.parse import quote_plus

from pydantic import BaseModel, Field, ValidationError, model_validator
from pydantic.networks import PostgresDsn


class AsyncPostgresDsn(BaseModel):
    """비동기 PostgreSQL 연결 설정 모델"""

    user: str | None = Field(
        None, description="DB 사용자명 (dsn 미제공 시 필수)", min_length=1
    )
    password: str | None = Field(
        None, description="DB 패스워드 (dsn 미제공 시 필수)", min_length=1
    )
    host: str | None = Field(
        None, description="DB 호스트 주소 (dsn 미제공 시 필수)", min_length=1
    )
    port: int | None = Field(
        None, description="DB 포트 (dsn 미제공 시 필수)", gt=0, lt=65536
    )
    db: str | None = Field(
        None, description="연결 대상 데이터베이스 (dsn 미제공 시 필수)", min_length=1
    )
    dsn: PostgresDsn | None = Field(
        None, description="PostgreSQL 연결 DSN (직접 제공 시 다른 필드 무시)"
    )

    @model_validator(mode="after")
    def _build_async_dsn(self) -> "AsyncPostgresDsn":
        if self.dsn is not None:
            return self

        # 필수 필드 검증
        required_fields = {
            "user": self.user,
            "password": self.password,
            "host": self.host,
            "port": self.port,
            "db": self.db,
        }
        missing = [k for k, v in required_fields.items() if v is None]
        if missing:
            raise ValueError(f"DSN 미제공 시 필수 항목 누락: {missing}")

        # 특수 문자 URL 인코딩
        safe_user = quote_plus(self.user)
        safe_password = quote_plus(self.password)

        # asyncpg 스키마 적용
        dsn_str = (
            f"postgresql+asyncpg://{safe_user}:{safe_password}"
            f"@{self.host}:{self.port}/{self.db}"
        )

        # DSN 유효성 검증
        try:
            self.dsn = PostgresDsn(dsn_str)
        except ValidationError as e:
            raise ValueError(f"생성된 DSN 유효성 검증 실패: {dsn_str}") from e

        return self

    @property
    def async_dsn(self) -> str:
        """SQLAlchemy AsyncEngine용 DSN 반환"""
        if self.dsn is None:
            raise ValueError("DSN이 초기화되지 않았습니다")
        return str(self.dsn)


class DatabaseConfig(BaseModel):
    """데이터베이스 연결 설정"""

    dsn: AsyncPostgresDsn | str = Field(..., description="PostgreSQL 연결 DSN")
    echo: bool = Field(False, description="SQL 로깅 활성화")
    pool_size: int = Field(5, ge=1, description="커넥션 풀 크기")
    max_overflow: int = Field(10, ge=0, description="최대 오버플로우 연결 수")
    pool_timeout: int = Field(30, ge=1, description="풀 타임아웃(초)")
    pool_pre_ping: bool = Field(True, description="커넥션 체크 활성화")
    auto_commit: bool = Field(False, description="자동 커밋 여부")

    @model_validator(mode="after")
    def _build_async_dsn(self) -> "DatabaseConfig":
        if isinstance(self.dsn, AsyncPostgresDsn):
            self.dsn = self.dsn.async_dsn
        return self
