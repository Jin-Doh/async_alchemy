# Async Alchemy

SQLAlchemy를 사용한 비동기 데이터베이스 연결 관리자입니다. 이 라이브러리는 Python의 비동기 프로그래밍을 활용하여 효율적인 데이터베이스 연결 관리를 제공합니다.

## 주요 기능

- 비동기 데이터베이스 연결 관리
- 커넥션 풀링을 통한 효율적인 리소스 관리
- 세션 관리 기능 제공
- 환경 설정 기반의 유연한 데이터베이스 설정

## 설치 방법

### pip를 사용한 설치

```bash
# 가상환경 생성 및 활성화
python -m venv .venv
source .venv/bin/activate  # macOS/Linux
# Windows: .venv\Scripts\activate

# 패키지 설치
pip install sqlalchemy asyncpg greenlet pydantic python-dotenv
```

### uv를 사용한 설치

```bash
# uv 설치 (처음 한 번만)
curl -LsSf https://astral.sh/uv/install.sh | sh

# 가상환경 생성 및 의존성 설치
uv venv
source .venv/bin/activate  # macOS/Linux
# Windows: .venv\Scripts\activate

uv pip install sqlalchemy asyncpg greenlet pydantic python-dotenv
```

## 사용 방법

1. 데이터베이스 설정:

```python
from app.modules.configs import DatabaseConfig, AsyncPostgresDsn

# DSN 설정
dsn = AsyncPostgresDsn(
    host="localhost",
    port=5432,
    user="user",
    password="password",
    db="database"
)

# 데이터베이스 설정
config = DatabaseConfig(
    dsn=dsn,
    echo=False,  # SQL 로깅
    pool_size=5,  # 커넥션 풀 크기
    max_overflow=10  # 최대 추가 연결 수
)
```

2. 데이터베이스 연결 관리자 생성:

```python
from app.main import Database
from sqlalchemy.orm import DeclarativeBase

# 모델 베이스 클래스 정의
class Base(DeclarativeBase):
    pass

# 데이터베이스 관리자 생성
db = Database(config=config, base=Base)

# 데이터베이스 초기화 (테이블 생성)
await db.initialize()
```

3. 세션 사용:

```python
# 세션 컨텍스트 매니저 사용
async with db.session.begin() as session:
    # 데이터베이스 작업 수행
    result = await session.execute(query)
    await session.commit()

# 작업 완료 후 리소스 정리
await db.dispose()
```

## 테스트

테스트를 실행하기 위해 추가 의존성을 설치합니다:

```bash
# pip 사용
pip install pytest pytest-asyncio

# 또는 uv 사용
uv pip install pytest pytest-asyncio
```

테스트 실행:

```bash
pytest tests/ -v
```

## 환경 변수 설정

`.env` 파일을 생성하여 데이터베이스 연결 정보를 관리할 수 있습니다:

```env
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=database
POSTGRES_USER=user
POSTGRES_PASSWORD=password
```

## 라이선스

MIT License

## Test Coverage

```
--------- coverage: platform darwin, python 3.11.11-final-0 ----------
Name                     Stmts   Miss  Cover
--------------------------------------------
app/__init__.py              2      0   100%
app/main.py                 30      0   100%
app/modules/configs.py      43      6    86%
app/modules/session.py      25     14    44%
--------------------------------------------
TOTAL                      100     20    80%
```
