# packages/components/extractors/extractor_sql/tests/conftest.py

import asyncio
from datetime import UTC
from unittest.mock import AsyncMock, Mock
import warnings

import pytest

# Фикс для подавления предупреждений
warnings.filterwarnings("ignore", category=DeprecationWarning)
warnings.filterwarnings("ignore", category=PendingDeprecationWarning)

# Настройка asyncio для тестов
@pytest.fixture(scope="session")
def event_loop():
    """Создание event loop для всей сессии тестов"""
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()

    yield loop

    # Очистка
    try:
        # Закрываем все pending tasks
        pending = asyncio.all_tasks(loop)
        for task in pending:
            task.cancel()

        if pending:
            loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))

        loop.close()
    except Exception:
        pass


@pytest.fixture(autouse=True)
def clean_prometheus_registry():
    """Автоматическая очистка Prometheus registry между тестами"""
    import prometheus_client

    # Сохраняем оригинальный registry
    original_registry = prometheus_client.REGISTRY

    # Создаем чистый registry для тестов
    test_registry = prometheus_client.CollectorRegistry()
    prometheus_client.REGISTRY = test_registry

    yield

    # Восстанавливаем оригинальный registry
    prometheus_client.REGISTRY = original_registry


@pytest.fixture
def mock_sqlalchemy_engine():
    """Мок для SQLAlchemy engine"""
    mock_engine = AsyncMock()
    mock_engine.dispose = AsyncMock()

    # Мок для connection
    mock_connection = AsyncMock()
    mock_connection.execute = AsyncMock()
    mock_connection.sync_connection = Mock()

    # Мок для результата запроса
    mock_result = AsyncMock()
    mock_result.fetchall = AsyncMock(return_value=[("test_value",)])
    mock_result.fetchone = AsyncMock(return_value=("test_value",))
    mock_result.keys = Mock(return_value=["test_column"])

    mock_connection.execute.return_value = mock_result
    mock_engine.begin.return_value.__aenter__ = AsyncMock(return_value=mock_connection)
    mock_engine.begin.return_value.__aexit__ = AsyncMock(return_value=None)

    return mock_engine


@pytest.fixture
def mock_session_factory():
    """Мок для session factory"""
    mock_session = AsyncMock()
    mock_session.execute = AsyncMock()
    mock_session.close = AsyncMock()

    mock_factory = Mock()
    mock_factory.return_value = mock_session

    return mock_factory


@pytest.fixture
def sample_sql_data():
    """Тестовые данные для SQL запросов"""
    return {
        "simple_select": {
            "query": "SELECT id, name FROM users",
            "parameters": {},
            "expected_columns": ["id", "name"],
            "expected_rows": [
                {"id": 1, "name": "Alice"},
                {"id": 2, "name": "Bob"},
                {"id": 3, "name": "Charlie"},
            ]
        },
        "parameterized_query": {
            "query": "SELECT * FROM users WHERE department = :dept",
            "parameters": {"dept": "Engineering"},
            "expected_columns": ["id", "name", "department"],
            "expected_rows": [
                {"id": 1, "name": "Alice", "department": "Engineering"},
                {"id": 2, "name": "Bob", "department": "Engineering"},
            ]
        },
        "aggregated_query": {
            "query": "SELECT department, COUNT(*) as count FROM users GROUP BY department",
            "parameters": {},
            "expected_columns": ["department", "count"],
            "expected_rows": [
                {"department": "Engineering", "count": 5},
                {"department": "Marketing", "count": 3},
                {"department": "Sales", "count": 7},
            ]
        }
    }


@pytest.fixture
def connection_strings():
    """Тестовые строки подключения для различных БД"""
    return {
        "postgresql": "postgresql+asyncpg://user:password@localhost:5432/testdb",
        "mysql": "mysql+aiomysql://user:password@localhost:3306/testdb",
        "sqlite": "sqlite+aiosqlite:///test.db",
        "sqlite_memory": "sqlite+aiosqlite:///:memory:",
        "oracle": "oracle+cx_oracle://user:password@localhost:1521/xe",
        "mssql": "mssql+aioodbc://user:password@localhost:1433/testdb",
        "snowflake": "snowflake://user:password@account/testdb",
        "bigquery": "bigquery://project/dataset",
    }


@pytest.fixture
def mock_database_responses():
    """Моки ответов от различных типов БД"""
    return {
        "postgresql": {
            "version_query": "SELECT version()",
            "version_response": "PostgreSQL 15.0 on x86_64-pc-linux-gnu",
        },
        "mysql": {
            "version_query": "SELECT VERSION()",
            "version_response": "8.0.33",
        },
        "sqlite": {
            "version_query": "SELECT sqlite_version()",
            "version_response": "3.41.2",
        }
    }


@pytest.fixture
def performance_test_data():
    """Тестовые данные для проверки производительности"""
    import pandas as pd

    # Генерируем большой dataset для тестов производительности
    large_data = pd.DataFrame({
        "id": range(10000),
        "name": [f"user_{i}" for i in range(10000)],
        "score": [i * 0.1 for i in range(10000)],
        "active": [i % 2 == 0 for i in range(10000)],
        "department": [f"dept_{i % 10}" for i in range(10000)],
    })

    return {
        "large_dataset": large_data,
        "expected_memory_usage": large_data.memory_usage(deep=True).sum(),
        "chunk_size": 1000,
        "expected_chunks": 10,
    }


@pytest.fixture
def error_scenarios():
    """Сценарии ошибок для тестирования обработки исключений"""
    from sqlalchemy.exc import DisconnectionError, OperationalError, ProgrammingError

    return {
        "connection_timeout": {
            "exception": DisconnectionError,
            "message": "Connection timeout",
            "retry_expected": True,
        },
        "syntax_error": {
            "exception": ProgrammingError,
            "message": "Syntax error in SQL",
            "retry_expected": False,
        },
        "operational_error": {
            "exception": OperationalError,
            "message": "Database operational error",
            "retry_expected": True,
        },
    }


@pytest.fixture
def mock_temporal_context():
    """Мок контекста для интеграции с Temporal"""
    from datetime import datetime

    from pipeline_core.components.base import ExecutionContext

    return ExecutionContext(
        pipeline_id="test_pipeline_123",
        stage_name="test_stage",
        execution_time=datetime.now(UTC),
        previous_results=[],
        metadata={
            "environment": "test",
            "version": "1.0.0",
            "user": "test_user",
        }
    )


@pytest.fixture
def execution_context():
    """Базовый контекст выполнения для тестов"""
    from datetime import datetime

    from pipeline_core.components.base import ExecutionContext

    return ExecutionContext(
        pipeline_id="test_pipeline",
        stage_name="test_stage",
        execution_time=datetime.now(UTC),
        previous_results=[],
        metadata={}
    )


@pytest.fixture
def temp_sqlite_db():
    """Временная SQLite база данных для тестов"""
    import os
    import tempfile

    # Создаем временный файл
    fd, path = tempfile.mkstemp(suffix='.db')
    os.close(fd)

    connection_string = f"sqlite+aiosqlite:///{path}"

    yield connection_string

    # Удаляем временный файл
    try:
        os.unlink(path)
    except FileNotFoundError:
        pass


@pytest.fixture
def basic_sql_config(temp_sqlite_db):
    """Базовая конфигурация для SQL тестов"""
    from extractor_sql.components import QueryConfig, SQLExtractorConfig

    return SQLExtractorConfig(
        connection_string=temp_sqlite_db,
        query_config=QueryConfig(
            query="SELECT 1 AS test_column",
            parameters={},
            timeout=30.0,
        ),
        output_format="pandas",
    )


@pytest.fixture
def memory_optimization_test_data():
    """Данные для тестирования оптимизации памяти"""
    import pandas as pd

    # Создаем DataFrame с избыточными типами данных
    inefficient_df = pd.DataFrame({
        "small_int": pd.Series([1, 2, 3, 4, 5], dtype="int64"),  # Можно int8
        "category_col": pd.Series(["A", "B", "A", "B", "A"], dtype="object"),  # Можно category
        "boolean_int": pd.Series([1, 0, 1, 0, 1], dtype="int64"),  # Можно bool
        "small_float": pd.Series([1.1, 2.2, 3.3, 4.4, 5.5], dtype="float64"),  # Можно float32
    })

    return {
        "inefficient_df": inefficient_df,
        "original_memory": inefficient_df.memory_usage(deep=True).sum(),
        "expected_optimizations": {
            "small_int": ["int8", "int16"],
            "category_col": ["category"],
            "boolean_int": ["bool"],
            "small_float": ["float32"],
        }
    }


@pytest.fixture(autouse=True)
def setup_logging():
    """Настройка логирования для тестов"""
    import logging

    import structlog

    # Настраиваем structlog для тестов
    structlog.configure(
        processors=[
            structlog.testing.LogCapture(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(logging.CRITICAL),
        logger_factory=structlog.testing.CapturingLoggerFactory(),
        cache_logger_on_first_use=True,
    )


@pytest.fixture
def capture_logs():
    """Фикстура для захвата логов в тестах"""
    import structlog

    cap = structlog.testing.LogCapture()
    old_processors = structlog.get_config()["processors"]

    structlog.configure(processors=[cap])

    yield cap

    structlog.configure(processors=old_processors)


# Маркеры для группировки тестов
def pytest_configure(config):
    """Настройка pytest маркеров"""
    config.addinivalue_line(
        "markers", "unit: marks tests as unit tests"
    )
    config.addinivalue_line(
        "markers", "integration: marks tests as integration tests"
    )
    config.addinivalue_line(
        "markers", "performance: marks tests as performance tests"
    )
    config.addinivalue_line(
        "markers", "slow: marks tests as slow running"
    )


# Настройка для параллельного выполнения тестов
@pytest.fixture(scope="session", autouse=True)
def configure_test_environment():
    """Глобальная настройка тестового окружения"""
    import os

    # Устанавливаем переменные окружения для тестов
    os.environ["TESTING"] = "true"
    os.environ["LOG_LEVEL"] = "ERROR"  # Минимизируем логи в тестах

    # Отключаем метрики в тестах по умолчанию
    os.environ["METRICS_ENABLED"] = "false"

    yield

    # Очистка после тестов
    for key in ["TESTING", "LOG_LEVEL", "METRICS_ENABLED"]:
        os.environ.pop(key, None)
