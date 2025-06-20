# packages/components/extractors/extractor_sql/tests/test_components.py

"""
Тесты для SQL Extractor компонентов

Включает unit и integration тесты для всех основных компонентов.
"""

import sqlite3
import tempfile
from pathlib import Path

import pandas as pd
import polars as pl
import pytest
from extractor_sql.components import (
    MySQLExtractor,
    PostgreSQLExtractor,
    QueryConfig,
    RetryConfig,
    SQLExtractor,
    SQLExtractorConfig,
    SQLiteExtractor,
)
from extractor_sql.exceptions import (
    ConnectionError,
    QueryExecutionError,
    SQLExtractorError,
)
from extractor_sql.utils import (
    build_connection_string,
    estimate_query_cost,
    infer_pandas_dtypes,
    optimize_dataframe_memory,
    sanitize_query,
    validate_connection_string,
)

from pipeline_core.components.base import ExecutionContext

# ================================
# Fixtures
# ================================


@pytest.fixture
def temp_sqlite_db():
    """Создание временной SQLite базы данных для тестов"""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp_file:
        db_path = tmp_file.name

    # Создаем тестовую таблицу
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE test_users (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            age INTEGER,
            email TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            is_active BOOLEAN DEFAULT 1
        )
    """)

    # Вставляем тестовые данные
    test_data = [
        (1, "Alice", 25, "alice@example.com", "2024-01-01 10:00:00", 1),
        (2, "Bob", 30, "bob@example.com", "2024-01-02 11:00:00", 1),
        (3, "Charlie", 35, "charlie@example.com", "2024-01-03 12:00:00", 0),
        (4, "Diana", 28, "diana@example.com", "2024-01-04 13:00:00", 1),
        (5, "Eve", 22, "eve@example.com", "2024-01-05 14:00:00", 1),
    ]

    cursor.executemany(
        """
        INSERT INTO test_users (id, name, age, email, created_at, is_active)
        VALUES (?, ?, ?, ?, ?, ?)
    """,
        test_data,
    )

    conn.commit()
    conn.close()

    yield f"sqlite+aiosqlite:///{db_path}"

    # Cleanup
    Path(db_path).unlink(missing_ok=True)


@pytest.fixture
def basic_query_config():
    """Базовая конфигурация запроса"""
    return QueryConfig(
        query="SELECT * FROM test_users",
        timeout=10.0,
        fetch_size=1000,
    )


@pytest.fixture
def basic_sql_config(temp_sqlite_db, basic_query_config):
    """Базовая конфигурация SQL Extractor"""
    return SQLExtractorConfig(
        connection_string=temp_sqlite_db,
        query_config=basic_query_config,
    )


@pytest.fixture
def execution_context():
    """Контекст выполнения для тестов"""
    return ExecutionContext(
        pipeline_id="test_pipeline",
        stage_name="test_extraction",
    )


# ================================
# Configuration Tests
# ================================


class TestQueryConfig:
    """Тесты для QueryConfig"""

    def test_valid_query_config(self):
        """Тест создания валидной конфигурации запроса"""
        config = QueryConfig(
            query="SELECT * FROM users",
            parameters={"user_id": 123},
            timeout=300.0,
            fetch_size=5000,
        )

        assert config.query == "SELECT * FROM users"
        assert config.parameters == {"user_id": 123}
        assert config.timeout == 300.0
        assert config.fetch_size == 5000

    def test_empty_query_validation(self):
        """Тест валидации пустого запроса"""
        with pytest.raises(ValueError, match="SQL query cannot be empty"):
            QueryConfig(query="")

    def test_dangerous_query_warning(self):
        """Тест предупреждения о потенциально опасных запросах"""
        with pytest.warns(UserWarning, match="Potentially dangerous SQL keyword"):
            QueryConfig(query="DROP TABLE users")


class TestSQLExtractorConfig:
    """Тесты для SQLExtractorConfig"""

    def test_valid_config_creation(self, basic_query_config):
        """Тест создания валидной конфигурации"""
        config = SQLExtractorConfig(
            connection_string="postgresql+asyncpg://user:pass@localhost/db",
            query_config=basic_query_config,
        )

        assert config.connection_string == "postgresql+asyncpg://user:pass@localhost/db"
        assert config.inferred_dialect == "postgresql"

    def test_dialect_inference(self, basic_query_config):
        """Тест автоматического определения диалекта"""
        test_cases = [
            ("postgresql://user:pass@localhost/db", "postgresql"),
            ("mysql+aiomysql://user:pass@localhost/db", "mysql"),
            ("sqlite+aiosqlite:///path/to/db.sqlite", "sqlite"),
        ]

        for conn_str, expected_dialect in test_cases:
            config = SQLExtractorConfig(
                connection_string=conn_str,
                query_config=basic_query_config,
            )
            assert config.inferred_dialect == expected_dialect

    def test_invalid_connection_string(self, basic_query_config):
        """Тест валидации неверной строки подключения"""
        with pytest.raises(ValueError, match="Invalid connection string"):
            SQLExtractorConfig(
                connection_string="invalid://",
                query_config=basic_query_config,
            )


# ================================
# SQL Extractor Tests
# ================================


class TestSQLExtractor:
    """Тесты для основного SQL Extractor"""

    @pytest.mark.asyncio
    async def test_initialization(self, basic_sql_config):
        """Тест инициализации extractor'а"""
        extractor = SQLExtractor(basic_sql_config)

        assert extractor.name == "sql-extractor"
        assert extractor.config == basic_sql_config

        await extractor.initialize()
        assert extractor._engine is not None

        await extractor.cleanup()

    @pytest.mark.asyncio
    async def test_basic_extraction(self, basic_sql_config, execution_context):
        """Тест базового извлечения данных"""
        extractor = SQLExtractor(basic_sql_config)

        await extractor.initialize()
        result = await extractor.execute(execution_context)
        await extractor.cleanup()

        assert result.success is True
        assert result.data is not None
        assert isinstance(result.data, pd.DataFrame)
        assert len(result.data) == 5  # 5 test users
        assert result.metadata.rows_processed == 5

    @pytest.mark.asyncio
    async def test_different_output_formats(self, temp_sqlite_db, execution_context):
        """Тест различных форматов вывода"""
        formats_to_test = ["pandas", "polars", "dict", "raw"]

        for output_format in formats_to_test:
            config = SQLExtractorConfig(
                connection_string=temp_sqlite_db,
                query_config=QueryConfig(query="SELECT * FROM test_users LIMIT 2"),
                output_format=output_format,
            )

            extractor = SQLExtractor(config)
            await extractor.initialize()
            result = await extractor.execute(execution_context)
            await extractor.cleanup()

            assert result.success is True

            if output_format == "pandas":
                assert isinstance(result.data, pd.DataFrame)
            elif output_format == "polars":
                assert isinstance(result.data, pl.DataFrame)
            elif output_format == "dict":
                assert isinstance(result.data, list)
                assert all(isinstance(row, dict) for row in result.data)
            elif output_format == "raw":
                assert isinstance(result.data, list)

    @pytest.mark.asyncio
    async def test_query_with_parameters(self, temp_sqlite_db, execution_context):
        """Тест запроса с параметрами"""
        config = SQLExtractorConfig(
            connection_string=temp_sqlite_db,
            query_config=QueryConfig(
                query="SELECT * FROM test_users WHERE age > :min_age",
                parameters={"min_age": 25},
            ),
        )

        extractor = SQLExtractor(config)
        await extractor.initialize()
        result = await extractor.execute(execution_context)
        await extractor.cleanup()

        assert result.success is True
        assert len(result.data) == 3  # Users with age > 25

    @pytest.mark.asyncio
    async def test_connection_error_handling(self, execution_context):
        """Тест обработки ошибок подключения"""
        config = SQLExtractorConfig(
            connection_string="postgresql://invalid:invalid@nonexistent:5432/db",
            query_config=QueryConfig(query="SELECT 1"),
        )

        extractor = SQLExtractor(config)

        with pytest.raises(SQLExtractorError):
            await extractor.initialize()

    @pytest.mark.asyncio
    async def test_query_error_handling(self, basic_sql_config, execution_context):
        """Тест обработки ошибок запроса"""
        # Создаем конфигурацию с неверным запросом
        config = SQLExtractorConfig(
            connection_string=basic_sql_config.connection_string,
            query_config=QueryConfig(query="SELECT * FROM nonexistent_table"),
        )

        extractor = SQLExtractor(config)
        await extractor.initialize()

        result = await extractor.execute(execution_context)

        assert result.success is False
        assert result.error is not None

        await extractor.cleanup()


class TestSpecializedExtractors:
    """Тесты для специализированных extractor'ов"""

    @pytest.mark.asyncio
    async def test_postgresql_extractor(self):
        """Тест PostgreSQL extractor"""
        config = SQLExtractorConfig(
            connection_string="postgresql+asyncpg://user:pass@localhost/db",
            query_config=QueryConfig(query="SELECT 1"),
        )

        extractor = PostgreSQLExtractor(config)
        assert extractor.name == "postgresql-extractor"

    @pytest.mark.asyncio
    async def test_mysql_extractor(self):
        """Тест MySQL extractor"""
        config = SQLExtractorConfig(
            connection_string="mysql+aiomysql://user:pass@localhost/db",
            query_config=QueryConfig(query="SELECT 1"),
        )

        extractor = MySQLExtractor(config)
        assert extractor.name == "mysql-extractor"

    @pytest.mark.asyncio
    async def test_sqlite_extractor(self, temp_sqlite_db):
        """Тест SQLite extractor"""
        config = SQLExtractorConfig(
            connection_string=temp_sqlite_db,
            query_config=QueryConfig(query="SELECT 1"),
        )

        extractor = SQLiteExtractor(config)
        assert extractor.name == "sqlite-extractor"


# ================================
# Utility Function Tests
# ================================


class TestConnectionStringUtils:
    """Тесты для утилит работы с connection strings"""

    def test_build_connection_string(self):
        """Тест построения connection string"""
        conn_str = build_connection_string(
            dialect="postgresql",
            username="user",
            password="pass",
            host="localhost",
            port=5432,
            database="mydb",
            driver="asyncpg",
        )

        expected = "postgresql+asyncpg://user:pass@localhost:5432/mydb"
        assert conn_str == expected

    def test_build_sqlite_connection_string(self):
        """Тест построения SQLite connection string"""
        conn_str = build_connection_string(
            dialect="sqlite",
            username="",
            password="",
            host="",
            database="/path/to/db.sqlite",
        )

        expected = "sqlite+aiosqlite:///path/to/db.sqlite"
        assert conn_str == expected

    def test_validate_connection_string(self):
        """Тест валидации connection strings"""
        valid_cases = [
            "postgresql://user:pass@localhost/db",
            "mysql+aiomysql://user:pass@localhost:3306/db",
            "sqlite+aiosqlite:///path/to/db.sqlite",
        ]

        for conn_str in valid_cases:
            is_valid, error = validate_connection_string(conn_str)
            assert is_valid is True
            assert error is None

        invalid_cases = [
            "invalid://",
            "unsupported://user:pass@localhost/db",
            "",
        ]

        for conn_str in invalid_cases:
            is_valid, error = validate_connection_string(conn_str)
            assert is_valid is False
            assert error is not None


class TestQueryUtils:
    """Тесты для утилит работы с запросами"""

    def test_sanitize_query(self):
        """Тест санитизации запросов"""
        query = "  SELECT   *   FROM   users  "
        sanitized = sanitize_query(query)
        assert sanitized == "SELECT * FROM users"

    def test_dangerous_query_detection(self):
        """Тест обнаружения опасных запросов"""
        dangerous_query = "SELECT * FROM users; DROP TABLE users;"

        with pytest.warns(UserWarning, match="Potentially dangerous SQL pattern"):
            sanitize_query(dangerous_query)

    def test_estimate_query_cost(self):
        """Тест оценки стоимости запроса"""
        simple_query = "SELECT * FROM users"
        cost = estimate_query_cost(simple_query)
        assert cost["complexity"] == "simple"
        assert cost["join_count"] == 0

        complex_query = """
        SELECT u.*, p.name as project_name
        FROM users u
        JOIN user_projects up ON u.id = up.user_id
        JOIN projects p ON up.project_id = p.id
        WHERE u.created_at > (SELECT AVG(created_at) FROM users)
        GROUP BY u.id, p.name
        """
        cost = estimate_query_cost(complex_query)
        assert cost["complexity"] in ["complex", "very_complex"]
        assert cost["join_count"] >= 2


class TestDataUtils:
    """Тесты для утилит работы с данными"""

    def test_infer_pandas_dtypes(self):
        """Тест инференции типов данных pandas"""
        # Создаем тестовый DataFrame
        test_data = {
            "int_col": [1, 2, 3, 4, 5],
            "float_col": [1.1, 2.2, 3.3, 4.4, 5.5],
            "bool_col": [True, False, True, False, True],
            "str_col": ["a", "b", "c", "d", "e"],
            "category_col": ["cat", "dog", "cat", "dog", "cat"],
        }

        df = pd.DataFrame(test_data)
        dtypes = infer_pandas_dtypes(df)

        assert dtypes["int_col"] in ["int8", "int16", "int32", "int64"]
        assert dtypes["float_col"] in ["float32", "float64"]
        assert dtypes["bool_col"] == "boolean"
        assert dtypes["str_col"] == "string"
        assert dtypes["category_col"] == "category"

    def test_optimize_dataframe_memory(self):
        """Тест оптимизации памяти DataFrame"""
        # Создаем неоптимальный DataFrame
        test_data = {
            "small_int": [1, 2, 3] * 100,  # Может быть int8
            "category_str": ["A", "B", "C"] * 100,  # Может быть category
            "float_numbers": [1.0, 2.0, 3.0] * 100,  # Может быть float32
        }

        df = pd.DataFrame(test_data)
        original_memory = df.memory_usage(deep=True).sum()

        optimized_df = optimize_dataframe_memory(df)
        optimized_memory = optimized_df.memory_usage(deep=True).sum()

        # Оптимизированный DataFrame должен использовать меньше памяти
        assert optimized_memory <= original_memory


# ================================
# Exception Tests
# ================================


class TestExceptions:
    """Тесты для кастомных исключений"""

    def test_sql_extractor_error_creation(self):
        """Тест создания базового исключения"""
        error = SQLExtractorError(
            message="Test error",
            error_code="TEST_ERROR",
            context={"key": "value"},
        )

        assert error.message == "Test error"
        assert error.error_code == "TEST_ERROR"
        assert error.context == {"key": "value"}

    def test_connection_error_masking(self):
        """Тест маскирования чувствительных данных в ошибках подключения"""
        error = ConnectionError(
            message="Connection failed",
            connection_string="postgresql://user:secret@localhost/db",
        )

        # Пароль должен быть замаскирован
        assert "secret" not in str(error)
        assert "***" in str(error)

    def test_error_serialization(self):
        """Тест сериализации исключений"""
        error = QueryExecutionError(
            message="Query failed",
            query="SELECT * FROM table",
            parameters={"id": 123},
        )

        serialized = error.to_dict()

        assert serialized["error_type"] == "QueryExecutionError"
        assert serialized["message"] == "Query failed"
        assert serialized["context"]["query"] == "SELECT * FROM table"
        assert serialized["context"]["parameters"] == {"id": 123}


# ================================
# Integration Tests
# ================================


class TestIntegration:
    """Интеграционные тесты"""

    @pytest.mark.asyncio
    async def test_full_extraction_workflow(self, temp_sqlite_db, execution_context):
        """Тест полного workflow извлечения данных"""
        # Создаем extractor
        config = SQLExtractorConfig(
            connection_string=temp_sqlite_db,
            query_config=QueryConfig(
                query="SELECT name, age FROM test_users WHERE is_active = :active",
                parameters={"active": 1},
            ),
            output_format="pandas",
        )

        extractor = SQLExtractor(config)

        # Инициализация
        await extractor.initialize()

        # Выполнение
        result = await extractor.execute(execution_context)

        # Проверки
        assert result.success is True
        assert isinstance(result.data, pd.DataFrame)
        assert len(result.data) == 4  # 4 active users
        assert list(result.data.columns) == ["name", "age"]
        assert result.metadata.rows_processed == 4
        assert result.metadata.duration_seconds is not None

        # Очистка
        await extractor.cleanup()

    @pytest.mark.asyncio
    async def test_retry_mechanism(self, execution_context):
        """Тест механизма retry"""
        # Мокаем engine для симуляции временной ошибки
        config = SQLExtractorConfig(
            connection_string="sqlite+aiosqlite:///:memory:",
            query_config=QueryConfig(query="SELECT 1"),
            retry_config=RetryConfig(max_attempts=3, initial_wait=0.1),
        )

        extractor = SQLExtractor(config)

        # Мокаем _execute_query для симуляции ошибки на первых двух попытках
        original_execute = extractor._execute_query
        call_count = 0

        async def mock_execute_query(context):
            nonlocal call_count
            call_count += 1
            if call_count <= 2:
                raise ConnectionError("Temporary connection error")
            return await original_execute(context)

        extractor._execute_query = mock_execute_query

        await extractor.initialize()
        result = await extractor.execute(execution_context)
        await extractor.cleanup()

        # Должно быть успешно после retry
        assert result.success is True
        assert call_count == 3  # 2 failed + 1 successful


# ================================
# Performance Tests
# ================================


@pytest.mark.slow
class TestPerformance:
    """Тесты производительности (помечены как slow)"""

    @pytest.mark.asyncio
    async def test_large_dataset_extraction(self, execution_context):
        """Тест извлечения большого dataset"""
        # Создаем in-memory SQLite с большим количеством данных
        config = SQLExtractorConfig(
            connection_string="sqlite+aiosqlite:///:memory:",
            query_config=QueryConfig(
                query="""
                WITH RECURSIVE numbers(n) AS (
                    SELECT 1
                    UNION ALL
                    SELECT n + 1 FROM numbers WHERE n < 10000
                )
                SELECT 
                    n as id,
                    'user_' || n as name,
                    (n % 100) as age,
                    'user' || n || '@example.com' as email
                FROM numbers
                """,
                fetch_size=1000,
            ),
            output_format="pandas",
        )

        extractor = SQLExtractor(config)
        await extractor.initialize()

        import time

        start_time = time.time()
        result = await extractor.execute(execution_context)
        end_time = time.time()

        await extractor.cleanup()

        # Проверки производительности
        assert result.success is True
        assert len(result.data) == 10000
        assert end_time - start_time < 30  # Должно выполниться за < 30 секунд
        assert result.metadata.rows_processed == 10000

    @pytest.mark.asyncio
    async def test_memory_usage_optimization(self, execution_context):
        """Тест оптимизации использования памяти"""
        config = SQLExtractorConfig(
            connection_string="sqlite+aiosqlite:///:memory:",
            query_config=QueryConfig(
                query="""
                WITH RECURSIVE numbers(n) AS (
                    SELECT 1
                    UNION ALL
                    SELECT n + 1 FROM numbers WHERE n < 5000
                )
                SELECT 
                    n as small_int,
                    CASE WHEN n % 2 = 0 THEN 'even' ELSE 'odd' END as category,
                    CAST(n AS FLOAT) as float_val
                FROM numbers
                """
            ),
            output_format="pandas",
        )

        extractor = SQLExtractor(config)
        await extractor.initialize()
        result = await extractor.execute(execution_context)
        await extractor.cleanup()

        # Оптимизируем память
        original_memory = result.data.memory_usage(deep=True).sum()
        optimized_df = optimize_dataframe_memory(result.data)
        optimized_memory = optimized_df.memory_usage(deep=True).sum()

        # Проверяем, что оптимизация действительно уменьшила использование памяти
        memory_reduction = (original_memory - optimized_memory) / original_memory
        assert memory_reduction > 0.1  # Минимум 10% экономии памяти


# ================================
# Test Configuration
# ================================


def pytest_configure(config):
    """Настройка pytest"""
    config.addinivalue_line(
        "markers", "slow: marks tests as slow (deselect with '-m \"not slow\"')"
    )
    config.addinivalue_line("markers", "integration: marks tests as integration tests")
    config.addinivalue_line("markers", "unit: marks tests as unit tests")


if __name__ == "__main__":
    pytest.main([__file__])
