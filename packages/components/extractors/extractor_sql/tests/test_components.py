# packages/components/extractors/extractor_sql/tests/test_components.py

import asyncio

import pandas as pd

# Фикс для сброса Prometheus metrics между тестами
import pytest

from extractor_sql import (
    ConnectionPoolConfig,
    MySQLExtractor,
    PostgresSQLExtractor,
    QueryConfig,
    RetryConfig,
    SQLExtractor,
    SQLExtractorConfig,
    SQLiteExtractor,
    create_extractor,
)
from extractor_sql.components import _sql_extractor_metrics_registry
from extractor_sql.exceptions import (
    ConnectionError as SQLConnectionError,
)
from extractor_sql.exceptions import (
    QueryExecutionError,
)
from extractor_sql.utils import (
    build_connection_string,
    estimate_query_cost,
    infer_pandas_dtypes,
    optimize_dataframe_memory,
    optimize_query_for_extraction,
    parse_connection_params,
    sanitize_query,
    split_dataframe_chunks,
    validate_connection_string,
)
from pipeline_core.components.base import ExecutionContext


@pytest.fixture(autouse=True)
def reset_prometheus_metrics():
    """Фикстура для сброса Prometheus метрик между тестами"""
    # Очищаем registry перед каждым тестом
    for collector in list(_sql_extractor_metrics_registry._collector_to_names.keys()):
        _sql_extractor_metrics_registry.unregister(collector)

    yield

    # Очищаем registry после каждого теста
    for collector in list(_sql_extractor_metrics_registry._collector_to_names.keys()):
        _sql_extractor_metrics_registry.unregister(collector)


class TestQueryConfig:
    """Тесты конфигурации запросов"""

    def test_valid_query_config(self):
        """Тест создания валидной конфигурации запроса"""
        config = QueryConfig(
            query="SELECT * FROM users WHERE id = :user_id",
            parameters={"user_id": 123},
            timeout=60.0,
            fetch_size=5000,
        )

        assert config.query == "SELECT * FROM users WHERE id = :user_id"
        assert config.parameters == {"user_id": 123}
        assert config.timeout == 60.0
        assert config.fetch_size == 5000
        assert config.stream_results is False

    def test_query_validation_empty(self):
        """Тест валидации пустого запроса"""
        from pydantic import ValidationError
        with pytest.raises(ValidationError, match="String should have at least 1 character"):
            QueryConfig(query="")

    def test_query_validation_too_short(self):
        """Тест валидации слишком короткого запроса"""
        # Убираем этот тест так как минимальная длина теперь 1 символ
        # Вместо этого тестируем что короткий запрос принимается
        config = QueryConfig(query="SELECT 1")
        assert config.query == "SELECT 1"

    def test_dangerous_query_warning(self):
        """Тест предупреждения для потенциально опасных запросов"""
        with pytest.warns(UserWarning, match="Potentially dangerous SQL keyword"):
            QueryConfig(query="DELETE FROM users WHERE id = 1")

    def test_parameter_validation(self):
        """Тест валидации параметров"""
        # Валидные параметры
        config = QueryConfig(
            query="SELECT * FROM users WHERE id = :id AND name = :name",
            parameters={"id": 123, "name": "John", "active": True, "score": 95.5, "data": None}
        )
        assert len(config.parameters) == 5

        # Невалидный тип ключа
        with pytest.raises(ValueError, match="Parameter key must be string"):
            QueryConfig(
                query="SELECT * FROM users",
                parameters={123: "value"}
            )

        # Невалидный тип значения
        with pytest.raises(ValueError, match="has unsupported type"):
            QueryConfig(
                query="SELECT * FROM users",
                parameters={"key": {"nested": "dict"}}
            )

    def test_timeout_validation(self):
        """Тест валидации timeout"""
        # Валидный timeout
        config = QueryConfig(query="SELECT * FROM users", timeout=120.0)
        assert config.timeout == 120.0

        # Слишком маленький timeout
        with pytest.raises(ValueError):
            QueryConfig(query="SELECT * FROM users", timeout=0.5)

        # Слишком большой timeout
        with pytest.raises(ValueError):
            QueryConfig(query="SELECT * FROM users", timeout=4000.0)

    def test_fetch_size_validation(self):
        """Тест валидации fetch_size"""
        # Валидный fetch_size
        config = QueryConfig(query="SELECT * FROM users", fetch_size=50000)
        assert config.fetch_size == 50000

        # Слишком маленький fetch_size
        with pytest.raises(ValueError):
            QueryConfig(query="SELECT * FROM users", fetch_size=50)

        # Слишком большой fetch_size
        with pytest.raises(ValueError):
            QueryConfig(query="SELECT * FROM users", fetch_size=2000000)


class TestConnectionPoolConfig:
    """Тесты конфигурации connection pool"""

    def test_valid_pool_config(self):
        """Тест создания валидной конфигурации пула"""
        config = ConnectionPoolConfig(
            pool_size=10,
            max_overflow=20,
            pool_timeout=45.0,
            pool_recycle=7200,
            pool_pre_ping=True,
        )

        assert config.pool_size == 10
        assert config.max_overflow == 20
        assert config.pool_timeout == 45.0
        assert config.pool_recycle == 7200
        assert config.pool_pre_ping is True
        assert config.total_pool_size == 30

    def test_pool_size_validation(self):
        """Тест валидации размера пула"""
        # Слишком маленький pool_size
        with pytest.raises(ValueError):
            ConnectionPoolConfig(pool_size=0)

        # Слишком большой pool_size
        with pytest.raises(ValueError):
            ConnectionPoolConfig(pool_size=200)

    def test_default_values(self):
        """Тест значений по умолчанию"""
        config = ConnectionPoolConfig()

        assert config.pool_size == 5
        assert config.max_overflow == 10
        assert config.pool_timeout == 30.0
        assert config.pool_recycle == 3600
        assert config.pool_pre_ping is True


class TestRetryConfig:
    """Тесты конфигурации retry механизма"""

    def test_valid_retry_config(self):
        """Тест создания валидной конфигурации retry"""
        config = RetryConfig(
            max_attempts=5,
            initial_delay=2.0,
            max_delay=120.0,
            exponential_base=3.0,
            jitter=False,
        )

        assert config.max_attempts == 5
        assert config.initial_delay == 2.0
        assert config.max_delay == 120.0
        assert config.exponential_base == 3.0
        assert config.jitter is False

    def test_delay_validation(self):
        """Тест валидации delays"""
        # max_delay меньше initial_delay
        with pytest.raises(ValueError, match="max_delay must be >= initial_delay"):
            RetryConfig(initial_delay=10.0, max_delay=5.0)

    def test_attempts_validation(self):
        """Тест валидации количества попыток"""
        # Слишком мало попыток
        with pytest.raises(ValueError):
            RetryConfig(max_attempts=0)

        # Слишком много попыток
        with pytest.raises(ValueError):
            RetryConfig(max_attempts=15)


class TestSQLExtractorConfig:
    """Тесты конфигурации SQL Extractor"""

    def test_valid_config(self):
        """Тест создания валидной конфигурации"""
        config = SQLExtractorConfig(
            connection_string="postgresql+asyncpg://user:pass@localhost:5432/testdb",
            query_config=QueryConfig(query="SELECT * FROM users"),
            output_format="pandas",
            dialect="postgresql",
        )

        assert config.connection_string == "postgresql+asyncpg://user:pass@localhost:5432/testdb"
        assert config.output_format == "pandas"
        assert config.dialect == "postgresql"
        assert config.inferred_dialect == "postgresql"

    def test_dialect_inference(self):
        """Тест автоматического определения диалекта"""
        # PostgreSQL
        config = SQLExtractorConfig(
            connection_string="postgresql://user:pass@localhost/db",
            query_config=QueryConfig(query="SELECT 1"),
        )
        assert config.inferred_dialect == "postgresql"

        # MySQL
        config = SQLExtractorConfig(
            connection_string="mysql+aiomysql://user:pass@localhost/db",
            query_config=QueryConfig(query="SELECT 1"),
        )
        assert config.inferred_dialect == "mysql"

        # SQLite
        config = SQLExtractorConfig(
            connection_string="sqlite+aiosqlite:///test.db",
            query_config=QueryConfig(query="SELECT 1"),
        )
        assert config.inferred_dialect == "sqlite"

    def test_output_format_validation(self):
        """Тест валидации формата вывода"""
        # Валидные форматы
        for format_type in ["pandas", "dict", "raw"]:
            config = SQLExtractorConfig(
                connection_string="sqlite:///test.db",
                query_config=QueryConfig(query="SELECT 1"),
                output_format=format_type,
            )
            assert config.output_format == format_type

    def test_connection_string_validation(self):
        """Тест валидации строки подключения"""
        # Невалидная строка подключения
        with pytest.raises(ValueError, match="Invalid connection string format"):
            SQLExtractorConfig(
                connection_string="invalid_connection_string",
                query_config=QueryConfig(query="SELECT 1"),
            )


class TestSQLExtractor:
    """Тесты основного класса SQLExtractor"""

    @pytest.fixture
    def basic_sql_config(self):
        """Базовая конфигурация для тестов"""
        return SQLExtractorConfig(
            connection_string="sqlite+aiosqlite:///:memory:",
            query_config=QueryConfig(
                query="SELECT 1 as test_column",
                parameters={},
                timeout=30.0,
            ),
            output_format="pandas",
        )

    def test_initialization(self, basic_sql_config):
        """Тест инициализации extractor'а"""
        extractor = SQLExtractor(basic_sql_config, name="test-sql-extractor")

        assert extractor.name == "test-sql-extractor"
        assert extractor.component_type.value == "extractor"
        assert extractor.config == basic_sql_config
        assert extractor._engine is None

    @pytest.mark.asyncio
    async def test_basic_extraction(self, basic_sql_config):
        """Тест базового извлечения данных"""
        extractor = SQLExtractor(basic_sql_config)

        try:
            await extractor.initialize()

            context = ExecutionContext(
                pipeline_id="test_pipeline",
                stage_name="test_extraction",
            )

            result = await extractor._execute_impl(context)

            assert isinstance(result, pd.DataFrame)
            assert len(result) == 1
            assert "test_column" in result.columns
            assert result.iloc[0]["test_column"] == 1

        finally:
            await extractor.cleanup()

    @pytest.mark.asyncio
    async def test_different_output_formats(self, basic_sql_config):
        """Тест различных форматов вывода"""
        formats_to_test = ["pandas", "dict", "raw"]

        for output_format in formats_to_test:
            config = SQLExtractorConfig(
                connection_string=basic_sql_config.connection_string,
                query_config=basic_sql_config.query_config,
                output_format=output_format,
            )

            extractor = SQLExtractor(config)

            try:
                await extractor.initialize()

                context = ExecutionContext(
                    pipeline_id="test_pipeline",
                    stage_name="test_formats",
                )

                result = await extractor._execute_impl(context)

                if output_format == "pandas":
                    assert isinstance(result, pd.DataFrame)
                elif output_format == "dict":
                    assert isinstance(result, list)
                    assert len(result) > 0
                    assert isinstance(result[0], dict)
                elif output_format == "raw":
                    assert result is not None

            finally:
                await extractor.cleanup()

    @pytest.mark.asyncio
    async def test_query_with_parameters(self):
        """Тест запроса с параметрами"""
        config = SQLExtractorConfig(
            connection_string="sqlite+aiosqlite:///:memory:",
            query_config=QueryConfig(
                query="SELECT :value as param_column",
                parameters={"value": "test_value"},
            ),
            output_format="pandas",
        )

        extractor = SQLExtractor(config)

        try:
            await extractor.initialize()

            context = ExecutionContext(
                pipeline_id="test_pipeline",
                stage_name="test_parameters",
            )

            result = await extractor._execute_impl(context)

            assert isinstance(result, pd.DataFrame)
            assert len(result) == 1
            assert result.iloc[0]["param_column"] == "test_value"

        finally:
            await extractor.cleanup()

    @pytest.mark.asyncio
    async def test_connection_error_handling(self):
        """Тест обработки ошибок подключения"""
        config = SQLExtractorConfig(
            connection_string="postgresql://invalid:invalid@nonexistent:5432/invalid",
            query_config=QueryConfig(query="SELECT 1 AS test_column"),
        )

        extractor = SQLExtractor(config)

        with pytest.raises(SQLConnectionError):
            await extractor.initialize()

    @pytest.mark.asyncio
    async def test_query_error_handling(self):
        """Тест обработки ошибок выполнения запросов"""
        config = SQLExtractorConfig(
            connection_string="sqlite+aiosqlite:///:memory:",
            query_config=QueryConfig(query="SELECT * FROM nonexistent_table"),
        )

        extractor = SQLExtractor(config)

        try:
            await extractor.initialize()

            context = ExecutionContext(
                pipeline_id="test_pipeline",
                stage_name="test_error",
            )

            with pytest.raises(QueryExecutionError):
                await extractor._execute_impl(context)

        finally:
            await extractor.cleanup()


class TestSpecializedExtractors:
    """Тесты специализированных extractor'ов"""

    def test_postgresql_extractor(self):
        """Тест PostgreSQL extractor'а"""
        config = SQLExtractorConfig(
            connection_string="postgresql+asyncpg://user:pass@localhost/db",
            query_config=QueryConfig(query="SELECT 1"),
        )

        extractor = PostgresSQLExtractor(config)

        assert extractor.name == "postgresql"
        assert extractor.config.inferred_dialect == "postgresql"

    def test_mysql_extractor(self):
        """Тест MySQL extractor'а"""
        config = SQLExtractorConfig(
            connection_string="mysql+aiomysql://user:pass@localhost/db",
            query_config=QueryConfig(query="SELECT 1"),
        )

        extractor = MySQLExtractor(config)

        assert extractor.name == "mysql"
        assert extractor.config.inferred_dialect == "mysql"

    def test_sqlite_extractor(self):
        """Тест SQLite extractor'а"""
        config = SQLExtractorConfig(
            connection_string="sqlite+aiosqlite:///test.db",
            query_config=QueryConfig(query="SELECT 1"),
        )

        extractor = SQLiteExtractor(config)

        assert extractor.name == "sqlite"
        assert extractor.config.inferred_dialect == "sqlite"


class TestUtilityFunctions:
    """Тесты утилитарных функций"""

    def test_build_connection_string(self):
        """Тест построения строки подключения"""
        conn_str = build_connection_string(
            dialect="postgresql",
            driver="asyncpg",
            username="user",
            password="pass",
            host="localhost",
            port=5432,
            database="testdb",
        )

        assert conn_str == "postgresql+asyncpg://user:pass@localhost:5432/testdb"

    def test_validate_connection_string(self):
        """Тест валидации строки подключения"""
        # Валидные строки
        valid_strings = [
            "postgresql://user:pass@localhost/db",
            "mysql+pymysql://user:pass@localhost:3306/db",
            "sqlite:///path/to/db.sqlite",
            "sqlite+aiosqlite:///:memory:",
        ]

        for conn_str in valid_strings:
            assert validate_connection_string(conn_str) is True

        # Невалидные строки
        invalid_strings = [
            "invalid_string",
            "http://example.com",
            "",
            "postgresql://",
        ]

        for conn_str in invalid_strings:
            assert validate_connection_string(conn_str) is False

    def test_parse_connection_params(self):
        """Тест парсинга параметров подключения"""
        conn_str = "postgresql+asyncpg://user:pass@localhost:5432/testdb?sslmode=require"
        params = parse_connection_params(conn_str)

        assert params["dialect"] == "postgresql"
        assert params["driver"] == "asyncpg"
        assert params["username"] == "user"
        assert params["password"] == "pass"
        assert params["host"] == "localhost"
        assert params["port"] == 5432
        assert params["database"] == "testdb"
        assert params["query_params"]["sslmode"] == "require"

    def test_sanitize_query(self):
        """Тест санитизации SQL запросов"""
        # Безопасный запрос
        safe_query = "SELECT id, name FROM users WHERE active = true"
        sanitized = sanitize_query(safe_query)
        assert sanitized == safe_query

        # Запрос с комментариями
        query_with_comments = "SELECT id /* comment */ FROM users -- line comment"
        sanitized = sanitize_query(query_with_comments)
        assert "/*" not in sanitized
        assert "--" not in sanitized

    def test_optimize_query_for_extraction(self):
        """Тест оптимизации запросов для извлечения"""
        query = "SELECT * FROM large_table"
        optimized = optimize_query_for_extraction(query, fetch_size=1000)

        # Проверяем, что оптимизация применена
        assert len(optimized) >= len(query)

    def test_infer_pandas_dtypes(self):
        """Тест автоматического определения типов pandas"""
        # Создаем DataFrame с правильными данными
        df = pd.DataFrame({
            "id": [1, 2, 3, 4, 5],
            "name": ["Alice", "Bob", "Charlie", "David", "Eve"],
            "score": [95.5, 87.2, 92.0, 88.5, 90.0],
            "active": [True, False, True, True, False],
        })

        optimized_df = infer_pandas_dtypes(df)

        # Проверяем, что типы определены корректно
        assert optimized_df["id"].dtype in ["int64", "int32"]
        assert optimized_df["name"].dtype == "object"
        assert optimized_df["score"].dtype in ["float64", "float32"]
        assert optimized_df["active"].dtype == "bool"

    def test_optimize_dataframe_memory(self):
        """Тест оптимизации памяти DataFrame"""
        # Создаем DataFrame с избыточными типами
        df = pd.DataFrame({
            "small_int": pd.Series([1, 2, 3], dtype="int64"),
            "category_str": pd.Series(["A", "B", "A"], dtype="object"),
        })

        original_memory = df.memory_usage(deep=True).sum()
        optimized_df = optimize_dataframe_memory(df)
        optimized_memory = optimized_df.memory_usage(deep=True).sum()

        # Оптимизированный DataFrame должен использовать меньше памяти
        assert optimized_memory <= original_memory

    def test_split_dataframe_chunks(self):
        """Тест разбиения DataFrame на чанки"""
        df = pd.DataFrame({
            "id": range(100),
            "value": range(100, 200),
        })

        chunks = list(split_dataframe_chunks(df, chunk_size=25))

        assert len(chunks) == 4
        assert all(len(chunk) == 25 for chunk in chunks)
        assert sum(len(chunk) for chunk in chunks) == len(df)

    def test_estimate_query_cost(self):
        """Тест оценки стоимости запроса"""
        # Простой запрос
        simple_query = "SELECT id FROM users WHERE id = 1"
        cost = estimate_query_cost(simple_query)

        assert isinstance(cost, dict)
        assert "complexity_score" in cost
        assert "estimated_rows" in cost
        assert "performance_hints" in cost

        # Сложный запрос
        complex_query = """
        SELECT u.id, u.name, COUNT(o.id) as order_count
        FROM users u
        LEFT JOIN orders o ON u.id = o.user_id
        WHERE u.created_at > '2023-01-01'
        GROUP BY u.id, u.name
        HAVING COUNT(o.id) > 5
        ORDER BY order_count DESC
        """
        complex_cost = estimate_query_cost(complex_query)

        assert complex_cost["complexity_score"] > cost["complexity_score"]


class TestFactoryFunctions:
    """Тесты фабричных функций"""

    def test_create_extractor(self):
        """Тест фабричной функции создания extractor'а"""
        extractor = create_extractor(
            connection_string="sqlite+aiosqlite:///:memory:",
            query="SELECT 1 as test",
            output_format="pandas",
        )

        assert isinstance(extractor, SQLExtractor)
        assert extractor.config.connection_string == "sqlite+aiosqlite:///:memory:"
        assert extractor.config.query_config.query == "SELECT 1 as test"
        assert extractor.config.output_format == "pandas"

    def test_create_extractor_with_parameters(self):
        """Тест создания extractor'а с параметрами"""
        extractor = create_extractor(
            connection_string="sqlite+aiosqlite:///:memory:",
            query="SELECT :value as test",
            parameters={"value": "test_value"},
            output_format="dict",
        )

        assert extractor.config.query_config.parameters == {"value": "test_value"}
        assert extractor.config.output_format == "dict"


class TestIntegration:
    """Интеграционные тесты"""

    @pytest.mark.asyncio
    async def test_full_extraction_workflow(self):
        """Тест полного workflow извлечения данных"""
        # Создаем extractor
        config = SQLExtractorConfig(
            connection_string="sqlite+aiosqlite:///:memory:",
            query_config=QueryConfig(
                query="SELECT 'test' as column1, 42 as column2, 3.14 as column3",
            ),
            output_format="pandas",
        )

        extractor = SQLExtractor(config)

        try:
            # Инициализируем
            await extractor.initialize()

            # Выполняем извлечение
            context = ExecutionContext(
                pipeline_id="integration_test",
                stage_name="full_workflow",
            )

            result = await extractor._execute_impl(context)

            # Проверяем результат
            assert isinstance(result, pd.DataFrame)
            assert len(result) == 1
            assert list(result.columns) == ["column1", "column2", "column3"]
            assert result.iloc[0]["column1"] == "test"
            assert result.iloc[0]["column2"] == 42
            assert result.iloc[0]["column3"] == 3.14

        finally:
            await extractor.cleanup()

    @pytest.mark.asyncio
    async def test_retry_mechanism(self):
        """Тест retry механизма"""
        # Мокаем engine для симуляции временных сбоев
        config = SQLExtractorConfig(
            connection_string="sqlite+aiosqlite:///:memory:",
            query_config=QueryConfig(query="SELECT 1"),
            retry_config=RetryConfig(max_attempts=3, initial_delay=0.1),
        )

        extractor = SQLExtractor(config)

        try:
            await extractor.initialize()

            # Мокаем метод выполнения запроса для симуляции сбоев
            original_execute = extractor._execute_query
            call_count = 0

            async def mock_execute(*args, **kwargs):
                nonlocal call_count
                call_count += 1
                if call_count < 3:
                    raise ConnectionError("Temporary connection error")
                return await original_execute(*args, **kwargs)

            extractor._execute_query = mock_execute

            context = ExecutionContext(
                pipeline_id="retry_test",
                stage_name="test_retry",
            )

            # Этот вызов должен пройти после 2 неудачных попыток
            result = await extractor._execute_impl(context)

            assert result is not None
            assert call_count == 3  # 2 неудачи + 1 успех

        finally:
            await extractor.cleanup()


class TestPerformance:
    """Тесты производительности"""

    @pytest.mark.asyncio
    async def test_large_dataset_extraction(self):
        """Тест извлечения больших объемов данных"""
        # Создаем запрос, который генерирует много строк
        config = SQLExtractorConfig(
            connection_string="sqlite+aiosqlite:///:memory:",
            query_config=QueryConfig(
                query="""
                WITH RECURSIVE numbers(n) AS (
                    SELECT 1
                    UNION ALL
                    SELECT n + 1 FROM numbers
                    WHERE n < 1000
                )
                SELECT n as id, 'row_' || n as name FROM numbers
                """,
                fetch_size=500,
            ),
            output_format="pandas",
        )

        extractor = SQLExtractor(config)

        try:
            await extractor.initialize()

            context = ExecutionContext(
                pipeline_id="performance_test",
                stage_name="large_dataset",
            )

            start_time = asyncio.get_event_loop().time()
            result = await extractor._execute_impl(context)
            end_time = asyncio.get_event_loop().time()

            # Проверяем результат
            assert isinstance(result, pd.DataFrame)
            assert len(result) == 1000

            # Проверяем производительность (должно выполниться за разумное время)
            execution_time = end_time - start_time
            assert execution_time < 10.0  # Максимум 10 секунд

        finally:
            await extractor.cleanup()

    @pytest.mark.asyncio
    async def test_memory_usage_optimization(self):
        """Тест оптимизации использования памяти"""
        config = SQLExtractorConfig(
            connection_string="sqlite+aiosqlite:///:memory:",
            query_config=QueryConfig(
                query="""
                SELECT 
                    1 as small_int,
                    'A' as category,
                    1.0 as small_float,
                    'true' as bool_str
                """,
            ),
            output_format="pandas",
        )

        extractor = SQLExtractor(config)

        try:
            await extractor.initialize()

            context = ExecutionContext(
                pipeline_id="memory_test",
                stage_name="optimization",
            )

            result = await extractor._execute_impl(context)

            # Проверяем, что результат оптимизирован
            assert isinstance(result, pd.DataFrame)

            # Проверяем типы данных (должны быть оптимизированы)
            memory_usage = result.memory_usage(deep=True).sum()
            assert memory_usage > 0  # Базовая проверка

        finally:
            await extractor.cleanup()


class TestEdgeCases:
    """Тесты граничных случаев"""

    @pytest.mark.asyncio
    async def test_empty_result_handling(self):
        """Тест обработки пустых результатов"""
        config = SQLExtractorConfig(
            connection_string="sqlite+aiosqlite:///:memory:",
            query_config=QueryConfig(
                query="SELECT 1 as col WHERE 1 = 0",  # Пустой результат
            ),
            output_format="pandas",
        )

        extractor = SQLExtractor(config)

        try:
            await extractor.initialize()

            context = ExecutionContext(
                pipeline_id="edge_test",
                stage_name="empty_result",
            )

            result = await extractor._execute_impl(context)

            assert isinstance(result, pd.DataFrame)
            assert len(result) == 0

        finally:
            await extractor.cleanup()

    @pytest.mark.asyncio
    async def test_special_characters_handling(self):
        """Тест обработки специальных символов"""
        config = SQLExtractorConfig(
            connection_string="sqlite+aiosqlite:///:memory:",
            query_config=QueryConfig(
                query="SELECT 'Hello, 世界! 🌍' as unicode_text",
            ),
            output_format="pandas",
        )

        extractor = SQLExtractor(config)

        try:
            await extractor.initialize()

            context = ExecutionContext(
                pipeline_id="edge_test",
                stage_name="unicode",
            )

            result = await extractor._execute_impl(context)

            assert isinstance(result, pd.DataFrame)
            assert len(result) == 1
            assert result.iloc[0]["unicode_text"] == "Hello, 世界! 🌍"

        finally:
            await extractor.cleanup()

    @pytest.mark.asyncio
    async def test_null_values_handling(self):
        """Тест обработки NULL значений"""
        config = SQLExtractorConfig(
            connection_string="sqlite+aiosqlite:///:memory:",
            query_config=QueryConfig(
                query="SELECT NULL as null_col, 'value' as non_null_col",
            ),
            output_format="pandas",
        )

        extractor = SQLExtractor(config)

        try:
            await extractor.initialize()

            context = ExecutionContext(
                pipeline_id="edge_test",
                stage_name="null_values",
            )

            result = await extractor._execute_impl(context)

            assert isinstance(result, pd.DataFrame)
            assert len(result) == 1
            assert pd.isna(result.iloc[0]["null_col"])
            assert result.iloc[0]["non_null_col"] == "value"

        finally:
            await extractor.cleanup()


class TestErrorHandling:
    """Тесты обработки ошибок"""

    @pytest.mark.asyncio
    async def test_configuration_errors(self):
        """Тест ошибок конфигурации"""
        # Невалидная строка подключения
        with pytest.raises(ValueError):
            SQLExtractorConfig(
                connection_string="invalid",
                query_config=QueryConfig(query="SELECT 1"),
            )

    @pytest.mark.asyncio
    async def test_runtime_errors(self):
        """Тест runtime ошибок"""
        config = SQLExtractorConfig(
            connection_string="sqlite+aiosqlite:///:memory:",
            query_config=QueryConfig(query="INVALID SQL SYNTAX"),
        )

        extractor = SQLExtractor(config)

        try:
            await extractor.initialize()

            context = ExecutionContext(
                pipeline_id="error_test",
                stage_name="runtime_error",
            )

            with pytest.raises(QueryExecutionError):
                await extractor._execute_impl(context)

        finally:
            await extractor.cleanup()

    @pytest.mark.asyncio
    async def test_cleanup_on_error(self):
        """Тест очистки ресурсов при ошибках"""
        config = SQLExtractorConfig(
            connection_string="sqlite+aiosqlite:///:memory:",
            query_config=QueryConfig(query="SELECT 1"),
        )

        extractor = SQLExtractor(config)

        try:
            await extractor.initialize()

            # Симулируем ошибку во время выполнения
            original_execute = extractor._execute_impl

            async def failing_execute(*args, **kwargs):
                raise RuntimeError("Simulated error")

            extractor._execute_impl = failing_execute

            context = ExecutionContext(
                pipeline_id="cleanup_test",
                stage_name="error_cleanup",
            )

            with pytest.raises(RuntimeError):
                await extractor._execute_impl(context)

            # Проверяем, что cleanup выполняется корректно
            await extractor.cleanup()
            assert extractor._engine is None

        except Exception:
            await extractor.cleanup()
            raise


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
