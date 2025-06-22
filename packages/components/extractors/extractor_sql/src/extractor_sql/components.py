# packages/components/extractors/extractor_sql/src/extractor_sql/components.py

from __future__ import annotations

import asyncio
import time
from typing import Any, Literal, TypeAlias, Union
from urllib.parse import urlparse
import warnings

import pandas as pd
import polars as pl
from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram
from pydantic import BaseModel, Field, computed_field, field_validator
from sqlalchemy import MetaData, Table, text
from sqlalchemy.exc import DisconnectionError, SQLAlchemyError
from sqlalchemy.ext.asyncio import (
    AsyncConnection,
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import NullPool, QueuePool
import structlog
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

# Импорты из core framework
from pipeline_core.components.base import (
    BaseExtractor,
    ComponentType,
    ExecutionContext,
)
from pipeline_core.config import ComponentSettings

from .exceptions import (
    ConnectionError as SQLConnectionError,
)

# Локальные импорты
from .exceptions import (
    DataFormatError,
    QueryExecutionError,
)
from .utils import (
    infer_pandas_dtypes,
    optimize_dataframe_memory,
    optimize_query_for_extraction,
    parse_connection_params,
    sanitize_query,
    validate_connection_string,
)

# Типы для работы с данными
DataFormat: TypeAlias = Literal["pandas", "polars", "dict", "raw"]
DatabaseDialect: TypeAlias = Literal[
    "postgresql", "mysql", "sqlite", "oracle", "mssql", "snowflake", "bigquery"
]

# Логгер
logger = structlog.get_logger(__name__)

# Создаем отдельный registry для метрик
_sql_extractor_metrics_registry = CollectorRegistry()

# Метрики Prometheus - инициализируем только один раз
try:
    SQL_QUERIES_TOTAL = Counter(
        "sql_extractor_queries_total",
        "Total number of SQL queries executed",
        ["extractor_name", "dialect", "status"],
        registry=_sql_extractor_metrics_registry
    )

    SQL_QUERY_DURATION = Histogram(
        "sql_extractor_query_duration_seconds",
        "Duration of SQL query execution",
        ["extractor_name", "dialect"],
        registry=_sql_extractor_metrics_registry
    )

    SQL_ROWS_EXTRACTED = Counter(
        "sql_extractor_rows_extracted_total",
        "Total number of rows extracted",
        ["extractor_name", "dialect"],
        registry=_sql_extractor_metrics_registry
    )

    SQL_CONNECTION_POOL_SIZE = Gauge(
        "sql_extractor_connection_pool_size",
        "Current connection pool size",
        ["extractor_name", "dialect"],
        registry=_sql_extractor_metrics_registry
    )

    SQL_ACTIVE_CONNECTIONS = Gauge(
        "sql_extractor_active_connections",
        "Number of active database connections",
        ["extractor_name", "dialect"],
        registry=_sql_extractor_metrics_registry
    )
except Exception as e:
    logger.warning("Failed to initialize Prometheus metrics", error=str(e))
    # Создаем заглушки
    class _DummyMetric:
        def labels(self, **kwargs): return self
        def inc(self, *args): pass
        def observe(self, *args): pass
        def set(self, *args): pass
        def time(self): return lambda: None

    SQL_QUERIES_TOTAL = _DummyMetric()
    SQL_QUERY_DURATION = _DummyMetric()
    SQL_ROWS_EXTRACTED = _DummyMetric()
    SQL_CONNECTION_POOL_SIZE = _DummyMetric()
    SQL_ACTIVE_CONNECTIONS = _DummyMetric()


class RetryConfig(BaseModel):
    """Конфигурация retry механизма"""

    max_attempts: int = Field(default=3, ge=1, le=10)
    initial_delay: float = Field(default=1.0, ge=0.1, le=60.0)
    max_delay: float = Field(default=60.0, ge=1.0, le=300.0)
    exponential_base: float = Field(default=2.0, ge=1.1, le=10.0)
    jitter: bool = Field(default=True)

    @field_validator("max_delay")
    @classmethod
    def validate_max_delay(cls, v, info):
        if "initial_delay" in info.data and v < info.data["initial_delay"]:
            raise ValueError("max_delay must be >= initial_delay")
        return v


class ConnectionPoolConfig(BaseModel):
    """Конфигурация connection pool"""

    pool_size: int = Field(default=5, ge=1, le=100)
    max_overflow: int = Field(default=10, ge=0, le=100)
    pool_timeout: float = Field(default=30.0, ge=1.0, le=300.0)
    pool_recycle: int = Field(default=3600, ge=300, le=86400)
    pool_pre_ping: bool = Field(default=True)

    @computed_field
    @property
    def total_pool_size(self) -> int:
        """Общий размер пула включая overflow"""
        return self.pool_size + self.max_overflow


class QueryConfig(BaseModel):
    """Конфигурация SQL запроса"""

    query: str = Field(..., min_length=1, description="SQL запрос")
    parameters: dict[str, Any] = Field(default_factory=dict)
    timeout: float = Field(default=300.0, ge=1.0, le=3600.0)
    fetch_size: int = Field(default=10000, ge=100, le=1000000)
    stream_results: bool = Field(default=False)

    # Дополнительные параметры
    memory_limit_mb: int | None = Field(default=None, ge=100, le=32768)
    enable_query_cache: bool = Field(default=False)
    query_hints: dict[str, str] = Field(default_factory=dict)

    @field_validator("query")
    @classmethod
    def validate_query(cls, v):
        """Валидация SQL запроса"""
        if not v or not v.strip():
            raise ValueError("Query cannot be empty")

        # Проверка на потенциально опасные операции
        dangerous_keywords = [
            "DELETE", "DROP", "TRUNCATE", "ALTER", "CREATE", "GRANT", "REVOKE",
            "INSERT", "UPDATE", "EXEC", "EXECUTE", "CALL"
        ]

        query_upper = v.upper().strip()
        for keyword in dangerous_keywords:
            if query_upper.startswith(keyword):
                warnings.warn(
                    f"Potentially dangerous SQL keyword detected: {keyword}. "
                    f"SQL extractors should typically use SELECT statements only.",
                    UserWarning,
                    stacklevel=2
                )
                break

        # Базовая валидация синтаксиса
        if not query_upper.startswith(("SELECT", "WITH", "(")):
            warnings.warn(
                "Query should typically start with SELECT, WITH, or parenthesis for subquery",
                UserWarning,
                stacklevel=2
            )

        return v.strip()

    @field_validator("parameters")
    @classmethod
    def validate_parameters(cls, v):
        """Валидация параметров запроса"""
        if not isinstance(v, dict):
            raise ValueError("Parameters must be a dictionary")

        # Проверяем типы значений параметров
        for key, value in v.items():
            if not isinstance(key, str):
                raise ValueError(f"Parameter key must be string, got {type(key)}")

            # Разрешенные типы для параметров
            allowed_types = (str, int, float, bool, type(None))
            if not isinstance(value, allowed_types):
                raise ValueError(
                    f"Parameter '{key}' has unsupported type {type(value)}. "
                    f"Allowed types: {allowed_types}"
                )

        return v


class SQLExtractorConfig(ComponentSettings):
    """Конфигурация SQL Extractor"""

    # Подключение к базе данных
    connection_string: str = Field(..., min_length=10)
    dialect: DatabaseDialect | None = Field(default=None)

    # Конфигурация запроса
    query_config: QueryConfig

    # Формат выходных данных
    output_format: DataFormat = Field(default="pandas")
    chunk_size: int | None = Field(default=None, ge=1000, le=1000000)

    # Connection pooling
    pool_config: ConnectionPoolConfig = Field(default_factory=ConnectionPoolConfig)

    # Retry механизм
    retry_config: RetryConfig = Field(default_factory=RetryConfig)

    # SSL и безопасность
    ssl_config: dict[str, Any] = Field(default_factory=dict)
    mask_credentials: bool = Field(default=True)

    # Производительность
    enable_streaming: bool = Field(default=False)
    enable_compression: bool = Field(default=False)
    parallel_connections: int = Field(default=1, ge=1, le=10)

    @field_validator("connection_string")
    @classmethod
    def validate_connection_string(cls, v):
        """Валидация строки подключения"""
        if not validate_connection_string(v):
            raise ValueError(f"Invalid connection string format: {v}")
        return v

    @computed_field
    @property
    def inferred_dialect(self) -> str:
        """Автоматическое определение диалекта из connection string"""
        if self.dialect:
            return self.dialect

        try:
            parsed = urlparse(self.connection_string)
            scheme = parsed.scheme.split("+")[0].lower()

            dialect_mapping = {
                "postgresql": "postgresql",
                "postgres": "postgresql",
                "mysql": "mysql",
                "sqlite": "sqlite",
                "oracle": "oracle",
                "mssql": "mssql",
                "sqlserver": "mssql",
                "snowflake": "snowflake",
                "bigquery": "bigquery",
            }

            return dialect_mapping.get(scheme, "unknown")
        except Exception:
            return "unknown"

    @field_validator("output_format")
    @classmethod
    def validate_output_format(cls, v):
        """Валидация формата вывода"""
        if v == "polars":
            try:
                import polars
            except ImportError:
                raise ValueError("polars is required for 'polars' output format")
        elif v == "pandas":
            try:
                import pandas
            except ImportError:
                raise ValueError("pandas is required for 'pandas' output format")
        return v


class SQLExtractor(BaseExtractor[Union[pd.DataFrame, pl.DataFrame, dict, Any], SQLExtractorConfig]):
    """
    Универсальный SQL Extractor для извлечения данных из реляционных БД

    Поддерживает:
    - Асинхронные операции с SQLAlchemy
    - Connection pooling
    - Retry механизмы  
    - Множественные форматы данных
    - Streaming для больших dataset'ов
    - Мониторинг и метрики
    """

    def __init__(
            self,
            config: SQLExtractorConfig,
            name: str | None = None,
            **kwargs
    ):
        self.config = config
        self._name = name or "sql-extractor"
        self._engine: AsyncEngine | None = None
        self._session_factory: async_sessionmaker | None = None
        self._connection_params = {}

        # Кэш для метаданных
        self._metadata_cache = {}
        self._schema_cache = {}

        # Инициализируем базовый класс
        super().__init__(self.config)

        self.logger = structlog.get_logger(
            component=self.__class__.__name__,
            dialect=self.config.inferred_dialect,
            output_format=self.config.output_format,
        )

        self.logger.info(
            "SQLExtractor initialized",
            dialect=self.config.inferred_dialect,
            output_format=self.config.output_format,
        )

    @property
    def name(self) -> str:
        """Имя компонента"""
        return self._name

    @property
    def component_type(self) -> ComponentType:
        """Тип компонента"""
        return ComponentType.EXTRACTOR

    def _setup_metrics(self):
        """Переопределяем настройку метрик для избежания дублирования"""
        # Используем глобальные метрики вместо создания новых
        pass

    async def initialize(self) -> None:
        """Инициализация extractor'а"""
        if self._engine is not None:
            return

        try:
            # Парсим параметры подключения
            self._connection_params = parse_connection_params(
                self.config.connection_string
            )

            # Создаем engine с connection pooling
            pool_config = self.config.pool_config

            # Выбираем тип пула
            if self.config.inferred_dialect == "sqlite":
                poolclass = NullPool
            else:
                poolclass = QueuePool

            self._engine = create_async_engine(
                self.config.connection_string,
                poolclass=poolclass,
                pool_size=pool_config.pool_size,
                max_overflow=pool_config.max_overflow,
                pool_timeout=pool_config.pool_timeout,
                pool_recycle=pool_config.pool_recycle,
                pool_pre_ping=pool_config.pool_pre_ping,
                echo=False,  # Устанавливаем в True для отладки SQL
                **self.config.ssl_config,
            )

            # Создаем session factory
            self._session_factory = async_sessionmaker(
                self._engine,
                class_=AsyncSession,
                expire_on_commit=False,
            )

            # Тестируем подключение
            await self._test_connection()

            # Обновляем метрики
            SQL_CONNECTION_POOL_SIZE.labels(
                extractor_name=self.name,
                dialect=self.config.inferred_dialect
            ).set(pool_config.pool_size)

            self.logger.info(
                "Database engine initialized",
                pool_size=pool_config.pool_size,
                max_overflow=pool_config.max_overflow,
            )

        except Exception as e:
            raise SQLConnectionError(f"Failed to initialize SQL extractor: {e}") from e

    async def _test_connection(self) -> None:
        """Тестирование подключения к БД"""
        try:
            async with self._engine.begin() as conn:
                # Простой тестовый запрос для каждой БД
                if self.config.inferred_dialect == "postgresql" or self.config.inferred_dialect == "mysql":
                    result = await conn.execute(text("SELECT version()"))
                elif self.config.inferred_dialect == "sqlite":
                    result = await conn.execute(text("SELECT sqlite_version()"))
                else:
                    result = await conn.execute(text("SELECT 1"))

                await result.fetchone()

            self.logger.info("Database connection test successful")

        except Exception as e:
            raise SQLConnectionError(f"Database connection test failed: {e}") from e

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type((DisconnectionError, SQLAlchemyError)),
        before_sleep=before_sleep_log(logger, "warning"),
    )
    async def _execute_query(
            self,
            connection: AsyncConnection,
            query: str,
            parameters: dict[str, Any]
    ) -> Any:
        """Выполнение SQL запроса с retry логикой"""
        start_time = time.time()

        try:
            # Обновляем метрики активных соединений
            SQL_ACTIVE_CONNECTIONS.labels(
                extractor_name=self.name,
                dialect=self.config.inferred_dialect
            ).inc()

            # Выполняем запрос
            if self.config.query_config.stream_results:
                result = await connection.stream(text(query), parameters)
            else:
                result = await connection.execute(text(query), parameters)

            # Записываем метрики успеха
            execution_time = time.time() - start_time
            SQL_QUERY_DURATION.labels(
                extractor_name=self.name,
                dialect=self.config.inferred_dialect
            ).observe(execution_time)

            SQL_QUERIES_TOTAL.labels(
                extractor_name=self.name,
                dialect=self.config.inferred_dialect,
                status="success"
            ).inc()

            self.logger.info(
                "SQL query executed successfully",
                execution_time=execution_time,
                query_length=len(query),
            )

            return result

        except Exception as e:
            # Записываем метрики ошибок
            SQL_QUERIES_TOTAL.labels(
                extractor_name=self.name,
                dialect=self.config.inferred_dialect,
                status="error"
            ).inc()

            self.logger.error(
                "SQL query execution failed",
                error=str(e),
                query_length=len(query),
                execution_time=time.time() - start_time,
            )
            raise QueryExecutionError(f"Query execution failed: {e}") from e

        finally:
            # Уменьшаем счетчик активных соединений
            SQL_ACTIVE_CONNECTIONS.labels(
                extractor_name=self.name,
                dialect=self.config.inferred_dialect
            ).dec()

    async def _format_output(self, result_data: Any) -> pd.DataFrame | pl.DataFrame | dict | Any:
        """Форматирование результата в требуемый формат"""
        try:
            if self.config.output_format == "pandas":
                if hasattr(result_data, "fetchall"):
                    rows = await result_data.fetchall()
                    columns = list(result_data.keys()) if hasattr(result_data, "keys") else None
                else:
                    rows = result_data
                    columns = None

                if not rows:
                    return pd.DataFrame()

                df = pd.DataFrame(rows, columns=columns)

                # Оптимизация типов данных
                df = infer_pandas_dtypes(df)
                df = optimize_dataframe_memory(df)

                return df

            elif self.config.output_format == "polars":
                if hasattr(result_data, "fetchall"):
                    rows = await result_data.fetchall()
                    columns = list(result_data.keys()) if hasattr(result_data, "keys") else None
                else:
                    rows = result_data
                    columns = None

                if not rows:
                    return pl.DataFrame()

                # Создаем DataFrame через pandas и конвертируем в polars
                df_pandas = pd.DataFrame(rows, columns=columns)
                return pl.from_pandas(df_pandas)

            elif self.config.output_format == "dict":
                if hasattr(result_data, "fetchall"):
                    rows = await result_data.fetchall()
                    columns = list(result_data.keys()) if hasattr(result_data, "keys") else None

                    if columns:
                        return [dict(zip(columns, row, strict=False)) for row in rows]
                    else:
                        return [{"data": row} for row in rows]
                else:
                    return result_data

            elif self.config.output_format == "raw":
                return result_data

            else:
                raise DataFormatError(f"Unsupported output format: {self.config.output_format}")

        except Exception as e:
            raise DataFormatError(f"Failed to format output: {e}") from e

    async def _execute_impl(self, context: ExecutionContext) -> pd.DataFrame | pl.DataFrame | dict | Any:
        """Основная логика извлечения данных"""
        if not self._engine:
            await self.initialize()

        query = self.config.query_config.query
        parameters = self.config.query_config.parameters.copy()

        # Добавляем контекстные параметры
        if context:
            parameters.update({
                "pipeline_id": context.pipeline_id,
                "stage_name": context.stage_name,
                "execution_time": context.execution_time,
            })

        try:
            # Оптимизируем запрос
            optimized_query = optimize_query_for_extraction(
                query,
                self.config.query_config.fetch_size
            )

            # Санитизируем запрос
            safe_query = sanitize_query(optimized_query)

            async with self._engine.begin() as connection:
                result = await self._execute_query(connection, safe_query, parameters)
                formatted_data = await self._format_output(result)

                # Обновляем метрики количества строк
                if hasattr(formatted_data, "__len__"):
                    row_count = len(formatted_data)
                elif hasattr(formatted_data, "shape"):
                    row_count = formatted_data.shape[0]
                else:
                    row_count = 1

                SQL_ROWS_EXTRACTED.labels(
                    extractor_name=self.name,
                    dialect=self.config.inferred_dialect
                ).inc(row_count)

                self.logger.info(
                    "Data extraction completed",
                    rows_extracted=row_count,
                    output_format=self.config.output_format,
                )

                return formatted_data

        except Exception as e:
            self.logger.error(
                "Data extraction failed",
                error=str(e),
                query_length=len(query),
            )
            raise

    async def cleanup(self) -> None:
        """Очистка ресурсов"""
        if self._engine:
            await self._engine.dispose()
            self._engine = None
            self._session_factory = None

        self.logger.info("SQL extractor cleaned up")

    async def get_table_schema(self, table_name: str, schema_name: str | None = None) -> dict[str, Any]:
        """Получение схемы таблицы"""
        if not self._engine:
            await self.initialize()

        cache_key = f"{schema_name}.{table_name}" if schema_name else table_name
        if cache_key in self._schema_cache:
            return self._schema_cache[cache_key]

        try:
            async with self._engine.begin() as connection:
                metadata = MetaData()
                table = Table(
                    table_name,
                    metadata,
                    autoload_with=connection.sync_connection,
                    schema=schema_name,
                )

                schema_info = {
                    "table_name": table_name,
                    "schema_name": schema_name,
                    "columns": [
                        {
                            "name": col.name,
                            "type": str(col.type),
                            "nullable": col.nullable,
                            "primary_key": col.primary_key,
                            "foreign_keys": [str(fk) for fk in col.foreign_keys],
                        }
                        for col in table.columns
                    ],
                }

                self._schema_cache[cache_key] = schema_info
                return schema_info

        except Exception as e:
            raise QueryExecutionError(f"Failed to get table schema: {e}") from e

    async def test_query(self, query: str, parameters: dict[str, Any] | None = None) -> bool:
        """Тестирование SQL запроса без выполнения"""
        if not self._engine:
            await self.initialize()

        try:
            # Создаем EXPLAIN запрос для проверки синтаксиса
            if self.config.inferred_dialect in ["postgresql", "mysql"]:
                test_query = f"EXPLAIN {query}"
            elif self.config.inferred_dialect == "sqlite":
                test_query = f"EXPLAIN QUERY PLAN {query}"
            else:
                # Для других БД просто пытаемся подготовить запрос
                test_query = query

            async with self._engine.begin() as connection:
                result = await connection.execute(
                    text(test_query),
                    parameters or {}
                )
                await result.fetchall()  # Потребляем результат

            return True

        except Exception as e:
            self.logger.warning(
                "Query test failed",
                error=str(e),
                query_length=len(query),
            )
            return False


# Специализированные экстракторы для разных БД
class PostgresSQLExtractor(SQLExtractor):
    """Специализированный PostgreSQL extractor"""

    def __init__(self, config: SQLExtractorConfig, **kwargs):
        super().__init__(config, name="postgresql-extractor", **kwargs)

    async def get_database_version(self) -> str:
        """Получение версии PostgreSQL"""
        if not self._engine:
            await self.initialize()

        async with self._engine.begin() as connection:
            result = await connection.execute(text("SELECT version()"))
            row = await result.fetchone()
            return row[0] if row else "Unknown"


class MySQLExtractor(SQLExtractor):
    """Специализированный MySQL extractor"""

    def __init__(self, config: SQLExtractorConfig, **kwargs):
        super().__init__(config, name="mysql-extractor", **kwargs)

    async def get_database_version(self) -> str:
        """Получение версии MySQL"""
        if not self._engine:
            await self.initialize()

        async with self._engine.begin() as connection:
            result = await connection.execute(text("SELECT VERSION()"))
            row = await result.fetchone()
            return row[0] if row else "Unknown"


class SQLiteExtractor(SQLExtractor):
    """Специализированный SQLite extractor"""

    def __init__(self, config: SQLExtractorConfig, **kwargs):
        super().__init__(config, name="sqlite-extractor", **kwargs)

    async def get_database_version(self) -> str:
        """Получение версии SQLite"""
        if not self._engine:
            await self.initialize()

        async with self._engine.begin() as connection:
            result = await connection.execute(text("SELECT sqlite_version()"))
            row = await result.fetchone()
            return row[0] if row else "Unknown"


# Утилитарные функции для создания экстракторов
def create_extractor(
        connection_string: str,
        query: str,
        parameters: dict[str, Any] | None = None,
        output_format: DataFormat = "pandas",
        **kwargs
) -> SQLExtractor:
    """Фабричная функция для быстрого создания SQL extractor'а"""

    config = SQLExtractorConfig(
        connection_string=connection_string,
        query_config=QueryConfig(
            query=query,
            parameters=parameters or {},
        ),
        output_format=output_format,
        **kwargs
    )

    return SQLExtractor(config)


def get_table_schema(connection_string: str, table_name: str, schema_name: str | None = None) -> dict[str, Any]:
    """Утилитарная функция для получения схемы таблицы"""

    async def _get_schema():
        config = SQLExtractorConfig(
            connection_string=connection_string,
            query_config=QueryConfig(query="SELECT 1"),  # Dummy query
        )

        extractor = SQLExtractor(config)
        try:
            schema = await extractor.get_table_schema(table_name, schema_name)
            return schema
        finally:
            await extractor.cleanup()

    return asyncio.run(_get_schema())


def test_query_syntax(connection_string: str, query: str, parameters: dict[str, Any] | None = None) -> bool:
    """Утилитарная функция для тестирования синтаксиса SQL запроса"""

    async def _test_query():
        config = SQLExtractorConfig(
            connection_string=connection_string,
            query_config=QueryConfig(query=query, parameters=parameters or {}),
        )

        extractor = SQLExtractor(config)
        try:
            return await extractor.test_query(query, parameters)
        finally:
            await extractor.cleanup()

    return asyncio.run(_test_query())
