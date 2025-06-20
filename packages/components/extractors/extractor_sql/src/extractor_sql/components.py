# packages/components/extractors/extractor_sql/src/extractor_sql/components.py

from __future__ import annotations

import time
from typing import Any, Literal, TypeAlias, Dict, Union, Optional
from urllib.parse import urlparse

import pandas as pd
import polars as pl
from prometheus_client import Counter, Gauge, Histogram
from pydantic import BaseModel, Field, computed_field, field_validator
from sqlalchemy import MetaData, Table, inspect, text
from sqlalchemy.events import PoolEvents
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
    ExecutionMetadata,
    ExecutionResult,
)
from pipeline_core.config import ComponentSettings

# Типы для работы с данными
DataFormat: TypeAlias = Literal["pandas", "polars", "dict", "raw"]
DatabaseDialect: TypeAlias = Literal[
    "postgresql", "mysql", "sqlite", "oracle", "mssql", "snowflake", "bigquery"
]

# Логгер
logger = structlog.get_logger(__name__)

# Метрики Prometheus
SQL_QUERIES_TOTAL = Counter(
    "sql_extractor_queries_total",
    "Total number of SQL queries executed",
    ["extractor_name", "dialect", "status"],
)

SQL_QUERY_DURATION = Histogram(
    "sql_extractor_query_duration_seconds",
    "Duration of SQL query execution",
    ["extractor_name", "dialect"],
)

SQL_ROWS_EXTRACTED = Counter(
    "sql_extractor_rows_extracted_total",
    "Total number of rows extracted",
    ["extractor_name", "dialect"],
)

SQL_ACTIVE_CONNECTIONS = Gauge(
    "sql_extractor_active_connections",
    "Number of active database connections",
    ["extractor_name", "dialect"],
)


# ================================
# Configuration Models
# ================================


class ConnectionPoolConfig(BaseModel):
    """Конфигурация connection pool"""

    pool_size: int = Field(default=5, ge=1, le=50, description="Размер connection pool")
    max_overflow: int = Field(
        default=10,
        ge=0,
        le=100,
        description="Максимальное количество overflow connections",
    )
    pool_timeout: float = Field(
        default=30.0, ge=1.0, le=300.0, description="Timeout для получения connection"
    )
    pool_recycle: int = Field(
        default=3600,
        ge=300,
        le=86400,
        description="Время переcоздания connection (сек)",
    )
    pool_pre_ping: bool = Field(
        default=True, description="Проверка соединения перед использованием"
    )


class QueryConfig(BaseModel):
    """Конфигурация SQL запроса"""

    query: str = Field(..., min_length=1, description="SQL запрос для выполнения")
    parameters: dict[str, Any] = Field(
        default_factory=dict, description="Параметры для запроса"
    )
    timeout: float = Field(
        default=300.0, ge=1.0, le=3600.0, description="Timeout выполнения запроса"
    )
    fetch_size: int = Field(
        default=10000,
        ge=1,
        le=1000000,
        description="Размер batch для извлечения данных",
    )
    stream_results: bool = Field(
        default=False, description="Использовать streaming для больших результатов"
    )

    @field_validator("query")
    @classmethod
    def validate_query_not_empty(cls, v):
        """Валидация что запрос не пустой"""
        if not v or not v.strip():
            raise ValueError("SQL query cannot be empty")
        return v.strip()


class RetryConfig(BaseModel):
    """Конфигурация retry политики"""

    max_attempts: int = Field(
        default=3, ge=1, le=10, description="Максимальное количество попыток"
    )
    initial_wait: float = Field(
        default=1.0, ge=0.1, le=60.0, description="Начальная задержка"
    )
    max_wait: float = Field(
        default=60.0, ge=1.0, le=300.0, description="Максимальная задержка"
    )
    multiplier: float = Field(
        default=2.0, ge=1.0, le=10.0, description="Множитель для exponential backoff"
    )
    jitter: bool = Field(default=True, description="Добавлять случайную задержку")


class SQLExtractorConfig(ComponentSettings):
    """Конфигурация SQL Extractor"""

    # Основные настройки подключения
    connection_string: str = Field(..., description="Строка подключения к БД")
    dialect: DatabaseDialect | None = Field(
        default=None, description="Диалект БД (автоопределение если не указан)"
    )

    # Конфигурация запроса
    query_config: QueryConfig = Field(..., description="Настройки SQL запроса")

    # Настройки обработки данных
    output_format: DataFormat = Field(
        default="pandas", description="Формат выходных данных"
    )
    chunk_size: int | None = Field(
        default=None, ge=1, le=1000000, description="Размер чанка для streaming"
    )

    # Настройки подключения
    pool_config: ConnectionPoolConfig = Field(
        default_factory=ConnectionPoolConfig, description="Настройки connection pool"
    )

    # Retry настройки
    retry_config: RetryConfig = Field(
        default_factory=RetryConfig, description="Настройки retry политики"
    )

    # Дополнительные настройки SQLAlchemy
    engine_options: dict[str, Any] = Field(
        default_factory=dict, description="Дополнительные опции для SQLAlchemy engine"
    )

    # Настройки безопасности
    ssl_config: dict[str, Any] | None = Field(
        default=None, description="SSL конфигурация для подключения"
    )

    @computed_field
    @property
    def inferred_dialect(self) -> DatabaseDialect:
        """Автоматическое определение диалекта из connection string"""
        if self.dialect:
            return self.dialect

        parsed = urlparse(self.connection_string)
        scheme = parsed.scheme.lower()

        # Маппинг схем на диалекты
        dialect_mapping = {
            "postgresql": "postgresql",
            "postgresql+asyncpg": "postgresql",
            "mysql": "mysql",
            "mysql+aiomysql": "mysql",
            "sqlite": "sqlite",
            "sqlite+aiosqlite": "sqlite",
            "oracle": "oracle",
            "mssql": "mssql",
            "snowflake": "snowflake",
            "bigquery": "bigquery",
        }

        return dialect_mapping.get(scheme, "postgresql")  # Default fallback

    @field_validator("connection_string")
    @classmethod
    def validate_connection_string(cls, v: str) -> str:
        """Валидация строки подключения"""
        if not v or not v.strip():
            raise ValueError("Connection string cannot be empty")

        try:
            parsed = urlparse(v)
            if not parsed.scheme:
                raise ValueError("Invalid connection string: missing scheme")
            return v
        except Exception as e:
            raise ValueError(f"Invalid connection string: {e}")


# ================================
# Base SQL Extractor
# ================================


class SQLExtractor(BaseExtractor[pd.DataFrame, SQLExtractorConfig]):
    """
    Базовый SQL Extractor для извлечения данных из реляционных БД

    Поддерживает:
    - Различные диалекты SQL (PostgreSQL, MySQL, SQLite и др.)
    - Асинхронное выполнение запросов
    - Connection pooling
    - Retry механизмы
    - Streaming больших результатов
    - Мониторинг и метрики
    """

    name = "base_sql"

    def __init__(self, config: Union[Dict[str, Any], SQLExtractorConfig]):
        # Создаем конфигурацию если передан словарь
        if isinstance(config, dict):
            self.config = SQLExtractorConfig(**config)
        else:
            self.config = config

        # Инициализируем базовый компонент
        super().__init__(self.config)

        # Инициализируем SQLAlchemy engine (будет создан при первом использовании)
        self._engine: Optional[AsyncEngine] = None
        self._session_factory: Optional[async_sessionmaker] = None

        # Настройка метрик
        self._setup_sql_metrics()

        # Валидация конфигурации
        self._validate_config()

        self.logger.info(
            "SQLExtractor initialized",
            dialect=self.config.inferred_dialect,
            output_format=self.config.output_format,
        )

    def _setup_sql_metrics(self):
        """Настройка SQL-специфичных метрик"""
        try:
            # Настраиваем метрики если prometheus доступен
            self.sql_queries_counter = SQL_QUERIES_TOTAL.labels(
                extractor_name=self.name,
                dialect=self.config.inferred_dialect,
                status="success",
            )
            self.sql_duration_histogram = SQL_QUERY_DURATION.labels(
                extractor_name=self.name, dialect=self.config.inferred_dialect
            )
        except Exception as e:
            self.logger.warning("Failed to setup SQL metrics", error=str(e))

    def _validate_config(self):
        """Валидация конфигурации экстрактора"""
        try:
            # Проверяем строку подключения
            if not self.config.connection_string:
                raise ValueError("Connection string is required")

            # Проверяем что запрос не пустой
            if not self.config.query_config.query:
                raise ValueError("SQL query is required")

            # Проверяем формат вывода
            valid_formats = ["pandas", "polars", "dict", "raw"]
            if self.config.output_format not in valid_formats:
                raise ValueError(
                    f"Invalid output format. Must be one of: {valid_formats}"
                )

            self.logger.debug("Configuration validation passed")

        except Exception as e:
            self.logger.error("Configuration validation failed", error=str(e))
            raise

    async def initialize(self) -> None:
        """Инициализация экстрактора"""
        await super().initialize()

        # Создаем engine при инициализации
        await self._ensure_engine()

        self.logger.info("SQLExtractor initialized successfully")

    async def cleanup(self) -> None:
        """Очистка ресурсов"""
        if self._engine:
            await self._engine.dispose()
            self._engine = None
            self._session_factory = None

        await super().cleanup()
        self.logger.info("SQLExtractor cleanup completed")

    @property
    def component_type(self) -> ComponentType:
        return ComponentType.EXTRACTOR

    async def initialize(self) -> None:
        """Инициализация SQL Extractor"""
        logger.info(
            "Initializing SQL Extractor",
            name=self.name,
            dialect=self.config.inferred_dialect,
        )

        # Создаем engine
        await self._create_engine()

        # Создаем session factory
        self._session_factory = async_sessionmaker(
            self._engine,
            class_=AsyncSession,
            expire_on_commit=False,
        )

        # Тестируем подключение
        await self._test_connection()

        logger.info("SQL Extractor initialized successfully", name=self.name)

    async def cleanup(self) -> None:
        """Очистка ресурсов"""
        if self._engine:
            await self._engine.dispose()
            logger.info("SQL Engine disposed", name=self.name)

    async def execute(self, context: ExecutionContext) -> ExecutionResult[pd.DataFrame]:
        """Основной метод извлечения данных"""
        start_time = time.time()
        metadata = ExecutionMetadata()

        try:
            logger.info(
                "Starting SQL data extraction",
                name=self.name,
                query_preview=self.config.query_config.query[:100] + "...",
            )

            # Выполняем запрос с retry механизмом
            data = await self._execute_query_with_retry(context)

            # Преобразуем в нужный формат
            formatted_data = await self._format_output(data)

            # Обновляем метаданные
            metadata.rows_processed = (
                len(formatted_data) if formatted_data is not None else 0
            )
            metadata.duration_seconds = time.time() - start_time
            metadata.custom_metrics.update(
                {
                    "query_hash": hash(self.config.query_config.query),
                    "dialect": self.config.inferred_dialect,
                    "output_format": self.config.output_format,
                }
            )

            # Обновляем метрики Prometheus
            SQL_QUERIES_TOTAL.labels(
                extractor_name=self.name,
                dialect=self.config.inferred_dialect,
                status="success",
            ).inc()

            SQL_QUERY_DURATION.labels(
                extractor_name=self.name,
                dialect=self.config.inferred_dialect,
            ).observe(metadata.duration_seconds)

            if metadata.rows_processed:
                SQL_ROWS_EXTRACTED.labels(
                    extractor_name=self.name,
                    dialect=self.config.inferred_dialect,
                ).inc(metadata.rows_processed)

            logger.info(
                "SQL data extraction completed",
                name=self.name,
                rows=metadata.rows_processed,
                duration=metadata.duration_seconds,
            )

            return ExecutionResult(
                data=formatted_data,
                metadata=metadata,
                success=True,
            )

        except Exception as e:
            metadata.add_error(e, "SQL extraction failed")
            metadata.duration_seconds = time.time() - start_time

            SQL_QUERIES_TOTAL.labels(
                extractor_name=self.name,
                dialect=self.config.inferred_dialect,
                status="error",
            ).inc()

            logger.error(
                "SQL data extraction failed",
                name=self.name,
                error=str(e),
                duration=metadata.duration_seconds,
                exc_info=True,
            )

            return ExecutionResult(
                data=None,
                metadata=metadata,
                success=False,
                error=str(e),
            )

    # ================================
    # Private Methods
    # ================================

    async def _create_engine(self) -> None:
        """Создание SQLAlchemy engine с настройками"""
        pool_config = self.config.pool_config

        # Базовые настройки engine
        engine_kwargs = {
            "echo": False,  # Включить для debug SQL запросов
            "pool_size": pool_config.pool_size,
            "max_overflow": pool_config.max_overflow,
            "pool_timeout": pool_config.pool_timeout,
            "pool_recycle": pool_config.pool_recycle,
            "pool_pre_ping": pool_config.pool_pre_ping,
            **self.config.engine_options,
        }

        # Специфичные настройки для разных диалектов
        dialect = self.config.inferred_dialect

        if dialect == "sqlite":
            # SQLite не поддерживает pooling
            engine_kwargs.update(
                {
                    "poolclass": NullPool,
                    "pool_size": 0,
                    "max_overflow": 0,
                }
            )
        elif dialect in ["postgresql", "mysql"]:
            engine_kwargs["poolclass"] = QueuePool

        self._engine = create_async_engine(
            self.config.connection_string,
            **engine_kwargs,
        )

        # Регистрируем обработчики событий pool
        self._setup_pool_events()

    def _setup_pool_events(self) -> None:
        """Настройка обработчиков событий connection pool"""
        if not self._engine:
            return

        @PoolEvents.connect
        def on_connect(dbapi_connection, connection_record):
            SQL_ACTIVE_CONNECTIONS.labels(
                extractor_name=self.name,
                dialect=self.config.inferred_dialect,
            ).inc()

        @PoolEvents.checkout
        def on_checkout(dbapi_connection, connection_record, connection_proxy):
            pass

        @PoolEvents.checkin
        def on_checkin(dbapi_connection, connection_record):
            pass

        @PoolEvents.close
        def on_close(dbapi_connection, connection_record):
            SQL_ACTIVE_CONNECTIONS.labels(
                extractor_name=self.name,
                dialect=self.config.inferred_dialect,
            ).dec()

    def _setup_retry_decorator(self) -> None:
        """Настройка retry декоратора"""
        retry_config = self.config.retry_config

        self._retry_decorator = retry(
            stop=stop_after_attempt(retry_config.max_attempts),
            wait=wait_exponential(
                multiplier=retry_config.multiplier,
                min=retry_config.initial_wait,
                max=retry_config.max_wait,
            ),
            retry=retry_if_exception_type(
                (
                    DisconnectionError,
                    ConnectionError,
                    TimeoutError,
                )
            ),
            before_sleep=before_sleep_log(logger, "warning"),
            reraise=True,
        )

    async def _test_connection(self) -> None:
        """Тестирование подключения к БД"""
        if not self._engine:
            raise RuntimeError("Engine not initialized")

        try:
            async with self._engine.begin() as conn:
                # Простой тестовый запрос
                result = await conn.execute(text("SELECT 1"))
                await result.fetchone()

            logger.info("Database connection test successful", name=self.name)

        except Exception as e:
            logger.error(
                "Database connection test failed", name=self.name, error=str(e)
            )
            raise

    async def _execute_query_with_retry(
        self, context: ExecutionContext
    ) -> pd.DataFrame:
        """Выполнение SQL запроса с retry механизмом"""

        @self._retry_decorator
        async def _execute():
            return await self._execute_query(context)

        return await _execute()

    async def _execute_query(self, context: ExecutionContext) -> pd.DataFrame:
        """Выполнение SQL запроса"""
        if not self._engine:
            raise RuntimeError("Engine not initialized")

        query_config = self.config.query_config

        try:
            async with self._engine.begin() as conn:
                # Выполняем запрос
                if query_config.stream_results and self.config.chunk_size:
                    # Streaming execution для больших результатов
                    return await self._execute_streaming_query(conn, query_config)
                else:
                    # Обычное выполнение
                    return await self._execute_regular_query(conn, query_config)

        except SQLAlchemyError as e:
            logger.error(
                "SQLAlchemy error during query execution",
                name=self.name,
                error=str(e),
                query=query_config.query[:200],
            )
            raise
        except Exception as e:
            logger.error(
                "Unexpected error during query execution",
                name=self.name,
                error=str(e),
                query=query_config.query[:200],
            )
            raise

    async def _execute_regular_query(
        self, conn: AsyncConnection, query_config: QueryConfig
    ) -> pd.DataFrame:
        """Обычное выполнение запроса"""
        result = await conn.execute(
            text(query_config.query),
            query_config.parameters,
        )

        # Извлекаем все данные
        rows = await result.fetchall()
        columns = list(result.keys())

        # Создаем DataFrame
        return pd.DataFrame(rows, columns=columns)

    async def _execute_streaming_query(
        self, conn: AsyncConnection, query_config: QueryConfig
    ) -> pd.DataFrame:
        """Streaming выполнение запроса для больших результатов"""
        result = await conn.execute(
            text(query_config.query),
            query_config.parameters,
        )

        # Получаем колонки
        columns = list(result.keys())
        all_data = []

        # Читаем данные чанками
        while True:
            chunk = await result.fetchmany(query_config.fetch_size)
            if not chunk:
                break
            all_data.extend(chunk)

        return pd.DataFrame(all_data, columns=columns)

    async def _format_output(
        self, data: pd.DataFrame
    ) -> pd.DataFrame | pl.DataFrame | dict | list:
        """Преобразование данных в нужный формат"""
        if data is None or data.empty:
            return data

        output_format = self.config.output_format

        if output_format == "pandas":
            return data
        elif output_format == "polars":
            return pl.from_pandas(data)
        elif output_format == "dict":
            return data.to_dict(orient="records")
        elif output_format == "raw":
            return data.values.tolist()
        else:
            raise ValueError(f"Unsupported output format: {output_format}")

    async def _execute_impl(self, context: ExecutionContext) -> pd.DataFrame:
        """
        Базовая реализация извлечения данных через SQL

        Args:
            context: Контекст выполнения

        Returns:
            DataFrame с данными
        """
        try:
            # Инициализация если нужно
            if not self._engine:
                await self._ensure_engine()

            # Выполнение запроса
            async with self._engine.begin() as conn:
                # Подготавливаем параметры
                params = self.config.query_config.parameters or {}

                # Выполняем запрос
                result = await conn.execute(
                    text(self.config.query_config.query), params
                )

                # Получаем данные
                rows = result.fetchall()
                columns = result.keys()

                # Создаем DataFrame
                if self.config.output_format == "pandas":
                    df = pd.DataFrame(rows, columns=columns)
                    return df
                elif self.config.output_format == "polars":
                    df = pd.DataFrame(rows, columns=columns)
                    return pl.from_pandas(df)
                elif self.config.output_format == "dict":
                    return [dict(zip(columns, row, strict=False)) for row in rows]
                else:  # raw
                    return {"columns": list(columns), "rows": rows}

        except Exception as e:
            self.logger.error("SQL execution failed", error=str(e))
            raise

    async def _ensure_engine(self):
        """Обеспечивает создание engine если его нет"""
        if not self._engine:
            self._engine = create_async_engine(
                self.config.connection_string, **self.config.engine_options
            )


# ================================
# Specialized SQL Extractors
# ================================


class PostgresSQLExtractor(SQLExtractor):
    """Специализированный экстрактор для PostgresSQL"""

    name = "postgresql"
    description = "PostgresSQL data extractor with advanced features"

    def __init__(self, config: Union[Dict[str, Any], SQLExtractorConfig]):
        # Настройки специфичные для PostgresSQL
        if isinstance(config, dict):
            # Устанавливаем драйвер по умолчанию для PostgresSQL
            if "connection_string" in config and not any(
                driver in config["connection_string"]
                for driver in ["asyncpg", "psycopg2"]
            ):
                # Добавляем asyncpg если драйвер не указан
                conn_str = config["connection_string"]
                if conn_str.startswith("postgresql://"):
                    config["connection_string"] = conn_str.replace(
                        "postgresql://", "postgresql+asyncpg://"
                    )

        super().__init__(config)

    async def _execute_impl(self, context: ExecutionContext) -> pd.DataFrame:
        """Реализация для PostgresSQL с дополнительными возможностями"""
        return await super()._execute_impl(context)


class MySQLExtractor(SQLExtractor):
    """Специализированный экстрактор для MySQL"""

    name = "mysql"
    description = "MySQL data extractor with performance optimizations"

    def __init__(self, config: Union[Dict[str, Any], SQLExtractorConfig]):
        # Настройки специфичные для MySQL
        if isinstance(config, dict):
            # Устанавливаем драйвер по умолчанию для MySQL
            if "connection_string" in config and not any(
                driver in config["connection_string"]
                for driver in ["aiomysql", "pymysql"]
            ):
                # Добавляем aiomysql если драйвер не указан
                conn_str = config["connection_string"]
                if conn_str.startswith("mysql://"):
                    config["connection_string"] = conn_str.replace(
                        "mysql://", "mysql+aiomysql://"
                    )

        super().__init__(config)

    async def _execute_impl(self, context: ExecutionContext) -> pd.DataFrame:
        """Реализация для MySQL с дополнительными возможностями"""
        # Используем базовую реализацию, но можем добавить MySQL-специфичную логику
        return await super()._execute_impl(context)


class SQLiteExtractor(SQLExtractor):
    """Специализированный экстрактор для SQLite"""

    name = "sqlite"
    description = "SQLite data extractor for local databases"

    def __init__(self, config: Union[Dict[str, Any], SQLExtractorConfig]):
        # Настройки специфичные для SQLite
        if isinstance(config, dict):
            # Устанавливаем драйвер по умолчанию для SQLite
            if "connection_string" in config and not any(
                driver in config["connection_string"]
                for driver in ["aiosqlite", "pysqlite"]
            ):
                # Добавляем aiosqlite если драйвер не указан
                conn_str = config["connection_string"]
                if conn_str.startswith("sqlite://"):
                    config["connection_string"] = conn_str.replace(
                        "sqlite://", "sqlite+aiosqlite://"
                    )

        super().__init__(config)

    async def _execute_impl(self, context: ExecutionContext) -> pd.DataFrame:
        """Реализация для SQLite с дополнительными возможностями"""
        # Используем базовую реализацию, но можем добавить SQLite-специфичную логику
        return await super()._execute_impl(context)


# ================================
# Utility Functions
# ================================


async def get_table_schema(
    engine: AsyncEngine, table_name: str, schema: str | None = None
) -> dict[str, Any]:
    """Получение схемы таблицы"""
    async with engine.begin() as conn:
        inspector = inspect(conn.sync_connection)
        columns = inspector.get_columns(table_name, schema=schema)
        return {
            "table_name": table_name,
            "schema": schema,
            "columns": columns,
        }


async def test_query_syntax(engine: AsyncEngine, query: str) -> bool:
    """Тестирование синтаксиса SQL запроса"""
    try:
        async with engine.begin() as conn:
            # Используем LIMIT 0 для проверки синтаксиса без извлечения данных
            test_query = f"SELECT * FROM ({query}) AS test_query LIMIT 0"
            await conn.execute(text(test_query))
        return True
    except Exception:
        return False


def create_extractor(
    connection_string: str, query: str, output_format: DataFormat = "pandas", **kwargs
) -> SQLExtractor:
    """
    Фабричная функция для создания подходящего экстрактора

    Args:
        connection_string: Строка подключения к БД
        query: SQL запрос
        output_format: Формат выходных данных
        **kwargs: Дополнительные параметры конфигурации

    Returns:
        SQLExtractor: Подходящий экстрактор для типа БД
    """
    # Определяем тип БД по connection string
    if "postgresql" in connection_string:
        extractor_class = PostgresSQLExtractor
    elif "mysql" in connection_string:
        extractor_class = MySQLExtractor
    elif "sqlite" in connection_string:
        extractor_class = SQLiteExtractor
    else:
        # По умолчанию используем базовый SQLExtractor
        extractor_class = SQLExtractor

    # Создаем конфигурацию
    config = SQLExtractorConfig(
        connection_string=connection_string,
        query_config=QueryConfig(query=query),
        output_format=output_format,
        **kwargs,
    )

    return extractor_class(config)
