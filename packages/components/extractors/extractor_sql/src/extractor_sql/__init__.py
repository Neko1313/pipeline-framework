# packages/components/extractors/extractor_sql/src/extractor_sql/__init__.py

"""
SQL Extractor Package for Pipeline Framework

Этот package предоставляет компоненты для извлечения данных из реляционных баз данных
через SQLAlchemy с поддержкой асинхронных операций, connection pooling и retry механизмов.

Основные компоненты:
- SQLExtractor: Базовый SQL extractor
- PostgresSQLExtractor: Специализированный PostgreSQL extractor
- MySQLExtractor: Специализированный MySQL extractor
- SQLiteExtractor: Специализированный SQLite extractor

Пример использования:
    ```python
    from extractor_sql import SQLExtractor, SQLExtractorConfig, QueryConfig

    config = SQLExtractorConfig(
        connection_string="postgresql+asyncpg://user:pass@localhost/db",
        query_config=QueryConfig(
            query="SELECT * FROM users WHERE created_at > :start_date",
            parameters={"start_date": "2024-01-01"}
        )
    )

    extractor = SQLExtractor(config)
    await extractor.initialize()

    result = await extractor.execute(context)
    print(f"Extracted {len(result.data)} rows")
    ```
"""

from extractor_sql.components import (
    ConnectionPoolConfig,
    DatabaseDialect,
    # Type aliases
    DataFormat,
    MySQLExtractor,
    PostgresSQLExtractor,
    QueryConfig,
    RetryConfig,
    # Main extractor classes
    SQLExtractor,
    # Configuration classes
    SQLExtractorConfig,
    SQLiteExtractor,
    # Utility functions
    get_table_schema,
    test_query_syntax,
)
from extractor_sql.exceptions import (
    ConfigurationError,
    ConnectionError,
    DataFormatError,
    QueryExecutionError,
    SQLExtractorError,
)
from extractor_sql.utils import (
    # Connection utilities
    build_connection_string,
    estimate_query_cost,
    # Data utilities
    infer_pandas_dtypes,
    optimize_dataframe_memory,
    optimize_query_for_extraction,
    parse_connection_params,
    # Query utilities
    sanitize_query,
    split_dataframe_chunks,
    validate_connection_string,
)

# Version info
__version__ = "0.1.0"
__author__ = "Ryibalchenko.NV"
__email__ = "Ryibalchenko.NV@dns-shop.ru"

# Package metadata
__title__ = "extractor-sql"
__description__ = "SQL data extractor component for pipeline framework"
__url__ = "https://github.com/company/pipeline-framework"

# Supported database dialects
SUPPORTED_DIALECTS = [
    "postgresql",
    "mysql",
    "sqlite",
    "oracle",
    "mssql",
    "snowflake",
    "bigquery",
]

# Default configurations
DEFAULT_QUERY_CONFIG = QueryConfig(
    query="SELECT 1",
    timeout=300.0,
    fetch_size=10000,
    stream_results=False,
)

DEFAULT_POOL_CONFIG = ConnectionPoolConfig(
    pool_size=5,
    max_overflow=10,
    pool_timeout=30.0,
    pool_recycle=3600,
    pool_pre_ping=True,
)

DEFAULT_RETRY_CONFIG = RetryConfig(
    max_attempts=3,
    initial_wait=1.0,
    max_wait=60.0,
    multiplier=2.0,
    jitter=True,
)

# Registry of extractors by dialect
EXTRACTOR_REGISTRY = {
    "sql": SQLExtractor,
    "postgresql": PostgresSQLExtractor,
    "mysql": MySQLExtractor,
    "sqlite": SQLiteExtractor,
}


def get_extractor_for_dialect(dialect: str) -> type[SQLExtractor]:
    """
    Получение extractor класса для указанного диалекта БД

    Args:
        dialect: Название диалекта БД

    Returns:
        Класс extractor'а для данного диалекта

    Raises:
        ValueError: Если диалект не поддерживается
    """
    if dialect not in EXTRACTOR_REGISTRY:
        raise ValueError(
            f"Unsupported dialect: {dialect}. "
            f"Supported dialects: {list(EXTRACTOR_REGISTRY.keys())}"
        )

    return EXTRACTOR_REGISTRY[dialect]


def create_extractor(
    connection_string: str, query: str, dialect: str | None = None, **kwargs
) -> SQLExtractor:
    """
    Фабричная функция для создания SQL extractor'а

    Args:
        connection_string: Строка подключения к БД
        query: SQL запрос для выполнения
        dialect: Диалект БД (автоопределение если не указан)
        **kwargs: Дополнительные параметры конфигурации

    Returns:
        Сконфигурированный SQL extractor

    Example:
        ```python
        extractor = create_extractor(
            connection_string="postgresql://user:pass@localhost/db",
            query="SELECT * FROM users",
            output_format="polars",
            fetch_size=5000
        )
        ```
    """
    # Создаем конфигурацию запроса
    query_config = QueryConfig(query=query, **kwargs.pop("query_config", {}))

    # Создаем основную конфигурацию
    config = SQLExtractorConfig(
        connection_string=connection_string,
        dialect=dialect,
        query_config=query_config,
        **kwargs,
    )

    # Выбираем подходящий extractor
    if dialect:
        extractor_class = get_extractor_for_dialect(dialect)
    else:
        # Автоопределение на основе connection string
        inferred_dialect = config.inferred_dialect
        extractor_class = get_extractor_for_dialect(inferred_dialect)

    return extractor_class(config)


# Convenience imports для быстрого доступа
__all__ = [
    # Main classes
    "SQLExtractor",
    "PostgresSQLExtractor",
    "MySQLExtractor",
    "SQLiteExtractor",
    # Configuration
    "SQLExtractorConfig",
    "QueryConfig",
    "ConnectionPoolConfig",
    "RetryConfig",
    # Exceptions
    "SQLExtractorError",
    "ConnectionError",
    "QueryExecutionError",
    "ConfigurationError",
    "DataFormatError",
    # Utilities
    "get_table_schema",
    "test_query_syntax",
    "build_connection_string",
    "validate_connection_string",
    "sanitize_query",
    "infer_pandas_dtypes",
    # Factory functions
    "get_extractor_for_dialect",
    "create_extractor",
    # Constants
    "SUPPORTED_DIALECTS",
    "EXTRACTOR_REGISTRY",
    "DEFAULT_QUERY_CONFIG",
    "DEFAULT_POOL_CONFIG",
    "DEFAULT_RETRY_CONFIG",
    # Types
    "DataFormat",
    "DatabaseDialect",
]
