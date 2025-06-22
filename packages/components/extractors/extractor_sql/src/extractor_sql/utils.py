# packages/components/extractors/extractor_sql/src/extractor_sql/utils.py

"""
Утилитарные функции для SQL Extractor

Включает:
- Построение и валидация connection strings
- Парсинг параметров подключения
- Санитизация SQL запросов
- Оптимизация запросов для извлечения данных
- Работа с pandas DataFrame
- Оценка стоимости запросов
"""

from collections.abc import Iterator
import math
import re
from typing import Any
from urllib.parse import parse_qs, urlparse

import pandas as pd
import structlog

logger = structlog.get_logger(__name__)


def build_connection_string(
    dialect: str,
    driver: str | None = None,
    username: str | None = None,
    password: str | None = None,
    host: str | None = None,
    port: int | None = None,
    database: str | None = None,
    **query_params: Any,
) -> str:
    """
    Построение строки подключения к БД

    Args:
        dialect: Диалект БД (postgresql, mysql, sqlite, etc.)
        driver: Драйвер (asyncpg, aiomysql, aiosqlite, etc.)
        username: Имя пользователя
        password: Пароль
        host: Хост
        port: Порт
        database: Имя базы данных
        **query_params: Дополнительные параметры

    Returns:
        Строка подключения в формате SQLAlchemy URL

    Example:
        >>> build_connection_string("postgresql", "asyncpg", "user", "pass", "localhost", 5432, "mydb")
        'postgresql+asyncpg://user:pass@localhost:5432/mydb'
    """

    # Схема с драйвером
    scheme = dialect
    if driver:
        scheme = f"{dialect}+{driver}"

    # Для SQLite особый формат
    if dialect.lower() == "sqlite":
        if database == ":memory:":
            return f"{scheme}:///:memory:"
        else:
            return f"{scheme}:///{database or ''}"

    # Для остальных БД стандартный формат
    connection_parts = [f"{scheme}://"]

    # Credentials
    if username:
        auth = username
        if password:
            auth = f"{username}:{password}"
        connection_parts.append(f"{auth}@")

    # Host:port
    if host:
        host_part = host
        if port:
            host_part = f"{host}:{port}"
        connection_parts.append(host_part)

    # Database
    if database:
        connection_parts.append(f"/{database}")

    # Query parameters
    if query_params:
        query_string = "&".join(f"{k}={v}" for k, v in query_params.items())
        connection_parts.append(f"?{query_string}")

    return "".join(connection_parts)


def validate_connection_string(connection_string: str) -> bool:
    """
    Валидация строки подключения

    Args:
        connection_string: Строка подключения

    Returns:
        True если строка валидна, False иначе

    Example:
        >>> validate_connection_string("postgresql://user:pass@localhost/db")
        True
        >>> validate_connection_string("invalid_string")
        False
    """
    try:
        parsed = urlparse(connection_string)

        # Проверяем наличие схемы
        if not parsed.scheme:
            return False

        # Поддерживаемые схемы
        supported_schemes = {
            "postgresql",
            "postgres",
            "mysql",
            "sqlite",
            "oracle",
            "mssql",
            "sqlserver",
            "snowflake",
            "bigquery",
        }

        # Извлекаем базовый диалект (до знака +)
        base_scheme = parsed.scheme.split("+")[0].lower()

        if base_scheme not in supported_schemes:
            return False

        # Для SQLite дополнительные проверки
        if base_scheme == "sqlite":
            return True  # SQLite имеет особый формат

        # Для остальных БД проверяем наличие хоста (кроме некоторых случаев)
        if base_scheme not in ["bigquery"] and not parsed.hostname:
            return False

        return True

    except Exception:
        return False


def parse_connection_params(connection_string: str) -> dict[str, Any]:
    """
    Парсинг параметров из строки подключения

    Args:
        connection_string: Строка подключения

    Returns:
        Словарь с параметрами подключения

    Example:
        >>> params = parse_connection_params("postgresql+asyncpg://user:pass@localhost:5432/db?sslmode=require")
        >>> params["dialect"]
        'postgresql'
        >>> params["driver"]
        'asyncpg'
    """
    parsed = urlparse(connection_string)

    # Парсим схему и драйвер
    scheme_parts = parsed.scheme.split("+")
    dialect = scheme_parts[0]
    driver = scheme_parts[1] if len(scheme_parts) > 1 else None

    # Парсим query параметры
    query_params = {}
    if parsed.query:
        query_params = {
            k: v[0] if isinstance(v, list) else v
            for k, v in parse_qs(parsed.query).items()
        }

    return {
        "dialect": dialect,
        "driver": driver,
        "username": parsed.username,
        "password": parsed.password,
        "host": parsed.hostname,
        "port": parsed.port,
        "database": parsed.path.lstrip("/") if parsed.path else None,
        "query_params": query_params,
    }


def sanitize_query(query: str) -> str:
    """
    Санитизация SQL запроса

    Args:
        query: SQL запрос

    Returns:
        Санитизированный запрос

    Example:
        >>> sanitize_query("SELECT * FROM users /* comment */ -- line comment")
        'SELECT * FROM users   '
    """
    if not query:
        return ""

    sanitized = query.strip()

    # Удаляем SQL комментарии
    # Удаляем многострочные комментарии /* ... */
    sanitized = re.sub(r"/\*.*?\*/", "", sanitized, flags=re.DOTALL)

    # Удаляем однострочные комментарии --
    sanitized = re.sub(r"--.*?$", "", sanitized, flags=re.MULTILINE)

    # Удаляем лишние пробелы
    sanitized = re.sub(r"\s+", " ", sanitized).strip()

    return sanitized


def optimize_query_for_extraction(query: str, fetch_size: int | None = None) -> str:
    """
    Оптимизация SQL запроса для извлечения данных

    Args:
        query: Исходный SQL запрос
        fetch_size: Размер выборки (для добавления LIMIT если нужно)

    Returns:
        Оптимизированный запрос

    Example:
        >>> optimize_query_for_extraction("SELECT * FROM users", 1000)
        'SELECT * FROM users'
    """
    if not query:
        return ""

    optimized = query.strip()

    # Для простоты пока возвращаем исходный запрос
    # В будущем здесь можно добавить:
    # - Анализ плана выполнения
    # - Добавление подсказок для оптимизатора
    # - Автоматическое добавление индексов
    # - Партиционирование больших запросов

    logger.debug(
        "Query optimization completed",
        original_length=len(query),
        optimized_length=len(optimized),
    )

    return optimized


def infer_pandas_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    """
    Автоматическое определение и оптимизация типов данных pandas DataFrame

    Args:
        df: Исходный DataFrame

    Returns:
        DataFrame с оптимизированными типами данных

    Example:
        >>> df = pd.DataFrame({"id": ["1", "2", "3"], "active": ["true", "false", "true"]})
        >>> optimized_df = infer_pandas_dtypes(df)
        >>> optimized_df["id"].dtype
        dtype('int64')
    """
    if df.empty:
        return df

    optimized_df = df.copy()

    for column in optimized_df.columns:
        col_data = optimized_df[column]

        # Пропускаем колонки с только NaN значениями
        if col_data.isna().all():
            continue

        # Пытаемся конвертировать в числовые типы
        try:
            # Проверяем на целые числа
            if col_data.dtype == "object":
                # Убираем NaN для проверки
                non_null_data = col_data.dropna()

                # Пытаемся конвертировать в int
                try:
                    converted_int = pd.to_numeric(non_null_data, errors="raise")
                    if (converted_int == converted_int.astype(int)).all():
                        optimized_df[column] = pd.to_numeric(
                            col_data, errors="coerce"
                        ).astype("Int64")
                        continue
                except (ValueError, TypeError):
                    pass

                # Пытаемся конвертировать в float
                try:
                    optimized_df[column] = pd.to_numeric(col_data, errors="coerce")
                    continue
                except (ValueError, TypeError):
                    pass

                # Пытаемся конвертировать в bool
                if set(non_null_data.str.lower().unique()).issubset(
                    {"true", "false", "1", "0", "yes", "no"}
                ):
                    bool_map = {
                        "true": True,
                        "false": False,
                        "1": True,
                        "0": False,
                        "yes": True,
                        "no": False,
                    }
                    optimized_df[column] = col_data.str.lower().map(bool_map)
                    continue

                # Пытаемся конвертировать в datetime
                try:
                    optimized_df[column] = pd.to_datetime(col_data, errors="raise")
                    continue
                except (ValueError, TypeError):
                    pass

        except Exception as e:
            logger.debug(f"Failed to infer dtype for column {column}: {e}")
            continue

    return optimized_df


def optimize_dataframe_memory(df: pd.DataFrame) -> pd.DataFrame:
    """
    Оптимизация использования памяти DataFrame

    Args:
        df: Исходный DataFrame

    Returns:
        DataFrame с оптимизированным использованием памяти

    Example:
        >>> df = pd.DataFrame({"small_int": [1, 2, 3], "category": ["A", "B", "A"]})
        >>> optimized_df = optimize_dataframe_memory(df)
        >>> optimized_df.memory_usage(deep=True).sum() <= df.memory_usage(deep=True).sum()
        True
    """
    if df.empty:
        return df

    optimized_df = df.copy()

    for column in optimized_df.columns:
        col_data = optimized_df[column]
        col_type = col_data.dtype

        try:
            # Оптимизация целых чисел
            if pd.api.types.is_integer_dtype(col_type):
                col_min = col_data.min()
                col_max = col_data.max()

                if col_min >= 0:  # Беззнаковые типы
                    if col_max < 256:
                        optimized_df[column] = col_data.astype("uint8")
                    elif col_max < 65536:
                        optimized_df[column] = col_data.astype("uint16")
                    elif col_max < 4294967296:
                        optimized_df[column] = col_data.astype("uint32")
                else:  # Знаковые типы
                    if col_min > -128 and col_max < 128:
                        optimized_df[column] = col_data.astype("int8")
                    elif col_min > -32768 and col_max < 32768:
                        optimized_df[column] = col_data.astype("int16")
                    elif col_min > -2147483648 and col_max < 2147483648:
                        optimized_df[column] = col_data.astype("int32")

            # Оптимизация float
            elif pd.api.types.is_float_dtype(col_type):
                if col_type == "float64":
                    # Проверяем, можно ли безопасно конвертировать в float32
                    if (
                        col_data.isna() | (col_data == col_data.astype("float32"))
                    ).all():
                        optimized_df[column] = col_data.astype("float32")

            # Оптимизация строк
            elif pd.api.types.is_object_dtype(col_type):
                # Проверяем на категориальные данные
                unique_count = col_data.nunique()
                total_count = len(col_data)

                # Если уникальных значений меньше 50% от общего количества, конвертируем в category
                if unique_count / total_count < 0.5 and unique_count > 1:
                    optimized_df[column] = col_data.astype("category")

        except Exception as e:
            logger.debug(f"Failed to optimize memory for column {column}: {e}")
            continue

    return optimized_df


def split_dataframe_chunks(df: pd.DataFrame, chunk_size: int) -> Iterator[pd.DataFrame]:
    """
    Разбиение DataFrame на чанки

    Args:
        df: Исходный DataFrame
        chunk_size: Размер чанка

    Yields:
        Чанки DataFrame

    Example:
        >>> df = pd.DataFrame({"id": range(100), "value": range(100)})
        >>> chunks = list(split_dataframe_chunks(df, 25))
        >>> len(chunks)
        4
        >>> len(chunks[0])
        25
    """
    if df.empty or chunk_size <= 0:
        return

    num_chunks = math.ceil(len(df) / chunk_size)

    for i in range(num_chunks):
        start_idx = i * chunk_size
        end_idx = min((i + 1) * chunk_size, len(df))

        chunk = df.iloc[start_idx:end_idx].copy()
        yield chunk


def estimate_query_cost(query: str) -> dict[str, Any]:
    """
    Оценка стоимости выполнения SQL запроса

    Args:
        query: SQL запрос

    Returns:
        Словарь с оценкой стоимости

    Example:
        >>> cost = estimate_query_cost("SELECT * FROM users WHERE id = 1")
        >>> "complexity_score" in cost
        True
    """
    if not query:
        return {
            "complexity_score": 0,
            "estimated_rows": 0,
            "performance_hints": [],
            "risk_factors": [],
        }

    query_upper = query.upper().strip()

    # Базовый анализ сложности
    complexity_score = 0
    performance_hints = []
    risk_factors = []
    estimated_rows = 1

    # Ключевые слова, увеличивающие сложность
    complexity_keywords = {
        "JOIN": 2,
        "LEFT JOIN": 2,
        "RIGHT JOIN": 2,
        "INNER JOIN": 2,
        "OUTER JOIN": 3,
        "UNION": 3,
        "UNION ALL": 2,
        "SUBQUERY": 3,
        "GROUP BY": 2,
        "ORDER BY": 2,
        "HAVING": 2,
        "DISTINCT": 2,
        "WINDOW": 4,
        "RECURSIVE": 5,
    }

    # Подсчитываем сложность
    for keyword, weight in complexity_keywords.items():
        count = query_upper.count(keyword)
        complexity_score += count * weight

    # Анализируем SELECT *
    if "SELECT *" in query_upper:
        complexity_score += 1
        performance_hints.append(
            "Consider selecting specific columns instead of SELECT *"
        )

    # Анализируем отсутствие WHERE
    if "WHERE" not in query_upper and "SELECT" in query_upper:
        risk_factors.append("Query without WHERE clause may return all rows")
        estimated_rows = 10000  # Предполагаем много строк

    # Анализируем функции агрегации
    aggregate_functions = ["COUNT", "SUM", "AVG", "MAX", "MIN"]
    for func in aggregate_functions:
        if f"{func}(" in query_upper:
            complexity_score += 1

    # Анализируем вложенные запросы
    subquery_count = query_upper.count("(SELECT")
    if subquery_count > 0:
        complexity_score += subquery_count * 2
        if subquery_count > 2:
            performance_hints.append("Consider optimizing nested subqueries")

    # Анализируем ORDER BY без LIMIT
    if "ORDER BY" in query_upper and "LIMIT" not in query_upper:
        performance_hints.append("Consider adding LIMIT to ORDER BY queries")

    # Категоризация сложности
    if complexity_score <= 2:
        complexity_category = "simple"
    elif complexity_score <= 8:
        complexity_category = "moderate"
    elif complexity_score <= 15:
        complexity_category = "complex"
    else:
        complexity_category = "very_complex"
        risk_factors.append("Very complex query may have performance issues")

    return {
        "complexity_score": complexity_score,
        "complexity_category": complexity_category,
        "estimated_rows": estimated_rows,
        "performance_hints": performance_hints,
        "risk_factors": risk_factors,
        "subquery_count": subquery_count,
        "join_count": query_upper.count("JOIN"),
        "analysis_metadata": {
            "query_length": len(query),
            "word_count": len(query.split()),
            "has_aggregation": any(func in query_upper for func in aggregate_functions),
            "has_grouping": "GROUP BY" in query_upper,
            "has_ordering": "ORDER BY" in query_upper,
            "has_limit": "LIMIT" in query_upper,
        },
    }


# Дополнительные утилитарные функции


def mask_connection_string(connection_string: str) -> str:
    """
    Маскирование паролей в строке подключения для логирования

    Args:
        connection_string: Строка подключения

    Returns:
        Строка подключения с замаскированным паролем
    """
    try:
        parsed = urlparse(connection_string)
        if parsed.password:
            masked_password = "*" * len(parsed.password)
            netloc = parsed.netloc.replace(parsed.password, masked_password)
            masked_url = parsed._replace(netloc=netloc).geturl()
            return masked_url
        return connection_string
    except Exception:
        return connection_string


def generate_test_data(
    rows: int = 100, columns: list[str] | None = None, seed: int | None = None
) -> pd.DataFrame:
    """
    Генерация тестовых данных для отладки и тестирования

    Args:
        rows: Количество строк
        columns: Список имен колонок
        seed: Seed для воспроизводимости

    Returns:
        DataFrame с тестовыми данными
    """
    from datetime import datetime, timedelta
    import random
    import string

    if seed:
        random.seed(seed)

    if columns is None:
        columns = ["id", "name", "email", "age", "score", "active", "created_at"]

    data = {}

    for col in columns:
        if col == "id":
            data[col] = list(range(1, rows + 1))
        elif col == "name":
            data[col] = [f"User_{i}" for i in range(1, rows + 1)]
        elif col == "email":
            data[col] = [f"user{i}@example.com" for i in range(1, rows + 1)]
        elif col == "age":
            data[col] = [random.randint(18, 80) for _ in range(rows)]
        elif col == "score":
            data[col] = [round(random.uniform(0, 100), 2) for _ in range(rows)]
        elif col == "active":
            data[col] = [random.choice([True, False]) for _ in range(rows)]
        elif col == "created_at":
            base_date = datetime.now()
            data[col] = [
                base_date - timedelta(days=random.randint(0, 365)) for _ in range(rows)
            ]
        else:
            # Генерируем случайные строки для неизвестных колонок
            data[col] = [
                "".join(random.choices(string.ascii_letters, k=10)) for _ in range(rows)
            ]

    return pd.DataFrame(data)


def validate_sql_syntax(query: str, dialect: str = "generic") -> dict[str, Any]:
    """
    Базовая валидация синтаксиса SQL запроса

    Args:
        query: SQL запрос
        dialect: Диалект SQL

    Returns:
        Результат валидации
    """
    if not query or not query.strip():
        return {
            "valid": False,
            "errors": ["Query is empty"],
            "warnings": [],
        }

    errors = []
    warnings = []

    query_upper = query.upper().strip()

    # Базовые проверки
    if not any(
        keyword in query_upper
        for keyword in ["SELECT", "INSERT", "UPDATE", "DELETE", "WITH"]
    ):
        errors.append("Query must contain at least one SQL command")

    # Проверка парных скобок
    if query.count("(") != query.count(")"):
        errors.append("Unmatched parentheses in query")

    # Проверка парных кавычек
    single_quote_count = query.count("'")
    if single_quote_count % 2 != 0:
        errors.append("Unmatched single quotes in query")

    double_quote_count = query.count('"')
    if double_quote_count % 2 != 0:
        errors.append("Unmatched double quotes in query")

    # Предупреждения
    if "SELECT *" in query_upper:
        warnings.append("Using SELECT * may impact performance")

    if query_upper.count("JOIN") > 5:
        warnings.append("Query has many JOINs, consider optimization")

    return {
        "valid": len(errors) == 0,
        "errors": errors,
        "warnings": warnings,
        "dialect": dialect,
        "query_type": _detect_query_type(query_upper),
    }


def _detect_query_type(query_upper: str) -> str:
    """Определение типа SQL запроса"""
    if query_upper.strip().startswith("SELECT"):
        return "SELECT"
    elif query_upper.strip().startswith("INSERT"):
        return "INSERT"
    elif query_upper.strip().startswith("UPDATE"):
        return "UPDATE"
    elif query_upper.strip().startswith("DELETE"):
        return "DELETE"
    elif query_upper.strip().startswith("WITH"):
        return "CTE"
    else:
        return "UNKNOWN"
