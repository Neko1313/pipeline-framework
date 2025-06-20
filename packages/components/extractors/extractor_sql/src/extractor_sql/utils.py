# packages/components/extractors/extractor_sql/src/extractor_sql/utils.py

"""
Утилиты для SQL Extractor

Этот модуль содержит вспомогательные функции для работы с SQL extraction,
включая работу с connection strings, оптимизацию запросов, обработку данных и т.д.
"""

import hashlib
from pathlib import Path
import re
from typing import Any
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse
import warnings

import numpy as np
import pandas as pd
import structlog

logger = structlog.get_logger(__name__)


# ================================
# Connection String Utilities
# ================================


def build_connection_string(
    dialect: str,
    username: str,
    password: str,
    host: str,
    port: int | None = None,
    database: str | None = None,
    driver: str | None = None,
    **kwargs,
) -> str:
    """
    Построение connection string для базы данных

    Args:
        dialect: Диалект БД (postgresql, mysql, sqlite и т.д.)
        username: Имя пользователя
        password: Пароль
        host: Хост базы данных
        port: Порт (опционально)
        database: Название базы данных
        driver: Драйвер (опционально)
        **kwargs: Дополнительные параметры соединения

    Returns:
        Сформированный connection string

    Example:
        >>> build_connection_string(
        ...     dialect="postgresql",
        ...     username="user",
        ...     password="pass",
        ...     host="localhost",
        ...     port=5432,
        ...     database="mydb",
        ...     driver="asyncpg"
        ... )
        'postgresql+asyncpg://user:pass@localhost:5432/mydb'
    """
    # Обработка специальных случаев
    if dialect == "sqlite":
        if database:
            return f"sqlite+aiosqlite:///{database}"
        else:
            return "sqlite+aiosqlite:///:memory:"

    # Схема с драйвером
    scheme = f"{dialect}+{driver}" if driver else dialect

    # Базовая часть URL
    netloc = f"{username}:{password}@{host}"
    if port:
        netloc += f":{port}"

    # Путь (база данных)
    path = f"/{database}" if database else ""

    # Параметры запроса
    query_params = {}
    for key, value in kwargs.items():
        if value is not None:
            query_params[key] = str(value)

    query = urlencode(query_params) if query_params else ""

    return urlunparse((scheme, netloc, path, "", query, ""))


def parse_connection_params(connection_string: str) -> dict[str, Any]:
    """
    Разбор connection string на составные части

    Args:
        connection_string: Строка подключения

    Returns:
        Словарь с параметрами подключения

    Example:
        >>> parse_connection_params("postgresql+asyncpg://user:pass@localhost:5432/mydb?sslmode=require")
        {
            'dialect': 'postgresql',
            'driver': 'asyncpg',
            'username': 'user',
            'password': 'pass',
            'host': 'localhost',
            'port': 5432,
            'database': 'mydb',
            'params': {'sslmode': 'require'}
        }
    """
    parsed = urlparse(connection_string)

    # Разбираем схему на диалект и драйвер
    scheme_parts = parsed.scheme.split("+", 1)
    dialect = scheme_parts[0]
    driver = scheme_parts[1] if len(scheme_parts) > 1 else None

    # Разбираем параметры запроса
    params = {}
    if parsed.query:
        for key, values in parse_qs(parsed.query).items():
            params[key] = values[0] if len(values) == 1 else values

    result = {
        "dialect": dialect,
        "driver": driver,
        "username": parsed.username,
        "password": parsed.password,
        "host": parsed.hostname,
        "port": parsed.port,
        "database": parsed.path.lstrip("/") if parsed.path else None,
        "params": params,
    }

    return result


def validate_connection_string(connection_string: str) -> tuple[bool, str | None]:
    """
    Валидация connection string

    Args:
        connection_string: Строка подключения для проверки

    Returns:
        Кортеж (is_valid, error_message)

    Example:
        >>> validate_connection_string("postgresql://user:pass@localhost/db")
        (True, None)
        >>> validate_connection_string("invalid://")
        (False, "Invalid connection string format")
    """
    try:
        parsed = urlparse(connection_string)

        # Проверяем наличие схемы
        if not parsed.scheme:
            return False, "Missing database scheme"

        # Проверяем поддерживаемые диалекты
        supported_dialects = [
            "postgresql",
            "mysql",
            "sqlite",
            "oracle",
            "mssql",
            "snowflake",
            "bigquery",
        ]

        dialect = parsed.scheme.split("+")[0]
        if dialect not in supported_dialects:
            return False, f"Unsupported database dialect: {dialect}"

        # Специальная проверка для SQLite
        if dialect == "sqlite":
            return True, None

        # Для остальных БД проверяем наличие хоста
        if not parsed.hostname:
            return False, "Missing database host"

        return True, None

    except Exception as e:
        return False, f"Invalid connection string format: {e}"


def mask_connection_string(connection_string: str) -> str:
    """
    Маскирование чувствительной информации в connection string

    Args:
        connection_string: Исходная строка подключения

    Returns:
        Строка подключения с замаскированным паролем

    Example:
        >>> mask_connection_string("postgresql://user:secret@localhost/db")
        'postgresql://user:***@localhost/db'
    """
    return re.sub(r"://([^:]+):([^@]+)@", r"://\1:***@", connection_string)


# ================================
# Query Utilities
# ================================


def sanitize_query(query: str) -> str:
    """
    Базовая санитизация SQL запроса

    Args:
        query: SQL запрос для санитизации

    Returns:
        Санитизированный запрос

    Note:
        Это базовая проверка. Для production использования рекомендуется
        более продвинутые инструменты SQL санитизации.
    """
    # Удаляем лишние пробелы
    query = re.sub(r"\s+", " ", query.strip())

    # Проверяем на подозрительные паттерны
    dangerous_patterns = [
        r";\s*(DROP|DELETE|TRUNCATE|UPDATE|INSERT|ALTER)",
        r"UNION\s+SELECT",
        r"--",  # SQL комментарии
        r"/\*.*?\*/",  # Блочные комментарии
    ]

    for pattern in dangerous_patterns:
        if re.search(pattern, query, re.IGNORECASE):
            warnings.warn(
                f"Potentially dangerous SQL pattern detected: {pattern}",
                UserWarning,
                stacklevel=2,
            )

    return query


def estimate_query_cost(query: str) -> dict[str, Any]:
    """
    Примерная оценка стоимости выполнения запроса

    Args:
        query: SQL запрос для анализа

    Returns:
        Словарь с оценками сложности запроса

    Note:
        Это упрощенная эвристическая оценка.
        Для точной оценки используйте EXPLAIN ANALYZE в конкретной БД.
    """
    query_upper = query.upper()

    # Подсчет различных элементов запроса
    join_count = len(re.findall(r"\bJOIN\b", query_upper))
    subquery_count = len(re.findall(r"\(\s*SELECT", query_upper))
    aggregate_count = len(
        re.findall(r"\b(COUNT|SUM|AVG|MIN|MAX|GROUP BY)\b", query_upper)
    )

    # Оценка сложности
    complexity_score = 0
    complexity_score += join_count * 2  # JOINы увеличивают сложность
    complexity_score += subquery_count * 3  # Подзапросы еще больше
    complexity_score += aggregate_count * 1  # Агрегации умеренно сложные

    # Определение категории сложности
    if complexity_score == 0:
        complexity = "simple"
    elif complexity_score <= 5:
        complexity = "moderate"
    elif complexity_score <= 10:
        complexity = "complex"
    else:
        complexity = "very_complex"

    return {
        "complexity": complexity,
        "complexity_score": complexity_score,
        "join_count": join_count,
        "subquery_count": subquery_count,
        "aggregate_count": aggregate_count,
        "estimated_time_category": "fast" if complexity_score <= 3 else "slow",
    }


def optimize_query_for_extraction(
    query: str,
    limit: int | None = None,
    offset: int | None = None,
    add_order_by: bool = True,
) -> str:
    """
    Оптимизация запроса для извлечения данных

    Args:
        query: Исходный SQL запрос
        limit: Лимит записей (если нужен)
        offset: Смещение записей (если нужен)
        add_order_by: Добавлять ли ORDER BY для детерминированного порядка

    Returns:
        Оптимизированный запрос
    """
    query = query.strip()

    # Удаляем существующий LIMIT/OFFSET если есть
    query = re.sub(r"\bLIMIT\s+\d+", "", query, flags=re.IGNORECASE)
    query = re.sub(r"\bOFFSET\s+\d+", "", query, flags=re.IGNORECASE)

    # Добавляем ORDER BY если нет и требуется
    if add_order_by and not re.search(r"\bORDER\s+BY\b", query, re.IGNORECASE):
        # Попытка найти первичный ключ или похожую колонку
        if re.search(r"\bSELECT\s+\*\s+FROM\s+(\w+)", query, re.IGNORECASE):
            # Если SELECT *, добавляем ORDER BY по первой колонке
            query += " ORDER BY 1"
        elif "id" in query.lower():
            query += " ORDER BY id"

    # Добавляем LIMIT и OFFSET если нужно
    if limit is not None:
        query += f" LIMIT {limit}"

    if offset is not None:
        query += f" OFFSET {offset}"

    return query


def generate_query_hash(query: str, parameters: dict[str, Any] | None = None) -> str:
    """
    Генерация хеша для SQL запроса с параметрами

    Args:
        query: SQL запрос
        parameters: Параметры запроса

    Returns:
        MD5 хеш запроса и параметров
    """
    # Нормализуем запрос (убираем лишние пробелы, приводим к lower case)
    normalized_query = re.sub(r"\s+", " ", query.strip().lower())

    # Добавляем параметры к строке для хеширования
    hash_string = normalized_query
    if parameters:
        # Сортируем параметры для детерминированности
        sorted_params = sorted(parameters.items())
        params_str = str(sorted_params)
        hash_string += f"|{params_str}"

    return hashlib.md5(hash_string.encode()).hexdigest()


# ================================
# Data Processing Utilities
# ================================


def infer_pandas_dtypes(df: pd.DataFrame) -> dict[str, str]:
    """
    Автоматическое определение типов данных для pandas DataFrame

    Args:
        df: DataFrame для анализа

    Returns:
        Dict[str, str]: Словарь с оптимальными типами данных
    """
    dtypes = {}

    for col in df.columns:
        series = df[col]

        # Пропускаем колонки с NaN
        if series.isnull().all():
            dtypes[col] = "object"
            continue

        # Определяем тип данных
        if pd.api.types.is_integer_dtype(series):
            # Для целых чисел определяем минимальный тип
            min_val = series.min()
            max_val = series.max()

            # Приводим к signed типам для совместимости с тестами
            if min_val >= -128 and max_val <= 127:
                dtypes[col] = "int8"
            elif min_val >= -32768 and max_val <= 32767:
                dtypes[col] = "int16"
            elif min_val >= -2147483648 and max_val <= 2147483647:
                dtypes[col] = "int32"
            else:
                dtypes[col] = "int64"

        elif pd.api.types.is_float_dtype(series):
            # Для float определяем точность
            if series.abs().max() < 3.4e38:
                dtypes[col] = "float32"
            else:
                dtypes[col] = "float64"

        elif pd.api.types.is_bool_dtype(series):
            dtypes[col] = "bool"

        elif pd.api.types.is_datetime64_any_dtype(series):
            dtypes[col] = "datetime64[ns]"

        else:
            # Для строк определяем category vs object
            unique_ratio = series.nunique() / len(series)
            if unique_ratio < 0.5:  # Если много повторений - category
                dtypes[col] = "category"
            else:
                dtypes[col] = "object"

    return dtypes


def optimize_dataframe_memory(df: pd.DataFrame, inplace: bool = False) -> pd.DataFrame:
    """
    Оптимизация использования памяти pandas DataFrame

    Args:
        df: DataFrame для оптимизации
        inplace: Изменять ли исходный DataFrame

    Returns:
        Оптимизированный DataFrame
    """
    if not inplace:
        df = df.copy()

    # Получаем рекомендации по типам данных
    dtype_recommendations = infer_pandas_dtypes(df)

    # Применяем оптимизации
    for column, recommended_dtype in dtype_recommendations.items():
        try:
            if recommended_dtype == "category":
                df[column] = df[column].astype("category")
            elif recommended_dtype == "string":
                df[column] = df[column].astype("string")
            elif recommended_dtype == "boolean":
                df[column] = df[column].astype("boolean")
            elif recommended_dtype.startswith(("int", "uint", "float")):
                df[column] = pd.to_numeric(df[column], errors="coerce").astype(
                    recommended_dtype
                )
            elif recommended_dtype.startswith("datetime"):
                df[column] = pd.to_datetime(df[column], errors="coerce")
        except (ValueError, TypeError) as e:
            logger.warning(
                "Failed to optimize column dtype",
                column=column,
                recommended_dtype=recommended_dtype,
                error=str(e),
            )

    return df


def split_dataframe_chunks(
    df: pd.DataFrame, chunk_size: int, preserve_order: bool = True
) -> list[pd.DataFrame]:
    """
    Разделение DataFrame на чанки заданного размера

    Args:
        df: DataFrame для разделения
        chunk_size: Размер каждого чанка
        preserve_order: Сохранять ли порядок строк

    Returns:
        Список DataFrame чанков
    """
    if chunk_size <= 0:
        raise ValueError("chunk_size must be positive")

    chunks = []
    total_rows = len(df)

    for start_idx in range(0, total_rows, chunk_size):
        end_idx = min(start_idx + chunk_size, total_rows)
        chunk = df.iloc[start_idx:end_idx]

        if preserve_order:
            # Добавляем индекс для сохранения порядка
            chunk = chunk.copy()
            chunk["__chunk_order__"] = range(start_idx, end_idx)

        chunks.append(chunk)

    return chunks


def merge_dataframe_chunks(
    chunks: list[pd.DataFrame],
    preserve_order: bool = True,
    drop_chunk_order: bool = True,
) -> pd.DataFrame:
    """
    Объединение чанков DataFrame обратно в один DataFrame

    Args:
        chunks: Список DataFrame чанков
        preserve_order: Восстанавливать ли исходный порядок строк
        drop_chunk_order: Удалять ли служебную колонку __chunk_order__

    Returns:
        Объединенный DataFrame
    """
    if not chunks:
        return pd.DataFrame()

    # Объединяем все чанки
    result = pd.concat(chunks, ignore_index=True)

    # Восстанавливаем порядок если нужно
    if preserve_order and "__chunk_order__" in result.columns:
        result = result.sort_values("__chunk_order__").reset_index(drop=True)

    # Удаляем служебную колонку
    if drop_chunk_order and "__chunk_order__" in result.columns:
        result = result.drop("__chunk_order__", axis=1)

    return result


# ================================
# File and Path Utilities
# ================================


def ensure_directory(path: str | Path) -> Path:
    """
    Создание директории если она не существует

    Args:
        path: Путь к директории

    Returns:
        Path объект созданной/существующей директории
    """
    path_obj = Path(path)
    path_obj.mkdir(parents=True, exist_ok=True)
    return path_obj


def get_temp_file_path(prefix: str = "sql_extractor", suffix: str = ".tmp") -> Path:
    """
    Получение пути для временного файла

    Args:
        prefix: Префикс имени файла
        suffix: Суффикс имени файла

    Returns:
        Path объект для временного файла
    """
    import os
    import tempfile

    temp_dir = Path(tempfile.gettempdir())
    temp_file = (
        temp_dir / f"{prefix}_{os.getpid()}_{hash(os.urandom(8)) % 1000000}{suffix}"
    )

    return temp_file


# ================================
# Performance Monitoring Utilities
# ================================


def format_bytes(bytes_value: int) -> str:
    """
    Форматирование размера в байтах в человекочитаемый вид

    Args:
        bytes_value: Размер в байтах

    Returns:
        Отформатированная строка (например, "1.5 MB")
    """
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if bytes_value < 1024.0:
            return f"{bytes_value:.1f} {unit}"
        bytes_value /= 1024.0
    return f"{bytes_value:.1f} PB"


def format_duration(seconds: float) -> str:
    """
    Форматирование длительности в человекочитаемый вид

    Args:
        seconds: Длительность в секундах

    Returns:
        Отформатированная строка (например, "2m 30s")
    """
    if seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        minutes = int(seconds // 60)
        remaining_seconds = seconds % 60
        return f"{minutes}m {remaining_seconds:.0f}s"
    else:
        hours = int(seconds // 3600)
        remaining_minutes = int((seconds % 3600) // 60)
        return f"{hours}h {remaining_minutes}m"
