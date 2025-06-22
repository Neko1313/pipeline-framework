# packages/components/loaders/loaders_sql/src/loaders_sql/components.py

"""
SQL Loader для pipeline framework

Компоненты для загрузки данных в реляционные базы данных:
- SQLLoader: Универсальный SQL loader
- PostgreSQLLoader: Специализированный PostgreSQL loader
- MySQLLoader: Специализированный MySQL loader
- SQLiteLoader: Специализированный SQLite loader
"""

import time
from typing import Any, Dict, List, Optional, Literal
from urllib.parse import urlparse

import pandas as pd
import structlog
from pydantic import Field, field_validator, computed_field
from sqlalchemy import inspect, text, create_engine
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine, AsyncConnection

from pipeline_core.components.base import (
    BaseLoader,
    ComponentType,
    ExecutionContext,
    ComponentSettings,
)

# Типы данных
WriteMode = Literal["append", "replace", "upsert", "insert"]
DatabaseDialect = Literal["postgresql", "mysql", "sqlite", "oracle", "mssql"]

logger = structlog.get_logger(__name__)


class SQLLoaderConfig(ComponentSettings):
    """Конфигурация SQL Loader"""

    # Подключение к БД
    connection_string: str = Field(..., description="Строка подключения к БД")
    dialect: Optional[DatabaseDialect] = Field(default=None, description="Диалект БД")

    # Целевая таблица
    target_table: str = Field(..., description="Имя целевой таблицы")
    target_schema: Optional[str] = Field(default=None, description="Схема БД")

    # Режим записи
    write_mode: WriteMode = Field(default="append", description="Режим записи данных")

    # Настройки upsert
    upsert_keys: List[str] = Field(
        default_factory=list, description="Ключи для upsert операций"
    )

    # Создание таблицы
    create_table_if_not_exists: bool = Field(
        default=False, description="Создавать таблицу если не существует"
    )

    table_schema: Optional[Dict[str, str]] = Field(
        default=None, description="Схема таблицы для создания"
    )

    # Производительность
    batch_size: int = Field(
        default=10000, ge=100, le=100000, description="Размер батча для вставки"
    )

    chunk_size: Optional[int] = Field(
        default=None, ge=1000, description="Размер чанка для обработки"
    )

    # Дополнительные настройки
    if_exists: Literal["fail", "replace", "append"] = Field(
        default="append",
        description="Поведение если таблица существует (для pandas.to_sql)",
    )

    index: bool = Field(default=False, description="Записывать индекс DataFrame")

    # Валидация данных
    validate_before_load: bool = Field(
        default=True, description="Валидировать данные перед загрузкой"
    )

    max_error_rate_percent: float = Field(
        default=0.0, ge=0.0, le=100.0, description="Максимальный процент ошибок"
    )

    @field_validator("connection_string")
    @classmethod
    def validate_connection_string(cls, v: str) -> str:
        """Валидация строки подключения"""
        try:
            parsed = urlparse(v)
            if not parsed.scheme:
                raise ValueError("Connection string must have a scheme")
            return v
        except Exception:
            raise ValueError("Invalid connection string format")

    @field_validator("target_table")
    @classmethod
    def validate_target_table(cls, v: str) -> str:
        """Валидация имени таблицы"""
        if not v.strip():
            raise ValueError("Target table name cannot be empty")

        # Базовая валидация имени таблицы
        import re

        if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", v):
            raise ValueError("Invalid table name format")

        return v.strip()

    @computed_field
    @property
    def inferred_dialect(self) -> str:
        """Автоматическое определение диалекта"""
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
            }

            return dialect_mapping.get(scheme, "unknown")
        except Exception:
            return "unknown"

    @property
    def full_table_name(self) -> str:
        """Полное имя таблицы с схемой"""
        if self.target_schema:
            return f"{self.target_schema}.{self.target_table}"
        return self.target_table


class SQLLoader(BaseLoader[None, SQLLoaderConfig]):
    """
    Универсальный SQL Loader для загрузки данных в реляционные БД

    Поддерживает:
    - Различные режимы записи (append, replace, upsert)
    - Batch загрузку для производительности
    - Автоматическое создание таблиц
    - Валидацию данных
    - Upsert операции
    """

    def __init__(self, config: SQLLoaderConfig, name: str = "sql-loader"):
        super().__init__(config)
        self._name = name
        self._engine: Optional[AsyncEngine] = None

    @property
    def name(self) -> str:
        return self._name

    @property
    def component_type(self) -> ComponentType:
        return ComponentType.LOADER

    async def initialize(self) -> None:
        """Инициализация loader'а"""
        if self._engine is not None:
            return

        try:
            # Создаем async engine
            self._engine = create_async_engine(
                self.config.connection_string,
                echo=False,
                pool_pre_ping=True,
            )

            # Тестируем подключение
            await self._test_connection()

            self.logger.info(
                "SQL loader initialized",
                dialect=self.config.inferred_dialect,
                target_table=self.config.full_table_name,
            )

        except Exception as e:
            raise ConnectionError(f"Failed to initialize SQL loader: {e}") from e

    async def _test_connection(self) -> None:
        """Тестирование подключения к БД"""
        try:
            async with self._engine.begin() as conn:
                if self.config.inferred_dialect == "postgresql":
                    await conn.execute(text("SELECT 1"))
                elif self.config.inferred_dialect == "mysql":
                    await conn.execute(text("SELECT 1"))
                elif self.config.inferred_dialect == "sqlite":
                    await conn.execute(text("SELECT 1"))
                else:
                    await conn.execute(text("SELECT 1"))

            self.logger.info("Database connection test successful")

        except Exception as e:
            raise ConnectionError(f"Database connection test failed: {e}") from e

    async def _execute_impl(self, context: ExecutionContext) -> None:
        """Основная логика загрузки данных"""
        if not self._engine:
            await self.initialize()

        # Получаем данные для загрузки
        input_data = self._get_input_data(context)

        if input_data is None:
            self.logger.warning("No input data for loading")
            return

        # Конвертируем в pandas DataFrame
        if not isinstance(input_data, pd.DataFrame):
            input_data = self._convert_to_dataframe(input_data)

        if input_data.empty:
            self.logger.warning("Input DataFrame is empty, skipping load")
            return

        self.logger.info(
            "Starting data load",
            rows=len(input_data),
            columns=len(input_data.columns),
            target_table=self.config.full_table_name,
            write_mode=self.config.write_mode,
        )

        try:
            # Валидация данных
            if self.config.validate_before_load:
                self._validate_data(input_data)

            # Создаем таблицу если нужно
            if self.config.create_table_if_not_exists:
                await self._ensure_table_exists(input_data)

            # Загружаем данные
            start_time = time.time()

            if self.config.write_mode == "upsert":
                await self._upsert_data(input_data)
            else:
                await self._load_data(input_data)

            duration = time.time() - start_time

            self.logger.info(
                "Data load completed",
                rows_loaded=len(input_data),
                duration_seconds=duration,
                rows_per_second=len(input_data) / duration if duration > 0 else 0,
            )

        except Exception as e:
            self.logger.error("Data load failed", error=str(e))
            raise

    def _get_input_data(self, context: ExecutionContext) -> Any:
        """Получение входных данных"""
        if not context.previous_results:
            raise ValueError("No previous results available for loading")

        # Берем данные из последнего успешного этапа
        for result in reversed(context.previous_results):
            if (
                hasattr(result, "success")
                and result.success
                and result.data is not None
            ):
                return result.data

        raise ValueError("No valid input data found in previous results")

    def _convert_to_dataframe(self, data: Any) -> pd.DataFrame:
        """Конвертация данных в pandas DataFrame"""
        if isinstance(data, pd.DataFrame):
            return data
        elif isinstance(data, dict):
            return pd.DataFrame([data])
        elif isinstance(data, list):
            if data and isinstance(data[0], dict):
                return pd.DataFrame(data)
            else:
                return pd.DataFrame({"data": data})
        else:
            return pd.DataFrame({"data": [data]})

    def _validate_data(self, data: pd.DataFrame) -> None:
        """Валидация данных перед загрузкой"""
        # Проверка на пустые данные
        if data.empty:
            raise ValueError("Cannot load empty DataFrame")

        # Проверка на NaN в критических колонках
        if self.config.upsert_keys:
            for key in self.config.upsert_keys:
                if key in data.columns and data[key].isna().any():
                    raise ValueError(f"Upsert key column '{key}' contains NaN values")

        # Проверка типов данных
        for column in data.columns:
            if data[column].dtype == "object":
                # Проверяем длину строк для varchar полей
                max_length = data[column].astype(str).str.len().max()
                if max_length > 65535:  # Стандартный лимит для TEXT
                    self.logger.warning(
                        f"Column '{column}' has very long strings (max: {max_length})"
                    )

    async def _ensure_table_exists(self, data: pd.DataFrame) -> None:
        """Создание таблицы если не существует"""
        async with self._engine.begin() as conn:
            # Проверяем существование таблицы
            inspector = inspect(conn.sync_connection)

            table_exists = False
            if self.config.target_schema:
                table_exists = inspector.has_table(
                    self.config.target_table, schema=self.config.target_schema
                )
            else:
                table_exists = inspector.has_table(self.config.target_table)

            if not table_exists:
                await self._create_table(conn, data)

    async def _create_table(self, conn: AsyncConnection, data: pd.DataFrame) -> None:
        """Создание таблицы на основе схемы данных"""
        # Генерируем DDL на основе DataFrame или заданной схемы
        if self.config.table_schema:
            # Используем заданную схему
            columns_ddl = []
            for col_name, col_type in self.config.table_schema.items():
                columns_ddl.append(f"{col_name} {col_type}")
            columns_str = ",\n  ".join(columns_ddl)
        else:
            # Автоматически определяем типы из DataFrame
            columns_ddl = []
            for col_name, dtype in data.dtypes.items():
                sql_type = self._pandas_to_sql_type(dtype, col_name, data)
                columns_ddl.append(f"{col_name} {sql_type}")
            columns_str = ",\n  ".join(columns_ddl)

        # Создаем DDL
        create_table_sql = f"""
        CREATE TABLE {self.config.full_table_name} (
          {columns_str}
        )
        """

        await conn.execute(text(create_table_sql))

        self.logger.info(
            "Table created",
            table=self.config.full_table_name,
            columns=len(columns_ddl),
        )

    def _pandas_to_sql_type(self, dtype, col_name: str, data: pd.DataFrame) -> str:
        """Конвертация pandas типов в SQL типы"""
        dialect = self.config.inferred_dialect

        if pd.api.types.is_integer_dtype(dtype):
            if dialect == "postgresql":
                return "BIGINT"
            elif dialect == "mysql":
                return "BIGINT"
            else:
                return "INTEGER"

        elif pd.api.types.is_float_dtype(dtype):
            if dialect == "postgresql":
                return "DOUBLE PRECISION"
            elif dialect == "mysql":
                return "DOUBLE"
            else:
                return "REAL"

        elif pd.api.types.is_bool_dtype(dtype):
            if dialect == "sqlite":
                return "INTEGER"  # SQLite не имеет BOOLEAN
            else:
                return "BOOLEAN"

        elif pd.api.types.is_datetime64_any_dtype(dtype):
            if dialect == "postgresql":
                return "TIMESTAMP"
            elif dialect == "mysql":
                return "DATETIME"
            else:
                return "DATETIME"

        else:  # string/object типы
            # Определяем максимальную длину строки
            max_length = data[col_name].astype(str).str.len().max()

            if max_length <= 255:
                if dialect == "postgresql":
                    return f"VARCHAR({max(max_length, 50)})"
                else:
                    return f"VARCHAR({max(max_length, 50)})"
            else:
                if dialect == "postgresql":
                    return "TEXT"
                elif dialect == "mysql":
                    return "LONGTEXT"
                else:
                    return "TEXT"

    async def _load_data(self, data: pd.DataFrame) -> None:
        """Загрузка данных в обычном режиме"""
        # Используем pandas to_sql для простоты
        # В production среде лучше использовать более эффективные методы

        if self.config.chunk_size and len(data) > self.config.chunk_size:
            # Загружаем по частям
            for i in range(0, len(data), self.config.chunk_size):
                chunk = data.iloc[i : i + self.config.chunk_size]
                await self._load_chunk(chunk, i == 0)
        else:
            await self._load_chunk(data, True)

    async def _load_chunk(self, chunk: pd.DataFrame, is_first_chunk: bool) -> None:
        """Загрузка одного чанка данных"""
        # Определяем if_exists для первого чанка
        if_exists = self.config.if_exists if is_first_chunk else "append"

        # Корректируем if_exists на основе write_mode
        if self.config.write_mode == "replace":
            if_exists = "replace" if is_first_chunk else "append"
        elif self.config.write_mode == "append":
            if_exists = "append"

        # Используем синхронный engine для pandas.to_sql
        sync_engine = create_engine(self.config.connection_string)

        try:
            chunk.to_sql(
                name=self.config.target_table,
                con=sync_engine,
                schema=self.config.target_schema,
                if_exists=if_exists,
                index=self.config.index,
                chunksize=self.config.batch_size,
                method=None,  # Можно добавить кастомный метод для оптимизации
            )
        finally:
            sync_engine.dispose()

    async def _upsert_data(self, data: pd.DataFrame) -> None:
        """Upsert операция (обновление или вставка)"""
        if not self.config.upsert_keys:
            raise ValueError("Upsert keys must be specified for upsert mode")

        # Реализация upsert зависит от диалекта БД
        dialect = self.config.inferred_dialect

        if dialect == "postgresql":
            await self._postgresql_upsert(data)
        elif dialect == "mysql":
            await self._mysql_upsert(data)
        elif dialect == "sqlite":
            await self._sqlite_upsert(data)
        else:
            # Fallback: delete + insert
            await self._generic_upsert(data)

    async def _postgresql_upsert(self, data: pd.DataFrame) -> None:
        """PostgreSQL UPSERT через ON CONFLICT"""
        # Временно загружаем в staging таблицу и делаем UPSERT
        staging_table = f"{self.config.target_table}_staging_{int(time.time())}"

        async with self._engine.begin() as conn:
            try:
                # Создаем staging таблицу
                await conn.execute(
                    text(f"""
                    CREATE TEMP TABLE {staging_table} 
                    AS SELECT * FROM {self.config.full_table_name} WHERE FALSE
                """)
                )

                # Загружаем данные в staging
                sync_engine = create_engine(self.config.connection_string)
                try:
                    data.to_sql(
                        name=staging_table,
                        con=sync_engine,
                        if_exists="append",
                        index=False,
                    )
                finally:
                    sync_engine.dispose()

                # UPSERT из staging в основную таблицу
                columns = ", ".join(data.columns)
                conflict_columns = ", ".join(self.config.upsert_keys)
                update_set = ", ".join(
                    [
                        f"{col} = EXCLUDED.{col}"
                        for col in data.columns
                        if col not in self.config.upsert_keys
                    ]
                )

                upsert_sql = f"""
                    INSERT INTO {self.config.full_table_name} ({columns})
                    SELECT {columns} FROM {staging_table}
                    ON CONFLICT ({conflict_columns}) 
                    DO UPDATE SET {update_set}
                """

                await conn.execute(text(upsert_sql))

            finally:
                # Удаляем staging таблицу
                await conn.execute(text(f"DROP TABLE IF EXISTS {staging_table}"))

    async def _mysql_upsert(self, data: pd.DataFrame) -> None:
        """MySQL UPSERT через ON DUPLICATE KEY UPDATE"""
        # Аналогично PostgreSQL, но с MySQL синтаксисом
        # Здесь должна быть реализация для MySQL
        await self._generic_upsert(data)

    async def _sqlite_upsert(self, data: pd.DataFrame) -> None:
        """SQLite UPSERT через INSERT OR REPLACE"""
        # Реализация для SQLite
        await self._generic_upsert(data)

    async def _generic_upsert(self, data: pd.DataFrame) -> None:
        """Универсальный upsert через DELETE + INSERT"""
        async with self._engine.begin() as conn:
            # Для каждой строки делаем DELETE по ключам, затем INSERT
            for _, row in data.iterrows():
                # Создаем WHERE условие для удаления
                where_conditions = []
                params = {}

                for key in self.config.upsert_keys:
                    where_conditions.append(f"{key} = :{key}")
                    params[key] = row[key]

                where_clause = " AND ".join(where_conditions)

                # Удаляем существующую запись
                delete_sql = (
                    f"DELETE FROM {self.config.full_table_name} WHERE {where_clause}"
                )
                await conn.execute(text(delete_sql), params)

                # Вставляем новую запись
                columns = ", ".join(row.index)
                placeholders = ", ".join([f":{col}" for col in row.index])
                insert_sql = f"""
                    INSERT INTO {self.config.full_table_name} ({columns}) 
                    VALUES ({placeholders})
                """

                row_params = {col: row[col] for col in row.index}
                await conn.execute(text(insert_sql), row_params)

    async def cleanup(self) -> None:
        """Очистка ресурсов"""
        if self._engine:
            await self._engine.dispose()
            self._engine = None

        self.logger.info("SQL loader cleaned up")


# Специализированные loaders


class PostgreSQLLoader(SQLLoader):
    """Специализированный PostgreSQL loader"""

    def __init__(self, config: SQLLoaderConfig, **kwargs):
        super().__init__(config, name="postgresql-loader", **kwargs)


class MySQLLoader(SQLLoader):
    """Специализированный MySQL loader"""

    def __init__(self, config: SQLLoaderConfig, **kwargs):
        super().__init__(config, name="mysql-loader", **kwargs)


class SQLiteLoader(SQLLoader):
    """Специализированный SQLite loader"""

    def __init__(self, config: SQLLoaderConfig, **kwargs):
        super().__init__(config, name="sqlite-loader", **kwargs)


# Утилитарные функции


def create_sql_loader(
    connection_string: str,
    target_table: str,
    write_mode: WriteMode = "append",
    target_schema: Optional[str] = None,
    **kwargs,
) -> SQLLoader:
    """Фабричная функция для создания SQL loader'а"""

    config = SQLLoaderConfig(
        connection_string=connection_string,
        target_table=target_table,
        write_mode=write_mode,
        target_schema=target_schema,
        **kwargs,
    )

    return SQLLoader(config)
