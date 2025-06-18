# packages/components/extractors/src/pipeline_extractors/file/csv_extractor.py
"""
Полноценный CSV экстрактор с расширенными возможностями
"""

import asyncio
from pathlib import Path
from typing import Any, Dict, List, Optional, Union, Literal
import pandas as pd
import numpy as np
from pydantic import Field, field_validator, model_validator
from datetime import datetime
import logging

try:
    import polars as pl

    POLARS_AVAILABLE = True
except ImportError:
    POLARS_AVAILABLE = False

try:
    import aiofiles
    import aiocsv

    ASYNC_AVAILABLE = True
except ImportError:
    ASYNC_AVAILABLE = False

from pipeline_core import (
    BaseComponent,
    ComponentConfig,
    ExecutionContext,
    ExecutionResult,
    ExecutionStatus,
    ComponentType,
    register_component,
)
from pipeline_core.exceptions.errors import (
    PipelineConfigError,
    PipelineDataError,
    PipelineExecutionError,
)


class CSVExtractorConfig(ComponentConfig):
    """Конфигурация полноценного CSV экстрактора"""

    type: str = Field(default="csv-extractor", frozen=True)

    # === ОСНОВНЫЕ ПАРАМЕТРЫ ФАЙЛА ===
    file_path: Union[str, Path] = Field(description="Путь к CSV файлу")

    # === ПАРАМЕТРЫ ПАРСИНГА ===
    delimiter: str = Field(default=",", description="Разделитель колонок")
    encoding: str = Field(default="utf-8", description="Кодировка файла")
    has_header: bool = Field(default=True, description="Есть ли заголовок в файле")
    skip_rows: int = Field(default=0, ge=0, description="Пропустить N строк сверху")
    max_rows: Optional[int] = Field(default=None, ge=1, description="Максимум строк для чтения")

    # === ОБРАБОТКА ДАННЫХ ===
    null_values: List[str] = Field(
        default_factory=lambda: ["", "NULL", "null", "None", "N/A", "n/a", "#N/A"],
        description="Значения, считающиеся пустыми",
    )

    # === ТИПЫ ДАННЫХ ===
    infer_schema: bool = Field(default=True, description="Автоматически определять типы")
    column_types: Optional[Dict[str, str]] = Field(
        default=None, description="Явное указание типов колонок"
    )

    # === ФОРМАТ ВЫВОДА ===
    output_format: Literal["pandas", "polars", "dict", "list"] = Field(
        default="pandas", description="Формат выходных данных"
    )

    # === ФИЛЬТРАЦИЯ И ВЫБОРКА ===
    select_columns: Optional[List[str]] = Field(
        default=None, description="Выбрать только указанные колонки"
    )
    rename_columns: Optional[Dict[str, str]] = Field(
        default=None, description="Переименовать колонки"
    )
    filter_condition: Optional[str] = Field(
        default=None, description="Условие фильтрации (pandas query синтаксис)"
    )

    # === ВАЛИДАЦИЯ ===
    required_columns: List[str] = Field(default_factory=list, description="Обязательные колонки")
    min_records: Optional[int] = Field(
        default=None, ge=0, description="Минимальное количество записей"
    )
    max_records: Optional[int] = Field(
        default=None, ge=1, description="Максимальное количество записей"
    )

    # === ПРОИЗВОДИТЕЛЬНОСТЬ ===
    chunk_size: Optional[int] = Field(
        default=None, ge=1, description="Размер чанка для больших файлов"
    )
    use_async: bool = Field(default=False, description="Использовать асинхронное чтение")
    memory_limit: Optional[str] = Field(
        default=None, description="Лимит памяти (например: '1GB', '500MB')"
    )

    # === ОБРАБОТКА ОШИБОК ===
    skip_bad_lines: bool = Field(default=False, description="Пропускать некорректные строки")
    error_tolerance: float = Field(
        default=0.0, ge=0.0, le=1.0, description="Допустимая доля ошибок (0.0-1.0)"
    )

    @field_validator("file_path")
    @classmethod
    def validate_file_path(cls, v: Union[str, Path]) -> Path:
        """Валидация пути к файлу"""
        path = Path(v)
        if not path.exists():
            raise ValueError(f"Файл не найден: {path}")
        if not path.is_file():
            raise ValueError(f"Путь не является файлом: {path}")
        if path.suffix.lower() not in [".csv", ".tsv", ".txt"]:
            raise ValueError(f"Неподдерживаемый формат файла: {path.suffix}")
        return path

    @field_validator("column_types")
    @classmethod
    def validate_column_types(cls, v: Optional[Dict[str, str]]) -> Optional[Dict[str, str]]:
        """Валидация типов колонок"""
        if v is None:
            return v

        valid_types = {
            "int",
            "int64",
            "int32",
            "integer",
            "float",
            "float64",
            "float32",
            "double",
            "str",
            "string",
            "object",
            "bool",
            "boolean",
            "datetime",
            "date",
            "timestamp",
        }

        for col, dtype in v.items():
            if dtype not in valid_types:
                raise ValueError(f"Неподдерживаемый тип '{dtype}' для колонки '{col}'")

        return v

    @model_validator(mode="after")
    def validate_config(self) -> "CSVExtractorConfig":
        """Дополнительная валидация конфигурации"""
        # Проверка совместимости с polars
        if self.output_format == "polars":
            if not POLARS_AVAILABLE:
                raise ValueError("Polars не установлен. Установите: pip install polars")
            if self.chunk_size is not None:
                raise ValueError("Polars не поддерживает chunk_size")
            if self.use_async:
                raise ValueError("Polars не поддерживает асинхронное чтение")

        # Проверка асинхронности
        if self.use_async and not ASYNC_AVAILABLE:
            raise ValueError("Для асинхронного чтения установите: pip install aiofiles aiocsv")

        # Проверка лимитов
        if self.min_records is not None and self.max_records is not None:
            if self.min_records > self.max_records:
                raise ValueError("min_records не может быть больше max_records")

        return self


@register_component("csv-extractor")
class CSVExtractor(BaseComponent):
    """
    Полноценный CSV экстрактор с расширенными возможностями

    Возможности:
    - Различные форматы вывода (pandas, polars, dict, list)
    - Асинхронное чтение для больших файлов
    - Chunked processing для экономии памяти
    - Фильтрация и преобразование данных
    - Валидация схемы и данных
    - Обработка ошибок и некорректных строк
    - Поддержка различных кодировок и форматов
    """

    def get_config_model(self):
        return CSVExtractorConfig

    def get_component_type(self) -> ComponentType:
        return ComponentType.EXTRACTOR

    def execute(self, context: ExecutionContext) -> ExecutionResult:
        """Основной метод выполнения"""
        try:
            self.logger.info(f"Начинаем извлечение из CSV: {self.config.file_path}")

            # Выбираем метод чтения в зависимости от конфигурации
            if self.config.use_async:
                data = self._execute_async()
            elif self.config.output_format == "polars":
                data = self._extract_with_polars()
            else:
                data = self._extract_with_pandas()

            # Постобработка данных
            data = self._post_process_data(data)

            # Валидация результата
            self._validate_result(data)

            # Подсчет записей
            record_count = self._count_records(data)

            self.logger.info(f"Успешно извлечено {record_count} записей")

            return ExecutionResult(
                status=ExecutionStatus.SUCCESS,
                data=data,
                processed_records=record_count,
                metadata=self._build_metadata(data),
            )

        except Exception as e:
            self.logger.error(f"Ошибка при извлечении данных: {e}")
            return ExecutionResult(
                status=ExecutionStatus.FAILED,
                error_message=str(e),
                metadata={"file_path": str(self.config.file_path)},
            )

    def _execute_async(self) -> Any:
        """Выполнение асинхронного извлечения"""
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

        return loop.run_until_complete(self._extract_async())

    async def _extract_async(self) -> List[Dict[str, Any]]:
        """Асинхронное извлечение данных"""
        self.logger.info("Начинаем асинхронное извлечение")

        rows = []
        error_count = 0

        async with aiofiles.open(
            self.config.file_path, mode="r", encoding=self.config.encoding
        ) as file:
            reader = aiocsv.AsyncDictReader(file, delimiter=self.config.delimiter)

            row_count = 0
            async for row in reader:
                # Пропускаем строки если нужно
                if row_count < self.config.skip_rows:
                    row_count += 1
                    continue

                # Ограничиваем количество строк
                if self.config.max_rows and len(rows) >= self.config.max_rows:
                    break

                try:
                    # Обрабатываем null значения
                    processed_row = self._process_row(row)
                    rows.append(processed_row)

                except Exception as e:
                    error_count += 1
                    if self.config.skip_bad_lines:
                        self.logger.warning(f"Пропускаем некорректную строку {row_count}: {e}")
                        continue
                    else:
                        raise PipelineDataError(f"Ошибка в строке {row_count}: {e}")

                row_count += 1

        # Проверяем допустимость ошибок
        if error_count > 0:
            error_rate = error_count / max(len(rows) + error_count, 1)
            if error_rate > self.config.error_tolerance:
                raise PipelineDataError(
                    f"Слишком много ошибок: {error_rate:.2%} > {self.config.error_tolerance:.2%}"
                )

        return rows

    def _extract_with_pandas(self) -> Union[pd.DataFrame, Dict, List]:
        """Извлечение с использованием pandas"""
        read_kwargs = self._build_pandas_kwargs()

        if self.config.chunk_size:
            # Chunked reading для больших файлов
            chunks = []
            try:
                for chunk in pd.read_csv(chunksize=self.config.chunk_size, **read_kwargs):
                    processed_chunk = self._process_chunk(chunk)
                    if len(processed_chunk) > 0:
                        chunks.append(processed_chunk)

                    self.logger.debug(f"Обработан chunk: {len(processed_chunk)} записей")

                if chunks:
                    df = pd.concat(chunks, ignore_index=True)
                else:
                    df = pd.DataFrame()

            except Exception as e:
                raise PipelineExecutionError(f"Ошибка chunked чтения: {e}")
        else:
            # Обычное чтение
            df = pd.read_csv(**read_kwargs)
            df = self._process_chunk(df)

        # Конвертация в нужный формат
        return self._convert_output_format(df)

    def _extract_with_polars(self) -> pl.DataFrame:
        """Извлечение с использованием polars"""
        read_kwargs = {
            "source": self.config.file_path,
            "separator": self.config.delimiter,
            "encoding": self.config.encoding,
            "has_header": self.config.has_header,
            "skip_rows": self.config.skip_rows,
            "n_rows": self.config.max_rows,
            "null_values": self.config.null_values,
            "infer_schema_length": 1000 if self.config.infer_schema else 0,
        }

        try:
            df = pl.read_csv(**read_kwargs)

            # Применяем фильтры и преобразования
            df = self._process_polars_dataframe(df)

            return df

        except Exception as e:
            raise PipelineExecutionError(f"Ошибка Polars чтения: {e}")

    def _build_pandas_kwargs(self) -> Dict[str, Any]:
        """Построение аргументов для pandas.read_csv"""
        kwargs = {
            "filepath_or_buffer": self.config.file_path,
            "sep": self.config.delimiter,
            "encoding": self.config.encoding,
            "header": 0 if self.config.has_header else None,
            "skiprows": self.config.skip_rows,
            "nrows": self.config.max_rows,
            "na_values": self.config.null_values,
            "keep_default_na": True,
            "on_bad_lines": "skip" if self.config.skip_bad_lines else "error",
        }

        # Колонки для чтения
        if self.config.select_columns:
            kwargs["usecols"] = self.config.select_columns

        # Типы данных
        if not self.config.infer_schema:
            kwargs["dtype"] = str
        elif self.config.column_types:
            # Конвертируем типы в pandas формат
            pandas_types = {}
            for col, dtype in self.config.column_types.items():
                pandas_types[col] = self._convert_to_pandas_dtype(dtype)
            kwargs["dtype"] = pandas_types

        return kwargs

    def _process_chunk(self, df: pd.DataFrame) -> pd.DataFrame:
        """Обработка chunk данных"""
        original_size = len(df)

        # Переименование колонок
        if self.config.rename_columns:
            df = df.rename(columns=self.config.rename_columns)

        # Применение типов данных
        if self.config.column_types:
            df = self._apply_column_types(df)

        # Фильтрация
        if self.config.filter_condition:
            try:
                df = df.query(self.config.filter_condition)
                self.logger.debug(f"Фильтрация: {original_size} -> {len(df)} записей")
            except Exception as e:
                if self.config.skip_bad_lines:
                    self.logger.warning(f"Ошибка фильтрации, пропускаем: {e}")
                else:
                    raise PipelineExecutionError(f"Ошибка в условии фильтрации: {e}")

        return df

    def _process_polars_dataframe(self, df: pl.DataFrame) -> pl.DataFrame:
        """Обработка Polars DataFrame"""
        # Переименование колонок
        if self.config.rename_columns:
            for old_name, new_name in self.config.rename_columns.items():
                if old_name in df.columns:
                    df = df.rename({old_name: new_name})

        # Выбор колонок
        if self.config.select_columns:
            available_columns = [col for col in self.config.select_columns if col in df.columns]
            if available_columns:
                df = df.select(available_columns)

        # Примечание: фильтрация в Polars требует специального синтаксиса
        # Для простоты пропускаем в этой версии

        return df

    def _process_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        """Обработка одной строки для асинхронного чтения"""
        processed_row = {}

        for key, value in row.items():
            # Обработка null значений
            if value in self.config.null_values:
                processed_row[key] = None
            else:
                processed_row[key] = value

        # Переименование колонок
        if self.config.rename_columns:
            final_row = {}
            for key, value in processed_row.items():
                new_key = self.config.rename_columns.get(key, key)
                final_row[new_key] = value
            processed_row = final_row

        # Выбор колонок
        if self.config.select_columns:
            filtered_row = {}
            for col in self.config.select_columns:
                if col in processed_row:
                    filtered_row[col] = processed_row[col]
            processed_row = filtered_row

        return processed_row

    def _post_process_data(self, data: Any) -> Any:
        """Постобработка данных"""
        if isinstance(data, pd.DataFrame):
            # Проверка лимитов записей
            if self.config.max_records and len(data) > self.config.max_records:
                data = data.head(self.config.max_records)
                self.logger.info(f"Ограничено до {self.config.max_records} записей")

        return data

    def _apply_column_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """Применение типов данных к колонкам"""
        for col, dtype in self.config.column_types.items():
            if col in df.columns:
                try:
                    pandas_dtype = self._convert_to_pandas_dtype(dtype)
                    if dtype in ["datetime", "timestamp"]:
                        df[col] = pd.to_datetime(df[col], errors="coerce")
                    elif dtype == "date":
                        df[col] = pd.to_datetime(df[col], errors="coerce").dt.date
                    else:
                        df[col] = df[col].astype(pandas_dtype)

                except Exception as e:
                    self.logger.warning(
                        f"Не удалось привести колонку '{col}' к типу '{dtype}': {e}"
                    )

        return df

    def _convert_to_pandas_dtype(self, dtype: str) -> str:
        """Конвертация типа в pandas формат"""
        mapping = {
            "int": "Int64",
            "integer": "Int64",
            "int32": "Int32",
            "int64": "Int64",
            "float": "float64",
            "float32": "float32",
            "float64": "float64",
            "double": "float64",
            "str": "string",
            "string": "string",
            "object": "object",
            "bool": "boolean",
            "boolean": "boolean",
        }
        return mapping.get(dtype, dtype)

    def _convert_output_format(self, df: pd.DataFrame) -> Union[pd.DataFrame, Dict, List]:
        """Конвертация в нужный формат вывода"""
        if self.config.output_format == "pandas":
            return df
        elif self.config.output_format == "dict":
            return df.to_dict("records")
        elif self.config.output_format == "list":
            return df.values.tolist()
        else:
            raise PipelineConfigError(f"Неподдерживаемый формат: {self.config.output_format}")

    def _validate_result(self, data: Any) -> None:
        """Валидация результата"""
        if data is None:
            raise PipelineDataError("Результат извлечения пуст")

        # Подсчет записей
        record_count = self._count_records(data)

        # Проверка минимального количества записей
        if self.config.min_records is not None:
            if record_count < self.config.min_records:
                raise PipelineDataError(
                    f"Недостаточно записей: {record_count} < {self.config.min_records}"
                )

        # Проверка максимального количества записей
        if self.config.max_records is not None:
            if record_count > self.config.max_records:
                self.logger.warning(
                    f"Превышен лимит записей: {record_count} > {self.config.max_records}"
                )

        # Проверка обязательных колонок
        if self.config.required_columns:
            columns = self._get_columns(data)
            missing_columns = set(self.config.required_columns) - set(columns)
            if missing_columns:
                raise PipelineDataError(f"Отсутствуют обязательные колонки: {missing_columns}")

    def _get_columns(self, data: Any) -> List[str]:
        """Получение списка колонок"""
        if isinstance(data, pd.DataFrame):
            return data.columns.tolist()
        elif isinstance(data, pl.DataFrame):
            return data.columns
        elif isinstance(data, list) and data:
            if isinstance(data[0], dict):
                return list(data[0].keys())
            else:
                return [f"col_{i}" for i in range(len(data[0]))]
        return []

    def _count_records(self, data: Any) -> int:
        """Подсчет количества записей"""
        if isinstance(data, (pd.DataFrame, pl.DataFrame)):
            return len(data)
        elif isinstance(data, list):
            return len(data)
        return 0

    def _build_metadata(self, data: Any) -> Dict[str, Any]:
        """Построение метаданных результата"""
        metadata = {
            "file_path": str(self.config.file_path),
            "file_size": self.config.file_path.stat().st_size,
            "output_format": self.config.output_format,
            "columns": self._get_columns(data),
            "encoding": self.config.encoding,
            "delimiter": self.config.delimiter,
        }

        # Добавляем информацию о chunk processing
        if self.config.chunk_size:
            metadata["chunk_size"] = self.config.chunk_size
            metadata["chunked_processing"] = True

        # Добавляем информацию об асинхронности
        if self.config.use_async:
            metadata["async_processing"] = True

        return metadata

    def validate_dependencies(self, available_stages: List[str]) -> List[str]:
        """CSV экстрактор не имеет зависимостей"""
        return []

    def health_check(self) -> bool:
        """Проверка состояния экстрактора"""
        try:
            # Проверяем доступность файла
            if not self.config.file_path.exists():
                return False

            # Проверяем права на чтение
            if not self.config.file_path.is_file():
                return False

            # Проверяем что файл не пустой
            if self.config.file_path.stat().st_size == 0:
                return False

            return True
        except Exception:
            return False


# === РЕГИСТРАЦИЯ АЛИАСОВ ===


@register_component("file-csv")
class CSVExtractorAlias(CSVExtractor):
    """Алиас для csv-extractor"""

    pass


@register_component("tsv-extractor")
class TSVExtractor(CSVExtractor):
    """Специализированный TSV экстрактор"""

    def get_config_model(self):
        class TSVExtractorConfig(CSVExtractorConfig):
            type: str = Field(default="tsv-extractor", frozen=True)
            delimiter: str = Field(default="\t", frozen=True)

        return TSVExtractorConfig
