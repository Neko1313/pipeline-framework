"""
CSV Extractor - извлечение данных из CSV файлов
"""
from typing import Any, Dict, List, Optional, AsyncGenerator, Union
from pathlib import Path
import pandas as pd
import polars as pl
import aiofiles
import aiocsv
import asyncio
from urllib.parse import urlparse

from pipeline_core import ExecutionContext
from ..base import BaseExtractor, StreamingExtractor, ExtractorConfig
from pydantic import Field, field_validator


class CSVExtractorConfig(ExtractorConfig):
    """Конфигурация CSV экстрактора"""
    type: str = Field(default="csv-extractor", const=True)

    # Источник данных
    file_path: str = Field(..., description="Путь к CSV файлу или URL")

    # CSV параметры
    delimiter: str = Field(default=",", description="Разделитель CSV")
    encoding: str = Field(default="utf-8", description="Кодировка файла")
    has_header: bool = Field(default=True, description="Есть ли заголовки в файле")
    skip_rows: int = Field(default=0, description="Количество строк для пропуска")

    # Фильтрация колонок
    columns: Optional[List[str]] = Field(None, description="Список колонок для извлечения")
    exclude_columns: Optional[List[str]] = Field(None, description="Список колонок для исключения")

    # Параметры типов данных
    infer_schema: bool = Field(default=True, description="Автоматически определять типы")
    dtypes: Optional[Dict[str, str]] = Field(None, description="Явное указание типов колонок")

    # Потоковый режим
    streaming: bool = Field(default=False, description="Использовать потоковое чтение")
    chunk_size: Optional[int] = Field(None, description="Размер chunk для потокового чтения")

    @field_validator('file_path')
    @classmethod
    def validate_file_path(cls, v: str) -> str:
        if not v:
            raise ValueError("file_path не может быть пустым")

        # Проверяем URL или локальный путь
        parsed = urlparse(v)
        if parsed.scheme in ['http', 'https', 'ftp', 's3']:
            return v  # URL валиден

        # Для локальных путей проверяем существование
        path = Path(v)
        if not path.exists():
            raise ValueError(f"Файл не найден: {v}")
        if not path.is_file():
            raise ValueError(f"Путь не является файлом: {v}")

        return v

    @field_validator('chunk_size')
    @classmethod
    def validate_chunk_size(cls, v: Optional[int]) -> Optional[int]:
        if v is not None and v <= 0:
            raise ValueError("chunk_size должен быть положительным числом")
        return v


class CSVExtractor(StreamingExtractor):
    """
    Экстрактор для CSV файлов

    Поддерживает:
    - Локальные файлы и URL
    - Различные форматы CSV
    - Потоковое чтение больших файлов
    - Фильтрацию колонок
    - Автоматическое определение типов
    """

    def get_config_model(self):
        return CSVExtractorConfig

    def extract_data(self, context: ExecutionContext) -> Any:
        """Синхронное извлечение CSV данных"""
        config: CSVExtractorConfig = self.config

        if config.streaming:
            # Потоковое чтение через async
            return asyncio.run(self.extract_data_async(context))
        else:
            # Обычное чтение в память
            return self._read_csv_sync(config)

    async def extract_data_async(self, context: ExecutionContext) -> Any:
        """Асинхронное извлечение CSV данных"""
        config: CSVExtractorConfig = self.config

        if config.streaming:
            # Потоковое чтение по chunks
            all_data = []
            async for batch in self.extract_batches(context):
                all_data.extend(batch)
            return all_data
        else:
            # Асинхронное чтение всего файла
            return await self._read_csv_async(config)

    async def extract_batches(self, context: ExecutionContext) -> AsyncGenerator[List[Dict], None]:
        """Генератор batch'ей для потокового чтения"""
        config: CSVExtractorConfig = self.config

        if self._is_url(config.file_path):
            # Для URL используем синхронное чтение с pandas
            df_chunks = pd.read_csv(
                config.file_path,
                delimiter=config.delimiter,
                encoding=config.encoding,
                header=0 if config.has_header else None,
                skiprows=config.skip_rows,
                chunksize=config.chunk_size or config.batch_size,
                dtype=config.dtypes
            )

            for chunk in df_chunks:
                processed_chunk = self._process_dataframe(chunk, config)
                yield processed_chunk.to_dict('records')
        else:
            # Для локальных файлов используем aiofiles
            async with aiofiles.open(config.file_path, 'r', encoding=config.encoding) as file:
                # Пропускаем строки если нужно
                for _ in range(config.skip_rows):
                    await file.readline()

                # Читаем заголовки
                headers = None
                if config.has_header:
                    header_line = await file.readline()
                    headers = [h.strip() for h in header_line.strip().split(config.delimiter)]

                # Читаем данные по batch'ам
                batch = []
                batch_size = config.chunk_size or config.batch_size

                async for line in file:
                    if line.strip():
                        values = [v.strip() for v in line.strip().split(config.delimiter)]

                        if headers:
                            row = dict(zip(headers, values))
                        else:
                            row = {f"column_{i}": v for i, v in enumerate(values)}

                        batch.append(row)

                        if len(batch) >= batch_size:
                            yield self._process_batch(batch, config)
                            batch = []

                # Обрабатываем последний batch
                if batch:
                    yield self._process_batch(batch, config)

    def _read_csv_sync(self, config: CSVExtractorConfig) -> pd.DataFrame:
        """Синхронное чтение CSV файла"""
        self.logger.info(f"Чтение CSV файла: {config.file_path}")

        # Используем pandas для чтения
        df = pd.read_csv(
            config.file_path,
            delimiter=config.delimiter,
            encoding=config.encoding,
            header=0 if config.has_header else None,
            skiprows=config.skip_rows,
            dtype=config.dtypes
        )

        return self._process_dataframe(df, config)

    async def _read_csv_async(self, config: CSVExtractorConfig) -> List[Dict]:
        """Асинхронное чтение CSV файла"""
        if self._is_url(config.file_path):
            # Для URL используем синхронный метод
            return self._read_csv_sync(config)

        self.logger.info(f"Асинхронное чтение CSV файла: {config.file_path}")

        data = []
        async with aiofiles.open(config.file_path, 'r', encoding=config.encoding) as file:
            # Пропускаем строки
            for _ in range(config.skip_rows):
                await file.readline()

            # Читаем заголовки
            headers = None
            if config.has_header:
                header_line = await file.readline()
                headers = [h.strip() for h in header_line.strip().split(config.delimiter)]

            # Читаем все данные
            async for line in file:
                if line.strip():
                    values = [v.strip() for v in line.strip().split(config.delimiter)]

                    if headers:
                        row = dict(zip(headers, values))
                    else:
                        row = {f"column_{i}": v for i, v in enumerate(values)}

                    data.append(row)

        return self._process_batch(data, config)

    def _process_dataframe(self, df: pd.DataFrame, config: CSVExtractorConfig) -> pd.DataFrame:
        """Обработка pandas DataFrame"""

        # Фильтрация колонок
        if config.columns:
            available_columns = [col for col in config.columns if col in df.columns]
            if available_columns:
                df = df[available_columns]
            else:
                self.logger.warning("Ни одна из указанных колонок не найдена")

        if config.exclude_columns:
            columns_to_keep = [col for col in df.columns if col not in config.exclude_columns]
            df = df[columns_to_keep]

        # Автоматическое определение типов
        if config.infer_schema:
            df = df.infer_objects()

        return df

    def _process_batch(self, batch: List[Dict], config: CSVExtractorConfig) -> List[Dict]:
        """Обработка batch данных"""
        if not batch:
            return batch

        # Фильтрация колонок
        if config.columns:
            batch = [
                {k: v for k, v in row.items() if k in config.columns}
                for row in batch
            ]

        if config.exclude_columns:
            batch = [
                {k: v for k, v in row.items() if k not in config.exclude_columns}
                for row in batch
            ]

        # Простое приведение типов
        if config.infer_schema:
            batch = self._infer_types_batch(batch)

        return batch

    def _infer_types_batch(self, batch: List[Dict]) -> List[Dict]:
        """Простое определение типов для batch"""
        if not batch:
            return batch

        # Для каждой колонки пытаемся определить тип
        result = []
        for row in batch:
            new_row = {}
            for key, value in row.items():
                if value and isinstance(value, str):
                    # Пытаемся преобразовать в число
                    try:
                        if '.' in value:
                            new_row[key] = float(value)
                        else:
                            new_row[key] = int(value)
                    except ValueError:
                        # Пытаемся преобразовать в bool
                        if value.lower() in ['true', 'false']:
                            new_row[key] = value.lower() == 'true'
                        else:
                            new_row[key] = value
                else:
                    new_row[key] = value
            result.append(new_row)

        return result

    def _is_url(self, path: str) -> bool:
        """Проверка является ли путь URL"""
        parsed = urlparse(path)
        return parsed.scheme in ['http', 'https', 'ftp', 's3']

    def setup(self) -> None:
        """Инициализация"""
        config: CSVExtractorConfig = self.config
        self.logger.info(f"Инициализация CSV экстрактора для: {config.file_path}")

    def teardown(self) -> None:
        """Очистка ресурсов"""
        self.logger.debug("Очистка ресурсов CSV экстрактора")
