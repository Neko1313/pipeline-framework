"""
Базовые классы для компонентов извлечения данных
"""
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, AsyncGenerator, Union
from pathlib import Path
import asyncio
import time

from pipeline_core import (
    BaseComponent,
    ComponentConfig,
    ExecutionContext,
    ExecutionResult,
    ExecutionStatus,
    ComponentType,
    get_pipeline_logger
)
from pydantic import BaseModel, Field, field_validator


class ExtractorConfig(ComponentConfig):
    """Базовая конфигурация для всех экстракторов"""
    type: str = Field(..., description="Тип экстрактора")

    # Общие параметры
    batch_size: int = Field(default=1000, description="Размер batch для обработки")
    max_records: Optional[int] = Field(None, description="Максимальное количество записей")
    timeout: int = Field(default=300, description="Таймаут операции в секундах")

    # Retry параметры
    retry_attempts: int = Field(default=3, description="Количество попыток повтора")
    retry_delay: float = Field(default=1.0, description="Задержка между повторами")
    retry_exponential_base: float = Field(default=2.0, description="База для экспоненциальной задержки")

    # Выходной формат
    output_format: str = Field(default="pandas", description="Формат выходных данных")

    @field_validator('batch_size')
    @classmethod
    def validate_batch_size(cls, v: int) -> int:
        if v <= 0:
            raise ValueError("batch_size должен быть положительным числом")
        return v

    @field_validator('output_format')
    @classmethod
    def validate_output_format(cls, v: str) -> str:
        allowed_formats = ["pandas", "polars", "dict", "list"]
        if v not in allowed_formats:
            raise ValueError(f"output_format должен быть одним из: {allowed_formats}")
        return v


class BaseExtractor(BaseComponent, ABC):
    """
    Базовый класс для всех компонентов извлечения данных

    Предоставляет общую функциональность:
    - Retry логику
    - Batch обработку
    - Конвертацию форматов
    - Логирование прогресса
    - Валидацию данных
    """

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.logger = get_pipeline_logger(
            f"extractor.{self.config.type}",
            component_type=self.config.type
        )

    def get_component_type(self) -> ComponentType:
        return ComponentType.EXTRACTOR

    def get_config_model(self):
        return ExtractorConfig

    def execute(self, context: ExecutionContext) -> ExecutionResult:
        """Выполнение извлечения данных"""
        start_time = time.time()
        config: ExtractorConfig = self.config

        try:
            self.logger.info(f"Начало извлечения данных с помощью {config.type}")

            # Выполняем setup если нужно
            self.setup()

            # Основное извлечение данных
            if asyncio.iscoroutinefunction(self.extract_data):
                data = asyncio.run(self.extract_data_async(context))
            else:
                data = self.extract_data(context)

            # Конвертируем в нужный формат
            formatted_data = self._convert_output_format(data, config.output_format)

            # Применяем лимиты если заданы
            if config.max_records and len(formatted_data) > config.max_records:
                formatted_data = formatted_data[:config.max_records]
                self.logger.warning(f"Ограничено до {config.max_records} записей")

            execution_time = time.time() - start_time
            processed_records = len(formatted_data) if hasattr(formatted_data, '__len__') else 1

            self.logger.info(
                f"Извлечение завершено успешно. "
                f"Обработано {processed_records} записей за {execution_time:.2f} сек"
            )

            return ExecutionResult(
                status=ExecutionStatus.SUCCESS,
                data=formatted_data,
                execution_time=execution_time,
                processed_records=processed_records,
                metadata={
                    "extractor_type": config.type,
                    "output_format": config.output_format,
                    "batch_size": config.batch_size,
                    "records_limit": config.max_records
                }
            )

        except Exception as e:
            execution_time = time.time() - start_time
            self.logger.error(f"Ошибка извлечения данных: {e}", exc_info=True)

            return ExecutionResult(
                status=ExecutionStatus.FAILED,
                error_message=str(e),
                execution_time=execution_time,
                metadata={
                    "extractor_type": config.type,
                    "error_type": type(e).__name__
                }
            )
        finally:
            # Очистка ресурсов
            try:
                self.teardown()
            except Exception as e:
                self.logger.warning(f"Ошибка при очистке ресурсов: {e}")

    @abstractmethod
    def extract_data(self, context: ExecutionContext) -> Any:
        """
        Основной метод извлечения данных (синхронный)

        Args:
            context: Контекст выполнения pipeline

        Returns:
            Any: Извлеченные данные в любом формате
        """
        pass

    async def extract_data_async(self, context: ExecutionContext) -> Any:
        """
        Асинхронный метод извлечения данных
        По умолчанию вызывает синхронный метод
        """
        return self.extract_data(context)

    def with_retry(self, func, *args, **kwargs):
        """Выполнение функции с повторами"""
        config: ExtractorConfig = self.config
        last_exception = None

        for attempt in range(config.retry_attempts + 1):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                last_exception = e
                if attempt == config.retry_attempts:
                    break

                delay = config.retry_delay * (config.retry_exponential_base ** attempt)
                self.logger.warning(
                    f"Попытка {attempt + 1} неудачна: {e}. "
                    f"Повтор через {delay:.2f} сек"
                )
                time.sleep(delay)

        raise last_exception

    def _convert_output_format(self, data: Any, output_format: str) -> Any:
        """Конвертация данных в нужный формат"""
        if output_format == "pandas":
            return self._to_pandas(data)
        elif output_format == "polars":
            return self._to_polars(data)
        elif output_format == "dict":
            return self._to_dict(data)
        elif output_format == "list":
            return self._to_list(data)
        else:
            return data

    def _to_pandas(self, data: Any):
        """Конвертация в pandas DataFrame"""
        import pandas as pd

        if isinstance(data, pd.DataFrame):
            return data
        elif hasattr(data, 'to_pandas'):  # Polars DataFrame
            return data.to_pandas()
        elif isinstance(data, list):
            return pd.DataFrame(data)
        elif isinstance(data, dict):
            return pd.DataFrame([data])
        else:
            return pd.DataFrame({"data": [data]})

    def _to_polars(self, data: Any):
        """Конвертация в polars DataFrame"""
        import polars as pl

        if hasattr(data, '__class__') and 'polars' in str(type(data)):
            return data
        elif hasattr(data, 'to_polars'):  # Pandas DataFrame
            return pl.from_pandas(data)
        elif isinstance(data, list):
            return pl.DataFrame(data)
        elif isinstance(data, dict):
            return pl.DataFrame([data])
        else:
            return pl.DataFrame({"data": [data]})

    def _to_dict(self, data: Any) -> List[Dict]:
        """Конвертация в список словарей"""
        if isinstance(data, list) and all(isinstance(item, dict) for item in data):
            return data
        elif hasattr(data, 'to_dict'):  # DataFrame
            return data.to_dict('records')
        elif isinstance(data, dict):
            return [data]
        else:
            return [{"data": data}]

    def _to_list(self, data: Any) -> List:
        """Конвертация в список"""
        if isinstance(data, list):
            return data
        elif hasattr(data, 'values'):  # DataFrame
            return data.values.tolist()
        elif hasattr(data, 'to_list'):  # Series или аналог
            return data.to_list()
        else:
            return [data]

    def setup(self) -> None:
        """Инициализация ресурсов (переопределить при необходимости)"""
        pass

    def teardown(self) -> None:
        """Очистка ресурсов (переопределить при необходимости)"""
        pass


class StreamingExtractor(BaseExtractor):
    """
    Базовый класс для потоковых экстракторов
    Поддерживает извлечение данных по частям
    """

    @abstractmethod
    def extract_batches(self, context: ExecutionContext) -> AsyncGenerator[Any, None]:
        """
        Генератор для потокового извлечения данных

        Yields:
            Any: Batch данных
        """
        pass

    async def extract_data_async(self, context: ExecutionContext) -> Any:
        """Потоковое извлечение с объединением batch'ей"""
        config: ExtractorConfig = self.config
        all_data = []
        processed_count = 0

        async for batch in self.extract_batches(context):
            batch_data = self._convert_output_format(batch, "list")
            all_data.extend(batch_data)
            processed_count += len(batch_data)

            # Логирование прогресса
            if processed_count % (config.batch_size * 10) == 0:
                self.logger.info(f"Обработано {processed_count} записей...")

            # Проверка лимитов
            if config.max_records and processed_count >= config.max_records:
                self.logger.info(f"Достигнут лимит записей: {config.max_records}")
                break

        return all_data

    def extract_data(self, context: ExecutionContext) -> Any:
        """Синхронная обертка для потокового извлечения"""
        return asyncio.run(self.extract_data_async(context))