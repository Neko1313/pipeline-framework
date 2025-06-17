"""
Базовые классы для компонентов трансформации данных
"""
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Union, Callable
import time
import pandas as pd
import polars as pl

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


class TransformerConfig(ComponentConfig):
    """Базовая конфигурация для всех трансформеров"""
    type: str = Field(..., description="Тип трансформера")

    # Источник данных
    source_stage: str = Field(..., description="Имя стадии-источника данных")

    # Параметры обработки
    batch_size: int = Field(default=10000, description="Размер batch для обработки")
    parallel: bool = Field(default=False, description="Включить параллельную обработку")
    n_jobs: int = Field(default=1, description="Количество процессов для параллельной обработки")

    # Валидация данных
    validate_input: bool = Field(default=True, description="Валидировать входные данные")
    validate_output: bool = Field(default=True, description="Валидировать выходные данные")

    # Обработка ошибок
    error_handling: str = Field(default="raise", description="Стратегия обработки ошибок")
    skip_errors: bool = Field(default=False, description="Пропускать записи с ошибками")

    # Выходной формат
    output_format: str = Field(default="pandas", description="Формат выходных данных")

    @field_validator('source_stage')
    @classmethod
    def validate_source_stage(cls, v: str) -> str:
        if not v or not isinstance(v, str):
            raise ValueError("source_stage должен быть непустой строкой")
        return v.strip()

    @field_validator('error_handling')
    @classmethod
    def validate_error_handling(cls, v: str) -> str:
        allowed_strategies = ["raise", "skip", "default", "log"]
        if v not in allowed_strategies:
            raise ValueError(f"error_handling должен быть одним из: {allowed_strategies}")
        return v

    @field_validator('output_format')
    @classmethod
    def validate_output_format(cls, v: str) -> str:
        allowed_formats = ["pandas", "polars", "dict", "list"]
        if v not in allowed_formats:
            raise ValueError(f"output_format должен быть одним из: {allowed_formats}")
        return v


class BaseTransformer(BaseComponent, ABC):
    """
    Базовый класс для всех компонентов трансформации данных

    Предоставляет общую функциональность:
    - Получение данных от источника
    - Batch обработку
    - Параллельную обработку
    - Обработку ошибок
    - Валидацию данных
    - Конвертацию форматов
    """

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.logger = get_pipeline_logger(
            f"transformer.{self.config.type}",
            component_type=self.config.type
        )

    def get_component_type(self) -> ComponentType:
        return ComponentType.TRANSFORMER

    def get_config_model(self):
        return TransformerConfig

    def validate_dependencies(self, available_stages: List[str]) -> List[str]:
        """Валидация зависимостей"""
        config: TransformerConfig = self.config
        missing = []

        if config.source_stage not in available_stages:
            missing.append(config.source_stage)

        return missing

    def execute(self, context: ExecutionContext) -> ExecutionResult:
        """Выполнение трансформации данных"""
        start_time = time.time()
        config: TransformerConfig = self.config

        try:
            self.logger.info(f"Начало трансформации с помощью {config.type}")

            # Получаем данные от источника
            source_data = context.get_stage_data(config.source_stage)
            if source_data is None:
                raise ValueError(f"Данные от стадии '{config.source_stage}' не найдены")

            # Валидируем входные данные
            if config.validate_input:
                self._validate_input_data(source_data)

            # Выполняем setup
            self.setup()

            # Основная трансформация
            if config.parallel and config.n_jobs > 1:
                transformed_data = self._transform_parallel(source_data, context)
            elif config.batch_size and self._should_use_batching(source_data):
                transformed_data = self._transform_batched(source_data, context)
            else:
                transformed_data = self.transform_data(source_data, context)

            # Валидируем выходные данные
            if config.validate_output:
                self._validate_output_data(transformed_data)

            # Конвертируем в нужный формат
            formatted_data = self._convert_output_format(transformed_data, config.output_format)

            execution_time = time.time() - start_time
            processed_records = self._count_records(formatted_data)

            self.logger.info(
                f"Трансформация завершена успешно. "
                f"Обработано {processed_records} записей за {execution_time:.2f} сек"
            )

            return ExecutionResult(
                status=ExecutionStatus.SUCCESS,
                data=formatted_data,
                execution_time=execution_time,
                processed_records=processed_records,
                metadata={
                    "transformer_type": config.type,
                    "source_stage": config.source_stage,
                    "output_format": config.output_format,
                    "batch_size": config.batch_size,
                    "parallel": config.parallel
                }
            )

        except Exception as e:
            execution_time = time.time() - start_time
            self.logger.error(f"Ошибка трансформации: {e}", exc_info=True)

            return ExecutionResult(
                status=ExecutionStatus.FAILED,
                error_message=str(e),
                execution_time=execution_time,
                metadata={
                    "transformer_type": config.type,
                    "source_stage": config.source_stage,
                    "error_type": type(e).__name__
                }
            )
        finally:
            try:
                self.teardown()
            except Exception as e:
                self.logger.warning(f"Ошибка при очистке ресурсов: {e}")

    @abstractmethod
    def transform_data(self, data: Any, context: ExecutionContext) -> Any:
        """
        Основной метод трансформации данных

        Args:
            data: Входные данные
            context: Контекст выполнения

        Returns:
            Any: Трансформированные данные
        """
        pass

    def _transform_batched(self, data: Any, context: ExecutionContext) -> Any:
        """Batch трансформация данных"""
        config: TransformerConfig = self.config

        # Конвертируем в DataFrame для удобства работы с batch'ами
        if isinstance(data, (list, dict)):
            df = pd.DataFrame(data)
        elif hasattr(data, 'to_pandas'):
            df = data.to_pandas()
        else:
            df = data

        if not hasattr(df, 'iloc'):
            # Если не DataFrame, используем обычную трансформацию
            return self.transform_data(data, context)

        # Обрабатываем по batch'ам
        results = []
        total_rows = len(df)

        for start_idx in range(0, total_rows, config.batch_size):
            end_idx = min(start_idx + config.batch_size, total_rows)
            batch = df.iloc[start_idx:end_idx]

            try:
                batch_result = self.transform_data(batch, context)
                results.append(batch_result)

                # Логирование прогресса
                if start_idx % (config.batch_size * 10) == 0:
                    progress = (end_idx / total_rows) * 100
                    self.logger.info(f"Обработано {end_idx}/{total_rows} записей ({progress:.1f}%)")

            except Exception as e:
                if config.error_handling == "raise":
                    raise
                elif config.error_handling == "skip":
                    self.logger.warning(f"Пропуск batch {start_idx}-{end_idx} из-за ошибки: {e}")
                    continue
                elif config.error_handling == "log":
                    self.logger.error(f"Ошибка в batch {start_idx}-{end_idx}: {e}")
                    results.append(batch)  # Используем исходные данные
                else:
                    results.append(batch)  # default стратегия

        # Объединяем результаты
        if results:
            if isinstance(results[0], pd.DataFrame):
                return pd.concat(results, ignore_index=True)
            elif hasattr(results[0], '__class__') and 'polars' in str(type(results[0])):
                import polars as pl
                return pl.concat(results)
            elif isinstance(results[0], list):
                combined = []
                for result in results:
                    combined.extend(result)
                return combined
            else:
                return results
        else:
            return data  # Возвращаем исходные данные если ничего не обработано

    def _transform_parallel(self, data: Any, context: ExecutionContext) -> Any:
        """Параллельная трансформация данных"""
        import multiprocessing as mp
        from concurrent.futures import ProcessPoolExecutor

        config: TransformerConfig = self.config

        # Конвертируем в DataFrame для разделения
        if isinstance(data, (list, dict)):
            df = pd.DataFrame(data)
        elif hasattr(data, 'to_pandas'):
            df = data.to_pandas()
        else:
            df = data

        if not hasattr(df, 'iloc'):
            # Если не DataFrame, используем обычную трансформацию
            return self.transform_data(data, context)

        # Разделяем данные на части для параллельной обработки
        total_rows = len(df)
        chunk_size = max(1, total_rows // config.n_jobs)
        chunks = []

        for i in range(0, total_rows, chunk_size):
            chunk = df.iloc[i:i + chunk_size]
            chunks.append(chunk)

        # Параллельная обработка
        try:
            with ProcessPoolExecutor(max_workers=config.n_jobs) as executor:
                # Отправляем задачи
                futures = []
                for chunk in chunks:
                    future = executor.submit(self._transform_chunk_wrapper, chunk, context)
                    futures.append(future)

                # Собираем результаты
                results = []
                for future in futures:
                    try:
                        result = future.result()
                        results.append(result)
                    except Exception as e:
                        if config.error_handling == "raise":
                            raise
                        elif config.error_handling != "skip":
                            self.logger.error(f"Ошибка в параллельной обработке: {e}")

                # Объединяем результаты
                if results:
                    if isinstance(results[0], pd.DataFrame):
                        return pd.concat(results, ignore_index=True)
                    elif hasattr(results[0], '__class__') and 'polars' in str(type(results[0])):
                        import polars as pl
                        return pl.concat(results)
                    else:
                        combined = []
                        for result in results:
                            if isinstance(result, list):
                                combined.extend(result)
                            else:
                                combined.append(result)
                        return combined
                else:
                    return data

        except Exception as e:
            self.logger.warning(f"Ошибка параллельной обработки, переключение на последовательную: {e}")
            return self.transform_data(data, context)

    def _transform_chunk_wrapper(self, chunk: Any, context: ExecutionContext) -> Any:
        """Обертка для трансформации chunk'а в отдельном процессе"""
        try:
            return self.transform_data(chunk, context)
        except Exception as e:
            config: TransformerConfig = self.config
            if config.error_handling == "raise":
                raise
            else:
                return chunk  # Возвращаем исходные данные при ошибке

    def _should_use_batching(self, data: Any) -> bool:
        """Определяет нужно ли использовать batch обработку"""
        config: TransformerConfig = self.config

        # Используем batching если данных много
        if hasattr(data, '__len__'):
            return len(data) > config.batch_size

        # Для DataFrame проверяем количество строк
        if hasattr(data, 'shape'):
            return data.shape[0] > config.batch_size

        return False

    def _validate_input_data(self, data: Any) -> None:
        """Валидация входных данных (переопределить при необходимости)"""
        if data is None:
            raise ValueError("Входные данные не могут быть None")

    def _validate_output_data(self, data: Any) -> None:
        """Валидация выходных данных (переопределить при необходимости)"""
        if data is None:
            raise ValueError("Выходные данные не могут быть None")

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

    def _count_records(self, data: Any) -> int:
        """Подсчет количества записей"""
        if hasattr(data, '__len__') and not isinstance(data, str):
            return len(data)
        elif hasattr(data, 'shape'):
            return data.shape[0]
        else:
            return 1

    def setup(self) -> None:
        """Инициализация ресурсов (переопределить при необходимости)"""
        pass

    def teardown(self) -> None:
        """Очистка ресурсов (переопределить при необходимости)"""
        pass


class DataTransformer(BaseTransformer):
    """
    Базовый класс для трансформеров работающих с табличными данными
    Предоставляет удобные методы для работы с DataFrame
    """

    def apply_column_operation(self, data: pd.DataFrame, column: str,
                             operation: Callable, **kwargs) -> pd.DataFrame:
        """Применение операции к колонке"""
        result = data.copy()
        try:
            result[column] = operation(result[column], **kwargs)
        except Exception as e:
            config: TransformerConfig = self.config
            if config.error_handling == "raise":
                raise
            elif config.error_handling == "log":
                self.logger.error(f"Ошибка в операции над колонкой {column}: {e}")

        return result

    def filter_rows(self, data: pd.DataFrame, condition: Union[str, Callable]) -> pd.DataFrame:
        """Фильтрация строк по условию"""
        try:
            if isinstance(condition, str):
                # Строковое условие (например: "age > 18")
                return data.query(condition)
            elif callable(condition):
                # Функция-фильтр
                return data[condition(data)]
            else:
                return data
        except Exception as e:
            config: TransformerConfig = self.config
            if config.error_handling == "raise":
                raise
            else:
                self.logger.error(f"Ошибка фильтрации: {e}")
                return data

    def add_computed_column(self, data: pd.DataFrame, column_name: str,
                          computation: Union[str, Callable], **kwargs) -> pd.DataFrame:
        """Добавление вычисляемой колонки"""
        result = data.copy()
        try:
            if isinstance(computation, str):
                # Вычисление через eval
                result[column_name] = result.eval(computation)
            elif callable(computation):
                # Функция вычисления
                result[column_name] = computation(result, **kwargs)

        except Exception as e:
            config: TransformerConfig = self.config
            if config.error_handling == "raise":
                raise
            elif config.error_handling == "log":
                self.logger.error(f"Ошибка добавления колонки {column_name}: {e}")

        return result

    def aggregate_data(self, data: pd.DataFrame, group_by: List[str],
                      aggregations: Dict[str, Union[str, List[str]]]) -> pd.DataFrame:
        """Агрегация данных"""
        try:
            return data.groupby(group_by).agg(aggregations).reset_index()
        except Exception as e:
            config: TransformerConfig = self.config
            if config.error_handling == "raise":
                raise
            else:
                self.logger.error(f"Ошибка агрегации: {e}")
                return data
