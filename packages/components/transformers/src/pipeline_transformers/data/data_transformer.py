"""
Data Transformer - базовые операции трансформации данных
"""
from typing import Any, Dict, List, Optional, Union, Callable
import pandas as pd
import numpy as np

from pipeline_core import ExecutionContext
from ..base import DataTransformer, TransformerConfig
from pydantic import Field, field_validator


class DataTransformerConfig(TransformerConfig):
    """Конфигурация базового трансформера данных"""
    type: str = Field(default="data-transformer", const=True)

    # Операции с колонками
    add_columns: Optional[Dict[str, str]] = Field(None, description="Добавить колонки: {name: expression}")
    drop_columns: Optional[List[str]] = Field(None, description="Удалить колонки")
    rename_columns: Optional[Dict[str, str]] = Field(None, description="Переименовать колонки")

    # Фильтрация данных
    filter_expression: Optional[str] = Field(None, description="Pandas query выражение для фильтрации")

    # Обработка пропущенных значений
    fill_na_strategy: str = Field(default="keep", description="Стратегия обработки NaN")
    fill_na_value: Optional[Union[str, int, float]] = Field(None, description="Значение для заполнения NaN")
    drop_na: bool = Field(default=False, description="Удалить строки с NaN")

    # Типы данных
    convert_dtypes: Optional[Dict[str, str]] = Field(None, description="Преобразование типов данных")

    # Сортировка
    sort_by: Optional[List[str]] = Field(None, description="Колонки для сортировки")
    sort_ascending: Union[bool, List[bool]] = Field(default=True, description="Порядок сортировки")

    # Дедупликация
    drop_duplicates: bool = Field(default=False, description="Удалить дубликаты")
    duplicate_subset: Optional[List[str]] = Field(None, description="Колонки для определения дубликатов")

    # Сэмплирование
    sample_n: Optional[int] = Field(None, description="Взять N случайных записей")
    sample_frac: Optional[float] = Field(None, description="Взять долю записей")
    random_state: int = Field(default=42, description="Seed для воспроизводимости")

    @field_validator('fill_na_strategy')
    @classmethod
    def validate_fill_na_strategy(cls, v: str) -> str:
        allowed_strategies = ["keep", "drop", "mean", "median", "mode", "forward", "backward", "value"]
        if v not in allowed_strategies:
            raise ValueError(f"fill_na_strategy должен быть одним из: {allowed_strategies}")
        return v

    @field_validator('sample_frac')
    @classmethod
    def validate_sample_frac(cls, v: Optional[float]) -> Optional[float]:
        if v is not None and (v <= 0 or v > 1):
            raise ValueError("sample_frac должен быть между 0 и 1")
        return v


class DataTransformerComponent(DataTransformer):
    """
    Универсальный трансформер для базовых операций с данными

    Поддерживает:
    - Добавление/удаление/переименование колонок
    - Фильтрацию данных
    - Обработку пропущенных значений
    - Преобразование типов данных
    - Сортировку и дедупликацию
    - Сэмплирование данных
    """

    def get_config_model(self):
        return DataTransformerConfig

    def transform_data(self, data: Any, context: ExecutionContext) -> Any:
        """Основная трансформация данных"""
        config: DataTransformerConfig = self.config

        # Конвертируем в pandas DataFrame для удобства работы
        if isinstance(data, list):
            df = pd.DataFrame(data)
        elif hasattr(data, 'to_pandas'):
            df = data.to_pandas()
        elif isinstance(data, dict):
            df = pd.DataFrame([data])
        else:
            df = data.copy() if hasattr(data, 'copy') else data

        self.logger.info(f"Начальные данные: {df.shape[0]} строк, {df.shape[1]} колонок")

        # Применяем трансформации в определенном порядке
        df = self._handle_missing_values(df, config)
        df = self._convert_data_types(df, config)
        df = self._add_columns(df, config)
        df = self._drop_columns(df, config)
        df = self._rename_columns(df, config)
        df = self._filter_data(df, config)
        df = self._sort_data(df, config)
        df = self._deduplicate_data(df, config)
        df = self._sample_data(df, config)

        self.logger.info(f"Результат: {df.shape[0]} строк, {df.shape[1]} колонок")

        return df

    def _handle_missing_values(self, df: pd.DataFrame, config: DataTransformerConfig) -> pd.DataFrame:
        """Обработка пропущенных значений"""
        if config.fill_na_strategy == "keep":
            return df

        if config.drop_na:
            initial_rows = len(df)
            df = df.dropna()
            dropped_rows = initial_rows - len(df)
            if dropped_rows > 0:
                self.logger.info(f"Удалено {dropped_rows} строк с пропущенными значениями")
            return df

        if config.fill_na_strategy == "drop":
            return df.dropna()
        elif config.fill_na_strategy == "mean":
            numeric_columns = df.select_dtypes(include=[np.number]).columns
            df[numeric_columns] = df[numeric_columns].fillna(df[numeric_columns].mean())
        elif config.fill_na_strategy == "median":
            numeric_columns = df.select_dtypes(include=[np.number]).columns
            df[numeric_columns] = df[numeric_columns].fillna(df[numeric_columns].median())
        elif config.fill_na_strategy == "mode":
            for column in df.columns:
                mode_value = df[column].mode()
                if not mode_value.empty:
                    df[column] = df[column].fillna(mode_value.iloc[0])
        elif config.fill_na_strategy == "forward":
            df = df.fillna(method='ffill')
        elif config.fill_na_strategy == "backward":
            df = df.fillna(method='bfill')
        elif config.fill_na_strategy == "value" and config.fill_na_value is not None:
            df = df.fillna(config.fill_na_value)

        return df

    def _convert_data_types(self, df: pd.DataFrame, config: DataTransformerConfig) -> pd.DataFrame:
        """Преобразование типов данных"""
        if not config.convert_dtypes:
            return df

        for column, dtype in config.convert_dtypes.items():
            if column in df.columns:
                try:
                    if dtype.lower() == "datetime":
                        df[column] = pd.to_datetime(df[column])
                    elif dtype.lower() == "category":
                        df[column] = df[column].astype('category')
                    else:
                        df[column] = df[column].astype(dtype)

                    self.logger.debug(f"Конвертирована колонка '{column}' в тип '{dtype}'")
                except Exception as e:
                    self.logger.warning(f"Не удалось конвертировать колонку '{column}' в тип '{dtype}': {e}")

        return df

    def _add_columns(self, df: pd.DataFrame, config: DataTransformerConfig) -> pd.DataFrame:
        """Добавление вычисляемых колонок"""
        if not config.add_columns:
            return df

        for column_name, expression in config.add_columns.items():
            try:
                # Используем pandas eval для вычисления выражений
                df[column_name] = df.eval(expression)
                self.logger.debug(f"Добавлена колонка '{column_name}' с выражением '{expression}'")
            except Exception as e:
                self.logger.warning(f"Не удалось добавить колонку '{column_name}': {e}")

        return df

    def _drop_columns(self, df: pd.DataFrame, config: DataTransformerConfig) -> pd.DataFrame:
        """Удаление колонок"""
        if not config.drop_columns:
            return df

        columns_to_drop = [col for col in config.drop_columns if col in df.columns]
        if columns_to_drop:
            df = df.drop(columns=columns_to_drop)
            self.logger.debug(f"Удалены колонки: {columns_to_drop}")

        missing_columns = [col for col in config.drop_columns if col not in df.columns]
        if missing_columns:
            self.logger.warning(f"Колонки для удаления не найдены: {missing_columns}")

        return df

    def _rename_columns(self, df: pd.DataFrame, config: DataTransformerConfig) -> pd.DataFrame:
        """Переименование колонок"""
        if not config.rename_columns:
            return df

        # Фильтруем только существующие колонки
        rename_dict = {old: new for old, new in config.rename_columns.items() if old in df.columns}

        if rename_dict:
            df = df.rename(columns=rename_dict)
            self.logger.debug(f"Переименованы колонки: {rename_dict}")

        missing_columns = [col for col in config.rename_columns.keys() if col not in df.columns]
        if missing_columns:
            self.logger.warning(f"Колонки для переименования не найдены: {missing_columns}")

        return df

    def _filter_data(self, df: pd.DataFrame, config: DataTransformerConfig) -> pd.DataFrame:
        """Фильтрация данных"""
        if not config.filter_expression:
            return df

        try:
            initial_rows = len(df)
            df = df.query(config.filter_expression)
            filtered_rows = initial_rows - len(df)

            if filtered_rows > 0:
                self.logger.info(f"Отфильтровано {filtered_rows} строк по условию: {config.filter_expression}")
        except Exception as e:
            self.logger.warning(f"Не удалось применить фильтр '{config.filter_expression}': {e}")

        return df

    def _sort_data(self, df: pd.DataFrame, config: DataTransformerConfig) -> pd.DataFrame:
        """Сортировка данных"""
        if not config.sort_by:
            return df

        # Фильтруем только существующие колонки
        sort_columns = [col for col in config.sort_by if col in df.columns]

        if sort_columns:
            try:
                df = df.sort_values(by=sort_columns, ascending=config.sort_ascending)
                self.logger.debug(f"Данные отсортированы по колонкам: {sort_columns}")
            except Exception as e:
                self.logger.warning(f"Не удалось отсортировать данные: {e}")

        missing_columns = [col for col in config.sort_by if col not in df.columns]
        if missing_columns:
            self.logger.warning(f"Колонки для сортировки не найдены: {missing_columns}")

        return df

    def _deduplicate_data(self, df: pd.DataFrame, config: DataTransformerConfig) -> pd.DataFrame:
        """Удаление дубликатов"""
        if not config.drop_duplicates:
            return df

        initial_rows = len(df)

        # Определяем колонки для проверки дубликатов
        subset = None
        if config.duplicate_subset:
            subset = [col for col in config.duplicate_subset if col in df.columns]
            if not subset:
                self.logger.warning("Ни одна из колонок для определения дубликатов не найдена")
                return df

        df = df.drop_duplicates(subset=subset)
        duplicates_removed = initial_rows - len(df)

        if duplicates_removed > 0:
            self.logger.info(f"Удалено {duplicates_removed} дубликатов")

        return df

    def _sample_data(self, df: pd.DataFrame, config: DataTransformerConfig) -> pd.DataFrame:
        """Сэмплирование данных"""
        if config.sample_n is None and config.sample_frac is None:
            return df

        try:
            if config.sample_n is not None:
                sample_size = min(config.sample_n, len(df))
                df = df.sample(n=sample_size, random_state=config.random_state)
                self.logger.info(f"Взято {sample_size} случайных записей")
            elif config.sample_frac is not None:
                df = df.sample(frac=config.sample_frac, random_state=config.random_state)
                self.logger.info(f"Взято {config.sample_frac*100:.1f}% записей ({len(df)} строк)")
        except Exception as e:
            self.logger.warning(f"Не удалось выполнить сэмплирование: {e}")

        return df
