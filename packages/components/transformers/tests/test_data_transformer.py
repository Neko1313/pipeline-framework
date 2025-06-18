"""
Тесты для Data трансформера
"""

import pytest
import pandas as pd
import numpy as np

from pipeline_core import ExecutionContext, ExecutionResult, ExecutionStatus
from pipeline_transformers import DataTransformerComponent, DataTransformerConfig


class TestDataTransformer:
    """Тесты Data трансформера"""

    @pytest.fixture
    def sample_data(self):
        """Создание тестовых данных"""
        return pd.DataFrame(
            {
                "name": ["Alice", "Bob", "Charlie", "Diana", "Eve"],
                "age": [25, 30, 35, 28, 32],
                "salary": [50000, 60000, 70000, 55000, 65000],
                "department": ["IT", "HR", "Finance", "IT", "Marketing"],
                "active": [True, True, False, True, True],
                "score": [85.5, 90.0, None, 88.0, 92.5],
            }
        )

    @pytest.fixture
    def execution_context(self, sample_data):
        """Создание контекста выполнения с данными"""
        context = ExecutionContext()
        result = ExecutionResult(
            status=ExecutionStatus.SUCCESS, data=sample_data, processed_records=len(sample_data)
        )
        context.set_stage_result("extract", result)
        return context

    def test_data_transformer_config_validation(self):
        """Тест валидации конфигурации"""
        # Валидная конфигурация
        config = DataTransformerConfig(type="data-transformer", source_stage="extract")
        assert config.type == "data-transformer"
        assert config.source_stage == "extract"

        # Невалидная конфигурация - пустой source_stage
        with pytest.raises(ValueError):
            DataTransformerConfig(type="data-transformer", source_stage="")

    def test_add_columns(self, execution_context):
        """Тест добавления колонок"""
        config = {
            "type": "data-transformer",
            "source_stage": "extract",
            "add_columns": {
                "annual_salary": "salary * 12",
                "age_group": "age > 30",
                "name_length": "name.str.len()",
            },
        }

        transformer = DataTransformerComponent(config)
        result = transformer.execute(execution_context)

        assert result.status == ExecutionStatus.SUCCESS
        assert "annual_salary" in result.data.columns
        assert "age_group" in result.data.columns
        assert "name_length" in result.data.columns

        # Проверяем значения
        assert result.data.iloc[0]["annual_salary"] == 50000 * 12
        assert result.data.iloc[1]["age_group"] == False  # Bob, 30 лет
        assert result.data.iloc[2]["age_group"] == True  # Charlie, 35 лет

    def test_drop_columns(self, execution_context):
        """Тест удаления колонок"""
        config = {
            "type": "data-transformer",
            "source_stage": "extract",
            "drop_columns": ["score", "active"],
        }

        transformer = DataTransformerComponent(config)
        result = transformer.execute(execution_context)

        assert result.status == ExecutionStatus.SUCCESS
        assert "score" not in result.data.columns
        assert "active" not in result.data.columns
        assert "name" in result.data.columns  # Остальные колонки должны остаться

    def test_rename_columns(self, execution_context):
        """Тест переименования колонок"""
        config = {
            "type": "data-transformer",
            "source_stage": "extract",
            "rename_columns": {"name": "employee_name", "department": "dept"},
        }

        transformer = DataTransformerComponent(config)
        result = transformer.execute(execution_context)

        assert result.status == ExecutionStatus.SUCCESS
        assert "employee_name" in result.data.columns
        assert "dept" in result.data.columns
        assert "name" not in result.data.columns
        assert "department" not in result.data.columns

    def test_filter_data(self, execution_context):
        """Тест фильтрации данных"""
        config = {
            "type": "data-transformer",
            "source_stage": "extract",
            "filter_expression": "active == True and salary > 55000",
        }

        transformer = DataTransformerComponent(config)
        result = transformer.execute(execution_context)

        assert result.status == ExecutionStatus.SUCCESS
        assert len(result.data) == 3  # Alice, Bob, Eve
        assert all(result.data["active"])
        assert all(result.data["salary"] > 55000)

    def test_fill_na_mean(self, execution_context):
        """Тест заполнения пропущенных значений средним"""
        config = {"type": "data-transformer", "source_stage": "extract", "fill_na_strategy": "mean"}

        transformer = DataTransformerComponent(config)
        result = transformer.execute(execution_context)

        assert result.status == ExecutionStatus.SUCCESS
        assert not result.data["score"].isna().any()

        # Проверяем что NaN заменен на среднее значение
        expected_mean = pd.Series([85.5, 90.0, 88.0, 92.5]).mean()
        assert abs(result.data["score"].iloc[2] - expected_mean) < 0.01

    def test_fill_na_value(self, execution_context):
        """Тест заполнения пропущенных значений конкретным значением"""
        config = {
            "type": "data-transformer",
            "source_stage": "extract",
            "fill_na_strategy": "value",
            "fill_na_value": 0,
        }

        transformer = DataTransformerComponent(config)
        result = transformer.execute(execution_context)

        assert result.status == ExecutionStatus.SUCCESS
        assert not result.data["score"].isna().any()
        assert result.data["score"].iloc[2] == 0

    def test_sort_data(self, execution_context):
        """Тест сортировки данных"""
        config = {
            "type": "data-transformer",
            "source_stage": "extract",
            "sort_by": ["salary"],
            "sort_ascending": False,
        }

        transformer = DataTransformerComponent(config)
        result = transformer.execute(execution_context)

        assert result.status == ExecutionStatus.SUCCESS

        # Проверяем что данные отсортированы по убыванию зарплаты
        salaries = result.data["salary"].tolist()
        assert salaries == sorted(salaries, reverse=True)
        assert result.data.iloc[0]["name"] == "Charlie"  # Самая высокая зарплата

    def test_drop_duplicates(self, execution_context):
        """Тест удаления дубликатов"""
        # Добавляем дубликат
        data_with_dup = execution_context.get_stage_data("extract").copy()
        data_with_dup = pd.concat([data_with_dup, data_with_dup.iloc[[0]]], ignore_index=True)

        # Обновляем контекст
        result = ExecutionResult(
            status=ExecutionStatus.SUCCESS, data=data_with_dup, processed_records=len(data_with_dup)
        )
        execution_context.set_stage_result("extract", result)

        config = {"type": "data-transformer", "source_stage": "extract", "drop_duplicates": True}

        transformer = DataTransformerComponent(config)
        result = transformer.execute(execution_context)

        assert result.status == ExecutionStatus.SUCCESS
        assert len(result.data) == 5  # Дубликат должен быть удален

    def test_sample_data(self, execution_context):
        """Тест сэмплирования данных"""
        config = {
            "type": "data-transformer",
            "source_stage": "extract",
            "sample_n": 3,
            "random_state": 42,
        }

        transformer = DataTransformerComponent(config)
        result = transformer.execute(execution_context)

        assert result.status == ExecutionStatus.SUCCESS
        assert len(result.data) == 3
        assert result.processed_records == 3

    def test_convert_dtypes(self, execution_context):
        """Тест преобразования типов данных"""
        config = {
            "type": "data-transformer",
            "source_stage": "extract",
            "convert_dtypes": {"age": "float64", "department": "category"},
        }

        transformer = DataTransformerComponent(config)
        result = transformer.execute(execution_context)

        assert result.status == ExecutionStatus.SUCCESS
        assert result.data["age"].dtype == "float64"
        assert result.data["department"].dtype.name == "category"

    def test_complex_transformation(self, execution_context):
        """Тест комплексной трансформации"""
        config = {
            "type": "data-transformer",
            "source_stage": "extract",
            "add_columns": {
                "salary_grade": "case when salary > 60000 then 'High' else 'Normal' end"
            },
            "filter_expression": "active == True",
            "drop_columns": ["score"],
            "rename_columns": {"name": "employee_name"},
            "sort_by": ["salary"],
            "sort_ascending": False,
            "fill_na_strategy": "drop",
        }

        transformer = DataTransformerComponent(config)
        result = transformer.execute(execution_context)

        assert result.status == ExecutionStatus.SUCCESS
        assert "salary_grade" in result.data.columns
        assert "employee_name" in result.data.columns
        assert "name" not in result.data.columns
        assert "score" not in result.data.columns
        assert all(result.data["active"])

        # Проверяем что данные отсортированы
        salaries = result.data["salary"].tolist()
        assert salaries == sorted(salaries, reverse=True)

    def test_error_handling(self, execution_context):
        """Тест обработки ошибок"""
        config = {"type": "data-transformer", "source_stage": "nonexistent_stage"}

        transformer = DataTransformerComponent(config)
        result = transformer.execute(execution_context)

        assert result.status == ExecutionStatus.FAILED
        assert "не найдены" in result.error_message
