"""
Тесты для CSV экстрактора
"""
import pytest
import tempfile
import pandas as pd
from pathlib import Path

from pipeline_core import ExecutionContext, ExecutionStatus
from pipeline_extractors import CSVExtractor, CSVExtractorConfig


class TestCSVExtractor:
    """Тесты CSV экстрактора"""

    @pytest.fixture
    def sample_csv_file(self):
        """Создание тестового CSV файла"""
        # Создаем временный файл
        temp_file = tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False)

        # Записываем тестовые данные
        data = """name,age,salary,department
Alice,25,50000,IT
Bob,30,60000,HR
Charlie,35,70000,Finance
Diana,28,55000,IT
Eve,32,65000,Marketing
"""
        temp_file.write(data)
        temp_file.close()

        yield temp_file.name

        # Удаляем файл после теста
        Path(temp_file.name).unlink()

    def test_csv_extractor_config_validation(self):
        """Тест валидации конфигурации"""
        # Валидная конфигурация
        config = CSVExtractorConfig(
            type="csv-extractor",
            file_path="/path/to/file.csv",
            delimiter=",",
            has_header=True
        )
        assert config.type == "csv-extractor"
        assert config.delimiter == ","

        # Невалидная конфигурация - пустой file_path
        with pytest.raises(ValueError):
            CSVExtractorConfig(
                type="csv-extractor",
                file_path="",
                delimiter=","
            )

    def test_csv_extractor_basic(self, sample_csv_file):
        """Базовый тест извлечения CSV"""
        config = {
            "type": "csv-extractor",
            "file_path": sample_csv_file,
            "delimiter": ",",
            "has_header": True,
            "output_format": "pandas"
        }

        extractor = CSVExtractor(config)
        context = ExecutionContext()

        result = extractor.execute(context)

        assert result.status == ExecutionStatus.SUCCESS
        assert result.data is not None
        assert result.processed_records == 5
        assert isinstance(result.data, pd.DataFrame)
        assert list(result.data.columns) == ["name", "age", "salary", "department"]
        assert result.data.iloc[0]["name"] == "Alice"

    def test_csv_extractor_column_filtering(self, sample_csv_file):
        """Тест фильтрации колонок"""
        config = {
            "type": "csv-extractor",
            "file_path": sample_csv_file,
            "columns": ["name", "salary"],
            "output_format": "pandas"
        }

        extractor = CSVExtractor(config)
        context = ExecutionContext()

        result = extractor.execute(context)

        assert result.status == ExecutionStatus.SUCCESS
        assert list(result.data.columns) == ["name", "salary"]
        assert len(result.data) == 5

    def test_csv_extractor_exclude_columns(self, sample_csv_file):
        """Тест исключения колонок"""
        config = {
            "type": "csv-extractor",
            "file_path": sample_csv_file,
            "exclude_columns": ["age", "department"],
            "output_format": "pandas"
        }

        extractor = CSVExtractor(config)
        context = ExecutionContext()

        result = extractor.execute(context)

        assert result.status == ExecutionStatus.SUCCESS
        assert list(result.data.columns) == ["name", "salary"]

    def test_csv_extractor_max_records(self, sample_csv_file):
        """Тест ограничения количества записей"""
        config = {
            "type": "csv-extractor",
            "file_path": sample_csv_file,
            "max_records": 3,
            "output_format": "pandas"
        }

        extractor = CSVExtractor(config)
        context = ExecutionContext()

        result = extractor.execute(context)

        assert result.status == ExecutionStatus.SUCCESS
        assert len(result.data) == 3
        assert result.processed_records == 3

    def test_csv_extractor_output_formats(self, sample_csv_file):
        """Тест различных форматов вывода"""
        base_config = {
            "type": "csv-extractor",
            "file_path": sample_csv_file,
        }

        context = ExecutionContext()

        # Pandas формат
        config_pandas = {**base_config, "output_format": "pandas"}
        extractor = CSVExtractor(config_pandas)
        result = extractor.execute(context)
        assert isinstance(result.data, pd.DataFrame)

        # Dict формат
        config_dict = {**base_config, "output_format": "dict"}
        extractor = CSVExtractor(config_dict)
        result = extractor.execute(context)
        assert isinstance(result.data, list)
        assert isinstance(result.data[0], dict)
        assert "name" in result.data[0]

    def test_csv_extractor_nonexistent_file(self):
        """Тест обработки несуществующего файла"""
        config = {
            "type": "csv-extractor",
            "file_path": "/nonexistent/file.csv",
        }

        # Должна быть ошибка валидации
        with pytest.raises(ValueError):
            CSVExtractor(config)

    @pytest.mark.asyncio
    async def test_csv_extractor_async(self, sample_csv_file):
        """Тест асинхронного извлечения"""
        config = {
            "type": "csv-extractor",
            "file_path": sample_csv_file,
            "output_format": "pandas"
        }

        extractor = CSVExtractor(config)
        context = ExecutionContext()

        # Тестируем асинхронный метод
        data = await extractor.extract_data_async(context)
        assert isinstance(data, pd.DataFrame)
        assert len(data) == 5
