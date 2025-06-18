# packages/components/extractors/tests/test_csv_extractor.py
"""
Comprehensive тесты для полноценного CSV экстрактора
"""

import pytest
import pandas as pd
import numpy as np
from pathlib import Path
from unittest.mock import patch
import tempfile
import os

from pipeline_core import ExecutionContext, ExecutionStatus
from pipeline_extractors.file.csv_extractor import CSVExtractor, CSVExtractorConfig


class TestCSVExtractorConfig:
    """Тесты конфигурации CSV экстрактора"""

    def test_valid_basic_config(self, sample_csv_file):
        """Тест базовой валидной конфигурации"""
        config = CSVExtractorConfig(file_path=sample_csv_file, delimiter=",", encoding="utf-8")

        assert config.file_path == Path(sample_csv_file)
        assert config.delimiter == ","
        assert config.output_format == "pandas"

    def test_file_not_exists(self):
        """Тест ошибки для несуществующего файла"""
        with pytest.raises(ValueError, match="Файл не найден"):
            CSVExtractorConfig(file_path="/nonexistent/file.csv")

    def test_invalid_file_extension(self, tmp_path):
        """Тест ошибки для неподдерживаемого формата"""
        invalid_file = tmp_path / "test.json"
        invalid_file.write_text('{"test": "data"}')

        with pytest.raises(ValueError, match="Неподдерживаемый формат файла"):
            CSVExtractorConfig(file_path=invalid_file)

    def test_column_types_validation(self, sample_csv_file):
        """Тест валидации типов колонок"""
        # Валидные типы
        config = CSVExtractorConfig(
            file_path=sample_csv_file,
            column_types={"age": "int", "name": "string", "active": "bool"},
        )
        assert config.column_types["age"] == "int"

        # Невалидный тип
        with pytest.raises(ValueError, match="Неподдерживаемый тип"):
            CSVExtractorConfig(file_path=sample_csv_file, column_types={"age": "invalid_type"})

    def test_record_limits_validation(self, sample_csv_file):
        """Тест валидации лимитов записей"""
        with pytest.raises(ValueError, match="min_records не может быть больше max_records"):
            CSVExtractorConfig(file_path=sample_csv_file, min_records=100, max_records=50)


class TestCSVExtractorBasic:
    """Базовые тесты CSV экстрактора"""

    def test_basic_extraction_pandas(self, sample_csv_file):
        """Тест базового извлечения с pandas"""
        config = {
            "type": "csv-extractor",
            "file_path": str(sample_csv_file),
            "output_format": "pandas",
        }

        extractor = CSVExtractor(config)
        context = ExecutionContext()
        result = extractor.execute(context)

        assert result.status == ExecutionStatus.SUCCESS
        assert result.data is not None
        assert isinstance(result.data, pd.DataFrame)
        assert result.processed_records > 0
        assert len(result.data) == result.processed_records

        # Проверяем метаданные
        assert "file_path" in result.metadata
        assert "columns" in result.metadata
        assert "file_size" in result.metadata
        assert "output_format" in result.metadata

    def test_output_formats(self, sample_csv_file):
        """Тест различных форматов вывода"""
        base_config = {"type": "csv-extractor", "file_path": str(sample_csv_file)}

        # Pandas format
        config_pandas = {**base_config, "output_format": "pandas"}
        extractor = CSVExtractor(config_pandas)
        result = extractor.execute(ExecutionContext())
        assert isinstance(result.data, pd.DataFrame)

        # Dict format
        config_dict = {**base_config, "output_format": "dict"}
        extractor = CSVExtractor(config_dict)
        result = extractor.execute(ExecutionContext())
        assert isinstance(result.data, list)
        assert len(result.data) > 0
        assert isinstance(result.data[0], dict)

        # List format
        config_list = {**base_config, "output_format": "list"}
        extractor = CSVExtractor(config_list)
        result = extractor.execute(ExecutionContext())
        assert isinstance(result.data, list)
        assert len(result.data) > 0
        assert isinstance(result.data[0], list)

    def test_custom_delimiter(self, tmp_path):
        """Тест кастомного разделителя"""
        # Создаем TSV файл
        tsv_content = "name\tage\tcity\nJohn\t25\tNY\nJane\t30\tLA"
        tsv_file = tmp_path / "test.tsv"
        tsv_file.write_text(tsv_content)

        config = {"type": "csv-extractor", "file_path": str(tsv_file), "delimiter": "\t"}

        extractor = CSVExtractor(config)
        result = extractor.execute(ExecutionContext())

        assert result.status == ExecutionStatus.SUCCESS
        assert len(result.data.columns) == 3
        assert "name" in result.data.columns

    def test_encoding_handling(self, tmp_path):
        """Тест различных кодировок"""
        # Создаем файл с UTF-8 кодировкой
        csv_content = "name,description\nИван,Тестовое описание\nМария,Еще одно описание"
        csv_file = tmp_path / "utf8_test.csv"
        csv_file.write_text(csv_content, encoding="utf-8")

        config = {"type": "csv-extractor", "file_path": str(csv_file), "encoding": "utf-8"}

        extractor = CSVExtractor(config)
        result = extractor.execute(ExecutionContext())

        assert result.status == ExecutionStatus.SUCCESS
        assert "Иван" in result.data["name"].values
        assert "Тестовое описание" in result.data["description"].values


class TestCSVExtractorAdvanced:
    """Продвинутые тесты CSV экстрактора"""

    def test_column_selection(self, sample_csv_file):
        """Тест выбора определенных колонок"""
        config = {
            "type": "csv-extractor",
            "file_path": str(sample_csv_file),
            "select_columns": ["name", "age"],
        }

        extractor = CSVExtractor(config)
        result = extractor.execute(ExecutionContext())

        assert result.status == ExecutionStatus.SUCCESS
        assert len(result.data.columns) == 2
        assert "name" in result.data.columns
        assert "age" in result.data.columns

    def test_column_renaming(self, sample_csv_file):
        """Тест переименования колонок"""
        config = {
            "type": "csv-extractor",
            "file_path": str(sample_csv_file),
            "rename_columns": {"name": "full_name", "age": "years"},
        }

        extractor = CSVExtractor(config)
        result = extractor.execute(ExecutionContext())

        assert result.status == ExecutionStatus.SUCCESS
        assert "full_name" in result.data.columns
        assert "years" in result.data.columns
        assert "name" not in result.data.columns

    def test_filtering(self, sample_csv_file):
        """Тест фильтрации данных"""
        config = {
            "type": "csv-extractor",
            "file_path": str(sample_csv_file),
            "filter_condition": "age > 25",
        }

        extractor = CSVExtractor(config)
        result = extractor.execute(ExecutionContext())

        assert result.status == ExecutionStatus.SUCCESS
        # Проверяем что все записи соответствуют условию
        assert all(result.data["age"] > 25)

    def test_row_limits(self, large_csv_file):
        """Тест ограничения количества строк"""
        config = {"type": "csv-extractor", "file_path": str(large_csv_file), "max_rows": 5}

        extractor = CSVExtractor(config)
        result = extractor.execute(ExecutionContext())

        assert result.status == ExecutionStatus.SUCCESS
        assert len(result.data) <= 5
        assert result.processed_records <= 5

    def test_skip_rows(self, tmp_path):
        """Тест пропуска строк"""
        csv_content = """# Комментарий
# Еще комментарий
name,age,city
John,25,NY
Jane,30,LA"""

        csv_file = tmp_path / "with_comments.csv"
        csv_file.write_text(csv_content)

        config = {
            "type": "csv-extractor",
            "file_path": str(csv_file),
            "skip_rows": 2,  # Пропускаем комментарии
        }

        extractor = CSVExtractor(config)
        result = extractor.execute(ExecutionContext())

        assert result.status == ExecutionStatus.SUCCESS
        assert "name" in result.data.columns
        assert len(result.data) == 2

    def test_column_types(self, typed_csv_file):
        """Тест приведения типов колонок"""
        config = {
            "type": "csv-extractor",
            "file_path": str(typed_csv_file),
            "column_types": {"id": "int", "name": "string", "score": "float", "active": "bool"},
        }

        extractor = CSVExtractor(config)
        result = extractor.execute(ExecutionContext())

        assert result.status == ExecutionStatus.SUCCESS
        df = result.data

        # Проверяем типы данных
        assert df["id"].dtype.name in ["Int64", "int64"]
        assert df["score"].dtype.name in ["float64"]
        assert df["active"].dtype.name in ["boolean", "bool"]


class TestCSVExtractorChunking:
    """Тесты chunked обработки"""

    def test_chunked_reading(self, large_csv_file):
        """Тест чтения по частям"""
        config = {"type": "csv-extractor", "file_path": str(large_csv_file), "chunk_size": 10}

        extractor = CSVExtractor(config)
        result = extractor.execute(ExecutionContext())

        assert result.status == ExecutionStatus.SUCCESS
        assert result.processed_records > 0
        assert "chunk_size" in result.metadata
        assert result.metadata["chunked_processing"] is True

    def test_chunked_with_filtering(self, large_csv_file):
        """Тест chunked чтения с фильтрацией"""
        config = {
            "type": "csv-extractor",
            "file_path": str(large_csv_file),
            "chunk_size": 10,
            "filter_condition": "id > 5",  # Предполагаем наличие колонки id
        }

        extractor = CSVExtractor(config)
        result = extractor.execute(ExecutionContext())

        assert result.status == ExecutionStatus.SUCCESS


class TestCSVExtractorValidation:
    """Тесты валидации данных"""

    def test_required_columns_success(self, sample_csv_file):
        """Тест успешной проверки обязательных колонок"""
        config = {
            "type": "csv-extractor",
            "file_path": str(sample_csv_file),
            "required_columns": ["name", "age"],
        }

        extractor = CSVExtractor(config)
        result = extractor.execute(ExecutionContext())

        assert result.status == ExecutionStatus.SUCCESS

    def test_required_columns_failure(self, sample_csv_file):
        """Тест ошибки при отсутствии обязательных колонок"""
        config = {
            "type": "csv-extractor",
            "file_path": str(sample_csv_file),
            "required_columns": ["name", "nonexistent_column"],
        }

        extractor = CSVExtractor(config)
        result = extractor.execute(ExecutionContext())

        assert result.status == ExecutionStatus.FAILED
        assert "Отсутствуют обязательные колонки" in result.error_message

    def test_min_records_validation(self, sample_csv_file):
        """Тест валидации минимального количества записей"""
        config = {
            "type": "csv-extractor",
            "file_path": str(sample_csv_file),
            "min_records": 1000,  # Больше чем в тестовом файле
        }

        extractor = CSVExtractor(config)
        result = extractor.execute(ExecutionContext())

        assert result.status == ExecutionStatus.FAILED
        assert "Недостаточно записей" in result.error_message


class TestCSVExtractorErrorHandling:
    """Тесты обработки ошибок"""

    def test_malformed_csv_skip_bad_lines(self, malformed_csv_file):
        """Тест пропуска некорректных строк"""
        config = {
            "type": "csv-extractor",
            "file_path": str(malformed_csv_file),
            "skip_bad_lines": True,
            "error_tolerance": 0.5,  # Допускаем 50% ошибок
        }

        extractor = CSVExtractor(config)
        result = extractor.execute(ExecutionContext())

        # Должно завершиться успешно, пропустив плохие строки
        assert result.status == ExecutionStatus.SUCCESS

    def test_malformed_csv_strict_mode(self, malformed_csv_file):
        """Тест строгого режима с ошибками"""
        config = {
            "type": "csv-extractor",
            "file_path": str(malformed_csv_file),
            "skip_bad_lines": False,
        }

        extractor = CSVExtractor(config)
        result = extractor.execute(ExecutionContext())

        # Должно завершиться с ошибкой
        assert result.status == ExecutionStatus.FAILED

    def test_empty_csv_file(self, empty_csv_file):
        """Тест с пустым CSV файлом"""
        config = {"type": "csv-extractor", "file_path": str(empty_csv_file)}

        extractor = CSVExtractor(config)
        result = extractor.execute(ExecutionContext())

        # Зависит от реализации - может быть успех с 0 записей или ошибка
        assert result.processed_records == 0


class TestCSVExtractorAsync:
    """Тесты асинхронного режима"""

    @pytest.mark.asyncio
    async def test_async_extraction(self, sample_csv_file):
        """Тест асинхронного извлечения"""
        config = {
            "type": "csv-extractor",
            "file_path": str(sample_csv_file),
            "use_async": True,
            "output_format": "dict",
        }

        extractor = CSVExtractor(config)

        # Проверяем что asnyc библиотеки доступны
        try:
            import aiofiles
            import aiocsv
        except ImportError:
            pytest.skip("Async библиотеки не установлены")

        result = extractor.execute(ExecutionContext())

        assert result.status == ExecutionStatus.SUCCESS
        assert isinstance(result.data, list)
        assert "async_processing" in result.metadata


class TestCSVExtractorSpecialCases:
    """Тесты особых случаев"""

    def test_tsv_extractor(self, tmp_path):
        """Тест специализированного TSV экстрактора"""
        tsv_content = "name\tage\tcity\nJohn\t25\tNY\nJane\t30\tLA"
        tsv_file = tmp_path / "test.tsv"
        tsv_file.write_text(tsv_content)

        from pipeline_extractors.file.csv_extractor import TSVExtractor

        config = {"type": "tsv-extractor", "file_path": str(tsv_file)}

        extractor = TSVExtractor(config)
        result = extractor.execute(ExecutionContext())

        assert result.status == ExecutionStatus.SUCCESS
        assert len(result.data) == 2
        assert len(result.data.columns) == 3

    def test_health_check(self, sample_csv_file):
        """Тест проверки состояния"""
        config = {"type": "csv-extractor", "file_path": str(sample_csv_file)}

        extractor = CSVExtractor(config)
        assert extractor.health_check() is True

        # Тест с несуществующим файлом
        config_bad = {"type": "csv-extractor", "file_path": "/nonexistent/file.csv"}

        # Создаем экстрактор напрямую, обходя валидацию
        extractor_bad = CSVExtractor.__new__(CSVExtractor)
        extractor_bad.config = type("Config", (), {"file_path": Path("/nonexistent/file.csv")})()

        assert extractor_bad.health_check() is False


# === FIXTURES ===


@pytest.fixture
def sample_csv_file(tmp_path):
    """Создание базового тестового CSV файла"""
    csv_content = """name,age,city,salary,active
John Doe,25,New York,50000,true
Jane Smith,30,Los Angeles,60000,true
Bob Johnson,35,Chicago,55000,false
Alice Brown,28,Houston,52000,true
Charlie Wilson,32,Phoenix,58000,false"""

    csv_file = tmp_path / "sample.csv"
    csv_file.write_text(csv_content)
    return csv_file


@pytest.fixture
def large_csv_file(tmp_path):
    """Создание большого CSV файла"""
    import pandas as pd

    data = {
        "id": range(1, 101),
        "name": [f"User_{i}" for i in range(1, 101)],
        "value": [i * 10 for i in range(1, 101)],
        "category": ["A", "B", "C"] * 33 + ["A"],  # 100 элементов
    }

    df = pd.DataFrame(data)
    csv_file = tmp_path / "large.csv"
    df.to_csv(csv_file, index=False)

    return csv_file


@pytest.fixture
def typed_csv_file(tmp_path):
    """Создание CSV файла с различными типами данных"""
    csv_content = """id,name,score,active,date
1,Alice,95.5,true,2024-01-01
2,Bob,87.2,false,2024-01-02
3,Charlie,92.8,true,2024-01-03"""

    csv_file = tmp_path / "typed.csv"
    csv_file.write_text(csv_content)
    return csv_file


@pytest.fixture
def malformed_csv_file(tmp_path):
    """Создание некорректного CSV файла"""
    csv_content = """name,age,city
John,25,New York
Jane,30  # Неполная строка
Bob,35,Chicago,Extra,Data,Here
Alice,"Unclosed quote,28,Boston"""

    csv_file = tmp_path / "malformed.csv"
    csv_file.write_text(csv_content)
    return csv_file


@pytest.fixture
def empty_csv_file(tmp_path):
    """Создание пустого CSV файла"""
    csv_file = tmp_path / "empty.csv"
    csv_file.write_text("name,age,city\n")  # Только заголовок
    return csv_file


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
