#!/usr/bin/env python3
"""
Демонстрация работы компонентов extractors и transformers
"""

import tempfile
import pandas as pd
from pathlib import Path
import sys
import os

# Добавляем пути к компонентам для импорта
sys.path.insert(
    0, os.path.join(os.path.dirname(__file__), "..", "packages", "pipeline-core", "src")
)
sys.path.insert(
    0, os.path.join(os.path.dirname(__file__), "..", "packages", "components", "extractors", "src")
)
sys.path.insert(
    0,
    os.path.join(os.path.dirname(__file__), "..", "packages", "components", "transformers", "src"),
)

# Импорты из pipeline-core
from pipeline_core import ExecutionContext, ExecutionStatus, PipelineYAMLParser, get_registry

# Импорты компонентов
try:
    from pipeline_extractors import CSVExtractor
    from pipeline_transformers import DataTransformerComponent

    extractors_available = True
except ImportError as e:
    print(f"⚠️  Компоненты не найдены: {e}")
    extractors_available = False


def create_sample_data():
    """Создание примера CSV файла для тестирования"""
    # Создаем временный CSV файл
    temp_file = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False)

    # Генерируем тестовые данные
    data = {
        "id": range(1, 101),
        "name": [f"User_{i}" for i in range(1, 101)],
        "age": [20 + (i % 50) for i in range(100)],
        "salary": [30000 + (i * 1000) for i in range(100)],
        "department": ["IT", "HR", "Finance", "Marketing", "Sales"] * 20,
        "active": [True if i % 3 != 0 else False for i in range(100)],
    }

    df = pd.DataFrame(data)
    df.to_csv(temp_file.name, index=False)
    temp_file.close()

    print(f"📄 Создан тестовый CSV файл: {temp_file.name}")
    print(f"📊 Размер данных: {len(df)} строк, {len(df.columns)} колонок")
    print(f"📋 Колонки: {list(df.columns)}")

    return temp_file.name


def test_csv_extractor():
    """Тест CSV экстрактора"""
    print("\n🔍 Тестирование CSV Extractor")
    print("=" * 50)

    # Создаем тестовые данные
    csv_file = create_sample_data()

    try:
        # Конфигурация экстрактора
        config = {
            "type": "csv-extractor",
            "file_path": csv_file,
            "delimiter": ",",
            "has_header": True,
            "output_format": "pandas",
        }

        # Создаем экстрактор
        extractor = CSVExtractor(config)

        # Выполняем извлечение
        context = ExecutionContext()
        result = extractor.execute(context)

        # Проверяем результат
        assert result.status == ExecutionStatus.SUCCESS
        assert result.data is not None
        assert result.processed_records > 0

        print(f"✅ Извлечение успешно!")
        print(f"📊 Обработано записей: {result.processed_records}")
        print(f"⏱️  Время выполнения: {result.execution_time:.3f} сек")
        print(f"📋 Форма данных: {result.data.shape}")
        print(f"🔍 Первые 3 строки:")
        print(result.data.head(3))

        return result.data

    finally:
        # Удаляем временный файл
        os.unlink(csv_file)


def test_data_transformer(input_data):
    """Тест трансформера данных"""
    print("\n🔄 Тестирование Data Transformer")
    print("=" * 50)

    # Конфигурация трансформера
    config = {
        "type": "data-transformer",
        "source_stage": "extract",  # Ссылка на стадию
        # Операции трансформации
        "add_columns": {
            "salary_monthly": "salary / 12",
            "is_senior": "age >= 40",
            "full_info": "name + ' (' + department + ')'",
        },
        "filter_expression": "active == True and salary > 40000",
        "drop_columns": ["id"],
        "rename_columns": {"name": "employee_name", "department": "dept"},
        "sort_by": ["salary"],
        "sort_ascending": False,
        "sample_n": 20,
        "output_format": "pandas",
    }

    # Создаем трансформер
    transformer = DataTransformerComponent(config)

    # Подготавливаем контекст с данными
    context = ExecutionContext()
    from pipeline_core import ExecutionResult, ExecutionStatus

    extract_result = ExecutionResult(
        status=ExecutionStatus.SUCCESS, data=input_data, processed_records=len(input_data)
    )
    context.set_stage_result("extract", extract_result)

    # Выполняем трансформацию
    result = transformer.execute(context)

    # Проверяем результат
    assert result.status == ExecutionStatus.SUCCESS
    assert result.data is not None

    print(f"✅ Трансформация успешна!")
    print(f"📊 Обработано записей: {result.processed_records}")
    print(f"⏱️  Время выполнения: {result.execution_time:.3f} сек")
    print(f"📋 Форма данных: {result.data.shape}")
    print(f"🔍 Колонки после трансформации: {list(result.data.columns)}")
    print(f"🔍 Первые 5 строк:")
    print(result.data.head())

    return result.data


def test_yaml_pipeline():
    """Тест полного pipeline через YAML"""
    print("\n📄 Тестирование YAML Pipeline")
    print("=" * 50)

    # Создаем тестовые данные
    csv_file = create_sample_data()

    try:
        # YAML конфигурация pipeline
        yaml_content = f"""
        metadata:
          name: "demo-pipeline"
          description: "Демонстрационный pipeline с extractors и transformers"
          version: "1.0.0"

        variables:
          MIN_SALARY: 35000
          OUTPUT_FORMAT: "pandas"

        # Стадия извлечения данных
        extract:
          type: "csv-extractor"
          config:
            file_path: "{csv_file}"
            delimiter: ","
            has_header: true
            output_format: "${{OUTPUT_FORMAT}}"

        # Стадия трансформации данных  
        transform:
          type: "data-transformer"
          depends_on: ["extract"]
          config:
            source_stage: "extract"
            add_columns:
              yearly_bonus: "salary * 0.1"
              experience_level: "case when age < 30 then 'Junior' when age < 45 then 'Middle' else 'Senior' end"
            filter_expression: "salary > ${{MIN_SALARY}} and active == True"
            sort_by: ["salary", "age"]
            sort_ascending: [false, true]
            drop_duplicates: true
            output_format: "${{OUTPUT_FORMAT}}"
        """

        # Парсим YAML
        parser = PipelineYAMLParser()
        pipeline_config = parser.parse_string(yaml_content)

        print(f"✅ YAML успешно распарсен!")
        print(f"📋 Pipeline: {pipeline_config.metadata.name}")
        print(f"📊 Стадий: {len(pipeline_config.stages)}")
        print(f"🔗 Порядок выполнения: {pipeline_config.get_execution_order()}")

        # Получаем реестр компонентов
        registry = get_registry()

        # Проверяем доступность компонентов
        available_components = registry.list_components()
        print(f"🔧 Доступные компоненты: {available_components}")

        # Если компоненты зарегистрированы, выполняем pipeline
        if "csv-extractor" in available_components and "data-transformer" in available_components:
            context = ExecutionContext()

            # Выполняем стадии по порядку
            for level in pipeline_config.get_execution_order():
                for stage_name in level:
                    stage_config = pipeline_config.stages[stage_name]

                    print(f"\n▶️  Выполнение стадии: {stage_name}")

                    # Создаем компонент
                    component = registry.create_component(stage_config.type, stage_config.config)

                    # Выполняем
                    result = component.execute(context)
                    context.set_stage_result(stage_name, result)

                    print(f"   Status: {result.status}")
                    print(f"   Records: {result.processed_records}")
                    print(f"   Time: {result.execution_time:.3f}s")

            print(f"\n🎉 Pipeline выполнен успешно!")

            # Показываем сводку
            summary = context.get_execution_summary()
            print(f"📊 Сводка выполнения:")
            print(f"   Всего стадий: {summary['total_stages']}")
            print(f"   Успешных: {summary['successful_stages']}")
            print(f"   Неудачных: {summary['failed_stages']}")
            print(f"   Общее время: {summary['total_execution_time']:.3f}s")

        else:
            print("⚠️  Компоненты не зарегистрированы в реестре")

    finally:
        # Удаляем временный файл
        os.unlink(csv_file)


def main():
    """Главная функция демонстрации"""
    print("🚀 Демонстрация Pipeline Components")
    print("=" * 60)

    if not extractors_available:
        print("❌ Компоненты недоступны. Проверьте установку пакетов.")
        return

    try:
        # Тест 1: CSV Extractor
        data = test_csv_extractor()

        # Тест 2: Data Transformer
        transformed_data = test_data_transformer(data)

        # Тест 3: YAML Pipeline
        test_yaml_pipeline()

        print(f"\n🎉 Все тесты прошли успешно!")
        print(f"💡 Компоненты готовы к использованию в production!")

    except Exception as e:
        print(f"\n❌ Ошибка при выполнении тестов: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    main()
