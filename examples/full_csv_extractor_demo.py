# examples/full_csv_extractor_demo.py
"""
Полная демонстрация возможностей CSV экстрактора
"""

import tempfile
import pandas as pd
import numpy as np
from pathlib import Path
import sys
import os
import time

# Добавляем пути для импорта
sys.path.insert(0, str(Path(__file__).parent.parent / "packages" / "pipeline-core" / "src"))
sys.path.insert(
    0, str(Path(__file__).parent.parent / "packages" / "components" / "extractors" / "src")
)

from pipeline_core import ExecutionContext, PipelineYAMLParser, get_registry
from pipeline_extractors.file.csv_extractor import CSVExtractor


def create_comprehensive_dataset():
    """Создание комплексного тестового датасета"""
    np.random.seed(42)  # Для воспроизводимости

    n_records = 1000

    # Генерируем данные
    data = {
        "user_id": range(1, n_records + 1),
        "name": [f"User_{i:04d}" for i in range(1, n_records + 1)],
        "email": [f"user{i}@example.com" for i in range(1, n_records + 1)],
        "age": np.random.randint(18, 80, n_records),
        "salary": np.random.normal(50000, 15000, n_records).round(2),
        "department": np.random.choice(["IT", "HR", "Finance", "Marketing", "Sales"], n_records),
        "active": np.random.choice([True, False], n_records, p=[0.8, 0.2]),
        "join_date": pd.date_range("2020-01-01", "2024-12-31", periods=n_records),
        "rating": np.random.uniform(1.0, 5.0, n_records).round(1),
        "city": np.random.choice(
            ["Moscow", "SPb", "Kazan", "Novosibirsk", "Yekaterinburg"], n_records
        ),
    }

    df = pd.DataFrame(data)

    # Добавляем некоторые проблемы в данные для демонстрации
    # Пустые значения
    df.loc[np.random.choice(df.index, 50, replace=False), "salary"] = np.nan
    df.loc[np.random.choice(df.index, 30, replace=False), "rating"] = np.nan

    # Неконсистентные данные
    df.loc[10:15, "age"] = ["unknown", "N/A", "25x", "null", "", "30"]

    return df


def demo_basic_extraction():
    """Демонстрация базового извлечения"""
    print("\n📋 БАЗОВОЕ ИЗВЛЕЧЕНИЕ")
    print("=" * 60)

    # Создаем тестовый файл
    df = create_comprehensive_dataset()
    temp_file = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False)
    df.to_csv(temp_file.name, index=False)
    temp_file.close()

    print(f"📄 Создан тестовый файл: {temp_file.name}")
    print(f"📊 Исходные данные: {len(df)} записей, {len(df.columns)} колонок")

    try:
        config = {"type": "csv-extractor", "file_path": temp_file.name, "output_format": "pandas"}

        extractor = CSVExtractor(config)
        context = ExecutionContext()

        start_time = time.time()
        result = extractor.execute(context)
        execution_time = time.time() - start_time

        print(f"\n✅ Результат извлечения:")
        print(f"   Status: {result.status}")
        print(f"   Записей: {result.processed_records}")
        print(f"   Время: {execution_time:.3f} сек")
        print(f"   Колонки: {len(result.metadata['columns'])}")
        print(f"   Размер файла: {result.metadata['file_size']:,} байт")

        if result.status.value == "success":
            print(f"\n📋 Первые 3 записи:")
            print(result.data.head(3).to_string(index=False))
            return temp_file.name, True

    except Exception as e:
        print(f"\n❌ Ошибка: {e}")
        return temp_file.name, False

    return temp_file.name, False


def demo_filtering_and_selection(csv_file):
    """Демонстрация фильтрации и выбора колонок"""
    print("\n🔍 ФИЛЬТРАЦИЯ И ВЫБОР КОЛОНОК")
    print("=" * 60)

    config = {
        "type": "csv-extractor",
        "file_path": csv_file,
        "select_columns": ["name", "email", "age", "salary", "department"],
        "rename_columns": {"name": "full_name", "email": "email_address"},
        "filter_condition": "age > 30 and department in ['IT', 'Finance']",
        "output_format": "pandas",
    }

    try:
        extractor = CSVExtractor(config)
        result = extractor.execute(ExecutionContext())

        print(f"✅ Фильтрация выполнена:")
        print(f"   Исходных записей: {1000}")
        print(f"   После фильтрации: {result.processed_records}")
        print(f"   Выбранные колонки: {result.metadata['columns']}")

        if result.status.value == "success":
            print(f"\n📋 Примеры отфильтрованных данных:")
            print(result.data.head().to_string(index=False))

            print(f"\n📊 Статистика по департаментам:")
            print(result.data["department"].value_counts().to_string())

    except Exception as e:
        print(f"❌ Ошибка фильтрации: {e}")


def demo_data_types_and_validation(csv_file):
    """Демонстрация работы с типами данных и валидацией"""
    print("\n🔧 ТИПЫ ДАННЫХ И ВАЛИДАЦИЯ")
    print("=" * 60)

    config = {
        "type": "csv-extractor",
        "file_path": csv_file,
        "column_types": {
            "user_id": "int",
            "age": "int",
            "salary": "float",
            "active": "bool",
            "join_date": "datetime",
        },
        "required_columns": ["user_id", "name", "email"],
        "min_records": 500,
        "skip_bad_lines": True,
        "error_tolerance": 0.1,
        "output_format": "pandas",
    }

    try:
        extractor = CSVExtractor(config)
        result = extractor.execute(ExecutionContext())

        print(f"✅ Типизация данных:")
        print(f"   Записей обработано: {result.processed_records}")

        if result.status.value == "success":
            df = result.data
            print(f"\n📊 Типы данных:")
            for col, dtype in df.dtypes.items():
                print(f"   {col}: {dtype}")

            print(f"\n📈 Статистика числовых колонок:")
            numeric_cols = df.select_dtypes(include=[np.number]).columns
            if len(numeric_cols) > 0:
                print(df[numeric_cols].describe().round(2).to_string())

    except Exception as e:
        print(f"❌ Ошибка типизации: {e}")


def demo_chunked_processing(csv_file):
    """Демонстрация chunked обработки"""
    print("\n⚡ CHUNKED ОБРАБОТКА")
    print("=" * 60)

    config = {
        "type": "csv-extractor",
        "file_path": csv_file,
        "chunk_size": 100,
        "filter_condition": "active == True",
        "max_rows": 500,
        "output_format": "pandas",
    }

    try:
        extractor = CSVExtractor(config)

        start_time = time.time()
        result = extractor.execute(ExecutionContext())
        execution_time = time.time() - start_time

        print(f"✅ Chunked обработка:")
        print(f"   Размер chunk: {config['chunk_size']}")
        print(f"   Записей обработано: {result.processed_records}")
        print(f"   Время выполнения: {execution_time:.3f} сек")
        print(f"   Chunked processing: {result.metadata.get('chunked_processing', False)}")

    except Exception as e:
        print(f"❌ Ошибка chunked обработки: {e}")


def demo_yaml_pipeline(csv_file):
    """Демонстрация использования в YAML pipeline"""
    print("\n📝 YAML PIPELINE")
    print("=" * 60)

    yaml_content = f"""
metadata:
  name: "comprehensive-csv-extraction"
  description: "Демонстрация полноценного CSV экстрактора"
  version: "1.0.0"

variables:
  DATA_FILE: "{csv_file}"
  MIN_AGE: 25
  TARGET_DEPT: "IT"

# Извлечение всех данных
extract_all:
  type: "csv-extractor"
  config:
    file_path: "${{DATA_FILE}}"
    output_format: "pandas"
    required_columns: ["user_id", "name", "email"]

# Извлечение IT сотрудников
extract_it_staff:
  type: "csv-extractor"
  config:
    file_path: "${{DATA_FILE}}"
    select_columns: ["name", "email", "age", "salary", "department"]
    filter_condition: "age >= ${{MIN_AGE}} and department == '${{TARGET_DEPT}}'"
    column_types:
      age: "int"
      salary: "float"
    output_format: "dict"

# Извлечение с chunked обработкой
extract_chunked:
  type: "csv-extractor"
  config:
    file_path: "${{DATA_FILE}}"
    chunk_size: 50
    max_rows: 200
    skip_bad_lines: true
    error_tolerance: 0.05
"""

    try:
        parser = PipelineYAMLParser()
        config = parser.parse_string(yaml_content)

        print(f"📋 Pipeline: {config.metadata.name}")
        print(f"📊 Стадий: {len(config.stages)}")
        print(f"🔗 Порядок выполнения: {config.get_execution_order()}")

        registry = get_registry()
        context = ExecutionContext()

        # Выполняем каждую стадию
        for stage_name in ["extract_all", "extract_it_staff", "extract_chunked"]:
            if stage_name in config.stages:
                stage_config = config.stages[stage_name]

                print(f"\n▶️  Выполнение стадии: {stage_name}")

                component = registry.create_component(stage_config.type, stage_config.config)

                start_time = time.time()
                result = component.execute(context)
                execution_time = time.time() - start_time

                context.set_stage_result(stage_name, result)

                print(f"   Status: {result.status}")
                print(f"   Records: {result.processed_records}")
                print(f"   Time: {execution_time:.3f}s")

                if result.status.value == "success":
                    if isinstance(result.data, pd.DataFrame):
                        print(f"   Shape: {result.data.shape}")
                    elif isinstance(result.data, list):
                        print(f"   Records: {len(result.data)}")

        print(f"\n🎉 Pipeline выполнен успешно!")

        # Показываем сводку
        summary = context.get_execution_summary()
        print(f"\n📊 Сводка выполнения:")
        print(f"   Всего стадий: {summary['total_stages']}")
        print(f"   Успешных: {summary['successful_stages']}")
        print(f"   Неудачных: {summary['failed_stages']}")

    except Exception as e:
        print(f"❌ Ошибка YAML pipeline: {e}")
        import traceback

        traceback.print_exc()


def demo_error_handling():
    """Демонстрация обработки ошибок"""
    print("\n🚨 ОБРАБОТКА ОШИБОК")
    print("=" * 60)

    # Создаем файл с проблемами
    problematic_data = """name,age,salary,department
John,25,50000,IT
Jane,thirty,invalid_salary,HR
Bob,35,60000,Finance
"Unclosed quote,40,70000,Marketing
Charlie,45,80000,Sales,Extra,Columns"""

    temp_file = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False)
    temp_file.write(problematic_data)
    temp_file.close()

    print(f"📄 Создан проблемный файл: {temp_file.name}")

    # Тест 1: Строгий режим (должен упасть)
    print(f"\n🔸 Тест 1: Строгий режим")
    config_strict = {
        "type": "csv-extractor",
        "file_path": temp_file.name,
        "skip_bad_lines": False,
        "column_types": {"age": "int", "salary": "float"},
    }

    try:
        extractor = CSVExtractor(config_strict)
        result = extractor.execute(ExecutionContext())
        print(f"   Результат: {result.status}")
        if result.status.value == "failed":
            print(f"   Ошибка: {result.error_message}")
    except Exception as e:
        print(f"   Исключение: {e}")

    # Тест 2: Толерантный режим
    print(f"\n🔸 Тест 2: Толерантный режим")
    config_tolerant = {
        "type": "csv-extractor",
        "file_path": temp_file.name,
        "skip_bad_lines": True,
        "error_tolerance": 0.5,  # Допускаем 50% ошибок
        "infer_schema": False,  # Читаем все как строки
    }

    try:
        extractor = CSVExtractor(config_tolerant)
        result = extractor.execute(ExecutionContext())
        print(f"   Результат: {result.status}")
        print(f"   Записей обработано: {result.processed_records}")

        if result.status.value == "success":
            print(f"   Данные:")
            print(result.data.head().to_string(index=False))
    except Exception as e:
        print(f"   Исключение: {e}")

    # Очистка
    try:
        os.unlink(temp_file.name)
    except:
        pass


def demo_performance_comparison():
    """Демонстрация сравнения производительности"""
    print("\n⚡ СРАВНЕНИЕ ПРОИЗВОДИТЕЛЬНОСТИ")
    print("=" * 60)

    # Создаем большой файл
    print("📄 Создаем большой тестовый файл...")
    df_large = create_comprehensive_dataset()
    # Увеличиваем размер в 5 раз
    df_large = pd.concat([df_large] * 5, ignore_index=True)

    temp_file = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False)
    df_large.to_csv(temp_file.name, index=False)
    temp_file.close()

    print(f"📊 Создан файл: {len(df_large)} записей, {temp_file.name}")

    configs = [
        (
            "Обычное чтение",
            {"type": "csv-extractor", "file_path": temp_file.name, "output_format": "pandas"},
        ),
        (
            "Chunked чтение (chunk=500)",
            {
                "type": "csv-extractor",
                "file_path": temp_file.name,
                "chunk_size": 500,
                "output_format": "pandas",
            },
        ),
        (
            "С фильтрацией",
            {
                "type": "csv-extractor",
                "file_path": temp_file.name,
                "filter_condition": "active == True and age > 25",
                "output_format": "pandas",
            },
        ),
        (
            "Chunked + фильтрация",
            {
                "type": "csv-extractor",
                "file_path": temp_file.name,
                "chunk_size": 500,
                "filter_condition": "active == True and age > 25",
                "output_format": "pandas",
            },
        ),
    ]

    results = []

    for test_name, config in configs:
        print(f"\n🔸 {test_name}")

        try:
            extractor = CSVExtractor(config)

            start_time = time.time()
            result = extractor.execute(ExecutionContext())
            execution_time = time.time() - start_time

            if result.status.value == "success":
                print(f"   ✅ Время: {execution_time:.3f}s")
                print(f"   📊 Записей: {result.processed_records}")
                print(f"   💾 Память: chunked={config.get('chunk_size') is not None}")

                results.append(
                    {"test": test_name, "time": execution_time, "records": result.processed_records}
                )
            else:
                print(f"   ❌ Ошибка: {result.error_message}")

        except Exception as e:
            print(f"   💥 Исключение: {e}")

    # Сводка производительности
    if results:
        print(f"\n📈 СВОДКА ПРОИЗВОДИТЕЛЬНОСТИ:")
        print("-" * 50)
        for r in results:
            print(f"{r['test']:<25} {r['time']:>6.3f}s {r['records']:>8} записей")

    # Очистка
    try:
        os.unlink(temp_file.name)
    except:
        pass


def main():
    """Главная функция демонстрации"""
    print("🚀 ДЕМОНСТРАЦИЯ ПОЛНОЦЕННОГО CSV ЭКСТРАКТОРА")
    print("=" * 80)
    print("Эта демонстрация покажет все возможности CSV экстрактора:")
    print("• Базовое извлечение данных")
    print("• Фильтрация и выбор колонок")
    print("• Работа с типами данных")
    print("• Chunked обработка больших файлов")
    print("• Интеграция с YAML pipeline")
    print("• Обработка ошибок")
    print("• Сравнение производительности")

    success_count = 0
    total_tests = 6

    try:
        # Демо 1: Базовое извлечение
        csv_file, success = demo_basic_extraction()
        if success:
            success_count += 1

        if csv_file and os.path.exists(csv_file):
            # Демо 2: Фильтрация
            try:
                demo_filtering_and_selection(csv_file)
                success_count += 1
            except Exception as e:
                print(f"❌ Ошибка в демо фильтрации: {e}")

            # Демо 3: Типы данных
            try:
                demo_data_types_and_validation(csv_file)
                success_count += 1
            except Exception as e:
                print(f"❌ Ошибка в демо типов данных: {e}")

            # Демо 4: Chunked обработка
            try:
                demo_chunked_processing(csv_file)
                success_count += 1
            except Exception as e:
                print(f"❌ Ошибка в демо chunked обработки: {e}")

            # Демо 5: YAML pipeline
            try:
                demo_yaml_pipeline(csv_file)
                success_count += 1
            except Exception as e:
                print(f"❌ Ошибка в демо YAML: {e}")

            # Очистка основного файла
            try:
                os.unlink(csv_file)
            except:
                pass

        # Демо 6: Обработка ошибок
        try:
            demo_error_handling()
            success_count += 1
        except Exception as e:
            print(f"❌ Ошибка в демо обработки ошибок: {e}")

        # Демо 7: Производительность (бонус)
        try:
            demo_performance_comparison()
        except Exception as e:
            print(f"❌ Ошибка в демо производительности: {e}")

    except Exception as e:
        print(f"\n💥 Критическая ошибка: {e}")
        import traceback

        traceback.print_exc()

    # Финальная сводка
    print(f"\n" + "=" * 80)
    print(f"📈 ИТОГИ ДЕМОНСТРАЦИИ")
    print(f"Успешных демо: {success_count}/{total_tests}")

    if success_count == total_tests:
        print("🎉 Все демонстрации прошли успешно!")
        print("💡 CSV экстрактор готов к использованию в production!")
    elif success_count > total_tests // 2:
        print("✅ Большинство демонстраций прошли успешно!")
        print("⚠️  Некоторые функции могут требовать дополнительной настройки.")
    else:
        print("⚠️  Обнаружены проблемы в работе экстрактора.")
        print("🔧 Проверьте установку зависимостей и настройки.")

    print(f"\n🔧 Для использования CSV экстрактора:")
    print(f"   pip install pandas  # Основная зависимость")
    print(f"   pip install polars  # Опционально, для лучшей производительности")
    print(f"   pip install aiofiles aiocsv  # Опционально, для асинхронного чтения")


if __name__ == "__main__":
    main()
