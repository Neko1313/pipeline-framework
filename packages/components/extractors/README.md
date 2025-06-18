# CSV Extractor - Полноценный компонент для извлечения данных

Мощный и гибкий CSV экстрактор для Pipeline Framework с расширенными возможностями обработки данных.

## 🚀 Возможности

### ✅ Основные функции
- **Множественные форматы вывода**: pandas DataFrame, Polars DataFrame, dict, list
- **Асинхронное чтение**: для больших файлов без блокировки
- **Chunked обработка**: экономия памяти при работе с большими файлами
- **Гибкая конфигурация**: 20+ параметров настройки

### ✅ Обработка данных
- **Фильтрация**: pandas query синтаксис для сложных условий
- **Выбор колонок**: читать только нужные поля
- **Переименование**: автоматическое переименование колонок
- **Типизация**: автоматическое и ручное определение типов данных

### ✅ Валидация и качество
- **Проверка схемы**: обязательные колонки и их типы
- **Валидация данных**: минимум/максимум записей
- **Обработка ошибок**: толерантность к некорректным данным
- **Health check**: проверка доступности файлов

### ✅ Производительность
- **Оптимизированное чтение**: поддержка различных движков
- **Память**: контроль потребления через chunking
- **Кодировки**: поддержка различных текстовых кодировок
- **Форматы**: CSV, TSV и другие разделители

## 📦 Установка

```bash
# Базовые зависимости
pip install pandas>=2.0.0

# Опциональные зависимости для расширенных возможностей
pip install polars>=0.20.0          # Для лучшей производительности
pip install aiofiles>=23.0.0        # Для асинхронного чтения
pip install aiocsv>=1.3.0           # Для асинхронного чтения CSV
```

## 🔧 Базовое использование

### Простейший пример

```python
from pipeline_core import ExecutionContext
from pipeline_extractors.file.csv_extractor import CSVExtractor

# Минимальная конфигурация
config = {
    "type": "csv-extractor",
    "file_path": "data.csv"
}

extractor = CSVExtractor(config)
context = ExecutionContext()
result = extractor.execute(context)

if result.status.value == "success":
    df = result.data  # pandas DataFrame
    print(f"Извлечено {result.processed_records} записей")
```

### YAML конфигурация

```yaml
extract_users:
  type: "csv-extractor"
  config:
    file_path: "users.csv"
    delimiter: ","
    encoding: "utf-8"
    output_format: "pandas"
    required_columns: ["id", "name", "email"]
```

## ⚙️ Параметры конфигурации

### Основные параметры файла

| Параметр | Тип | По умолчанию | Описание |
|----------|-----|--------------|----------|
| `file_path` | str/Path | **обязательный** | Путь к CSV файлу |
| `delimiter` | str | `","` | Разделитель колонок |
| `encoding` | str | `"utf-8"` | Кодировка файла |
| `has_header` | bool | `true` | Есть ли заголовок |
| `skip_rows` | int | `0` | Пропустить N строк сверху |
| `max_rows` | int | `null` | Максимум строк для чтения |

### Обработка данных

| Параметр | Тип | По умолчанию | Описание |
|----------|-----|--------------|----------|
| `null_values` | list[str] | `["", "NULL", "null", ...]` | Значения = NULL |
| `infer_schema` | bool | `true` | Автоопределение типов |
| `column_types` | dict | `null` | Явные типы колонок |
| `output_format` | str | `"pandas"` | pandas/polars/dict/list |

### Фильтрация и выборка

| Параметр | Тип | По умолчанию | Описание |
|----------|-----|--------------|----------|
| `select_columns` | list[str] | `null` | Выбрать только эти колонки |
| `rename_columns` | dict | `null` | Переименовать колонки |
| `filter_condition` | str | `null` | Условие фильтрации |

### Валидация

| Параметр | Тип | По умолчанию | Описание |
|----------|-----|--------------|----------|
| `required_columns` | list[str] | `[]` | Обязательные колонки |
| `min_records` | int | `null` | Минимум записей |
| `max_records` | int | `null` | Максимум записей |

### Производительность

| Параметр | Тип | По умолчанию | Описание |
|----------|-----|--------------|----------|
| `chunk_size` | int | `null` | Размер чанка |
| `use_async` | bool | `false` | Асинхронное чтение |
| `memory_limit` | str | `null` | Лимит памяти |

### Обработка ошибок

| Параметр | Тип | По умолчанию | Описание |
|----------|-----|--------------|----------|
| `skip_bad_lines` | bool | `false` | Пропускать некорректные строки |
| `error_tolerance` | float | `0.0` | Допустимая доля ошибок (0.0-1.0) |

## 📚 Примеры использования

### 1. Фильтрация и выбор колонок

```python
config = {
    "type": "csv-extractor",
    "file_path": "employees.csv",
    "select_columns": ["name", "email", "salary", "department"],
    "rename_columns": {"name": "full_name", "email": "email_address"},
    "filter_condition": "salary > 50000 and department in ['IT', 'Engineering']",
    "output_format": "pandas"
}
```

### 2. Типизация данных

```python
config = {
    "type": "csv-extractor", 
    "file_path": "data.csv",
    "column_types": {
        "id": "int",
        "name": "string", 
        "price": "float",
        "active": "bool",
        "created_at": "datetime"
    },
    "infer_schema": false
}
```

### 3. Chunked обработка больших файлов

```python
config = {
    "type": "csv-extractor",
    "file_path": "big_data.csv",
    "chunk_size": 1000,           # Читать по 1000 строк
    "filter_condition": "active == True",
    "max_rows": 50000,            # Ограничить общее количество
    "output_format": "pandas"
}
```

### 4. Асинхронное чтение

```python
config = {
    "type": "csv-extractor",
    "file_path": "large_file.csv",
    "use_async": True,
    "output_format": "dict",      # Лучше для async
    "skip_bad_lines": True,
    "error_tolerance": 0.05       # Допускать 5% ошибок
}
```

### 5. Обработка проблемных данных

```python
config = {
    "type": "csv-extractor",
    "file_path": "messy_data.csv",
    "skip_bad_lines": True,       # Пропускать некорректные строки
    "error_tolerance": 0.1,       # Допускать 10% ошибок
    "null_values": ["", "NULL", "N/A", "unknown", "missing"],
    "infer_schema": False,        # Читать все как строки сначала
    "output_format": "pandas"
}
```

### 6. Валидация качества данных

```python
config = {
    "type": "csv-extractor",
    "file_path": "users.csv",
    "required_columns": ["user_id", "email", "name"],
    "min_records": 100,           # Минимум 100 записей
    "max_records": 1000000,       # Максимум 1M записей
    "filter_condition": "email.str.contains('@')",  # Валидные email
    "output_format": "pandas"
}
```

## 🔄 Интеграция с Pipeline

### YAML Pipeline

```yaml
metadata:
  name: "data-processing-pipeline"
  version: "1.0.0"

variables:
  DATA_DIR: "./data"
  MIN_SALARY: 30000

# Извлечение сырых данных
extract_raw:
  type: "csv-extractor"
  config:
    file_path: "${DATA_DIR}/employees.csv"
    output_format: "pandas"
    required_columns: ["id", "name", "salary"]

# Извлечение высокооплачиваемых
extract_high_earners:
  type: "csv-extractor"
  config:
    file_path: "${DATA_DIR}/employees.csv"
    filter_condition: "salary >= ${MIN_SALARY}"
    column_types:
      salary: "float"
      id: "int"
    output_format: "dict"

# Большой файл с chunking
extract_transactions:
  type: "csv-extractor"
  config:
    file_path: "${DATA_DIR}/transactions.csv"
    chunk_size: 5000
    select_columns: ["date", "amount", "user_id"]
    filter_condition: "amount > 0"
    max_rows: 100000
```

### Программное использование

```python
from pipeline_core import PipelineYAMLParser, ExecutionContext, get_registry

# Парсим pipeline
parser = PipelineYAMLParser()
config = parser.parse_file("pipeline.yml")

# Выполняем стадии
registry = get_registry()
context = ExecutionContext()

for level in config.get_execution_order():
    for stage_name in level:
        stage_config = config.stages[stage_name]
        
        component = registry.create_component(
            stage_config.type,
            stage_config.config
        )
        
        result = component.execute(context)
        context.set_stage_result(stage_name, result)
        
        print(f"✅ {stage_name}: {result.processed_records} записей")
```

## 🚨 Обработка ошибок

### Типы ошибок

1. **PipelineConfigError**: Некорректная конфигурация
2. **PipelineDataError**: Проблемы с данными
3. **PipelineExecutionError**: Ошибки выполнения

### Стратегии обработки

```python
# Строгий режим - падать при любой ошибке
config_strict = {
    "skip_bad_lines": False,
    "error_tolerance": 0.0
}

# Толерантный режим - продолжать при ошибках
config_tolerant = {
    "skip_bad_lines": True,
    "error_tolerance": 0.1,  # До 10% ошибок
    "infer_schema": False    # Безопаснее
}

# Проверка результата
result = extractor.execute(context)
if result.status.value == "failed":
    print(f"Ошибка: {result.error_message}")
    # Логика восстановления
```

## 📈 Производительность

### Рекомендации

**Для небольших файлов (< 100MB)**:
```python
config = {
    "file_path": "small.csv",
    "output_format": "pandas",
    "infer_schema": True
}
```

**Для средних файлов (100MB - 1GB)**:
```python
config = {
    "file_path": "medium.csv",
    "chunk_size": 10000,
    "output_format": "pandas"
}
```

**Для больших файлов (> 1GB)**:
```python
config = {
    "file_path": "large.csv",
    "output_format": "polars",    # Быстрее pandas
    "chunk_size": 50000,          # Или использовать polars без chunking
    "use_async": False            # Polars не поддерживает async
}
```

**Для очень больших файлов**:
```python
config = {
    "file_path": "huge.csv",
    "use_async": True,
    "chunk_size": 1000,           # Маленькие чанки
    "output_format": "dict",      # Меньше памяти
    "max_rows": 1000000          # Ограничить объем
}
```

### Сравнение производительности

| Метод | Скорость | Память | Когда использовать |
|-------|----------|--------|--------------------|
| pandas обычный | ⭐⭐⭐ | ⭐ | Файлы < 500MB |
| pandas chunked | ⭐⭐ | ⭐⭐⭐ | Файлы > 500MB |
| polars | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐ | Любые размеры |
| async | ⭐⭐ | ⭐⭐ | I/O bound задачи |

## 🔧 Расширение функциональности

### Создание специализированных экстракторов

```python
from pipeline_extractors.file.csv_extractor import CSVExtractor, CSVExtractorConfig

@register_component("excel-like-csv")
class ExcelLikeCSVExtractor(CSVExtractor):
    """CSV экстрактор для файлов из Excel"""
    
    def get_config_model(self):
        class ExcelCSVConfig(CSVExtractorConfig):
            delimiter: str = Field(default=";", frozen=True)
            encoding: str = Field(default="cp1251", frozen=True)
            decimal: str = Field(default=",")  # Европейский формат
        
        return ExcelCSVConfig
```

### Кастомная постобработка

```python
class DataCleaningExtractor(CSVExtractor):
    def _post_process_data(self, data):
        """Дополнительная очистка данных"""
        if isinstance(data, pd.DataFrame):
            # Удаляем пробелы в строковых колонках
            for col in data.select_dtypes(include=['object']).columns:
                data[col] = data[col].str.strip()
            
            # Стандартизируем email
            if 'email' in data.columns:
                data['email'] = data['email'].str.lower()
        
        return super()._post_process_data(data)
```

## 🧪 Тестирование

```bash
# Запуск всех тестов
cd packages/components/extractors
uv run pytest tests/test_csv_extractor.py -v

# Запуск конкретного теста
uv run pytest tests/test_csv_extractor.py::TestCSVExtractorBasic::test_basic_extraction_pandas -v

# Тесты с покрытием
uv run pytest tests/test_csv_extractor.py --cov=src/pipeline_extractors --cov-report=html
```

## 🎯 Лучшие практики

### ✅ Делайте правильно

1. **Всегда указывайте required_columns** для важных полей
2. **Используйте фильтрацию** для уменьшения объема данных
3. **Задавайте типы данных** явно для критичных колонок
4. **Используйте chunking** для файлов > 100MB
5. **Включайте валидацию** min_records/max_records

### ❌ Избегайте ошибок

1. **Не читайте все колонки** если нужны только некоторые
2. **Не игнорируйте ошибки** без error_tolerance
3. **Не используйте async с polars** - несовместимо
4. **Не полагайтесь на автоопределение типов** для критичных данных
5. **Не забывайте про кодировки** при работе с не-ASCII текстом

## 📞 Поддержка

При возникновении проблем:

1. Проверьте логи компонента
2. Убедитесь в корректности файла и кодировки
3. Протестируйте на малом образце данных
4. Включите skip_bad_lines для диагностики
5. Обратитесь к примерам в `examples/`

---

**Версия**: 0.1.0  
**Совместимость**: Python 3.11+  
**Зависимости**: pandas>=2.0.0  
**Лицензия**: Apache 2.0