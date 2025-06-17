# Pipeline Extractors

Компоненты для извлечения данных в pipeline фреймворке.

## Установка

```bash
# Базовая установка
pip install pipeline-extractors

# С дополнительными зависимостями
pip install pipeline-extractors[database,cloud,api,formats,all]
```

## Доступные экстракторы

### File Extractors

#### CSV Extractor
Извлечение данных из CSV файлов с поддержкой потокового чтения.

```yaml
extract_csv:
  type: "csv-extractor"
  config:
    file_path: "/path/to/data.csv"
    delimiter: ","
    encoding: "utf-8"
    has_header: true
    columns: ["name", "age", "salary"]  # опционально
    streaming: true
    chunk_size: 5000
    output_format: "pandas"
```

**Поддерживаемые параметры:**
- `file_path` - путь к файлу или URL
- `delimiter` - разделитель CSV (по умолчанию ",")
- `encoding` - кодировка файла (по умолчанию "utf-8")
- `has_header` - есть ли заголовки (по умолчанию true)
- `skip_rows` - пропустить N строк
- `columns` - список колонок для извлечения
- `exclude_columns` - список колонок для исключения
- `streaming` - потоковое чтение для больших файлов
- `chunk_size` - размер chunk для потокового чтения
- `infer_schema` - автоматически определять типы
- `dtypes` - явное указание типов колонок

#### JSON Extractor (планируется)
Извлечение данных из JSON файлов.

#### Excel Extractor (планируется) 
Извлечение данных из Excel файлов.

### API Extractors

#### HTTP Extractor
Извлечение данных из HTTP/REST API с поддержкой пагинации.

```yaml
extract_api:
  type: "http-extractor"
  config:
    base_url: "https://api.example.com"
    endpoint: "/users"
    method: "GET"
    headers:
      Authorization: "Bearer ${API_TOKEN}"
    params:
      limit: 100
    
    # Пагинация
    pagination_enabled: true
    pagination_type: "offset"  # offset, cursor, page
    pagination_param: "offset"
    pagination_size_param: "limit"
    pagination_size: 100
    
    # Обработка ответа
    response_data_path: "data"  # JSONPath к данным
    response_format: "json"
    output_format: "pandas"
```

**Поддерживаемые параметры:**
- `base_url` - базовый URL API
- `endpoint` - конечная точка
- `method` - HTTP метод (GET, POST, etc.)
- `headers` - HTTP заголовки
- `params` - query параметры
- `data` - данные для POST/PUT запросов
- `auth_type` - тип аутентификации (bearer, basic, api_key)
- `auth_token` - токен для аутентификации
- `timeout` - таймаут запроса
- `pagination_enabled` - включить пагинацию
- `pagination_type` - тип пагинации
- `response_data_path` - путь к данным в ответе

### Database Extractors (планируется)

#### SQL Extractor
Извлечение данных из SQL баз данных.

#### PostgreSQL Extractor
Специализированный экстрактор для PostgreSQL.

#### MongoDB Extractor
Извлечение данных из MongoDB.

## Базовые возможности

Все экстракторы наследуют от `BaseExtractor` и поддерживают:

### Retry механизм
```yaml
config:
  retry_attempts: 3
  retry_delay: 1.0
  retry_exponential_base: 2.0
```

### Batch обработка
```yaml
config:
  batch_size: 1000
  max_records: 10000
```

### Форматы вывода
```yaml
config:
  output_format: "pandas"  # pandas, polars, dict, list
```

### Потоковое чтение
Для больших файлов и API с пагинацией:

```yaml
config:
  streaming: true
  chunk_size: 5000
```

## Создание кастомного экстрактора

```python
from pipeline_extractors import BaseExtractor, ExtractorConfig
from pipeline_core import register_component, ExecutionContext
from pydantic import Field

class MyExtractorConfig(ExtractorConfig):
    type: str = Field(default="my-extractor", const=True)
    source_path: str = Field(..., description="Путь к источнику данных")

@register_component("my-extractor")
class MyExtractor(BaseExtractor):
    def get_config_model(self):
        return MyExtractorConfig
    
    def extract_data(self, context: ExecutionContext):
        config = self.config
        # Ваша логика извлечения данных
        data = self._load_from_source(config.source_path)
        return data
    
    def _load_from_source(self, path):
        # Реализация загрузки
        pass
```

## Примеры использования

### Простое извлечение CSV
```python
from pipeline_extractors import CSVExtractor
from pipeline_core import ExecutionContext

config = {
    "type": "csv-extractor",
    "file_path": "data.csv",
    "output_format": "pandas"
}

extractor = CSVExtractor(config)
context = ExecutionContext()
result = extractor.execute(context)

print(f"Извлечено {result.processed_records} записей")
print(result.data.head())
```

### API с аутентификацией
```python
from pipeline_extractors import HTTPExtractor

config = {
    "type": "http-extractor",
    "base_url": "https://api.github.com",
    "endpoint": "/user/repos",
    "headers": {
        "Authorization": "token your_token_here"
    },
    "pagination_enabled": True,
    "pagination_type": "page",
    "output_format": "pandas"
}

extractor = HTTPExtractor(config)
result = extractor.execute(context)
```

## Развитие

Планируемые экстракторы:
- **Database**: PostgreSQL, MySQL, MongoDB, Cassandra
- **Cloud Storage**: S3, GCS, Azure Blob
- **Message Queues**: Kafka, RabbitMQ, Redis
- **File Formats**: Parquet, Avro, ORC
- **APIs**: GraphQL, SOAP, gRPC

## Лицензия

Apache License 2.0