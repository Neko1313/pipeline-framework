# SQL Extractor

[![Python Version](https://img.shields.io/badge/python-3.13+-blue.svg)](https://www.python.org/downloads/)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)
[![Tests](https://github.com/company/pipeline-framework/workflows/Tests/badge.svg)](https://github.com/company/pipeline-framework/actions)
[![Coverage](https://codecov.io/gh/company/pipeline-framework/branch/main/graph/badge.svg)](https://codecov.io/gh/company/pipeline-framework)

Мощный и гибкий SQL Extractor для извлечения данных из реляционных баз данных в рамках pipeline framework. Поддерживает асинхронные операции, connection pooling, retry механизмы и интеграцию с Temporal для orchestration.

## 🚀 Ключевые возможности

- **Поддержка множества БД**: PostgreSQL, MySQL, SQLite, Oracle, SQL Server, Snowflake, BigQuery
- **Асинхронное выполнение**: Высокая производительность благодаря asyncio и SQLAlchemy async
- **Connection Pooling**: Эффективное управление соединениями с базой данных
- **Retry механизмы**: Автоматические повторы при временных сбоях
- **Множественные форматы данных**: pandas, polars, dict, raw данные
- **Streaming поддержка**: Обработка больших dataset'ов по частям
- **Мониторинг и метрики**: Интеграция с Prometheus и structured logging
- **Type Safety**: Полная поддержка type hints и Pydantic валидации
- **CLI интерфейс**: Удобные команды для тестирования и отладки

## 📦 Установка

### Базовая установка

```bash
# Через uv (рекомендуется)
uv add extractor-sql

# Через pip
pip install extractor-sql
```

### С дополнительными драйверами БД

```bash
# PostgreSQL
uv add "extractor-sql[postgresql]"

# MySQL
uv add "extractor-sql[mysql]"

# Oracle
uv add "extractor-sql[oracle]"

# SQL Server
uv add "extractor-sql[mssql]"

# Snowflake
uv add "extractor-sql[snowflake]"

# BigQuery
uv add "extractor-sql[bigquery]"

# Все драйверы
uv add "extractor-sql[all]"
```

### Для разработки

```bash
# Клонирование репозитория
git clone https://github.com/company/pipeline-framework.git
cd pipeline-framework

# Установка в режиме разработки
uv sync --group dev
```

## 🏁 Быстрый старт

### Простой пример

```python
import asyncio
from extractor_sql import SQLExtractor, SQLExtractorConfig, QueryConfig

async def main():
    # Создание конфигурации
    config = SQLExtractorConfig(
        connection_string="postgresql+asyncpg://user:password@localhost:5432/mydb",
        query_config=QueryConfig(
            query="SELECT * FROM users WHERE created_at > :start_date",
            parameters={"start_date": "2024-01-01"}
        ),
        output_format="pandas"
    )
    
    # Создание и инициализация extractor'а
    extractor = SQLExtractor(config)
    await extractor.initialize()
    
    # Выполнение извлечения данных
    from pipeline_core.components.base import ExecutionContext
    context = ExecutionContext(pipeline_id="demo", stage_name="extract_users")
    
    result = await extractor.execute(context)
    
    if result.success:
        print(f"Извлечено {len(result.data)} строк")
        print(result.data.head())
    else:
        print(f"Ошибка: {result.error}")
    
    # Очистка ресурсов
    await extractor.cleanup()

# Запуск
asyncio.run(main())
```

### Использование фабричной функции

```python
from extractor_sql import create_extractor

# Простое создание extractor'а
extractor = create_extractor(
    connection_string="sqlite+aiosqlite:///data.db",
    query="SELECT name, age FROM users",
    output_format="polars",
    fetch_size=5000
)
```

## 🛠️ Конфигурация

### Основные параметры

```python
from extractor_sql import (
    SQLExtractorConfig,
    QueryConfig,
    ConnectionPoolConfig,
    RetryConfig
)

config = SQLExtractorConfig(
    # Подключение к БД
    connection_string="postgresql+asyncpg://user:pass@localhost:5432/db",
    dialect="postgresql",  # Автоопределение если не указан
    
    # Конфигурация запроса
    query_config=QueryConfig(
        query="SELECT * FROM users WHERE department = :dept",
        parameters={"dept": "Engineering"},
        timeout=300.0,  # 5 минут
        fetch_size=10000,
        stream_results=False
    ),
    
    # Формат выходных данных
    output_format="pandas",  # pandas, polars, dict, raw
    chunk_size=None,  # Для streaming
    
    # Connection pooling
    pool_config=ConnectionPoolConfig(
        pool_size=5,
        max_overflow=10,
        pool_timeout=30.0,
        pool_recycle=3600,  # 1 час
        pool_pre_ping=True
    ),
    
    # Retry политика
    retry_config=RetryConfig(
        max_attempts=3,
        initial_wait=1.0,
        max_wait=60.0,
        multiplier=2.0,
        jitter=True
    ),
    
    # Дополнительные опции SQLAlchemy
    engine_options={"echo": False},
    
    # SSL конфигурация
    ssl_config={"sslmode": "require"}
)
```

### Конфигурация через YAML

```yaml
# config.yaml
connection_string: "postgresql+asyncpg://user:password@localhost:5432/mydb"
dialect: "postgresql"

query_config:
  query: |
    SELECT 
      u.name,
      u.email,
      d.name as department_name,
      COUNT(o.id) as order_count
    FROM users u
    JOIN departments d ON u.department_id = d.id
    LEFT JOIN orders o ON u.id = o.user_id
    WHERE u.is_active = :is_active
    GROUP BY u.id, u.name, u.email, d.name
    ORDER BY order_count DESC
  parameters:
    is_active: true
  timeout: 600.0
  fetch_size: 5000
  stream_results: false

output_format: "pandas"

pool_config:
  pool_size: 10
  max_overflow: 20
  pool_timeout: 30.0
  pool_recycle: 3600
  pool_pre_ping: true

retry_config:
  max_attempts: 3
  initial_wait: 1.0
  max_wait: 60.0
  multiplier: 2.0
  jitter: true

engine_options:
  echo: false
  isolation_level: "READ_COMMITTED"
```

```python
# Загрузка из YAML
import yaml
from extractor_sql import SQLExtractorConfig

with open("config.yaml") as f:
    config_data = yaml.safe_load(f)

config = SQLExtractorConfig(**config_data)
extractor = SQLExtractor(config)
```

## 🗃️ Поддерживаемые базы данных

### PostgreSQL

```python
from extractor_sql import PostgresSQLExtractor, SQLExtractorConfig

config = SQLExtractorConfig(
    connection_string="postgresql+asyncpg://user:pass@localhost:5432/db",
    query_config=QueryConfig(query="SELECT * FROM users")
)

extractor = PostgresSQLExtractor(config)
```

**Connection strings примеры:**
- `postgresql://user:password@localhost:5432/database`
- `postgresql+asyncpg://user:password@localhost:5432/database`
- `postgresql+asyncpg://user:password@localhost:5432/database?sslmode=require`

### MySQL

```python
from extractor_sql import MySQLExtractor, SQLExtractorConfig

config = SQLExtractorConfig(
    connection_string="mysql+aiomysql://user:pass@localhost:3306/db",
    query_config=QueryConfig(query="SELECT * FROM users")
)

extractor = MySQLExtractor(config)
```

**Connection strings примеры:**
- `mysql://user:password@localhost:3306/database`
- `mysql+aiomysql://user:password@localhost:3306/database`
- `mysql+asyncmy://user:password@localhost:3306/database`

### SQLite

```python
from extractor_sql import SQLiteExtractor, SQLExtractorConfig

config = SQLExtractorConfig(
    connection_string="sqlite+aiosqlite:///path/to/database.db",
    query_config=QueryConfig(query="SELECT * FROM users")
)

extractor = SQLiteExtractor(config)
```

**Connection strings примеры:**
- `sqlite+aiosqlite:///absolute/path/to/database.db`
- `sqlite+aiosqlite:///./relative/path/to/database.db`
- `sqlite+aiosqlite:///:memory:` (in-memory database)

### Snowflake

```python
config = SQLExtractorConfig(
    connection_string="snowflake://user:pass@account.region/database/schema?warehouse=wh&role=role",
    query_config=QueryConfig(query="SELECT * FROM users")
)
```

### BigQuery

```python
config = SQLExtractorConfig(
    connection_string="bigquery://project/dataset",
    query_config=QueryConfig(query="SELECT * FROM `project.dataset.table`"),
    engine_options={
        "credentials_path": "/path/to/service-account.json"
    }
)
```

## 📊 Форматы выходных данных

### pandas DataFrame (по умолчанию)

```python
config = SQLExtractorConfig(
    connection_string="...",
    query_config=QueryConfig(query="SELECT * FROM users"),
    output_format="pandas"
)

result = await extractor.execute(context)
df = result.data  # pandas.DataFrame
print(df.dtypes)
print(df.describe())
```

### polars DataFrame

```python
config = SQLExtractorConfig(
    # ...
    output_format="polars"
)

result = await extractor.execute(context)
df = result.data  # polars.DataFrame
print(df.schema)
print(df.describe())
```

### Словари (Records)

```python
config = SQLExtractorConfig(
    # ...
    output_format="dict"
)

result = await extractor.execute(context)
records = result.data  # List[Dict[str, Any]]
for record in records:
    print(record)
```

### Raw данные

```python
config = SQLExtractorConfig(
    # ...
    output_format="raw"
)

result = await extractor.execute(context)
rows = result.data  # List[Tuple]
for row in rows:
    print(row)
```

## 🔄 Streaming больших dataset'ов

Для обработки больших объемов данных используйте streaming:

```python
config = SQLExtractorConfig(
    connection_string="postgresql+asyncpg://user:pass@localhost:5432/db",
    query_config=QueryConfig(
        query="SELECT * FROM large_table",
        fetch_size=10000,  # Размер batch'а
        stream_results=True
    ),
    chunk_size=5000,  # Размер chunk'а для processing
    output_format="pandas"
)

extractor = SQLExtractor(config)
await extractor.initialize()

result = await extractor.execute(context)

# Результат будет содержать все данные, но они извлекались по частям
print(f"Извлечено {len(result.data)} строк")
```

## 🔧 CLI интерфейс

SQL Extractor включает мощный CLI для тестирования и отладки:

### Тестирование подключения

```bash
extractor-sql test-connection "postgresql://user:pass@localhost:5432/db"
```

### Извлечение данных

```bash
extractor-sql extract \
  "postgresql://user:pass@localhost:5432/db" \
  "SELECT * FROM users WHERE age > 25" \
  --output-format pandas \
  --output results.csv \
  --timeout 300 \
  --verbose
```

### Анализ запроса

```bash
extractor-sql analyze-query "SELECT u.*, COUNT(o.id) FROM users u LEFT JOIN orders o ON u.id = o.user_id GROUP BY u.id"
```

### Запуск из конфигурационного файла

```bash
extractor-sql run-from-config config.yaml --show-data --max-rows 20
```

### Генерация шаблона конфигурации

```bash
extractor-sql generate-config config.yaml \
  --connection-string "postgresql://user:pass@localhost/db" \
  --query "SELECT * FROM users" \
  --format yaml
```

### Показать версию

```bash
extractor-sql version
```

## 📈 Мониторинг и метрики

SQL Extractor автоматически собирает метрики Prometheus:

### Доступные метрики

- `sql_extractor_queries_total` - Общее количество выполненных запросов
- `sql_extractor_query_duration_seconds` - Длительность выполнения запросов
- `sql_extractor_rows_extracted_total` - Общее количество извлеченных строк
- `sql_extractor_active_connections` - Количество активных соединений

### Структурированное логирование

```python
import structlog

# Логи автоматически содержат контекст
logger = structlog.get_logger(__name__)

# Пример вывода:
# 2024-01-20T10:30:45.123Z [info] SQL data extraction completed 
#   name=postgresql-extractor rows=15420 duration=45.67 pipeline_id=user_analytics
```

### Интеграция с Grafana

Создайте дашборд в Grafana для мониторинга:

```promql
# Запросы в секунду
rate(sql_extractor_queries_total[5m])

# Средняя длительность запросов
rate(sql_extractor_query_duration_seconds_sum[5m]) / 
rate(sql_extractor_query_duration_seconds_count[5m])

# Строки в секунду
rate(sql_extractor_rows_extracted_total[5m])

# Активные соединения
sql_extractor_active_connections
```

## 🔍 Отладка и устранение неисправностей

### Включение debug логирования

```python
import logging
import structlog

# Настройка логирования
logging.basicConfig(level=logging.DEBUG)
structlog.configure(
    processors=[
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.dev.ConsoleRenderer()
    ],
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
    wrapper_class=structlog.stdlib.BoundLogger,
    cache_logger_on_first_use=True,
)

# Включение debug для SQLAlchemy
config = SQLExtractorConfig(
    # ...
    engine_options={"echo": True}  # Показывать все SQL запросы
)
```

### Общие проблемы и решения

#### 1. Ошибки подключения

```python
# Проверка connection string
from extractor_sql.utils import validate_connection_string

is_valid, error = validate_connection_string("your-connection-string")
if not is_valid:
    print(f"Ошибка: {error}")
```

#### 2. Медленные запросы

```python
# Анализ запроса
from extractor_sql.utils import estimate_query_cost

cost = estimate_query_cost("your-sql-query")
print(f"Сложность: {cost['complexity']}")
print(f"JOIN'ов: {cost['join_count']}")
print(f"Подзапросов: {cost['subquery_count']}")
```

#### 3. Проблемы с памятью

```python
# Оптимизация DataFrame
from extractor_sql.utils import optimize_dataframe_memory

optimized_df = optimize_dataframe_memory(result.data)
print(f"Экономия памяти: {original_size - optimized_size} bytes")
```

#### 4. Timeout'ы

```python
# Увеличение timeout'ов
config = SQLExtractorConfig(
    # ...
    query_config=QueryConfig(
        query="...",
        timeout=1800.0  # 30 минут
    ),
    pool_config=ConnectionPoolConfig(
        pool_timeout=60.0  # 1 минута на получение соединения
    )
)
```

## 🧪 Тестирование

### Запуск тестов

```bash
# Все тесты
pytest

# Только unit тесты
pytest -m "unit"

# Только integration тесты (требуют Docker)
pytest -m "integration"

# Исключить медленные тесты
pytest -m "not slow"

# С покрытием кода
pytest --cov=extractor_sql --cov-report=html

# Параллельный запуск
pytest -n auto
```

### Тестирование с реальными БД

```bash
# PostgreSQL (через testcontainers)
pytest -m "postgresql" 

# MySQL (через testcontainers)
pytest -m "mysql"

# SQLite (локально)
pytest -m "sqlite"
```

### Создание собственных тестов

```python
import pytest
from extractor_sql import SQLExtractor, SQLExtractorConfig, QueryConfig

@pytest.mark.asyncio
async def test_my_extraction():
    config = SQLExtractorConfig(
        connection_string="sqlite+aiosqlite:///:memory:",
        query_config=QueryConfig(query="SELECT 1 as test_column")
    )
    
    extractor = SQLExtractor(config)
    await extractor.initialize()
    
    from pipeline_core.components.base import ExecutionContext
    context = ExecutionContext(pipeline_id="test", stage_name="test")
    
    result = await extractor.execute(context)
    
    assert result.success is True
    assert len(result.data) == 1
    assert result.data.iloc[0]["test_column"] == 1
    
    await extractor.cleanup()
```

## 🔧 Расширение и кастомизация

### Создание кастомного extractor'а

```python
from extractor_sql import SQLExtractor, SQLExtractorConfig
from typing import Optional

class CustomSQLExtractor(SQLExtractor):
    """Кастомный SQL Extractor с дополнительной логикой"""
    
    def __init__(self, config: SQLExtractorConfig, **kwargs):
        super().__init__(config, name="custom-sql-extractor", **kwargs)
    
    async def _preprocess_query(self, query: str) -> str:
        """Предобработка запроса перед выполнением"""
        # Добавляем кастомную логику
        if "users" in query.lower():
            query += " AND is_deleted = false"
        return query
    
    async def _postprocess_data(self, data):
        """Постобработка данных после извлечения"""
        if hasattr(data, "columns"):
            # Конвертируем имена колонок в snake_case
            data.columns = [col.lower().replace(" ", "_") for col in data.columns]
        return data
    
    async def execute(self, context):
        """Переопределяем execute для добавления кастомной логики"""
        # Предобработка
        original_query = self.config.query_config.query
        self.config.query_config.query = await self._preprocess_query(original_query)
        
        # Выполняем базовую логику
        result = await super().execute(context)
        
        # Постобработка
        if result.success and result.data is not None:
            result.data = await self._postprocess_data(result.data)
        
        # Восстанавливаем оригинальный запрос
        self.config.query_config.query = original_query
        
        return result
```

### Кастомные форматы данных

```python
from extractor_sql.components import SQLExtractor
import pyarrow as pa

class ArrowSQLExtractor(SQLExtractor):
    """SQL Extractor с поддержкой Apache Arrow"""
    
    async def _format_output(self, data):
        """Переопределяем форматирование для поддержки Arrow"""
        if self.config.output_format == "arrow":
            # Конвертируем pandas DataFrame в Arrow Table
            return pa.Table.from_pandas(data)
        else:
            return await super()._format_output(data)
```

## 📚 Примеры использования

### 1. ETL Pipeline с Temporal

```python
from temporalio import workflow, activity
from extractor_sql import SQLExtractor, SQLExtractorConfig, QueryConfig

@activity.defn
async def extract_user_data() -> dict:
    config = SQLExtractorConfig(
        connection_string=os.getenv("SOURCE_DB_URL"),
        query_config=QueryConfig(
            query="""
            SELECT user_id, name, email, last_login_at
            FROM users 
            WHERE updated_at > :last_sync
            """,
            parameters={"last_sync": datetime.now() - timedelta(days=1)}
        )
    )
    
    extractor = SQLExtractor(config)
    await extractor.initialize()
    
    context = ExecutionContext(
        pipeline_id="user_sync",
        stage_name="extract_users"
    )
    
    result = await extractor.execute(context)
    await extractor.cleanup()
    
    return {
        "data": result.data.to_dict("records"),
        "metadata": result.metadata.to_dict()
    }

@workflow.defn
class UserSyncWorkflow:
    @workflow.run
    async def run(self) -> str:
        # Извлечение
        user_data = await workflow.execute_activity(
            extract_user_data,
            start_to_close_timeout=timedelta(minutes=10)
        )
        
        # Дальнейшая обработка...
        return f"Синхронизировано {len(user_data['data'])} пользователей"
```

### 2. Мониторинг качества данных

```python
import pandas as pd
from extractor_sql import SQLExtractor, SQLExtractorConfig, QueryConfig

class DataQualityChecker:
    def __init__(self, config: SQLExtractorConfig):
        self.extractor = SQLExtractor(config)
    
    async def check_data_quality(self) -> dict:
        await self.extractor.initialize()
        
        checks = {}
        
        # Проверка на NULL значения
        null_check_config = SQLExtractorConfig(
            connection_string=self.extractor.config.connection_string,
            query_config=QueryConfig(
                query="""
                SELECT 
                    column_name,
                    COUNT(*) as total_rows,
                    SUM(CASE WHEN column_value IS NULL THEN 1 ELSE 0 END) as null_count
                FROM information_schema.columns
                WHERE table_name = :table_name
                """,
                parameters={"table_name": "users"}
            )
        )
        
        context = ExecutionContext(pipeline_id="quality_check", stage_name="null_check")
        result = await self.extractor.execute(context)
        
        if result.success:
            checks["null_analysis"] = result.data
        
        await self.extractor.cleanup()
        return checks
```

### 3. Динамические запросы

```python
from jinja2 import Template

class DynamicQueryExtractor:
    def __init__(self, connection_string: str):
        self.connection_string = connection_string
    
    async def extract_with_template(self, template_str: str, **kwargs):
        # Рендерим SQL шаблон
        template = Template(template_str)
        rendered_query = template.render(**kwargs)
        
        config = SQLExtractorConfig(
            connection_string=self.connection_string,
            query_config=QueryConfig(query=rendered_query)
        )
        
        extractor = SQLExtractor(config)
        await extractor.initialize()
        
        context = ExecutionContext(
            pipeline_id="dynamic_query",
            stage_name="template_extraction"
        )
        
        result = await extractor.execute(context)
        await extractor.cleanup()
        
        return result

# Использование
extractor = DynamicQueryExtractor("postgresql://...")

sql_template = """
SELECT {{ columns | join(', ') }}
FROM {{ table_name }}
WHERE {{ date_column }} BETWEEN '{{ start_date }}' AND '{{ end_date }}'
{% if filters %}
  AND {{ filters | join(' AND ') }}
{% endif %}
ORDER BY {{ order_by }}
"""

result = await extractor.extract_with_template(
    sql_template,
    columns=["id", "name", "email"],
    table_name="users",
    date_column="created_at",
    start_date="2024-01-01",
    end_date="2024-01-31",
    filters=["is_active = true", "department = 'Engineering'"],
    order_by="created_at DESC"
)
```

## 🤝 Участие в разработке

Мы приветствуем вклад в развитие проекта! 

### Настройка среды разработки

```bash
# Клонирование репозитория
git clone https://github.com/company/pipeline-framework.git
cd pipeline-framework

# Установка зависимостей
uv sync --group dev

# Установка pre-commit hooks
pre-commit install

# Запуск тестов
pytest
```

### Руководство по стилю кода

- Используем `ruff` для форматирования и линтинга
- Покрытие тестами должно быть > 85%
- Обязательные type hints
- Документация в формате Google docstrings

### Отправка изменений

1. Создайте feature branch: `git checkout -b feature/amazing-feature`
2. Внесите изменения и добавьте тесты
3. Убедитесь, что все тесты проходят: `pytest`
4. Проверьте линтинг: `ruff check .`
5. Зафиксируйте изменения: `git commit -m 'Add amazing feature'`
6. Отправьте branch: `git push origin feature/amazing-feature`
7. Создайте Pull Request

## 📄 Лицензия

Этот проект лицензирован под MIT License - см. файл [LICENSE](LICENSE) для подробностей.

## 🙋‍♂️ Поддержка

- 📧 Email: [support@company.com](mailto:support@company.com)
- 💬 Slack: [#pipeline-framework](https://company.slack.com/channels/pipeline-framework)
- 🐛 Issues: [GitHub Issues](https://github.com/company/pipeline-framework/issues)
- 📖 Документация: [https://docs.pipeline-framework.com](https://docs.pipeline-framework.com)

## 🗺️ Roadmap

- [ ] Поддержка дополнительных БД (ClickHouse, DuckDB)
- [ ] Интеграция с dbt для трансформаций
- [ ] Автоматическое schema discovery
- [ ] GraphQL endpoint для метаданных
- [ ] Интеграция с Apache Iceberg
- [ ] Real-time streaming через Kafka
- [ ] Web UI для мониторинга

---

## 📋 Changelog

### v0.1.0 (2024-01-20)

#### Added
- Базовая функциональность SQL Extractor
- Поддержка PostgreSQL, MySQL, SQLite
- Асинхронное выполнение с SQLAlchemy
- Connection pooling и retry механизмы
- CLI интерфейс
- Comprehensive test suite
- Документация и примеры

#### Security
- Маскирование паролей в логах
- Валидация SQL запросов
- Secure connection strings handling

---

*Создано с ❤️ командой Data Engineering*