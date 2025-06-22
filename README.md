# Pipeline Framework

[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Tests](https://github.com/company/pipeline-framework/workflows/Tests/badge.svg)](https://github.com/company/pipeline-framework/actions)
[![Coverage](https://codecov.io/gh/company/pipeline-framework/branch/main/graph/badge.svg)](https://codecov.io/gh/company/pipeline-framework)

Современный, модульный framework для создания надежных data workflows с интеграцией Temporal для distributed orchestration.

## 🚀 Ключевые возможности

- **Plugin-Based архитектура** - Легко расширяемая система компонентов
- **Temporal Integration** - Надежная оркестрация через Temporal Workflow Service
- **YAML Configuration** - Декларативное описание pipeline с поддержкой template
- **Auto-Discovery** - Автоматическое обнаружение компонентов через entry points
- **Type Safety** - Полная поддержка type hints и Pydantic валидации
- **Observability** - Structured logging, Prometheus метрики, distributed tracing
- **Developer Experience** - CLI инструменты, hot reload, богатая документация

## 📦 Установка

### Базовая установка

```bash
# Через uv (рекомендуется)
uv add pipeline-framework

# Через pip
pip install pipeline-framework
```

### Установка с дополнительными компонентами

```bash
# С SQL компонентами
uv add "pipeline-framework[sql]"

# С Temporal интеграцией
uv add "pipeline-framework[temporal]"

# Полная установка
uv add "pipeline-framework[all]"
```

### Разработка

```bash
# Клонирование репозитория
git clone https://github.com/company/pipeline-framework.git
cd pipeline-framework

# Установка в режиме разработки
uv sync --group dev

# Запуск тестов
uv run pytest

# Проверка линтинга
uv run ruff check .
```

## 🏁 Быстрый старт

### 1. Создание нового проекта

```bash
# Создание нового pipeline проекта
pipeline init my-data-pipeline

cd my-data-pipeline
```

### 2. Конфигурация pipeline

Создайте `pipeline.yaml`:

```yaml
pipeline:
  name: "customer-analytics"
  version: "1.0.0"
  description: "Customer data processing pipeline"
  
  variables:
    database_url: "${DATABASE_URL}"
    batch_size: 10000
  
  stages:
    - name: "extract-customers"
      component: "extractor/sql"
      config:
        connection_string: "${database_url}"
        query: "SELECT * FROM customers WHERE updated_at > :last_run"
        output_format: "pandas"
      timeout: "10m"
      
    - name: "transform-data"
      component: "transformer/pandas"
      depends_on: ["extract-customers"]
      config:
        script_content: |
          def transform(df):
              # Ваша логика трансформации
              df['full_name'] = df['first_name'] + ' ' + df['last_name']
              return df.drop_duplicates()
      timeout: "15m"
      
    - name: "load-warehouse"
      component: "loader/sql"
      depends_on: ["transform-data"]
      config:
        connection_string: "${WAREHOUSE_URL}"
        target_table: "dim_customers"
        write_mode: "upsert"
        upsert_keys: ["customer_id"]
      timeout: "10m"
```

### 3. Валидация и запуск

```bash
# Валидация конфигурации
pipeline validate pipeline.yaml

# Локальный запуск
pipeline run pipeline.yaml

# Запуск с отладкой
pipeline run pipeline.yaml --dry-run

# Развертывание в Temporal
pipeline deploy pipeline.yaml --env production
```

## 🧩 Архитектура

Pipeline Framework построен на модульной архитектуре с четкими абстракциями:

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Extractors    │───▶│  Transformers   │───▶│    Loaders      │
│                 │    │                 │    │                 │
│ • SQL           │    │ • Pandas        │    │ • SQL           │
│ • API           │    │ • Polars        │    │ • File          │
│ • File          │    │ • Custom        │    │ • API           │
└─────────────────┘    └─────────────────┘    └─────────────────┘
        │                       │                       │
        └───────────────────────┼───────────────────────┘
                                │
                    ┌─────────────────┐
                    │ Pipeline Core   │
                    │                 │
                    │ • Registry      │
                    │ • Config        │
                    │ • Executor      │
                    │ • Observability │
                    └─────────────────┘
                                │
                    ┌─────────────────┐
                    │ Temporal        │
                    │ Integration     │
                    │                 │
                    │ • Workflows     │
                    │ • Activities    │
                    │ • Scheduling    │
                    └─────────────────┘
```

### Основные компоненты

- **Extractors** - Извлечение данных из различных источников
- **Transformers** - Трансформация и обогащение данных
- **Loaders** - Загрузка данных в целевые системы
- **Validators** - Проверка качества данных
- **Stages** - Произвольные операции

## 📚 Документация

### Создание собственных компонентов

#### Extractor

```python
from pipeline_core.components import BaseExtractor, ExecutionContext
from pydantic import BaseModel

class MyExtractorConfig(BaseModel):
    source_path: str
    format: str = "json"

class MyExtractor(BaseExtractor[dict, MyExtractorConfig]):
    @property
    def name(self) -> str:
        return "my-extractor"
    
    async def _execute_impl(self, context: ExecutionContext) -> dict:
        # Ваша логика извлечения данных
        with open(self.config.source_path) as f:
            return json.load(f)
```

#### Transformer

```python
from pipeline_core.components import BaseTransformer
import pandas as pd

class MyTransformer(BaseTransformer[pd.DataFrame, MyTransformerConfig]):
    @property
    def name(self) -> str:
        return "my-transformer"
    
    async def _execute_impl(self, context: ExecutionContext) -> pd.DataFrame:
        # Получаем данные из предыдущего этапа
        input_data = context.get_previous_result("extract-stage").data
        
        # Ваша логика трансформации
        result = input_data.copy()
        result['processed_at'] = pd.Timestamp.now()
        
        return result
```

#### Регистрация компонентов

```python
# Через декоратор
from pipeline_core.registry import register_component

@register_component("extractor", "my-custom")
class MyCustomExtractor(BaseExtractor):
    pass

# Через entry points в pyproject.toml
[project.entry-points."pipeline_framework.components"]
my-extractor = "my_package.extractors:MyExtractor"
```

### Конфигурация

#### Переменные окружения

```yaml
pipeline:
  variables:
    # Простая подстановка
    database_url: "${DATABASE_URL}"
    
    # С значением по умолчанию
    batch_size: "${BATCH_SIZE:1000}"
    
    # Jinja2 template
    today: "{{ ds }}"
    formatted_date: "{{ now().strftime('%Y-%m-%d') }}"
```

#### Includes и наследование

```yaml
# base-config.yaml
defaults: &defaults
  timeout: "30m"
  retry_policy:
    maximum_attempts: 3

# pipeline.yaml  
include:
  - "base-config.yaml"

pipeline:
  stages:
    - name: "my-stage"
      <<: *defaults
      component: "extractor/sql"
```

#### Environment-specific настройки

```yaml
pipeline:
  # ... основная конфигурация
  
environments:
  development:
    variables:
      batch_size: 100
    default_retry_policy:
      maximum_attempts: 1
      
  production:
    variables:
      batch_size: 10000
    monitoring:
      alert_on_failure: true
```

### Temporal Integration

#### Локальный запуск Temporal

```bash
# Запуск Temporal Server
temporal server start-dev

# Развертывание pipeline
pipeline deploy pipeline.yaml --temporal localhost:7233
```

#### Мониторинг через Temporal UI

Доступно по адресу: http://localhost:8233

### CLI Commands

```bash
# Управление проектами
pipeline init <name>                    # Создать новый проект
pipeline validate <config>              # Валидировать конфигурацию
pipeline run <config>                   # Запустить pipeline локально
pipeline deploy <config>                # Развернуть в Temporal

# Управление компонентами  
pipeline list                           # Список доступных компонентов
pipeline test --component <name>        # Тестировать компонент
pipeline dev <config>                   # Development сервер

# Утилиты
pipeline info                           # Информация о framework
```

## 🔍 Observability

### Structured Logging

```python
import structlog

logger = structlog.get_logger(__name__)

logger.info(
    "Processing customer data",
    customer_id=123,
    batch_size=1000,
    pipeline_id=context.pipeline_id
)
```

### Prometheus Метрики

Автоматически экспортируемые метрики:

- `pipeline_component_executions_total` - Количество выполнений компонентов
- `pipeline_component_duration_seconds` - Длительность выполнения
- `pipeline_data_processed_rows_total` - Количество обработанных строк
- `pipeline_errors_total` - Количество ошибок

### Custom Метрики

```python
from pipeline_core.observability import MetricsCollector

metrics = MetricsCollector()

# Counter
metrics.increment_counter("custom_events_total", {"event_type": "user_signup"})

# Histogram
metrics.observe_histogram("processing_duration", 0.5, {"component": "transformer"})

# Gauge
metrics.set_gauge("queue_size", 100, {"queue_name": "high_priority"})
```

## 🧪 Тестирование

### Unit тесты компонентов

```python
import pytest
from pipeline_core.components import ExecutionContext
from my_package.extractors import MyExtractor

@pytest.mark.asyncio
async def test_my_extractor():
    config = MyExtractorConfig(source_path="test_data.json")
    extractor = MyExtractor(config)
    
    context = ExecutionContext(
        pipeline_id="test",
        stage_name="test_extract"
    )
    
    result = await extractor.execute(context)
    
    assert result.success
    assert len(result.data) > 0
```

### Integration тесты

```python
@pytest.mark.asyncio
async def test_full_pipeline():
    from pipeline_core.config import load_pipeline_config
    from pipeline_core.pipeline import Pipeline
    
    config = load_pipeline_config("test_pipeline.yaml")
    pipeline = Pipeline(config)
    
    result = await pipeline.run()
    
    assert result.success
    assert len(result.stage_results) == 3
```

### Тестирование с Docker

```yaml
# docker-compose.test.yml
version: '3.8'
services:
  postgres:
    image: postgres:15
    environment:
      POSTGRES_DB: test
      POSTGRES_PASSWORD: test
  
  pipeline-test:
    build: .
    depends_on: [postgres]
    environment:
      DATABASE_URL: postgresql://postgres:test@postgres/test
    command: pytest tests/
```

## 🛠️ Development

### Настройка окружения

```bash
# Установка зависимостей
uv sync --group dev

# Pre-commit hooks
pre-commit install

# Настройка IDE (VSCode)
cp .vscode/settings.example.json .vscode/settings.json
```

### Code Quality

Проект использует:

- **Ruff** - Линтинг и форматирование кода
- **Pyright** - Type checking
- **Pytest** - Тестирование с asyncio поддержкой
- **Pre-commit** - Git hooks для качества кода

```bash
# Проверка всего кода
make lint
make test
make type-check

# Или через uv
uv run ruff check .
uv run pytest
uv run pyright
```

### Добавление новых компонентов

1. Создайте новый пакет в `packages/components/`
2. Наследуйте от соответствующего базового класса
3. Добавьте entry point в `pyproject.toml`
4. Напишите тесты
5. Обновите документацию

### Release Process

```bash
# Bump версии
uv run bump2version minor

# Создание release
git tag v1.2.0
git push origin v1.2.0

# Публикация в PyPI
uv build
uv publish
```

## 🤝 Contributing

Мы приветствуем вклад в развитие проекта! 

### Как внести изменения

1. Fork репозиторий
2. Создайте feature branch: `git checkout -b feature/amazing-feature`
3. Внесите изменения и добавьте тесты
4. Убедитесь что все тесты проходят: `make test`
5. Commit изменения: `git commit -m 'Add amazing feature'`
6. Push branch: `git push origin feature/amazing-feature`
7. Создайте Pull Request

### Coding Standards

- Используйте type hints для всех функций
- Покрытие тестами > 85%
- Следуйте PEP 8 (автоматически через Ruff)
- Документируйте публичные API в Google docstring формате
- Добавляйте structured logging для важных операций

## 📄 Лицензия

Этот проект лицензирован под MIT License - см. файл [LICENSE](LICENSE) для подробностей.

## 🙋‍♂️ Поддержка

- 📧 Email: [support@company.com](mailto:support@company.com)
- 💬 Discussions: [GitHub Discussions](https://github.com/company/pipeline-framework/discussions)
- 🐛 Issues: [GitHub Issues](https://github.com/company/pipeline-framework/issues)
- 📖 Документация: [https://pipeline-framework.readthedocs.io](https://pipeline-framework.readthedocs.io)

## 🗺️ Roadmap

### v1.1 (Q2 2024)
- [ ] Polars transformer поддержка
- [ ] ClickHouse connector
- [ ] Web UI для мониторинга
- [ ] Schema evolution поддержка

### v1.2 (Q3 2024)
- [ ] Apache Iceberg integration
- [ ] Real-time streaming поддержка
- [ ] dbt integration
- [ ] GraphQL API для метаданных

### v2.0 (Q4 2024)
- [ ] Kubernetes operator
- [ ] Auto-scaling capabilities
- [ ] ML model deployment integration
- [ ] Multi-tenant support

---

## 🏆 Acknowledgments

Проект вдохновлен лучшими практиками из:

- [Apache Airflow](https://airflow.apache.org/) - Workflow orchestration
- [Prefect](https://prefect.io/) - Modern data stack
- [dbt](https://getdbt.com/) - Analytics engineering
- [Temporal](https://temporal.io/) - Reliable workflows

**Создано с ❤️ командой Data Engineering**