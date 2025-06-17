# Pipeline Core

Ядро фреймворка для создания расширяемых data pipeline с поддержкой YAML конфигурации и модульной архитектуры компонентов.

## Основные возможности

- 🔧 **Модульная архитектура**: Компоненты как переиспользуемые строительные блоки
- 📝 **YAML конфигурация**: Декларативное описание pipeline с поддержкой шаблонов
- 🔍 **Автоматическое обнаружение**: Компоненты регистрируются через entry points
- 🛡️ **Типобезопасность**: Валидация конфигурации через Pydantic модели
- 📊 **DAG поддержка**: Автоматическое определение порядка выполнения стадий
- 🔄 **Управление зависимостями**: Проверка и разрешение зависимостей между стадиями
- 📋 **Структурированное логирование**: JSON логи с контекстом выполнения

## Быстрый старт

### Установка

```bash
pip install pipeline-core
```

### Создание простого компонента

```python
from pipeline_core import BaseComponent, ComponentConfig, ExecutionContext, ExecutionResult, ExecutionStatus, register_component
from pydantic import Field

class MyTransformConfig(ComponentConfig):
    type: str = Field(default="my-transform", const=True)
    source_stage: str
    multiplier: float = 1.0

@register_component("my-transform")
class MyTransformComponent(BaseComponent):
    def get_config_model(self):
        return MyTransformConfig
    
    def execute(self, context: ExecutionContext) -> ExecutionResult:
        # Получаем данные от предыдущей стадии
        data = context.get_stage_data(self.config.source_stage)
        
        # Выполняем трансформацию
        result = [x * self.config.multiplier for x in data]
        
        return ExecutionResult(
            status=ExecutionStatus.SUCCESS,
            data=result,
            processed_records=len(result)
        )
```

### YAML конфигурация pipeline

```yaml
metadata:
  name: "my-data-pipeline"
  description: "Пример обработки данных"
  version: "1.0.0"

variables:
  MULTIPLIER: 2.5
  ENV: "production"

# Стадия извлечения данных
extract:
  type: "data-extractor"
  config:
    source: "database"
    query: "SELECT * FROM users"

# Стадия трансформации
transform:
  type: "my-transform"
  depends_on: ["extract"]
  config:
    source_stage: "extract"
    multiplier: ${MULTIPLIER}

# Стадия загрузки
load:
  type: "data-loader"
  depends_on: ["transform"]
  config:
    target: "warehouse"
    table: "processed_users_${ENV}"
```

### Парсинг и выполнение

```python
from pipeline_core import PipelineYAMLParser, ExecutionContext, get_registry

# Парсим конфигурацию
parser = PipelineYAMLParser()
config = parser.parse_file("pipeline.yml")

# Получаем порядок выполнения
execution_order = config.get_execution_order()
print(f"Порядок выполнения: {execution_order}")

# Создаем контекст выполнения
context = ExecutionContext()
registry = get_registry()

# Выполняем стадии
for level in execution_order:
    for stage_name in level:
        stage_config = config.stages[stage_name]
        
        # Создаем компонент
        component = registry.create_component(
            stage_config.type, 
            stage_config.config
        )
        
        # Выполняем
        result = component.execute(context)
        context.set_stage_result(stage_name, result)
        
        print(f"Стадия {stage_name}: {result.status}")
```

## Архитектура

### Основные компоненты

- **BaseComponent**: Базовый класс для всех компонентов pipeline
- **ComponentRegistry**: Реестр компонентов с автоматическим обнаружением
- **PipelineYAMLParser**: Парсер YAML конфигурации с поддержкой шаблонов
- **ExecutionContext**: Контекст выполнения с доступом к результатам стадий

### Типы компонентов

- **Extractor**: Извлечение данных из источников
- **Transformer**: Трансформация и обработка данных  
- **Validator**: Валидация данных и качества
- **Loader**: Загрузка данных в целевые системы
- **Utility**: Вспомогательные операции

### Жизненный цикл компонента

1. **Регистрация**: Автоматическая через entry points или ручная
2. **Конфигурация**: Валидация через Pydantic модели
3. **Инициализация**: Вызов `setup()` метода
4. **Выполнение**: Вызов `execute()` с контекстом
5. **Очистка**: Вызов `teardown()` метода

## Расширение функциональности

### Создание кастомного компонента

```python
from typing import Type, List
from pipeline_core import BaseComponent, ComponentConfig, ComponentType

class DatabaseExtractorConfig(ComponentConfig):
    connection_string: str
    query: str
    batch_size: int = 1000

@register_component("database-extractor")
class DatabaseExtractorComponent(BaseComponent):
    def get_config_model(self) -> Type[ComponentConfig]:
        return DatabaseExtractorConfig
    
    def get_component_type(self) -> ComponentType:
        return ComponentType.EXTRACTOR
    
    def validate_dependencies(self, available_stages: List[str]) -> List[str]:
        # Этот компонент не имеет зависимостей
        return []
    
    def setup(self) -> None:
        # Инициализация соединения с БД
        self.connection = create_connection(self.config.connection_string)
    
    def execute(self, context: ExecutionContext) -> ExecutionResult:
        # Извлечение данных
        data = self.connection.execute(self.config.query)
        
        return ExecutionResult(
            status=ExecutionStatus.SUCCESS,
            data=data,
            processed_records=len(data)
        )
    
    def teardown(self) -> None:
        # Закрытие соединения
        if hasattr(self, 'connection'):
            self.connection.close()
```

### Простой компонент из функции

```python
from pipeline_core import create_simple_component, register_component

def csv_loader(context):
    """Простая функция для загрузки CSV"""
    data = context.get_stage_data("transform")
    
    import pandas as pd
    df = pd.DataFrame(data)
    df.to_csv("output.csv", index=False)
    
    return f"Сохранено {len(data)} записей в output.csv"

# Создаем и регистрируем компонент
CSVLoaderComponent = create_simple_component("csv-loader", csv_loader)
register_component("csv-loader", CSVLoaderComponent)
```

## Конфигурация

### Поддерживаемые возможности YAML

- **Переменные**: `${VAR}`, `${VAR:-default}`, `${VAR:?error}`
- **Зависимости**: `depends_on: ["stage1", "stage2"]`
- **Условное выполнение**: `condition: "${ENV} == 'production'"`
- **Параллельное выполнение**: `parallel: true`
- **Ограничения ресурсов**: `timeout`, `cpu_limit`, `memory_limit`

### Валидация конфигурации

```python
from pipeline_core import PipelineYAMLParser

parser = PipelineYAMLParser()
config = parser.parse_file("pipeline.yml")

# Получаем предупреждения
warnings = parser.validate_config(config)
for warning in warnings:
    print(f"Предупреждение: {warning}")
```

## Логирование

### Настройка логирования

```python
from pipeline_core import setup_logging
from pathlib import Path

setup_logging(
    level="INFO",
    format_type="json",
    log_file=Path("pipeline.log"),
    pipeline_id="my-pipeline-123"
)
```

### Использование в компонентах

```python
class MyComponent(BaseComponent):
    def execute(self, context: ExecutionContext) -> ExecutionResult:
        self.logger.info("Начало выполнения компонента")
        
        try:
            # Выполнение логики
            result = self.process_data()
            
            self.logger.info(f"Обработано {len(result)} записей")
            return ExecutionResult(status=ExecutionStatus.SUCCESS, data=result)
            
        except Exception as e:
            self.logger.error(f"Ошибка выполнения: {e}", exc_info=True)
            return ExecutionResult(status=ExecutionStatus.FAILED, error_message=str(e))
```

## Обработка ошибок

Фреймворк предоставляет иерархию исключений для различных типов ошибок:

- `PipelineError` - базовое исключение
- `PipelineConfigError` - ошибки конфигурации
- `PipelineComponentError` - ошибки компонентов
- `PipelineExecutionError` - ошибки выполнения
- `PipelineDependencyError` - ошибки зависимостей

## Разработка

### Требования

- Python 3.11+
- [uv](https://docs.astral.sh/uv/) для управления зависимостями

### Настройка среды разработки

```bash
# Клонируем репозиторий
git clone https://github.com/your-org/pipeline-framework
cd pipeline-framework/packages/pipeline-core

# Устанавливаем uv (если не установлен)
curl -LsSf https://astral.sh/uv/install.sh | sh

# Полная настройка среды разработки
make dev-setup
```

### Основные команды

```bash
# Установка зависимостей
make install-dev

# Запуск тестов
make test

# Запуск тестов с покрытием
make test-watch

# Проверка кода
make lint              # Линтинг с ruff
make format            # Форматирование с ruff
make type-check        # Проверка типов с pyright
make check-all         # Все проверки сразу

# Pre-commit хуки
make pre-commit        # Запуск всех хуков
make pre-commit-update # Обновление хуков

# Сборка пакета
make build

# Очистка
make clean
```

### Инструменты разработки

**Ruff** - современный и быстрый линтер и форматтер:
- Заменяет black, isort, flake8, pyupgrade
- Настроен в `pyproject.toml`
- Исправляет ошибки автоматически: `make lint-fix`

**Pyright** - продвинутый type checker:
- Более точный и быстрый чем mypy
- Конфигурация в `pyrightconfig.json`
- Strict mode для максимальной типобезопасности

**Pre-commit** - автоматические проверки перед коммитом:
- Форматирование и линтинг
- Проверка типов
- Проверка безопасности с bandit
- Конфигурация в `.pre-commit-config.yaml`

### Workflow разработки

1. **Создание feature branch**:
   ```bash
   git checkout -b feature/my-feature
   ```

2. **Разработка с автоматическими проверками**:
   ```bash
   # Pre-commit хуки запускаются автоматически
   git add .
   git commit -m "feat: добавить новую функциональность"
   ```

3. **Запуск полных тестов**:
   ```bash
   make test
   make check-all
   ```

4. **Создание PR** - GitHub Actions автоматически запустит все проверки

### Структура проекта

```
packages/pipeline-core/
├── src/pipeline_core/     # Исходный код
├── tests/                 # Тесты
├── pyproject.toml        # Конфигурация проекта и инструментов
├── pyrightconfig.json    # Конфигурация pyright
├── .pre-commit-config.yaml # Pre-commit хуки
├── Makefile              # Команды разработки
└── README.md
```

### Соглашения по коду

- **Форматирование**: Автоматически с ruff format
- **Импорты**: Сортировка с ruff isort
- **Типы**: Строгая типизация с pyright strict mode
- **Docstrings**: Google style
- **Тесты**: pytest с покрытием >90%

### Настройка IDE

**VS Code** (рекомендуется):
```json
{
  "python.defaultInterpreterPath": ".venv/bin/python",
  "python.linting.enabled": false,
  "ruff.enable": true,
  "ruff.organizeImports": true,
  "ruff.fixAll": true,
  "pyright.enable": true,
  "[python]": {
    "editor.defaultFormatter": "charliermarsh.ruff",
    "editor.codeActionsOnSave": {
      "source.organizeImports": true,
      "source.fixAll": true
    }
  }
}
```

**PyCharm**:
- Установить Ruff plugin
- Настроить Pyright как external tool
- Включить pre-commit в VCS settings.set_stage_result("source", ExecutionResult(
        status=ExecutionStatus.SUCCESS,
        data=[1, 2, 3, 4, 5]
    ))
    
    # Создаем компонент
    component = MyTransformComponent({
        "type": "my-transform",
        "source_stage": "source",
        "multiplier": 2.0
    })
    
    # Выполняем тест
    result = component.execute(context)
    
    assert result.status == ExecutionStatus.SUCCESS
    assert result.data == [2, 4, 6, 8, 10]
    assert result.processed_records == 5
```

## Лицензия

Apache License 2.0
