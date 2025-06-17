# Makefile для компонентов pipeline

.PHONY: help install install-dev test test-extractors test-transformers lint format type-check build clean demo

# Цвета для вывода
GREEN := \033[0;32m
YELLOW := \033[0;33m
RED := \033[0;31m
NC := \033[0m # No Color

help: ## Показать справку
	@echo "$(GREEN)Доступные команды для компонентов:$(NC)"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  $(YELLOW)%-20s$(NC) %s\n", $1, $2}'

install: ## Установить зависимости для всех компонентов
	@echo "$(GREEN)Устанавливаем зависимости extractors...$(NC)"
	cd extractors && uv sync
	@echo "$(GREEN)Устанавливаем зависимости transformers...$(NC)"
	cd transformers && uv sync

install-dev: ## Установить dev зависимости для всех компонентов
	@echo "$(GREEN)Устанавливаем dev зависимости extractors...$(NC)"
	cd extractors && uv sync --dev
	@echo "$(GREEN)Устанавливаем dev зависимости transformers...$(NC)"
	cd transformers && uv sync --dev

test: test-extractors test-transformers ## Запустить все тесты

test-extractors: ## Тесты для extractors
	@echo "$(GREEN)Запускаем тесты extractors...$(NC)"
	cd extractors && uv run pytest tests/ -v --cov=src/pipeline_extractors

test-transformers: ## Тесты для transformers
	@echo "$(GREEN)Запускаем тесты transformers...$(NC)"
	cd transformers && uv run pytest tests/ -v --cov=src/pipeline_transformers

lint: ## Линтинг всех компонентов
	@echo "$(GREEN)Линтинг extractors...$(NC)"
	cd extractors && uv run ruff check src tests
	@echo "$(GREEN)Линтинг transformers...$(NC)"
	cd transformers && uv run ruff check src tests

lint-fix: ## Исправление ошибок линтера
	@echo "$(GREEN)Исправляем ошибки в extractors...$(NC)"
	cd extractors && uv run ruff check --fix src tests
	@echo "$(GREEN)Исправляем ошибки в transformers...$(NC)"
	cd transformers && uv run ruff check --fix src tests

format: ## Форматирование кода
	@echo "$(GREEN)Форматируем extractors...$(NC)"
	cd extractors && uv run ruff format src tests
	@echo "$(GREEN)Форматируем transformers...$(NC)"
	cd transformers && uv run ruff format src tests

type-check: ## Проверка типов
	@echo "$(GREEN)Проверяем типы extractors...$(NC)"
	cd extractors && uv run pyright
	@echo "$(GREEN)Проверяем типы transformers...$(NC)"
	cd transformers && uv run pyright

check-all: lint format type-check ## Все проверки
	@echo "$(GREEN)Все проверки компонентов завершены!$(NC)"

build: clean ## Сборка всех пакетов
	@echo "$(GREEN)Собираем extractors...$(NC)"
	cd extractors && uv build
	@echo "$(GREEN)Собираем transformers...$(NC)"
	cd transformers && uv build

clean: ## Очистка временных файлов
	@echo "$(GREEN)Очищаем временные файлы...$(NC)"
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -delete
	find . -type d -name "*.egg-info" -exec rm -rf {} +
	find . -type d -name ".pytest_cache" -exec rm -rf {} +
	find . -type d -name ".ruff_cache" -exec rm -rf {} +
	rm -rf extractors/build/ extractors/dist/
	rm -rf transformers/build/ transformers/dist/

demo: ## Запуск демонстрации компонентов
	@echo "$(GREEN)Запускаем демонстрацию компонентов...$(NC)"
	cd ../../ && python examples/test_components.py

# Команды для отдельных компонентов
extractors-install: ## Установка только extractors
	cd extractors && uv sync --dev

transformers-install: ## Установка только transformers
	cd transformers && uv sync --dev

extractors-test: ## Тесты только extractors
	cd extractors && uv run pytest tests/ -v

transformers-test: ## Тесты только transformers
	cd transformers && uv run pytest tests/ -v

# Команды для разработки
dev-setup: install-dev ## Полная настройка среды разработки
	@echo "$(GREEN)Настраиваем pre-commit для extractors...$(NC)"
	cd extractors && uv run pre-commit install
	@echo "$(GREEN)Настраиваем pre-commit для transformers...$(NC)"
	cd transformers && uv run pre-commit install
	@echo "$(GREEN)Проверяем что все работает...$(NC)"
	$(MAKE) check-all
	@echo "$(GREEN)Среда разработки компонентов готова!$(NC)"

# Команды для CI/CD
ci-install: ## Установка для CI
	cd extractors && uv sync --dev --frozen
	cd transformers && uv sync --dev --frozen

ci-test: ## Тесты для CI
	cd extractors && uv run pytest tests/ -v --cov=src/pipeline_extractors --cov-report=xml
	cd transformers && uv run pytest tests/ -v --cov=src/pipeline_transformers --cov-report=xml

ci-check: ## Проверки для CI
	cd extractors && uv run ruff check src tests
	cd extractors && uv run ruff format --check src tests
	cd extractors && uv run pyright
	cd transformers && uv run ruff check src tests
	cd transformers && uv run ruff format --check src tests
	cd transformers && uv run pyright

# Полезная информация
status: ## Показать статус всех компонентов
	@echo "$(GREEN)Статус компонентов:$(NC)"
	@echo "$(YELLOW)Extractors:$(NC)"
	@cd extractors && echo "  Python: $(uv run python --version)"
	@cd extractors && echo "  Dependencies: $(uv pip list | wc -l) packages"
	@echo "$(YELLOW)Transformers:$(NC)"
	@cd transformers && echo "  Python: $(uv run python --version)"
	@cd transformers && echo "  Dependencies: $(uv pip list | wc -l) packages"

# Примеры использования
examples: ## Показать примеры использования
	@echo "$(GREEN)Примеры использования компонентов:$(NC)"
	@echo ""
	@echo "$(YELLOW)1. CSV Extractor:$(NC)"
	@echo "   extract:"
	@echo "     type: csv-extractor"
	@echo "     config:"
	@echo "       file_path: data.csv"
	@echo "       output_format: pandas"
	@echo ""
	@echo "$(YELLOW)2. Data Transformer:$(NC)"
	@echo "   transform:"
	@echo "     type: data-transformer"
	@echo "     config:"
	@echo "       source_stage: extract"
	@echo "       add_columns:"
	@echo "         total: price * quantity"
	@echo "       filter_expression: amount > 0"
	@echo ""
	@echo "$(YELLOW)3. HTTP API Extractor:$(NC)"
	@echo "   api_extract:"
	@echo "     type: http-extractor"
	@echo "     config:"
	@echo "       base_url: https://api.example.com"
	@echo "       endpoint: /users"
	@echo "       pagination_enabled: true"