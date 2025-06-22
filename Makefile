# Makefile

.PHONY: help install install-dev clean lint format test test-unit test-integration test-performance test-coverage build docs serve-docs release

# Variables
PYTHON_VERSION := 3.13
UV_VERSION := 0.5.2
PROJECT_NAME := pipeline-framework

# Help
help: ## Show this help message
	@echo "$(PROJECT_NAME) - Модульный framework для data workflows"
	@echo ""
	@echo "Available commands:"
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)
	@echo ""
	@echo "Examples:"
	@echo "  make install-dev    # Установка в режиме разработки"
	@echo "  make test          # Запуск всех тестов"
	@echo "  make lint          # Проверка кода"
	@echo "  make build         # Сборка пакетов"

# Installation
install: ## Install production dependencies
	uv sync --no-group dev

install-dev: ## Install development dependencies
	uv sync --group dev --group security --group docs --group performance
	uv run pre-commit install

install-minimal: ## Install minimal dependencies
	uv sync --no-group dev --no-group security --no-group docs

# Development setup
setup: install-dev ## Setup development environment
	@echo "🚀 Development environment setup complete!"
	@echo ""
	@echo "Next steps:"
	@echo "  1. Run tests: make test"
	@echo "  2. Check code: make lint"
	@echo "  3. Build docs: make docs"

# Code quality
lint: ## Run all linting checks
	@echo "🔍 Running linting checks..."
	uv run ruff check .
	uv run ruff format --check .
	uv run pyright

lint-fix: ## Fix linting issues
	@echo "🔧 Fixing linting issues..."
	uv run ruff check --fix .
	uv run ruff format .

format: ## Format code
	@echo "🎨 Formatting code..."
	uv run ruff format .

type-check: ## Run type checking
	@echo "🔍 Running type checks..."
	uv run pyright

security: ## Run security checks
	@echo "🔒 Running security checks..."
	uv run bandit -r packages/ -f json -o bandit-report.json
	uv run safety check --json --output safety-report.json || true

pre-commit: ## Run pre-commit hooks
	uv run pre-commit run --all-files

# Testing
test: ## Run all tests
	@echo "🧪 Running all tests..."
	uv run pytest -x

test-unit: ## Run unit tests only
	@echo "🧪 Running unit tests..."
	uv run pytest -m "not integration and not performance" -x

test-integration: ## Run integration tests
	@echo "🧪 Running integration tests..."
	uv run pytest -m integration -x

test-performance: ## Run performance tests
	@echo "⚡ Running performance tests..."
	uv run pytest -m performance --benchmark-only

test-coverage: ## Run tests with coverage report
	@echo "📊 Running tests with coverage..."
	uv run pytest \
		--cov=packages/pipeline-core/src \
		--cov=packages/components \
		--cov-report=html \
		--cov-report=term-missing \
		--cov-report=xml

test-watch: ## Run tests in watch mode
	@echo "👀 Running tests in watch mode..."
	uv run pytest-watch

test-verbose: ## Run tests with verbose output
	@echo "🔍 Running tests with verbose output..."
	uv run pytest -v -s

# Database setup for testing
setup-test-db: ## Setup test databases
	@echo "🗄️ Setting up test databases..."
	docker-compose -f docker-compose.test.yml up -d postgres mysql
	@echo "Waiting for databases to be ready..."
	sleep 10

stop-test-db: ## Stop test databases
	docker-compose -f docker-compose.test.yml down

# Building
build: ## Build all packages
	@echo "📦 Building packages..."
	uv build packages/pipeline-core
	uv build packages/components/extractors/extractor_sql
	uv build packages/components/transformers/transformer-pandas
	uv build packages/components/loaders/loaders_sql

build-core: ## Build core package only
	uv build packages/pipeline-core

build-components: ## Build component packages
	uv build packages/components/extractors/extractor_sql
	uv build packages/components/transformers/transformer-pandas
	uv build packages/components/loaders/loaders_sql

# Documentation
docs: ## Build documentation
	@echo "📚 Building documentation..."
	uv run mkdocs build

docs-serve: ## Serve documentation locally
	@echo "📚 Serving documentation at http://localhost:8000"
	uv run mkdocs serve

docs-deploy: ## Deploy documentation to GitHub Pages
	uv run mkdocs gh-deploy --force

# Release management
version-patch: ## Bump patch version
	uv run bump2version patch

version-minor: ## Bump minor version
	uv run bump2version minor

version-major: ## Bump major version
	uv run bump2version major

release-check: lint test build docs ## Check if ready for release
	@echo "✅ Release check passed!"

# Cleanup
clean: ## Clean build artifacts
	@echo "🧹 Cleaning build artifacts..."
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info/
	rm -rf packages/*/build/
	rm -rf packages/*/dist/
	rm -rf packages/*/*.egg-info/
	rm -rf .pytest_cache/
	rm -rf .coverage
	rm -rf htmlcov/
	rm -rf .mypy_cache/
	rm -rf .ruff_cache/
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete

clean-all: clean ## Clean everything including virtual environment
	rm -rf .venv/
	rm -rf .uv-cache/

# Utilities
check-deps: ## Check for dependency updates
	uv run pip list --outdated

audit: ## Run security audit
	@echo "🔍 Running security audit..."
	uv run safety check
	uv run bandit -r packages/

profile: ## Profile application performance
	@echo "⚡ Profiling application..."
	uv run py-spy record -o profile.svg -- python -m pytest tests/performance/

benchmark: ## Run benchmarks
	@echo "📊 Running benchmarks..."
	uv run pytest tests/performance/ --benchmark-only --benchmark-json=benchmark.json

# Docker commands
docker-build: ## Build Docker image
	docker build -t pipeline-framework:latest .

docker-test: ## Run tests in Docker
	docker-compose -f docker-compose.test.yml up --build --abort-on-container-exit

docker-dev: ## Start development environment with Docker
	docker-compose -f docker-compose.dev.yml up -d

# Development shortcuts
dev: install-dev setup-test-db ## Quick development setup
	@echo "🚀 Development environment ready!"

ci: lint test-coverage security build ## Run CI pipeline locally
	@echo "✅ CI pipeline completed successfully!"

# Project information
info: ## Show project information
	@echo "Project: $(PROJECT_NAME)"
	@echo "Python version: $(PYTHON_VERSION)"
	@echo "UV version: $(UV_VERSION)"
	@echo ""
	@echo "Available features:"
	@uv run python -c "from pipeline_core import get_available_features; print('\n'.join(f'  - {f}' for f in get_available_features()))"

status: ## Show project status
	@echo "🔍 Project Status"
	@echo "=================="
	@echo ""
	@echo "Git status:"
	@git status --porcelain || echo "Not a git repository"
	@echo ""
	@echo "Dependencies:"
	@uv run pip list | head -10
	@echo ""
	@echo "Test coverage:"
	@uv run coverage report --show-missing 2>/dev/null || echo "No coverage data available"

# Example pipelines
example-basic: ## Run basic example pipeline
	@echo "🚀 Running basic example..."
	uv run pipeline run examples/pipelines/basic-example.yaml

example-customer: ## Run customer analytics example
	@echo "🚀 Running customer analytics example..."
	uv run pipeline run examples/pipelines/customer-analytics-pipeline.yaml --dry-run

# Performance monitoring
monitor: ## Monitor application performance
	@echo "📊 Starting performance monitoring..."
	uv run py-spy top --pid $(pgrep -f python)

# Advanced testing scenarios
test-sql-extractors: ## Test SQL extractors specifically
	uv run pytest packages/components/extractors/extractor_sql/tests/ -v

test-transformers: ## Test transformers specifically
	uv run pytest packages/components/transformers/transformer-pandas/tests/ -v

test-loaders: ## Test loaders specifically
	uv run pytest packages/components/loaders/loaders_sql/tests/ -v

test-core: ## Test core framework
	uv run pytest packages/pipeline-core/tests/ -v

# Development utilities
install-hooks: ## Install git hooks
	uv run pre-commit install --install-hooks

update-deps: ## Update dependencies
	uv lock --upgrade

check-licenses: ## Check license compatibility
	uv run pip-licenses --format=table

check-vulnerabilities: ## Check for known vulnerabilities
	uv run safety check --json

# Default target
.DEFAULT_GOAL := help