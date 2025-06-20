"""
Pipeline Core - Модульный framework для data workflows с Temporal integration

Основные компоненты:
- BaseComponent: Базовый класс для всех компонентов
- ComponentRegistry: Система регистрации и обнаружения компонентов
- Pipeline: Orchestrator для выполнения workflows
- TemporalClient: Интеграция с Temporal
"""

from pipeline_core.components import (
    BaseComponent,
    BaseExtractor,
    BaseTransformer,
    BaseLoader,
    BaseValidator,
    ExecutionContext,
    ExecutionResult,
    ExecutionStatus,
    ComponentType,
    ExecutionMetadata,
)

from pipeline_core.registry import ComponentRegistry

from .pipeline.executor import Pipeline, PipelineBuilder

from pipeline_core.config import (
    PipelineConfig,
    StageConfig,
    RetryPolicy,
    PipelineMetadata,
    YAMLConfigLoader,
)

from pipeline_core.temporal import TemporalClient
from pipeline_core.temporal import ComponentActivity
from pipeline_core.temporal import DataPipelineWorkflow

from pipeline_core.observability import setup_logging, get_logger
from pipeline_core.observability import MetricsCollector

# Version
__version__ = "0.1.0"

# Public API
__all__ = [
    # Core components
    "BaseComponent",
    "BaseExtractor",
    "BaseTransformer",
    "BaseLoader",
    "BaseValidator",
    # Execution context and results
    "ExecutionContext",
    "ExecutionResult",
    "ExecutionStatus",
    "ComponentType",
    "ExecutionMetadata",
    # Registry
    "ComponentRegistry",
    # Pipeline orchestration
    "Pipeline",
    "PipelineBuilder",
    # Configuration
    "PipelineConfig",
    "StageConfig",
    "RetryPolicy",
    "PipelineMetadata",
    "YAMLConfigLoader",
    # Temporal integration
    "TemporalClient",
    "ComponentActivity",
    "DataPipelineWorkflow",
    # Observability
    "setup_logging",
    "get_logger",
    "MetricsCollector",
    # Version
    "__version__",
]


def main() -> None:
    """Entry point для CLI"""
    from .cli.main import app

    app()


# Автоматическая инициализация при импорте
def _auto_initialize():
    """Автоматическая инициализация framework'а"""
    # Настройка логирования по умолчанию
    setup_logging()

    # Инициализация реестра компонентов
    ComponentRegistry()


# Выполняем автоинициализацию
_auto_initialize()
