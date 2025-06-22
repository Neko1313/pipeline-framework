# packages/pipeline-core/src/pipeline_core/__init__.py

"""
Pipeline Core - Модульный framework для data workflows с Temporal integration

Основные компоненты:
- BaseComponent и его наследники для создания pipeline компонентов
- ComponentRegistry для автоматического обнаружения компонентов
- Конфигурационная система на основе Pydantic и YAML
- Temporal интеграция для распределенной оркестрации
- Observability: структурированные логи и метрики
"""

# Версия
__version__ = "0.1.0"

# 1. Core components - импортируем первыми
try:
    from pipeline_core.components.base import (
        BaseComponent,
        BaseExtractor,
        BaseTransformer,
        BaseLoader,
        BaseValidator,
        BaseStage,
        ExecutionContext,
        ExecutionResult,
        ExecutionStatus,
        ComponentType,
        ExecutionMetadata,
        CheckpointStrategy,
    )

    CORE_AVAILABLE = True
except ImportError as e:
    import warnings

    warnings.warn(f"Core components import failed: {e}")
    CORE_AVAILABLE = False

# 2. Registry
try:
    from pipeline_core.registry.component_registry import ComponentRegistry

    REGISTRY_AVAILABLE = True
except ImportError as e:
    import warnings

    warnings.warn(f"Registry import failed: {e}")
    REGISTRY_AVAILABLE = False

# 3. Config
try:
    from pipeline_core.config import (
        PipelineConfig,
        StageConfig,
        RetryPolicy,
        PipelineMetadata,
        TemporalConfig,
        RuntimeConfig,
        ComponentSettings,
    )

    CONFIG_AVAILABLE = True
except ImportError as e:
    import warnings

    warnings.warn(f"Config import failed: {e}")
    CONFIG_AVAILABLE = False

# 4. YAML Loader
try:
    from pipeline_core.config import YAMLConfigLoader

    YAML_LOADER_AVAILABLE = True
except ImportError as e:
    import warnings

    warnings.warn(f"YAML loader import failed: {e}")
    YAML_LOADER_AVAILABLE = False

# 5. Pipeline executor
try:
    from pipeline_core.pipeline.executor import Pipeline, PipelineBuilder

    PIPELINE_AVAILABLE = True
except ImportError as e:
    import warnings

    warnings.warn(f"Pipeline executor import failed: {e}")
    PIPELINE_AVAILABLE = False

# 6. Temporal integration - используем lazy loading
try:
    # Проверяем доступность temporal модуля без импорта всех классов
    from pipeline_core.temporal import is_temporal_available

    TEMPORAL_AVAILABLE = is_temporal_available()
except ImportError as e:
    import warnings

    warnings.warn(f"Temporal integration import failed: {e}")
    TEMPORAL_AVAILABLE = False

# 7. Observability
try:
    from pipeline_core.observability.logging import setup_logging, get_logger
    from pipeline_core.observability.metrics import MetricsCollector

    OBSERVABILITY_AVAILABLE = True
except ImportError as e:
    import warnings

    warnings.warn(f"Observability import failed: {e}")
    OBSERVABILITY_AVAILABLE = False

# Динамическое формирование __all__
__all__ = ["__version__"]

if CORE_AVAILABLE:
    __all__.extend(
        [
            "BaseComponent",
            "BaseExtractor",
            "BaseTransformer",
            "BaseLoader",
            "BaseValidator",
            "BaseStage",
            "ExecutionContext",
            "ExecutionResult",
            "ExecutionStatus",
            "ComponentType",
            "ExecutionMetadata",
            "CheckpointStrategy",
        ]
    )

if REGISTRY_AVAILABLE:
    __all__.append("ComponentRegistry")

if CONFIG_AVAILABLE:
    __all__.extend(
        [
            "PipelineConfig",
            "StageConfig",
            "RetryPolicy",
            "PipelineMetadata",
            "TemporalConfig",
            "RuntimeConfig",
            "ComponentSettings",
        ]
    )

if YAML_LOADER_AVAILABLE:
    __all__.append("YAMLConfigLoader")

if PIPELINE_AVAILABLE:
    __all__.extend(["Pipeline", "PipelineBuilder"])

if TEMPORAL_AVAILABLE:
    __all__.extend(["TemporalClient", "ComponentActivity", "DataPipelineWorkflow"])

if OBSERVABILITY_AVAILABLE:
    __all__.extend(["setup_logging", "get_logger", "MetricsCollector"])


def __getattr__(name: str):
    """Lazy loading для Temporal компонентов"""
    if TEMPORAL_AVAILABLE and name in [
        "TemporalClient",
        "ComponentActivity",
        "DataPipelineWorkflow",
    ]:
        if name == "TemporalClient":
            from pipeline_core.temporal import TemporalClient

            return TemporalClient
        elif name == "ComponentActivity":
            from pipeline_core.temporal import ComponentActivity

            return ComponentActivity
        elif name == "DataPipelineWorkflow":
            from pipeline_core.temporal import DataPipelineWorkflow

            return DataPipelineWorkflow

    raise AttributeError(f"module '{__name__}' has no attribute '{name}'")


def get_available_features() -> list[str]:
    """Получение списка доступных функций framework'а"""
    features = []
    if CORE_AVAILABLE:
        features.append("core")
    if REGISTRY_AVAILABLE:
        features.append("registry")
    if CONFIG_AVAILABLE:
        features.append("config")
    if YAML_LOADER_AVAILABLE:
        features.append("yaml_loader")
    if PIPELINE_AVAILABLE:
        features.append("pipeline")
    if TEMPORAL_AVAILABLE:
        features.append("temporal")
    if OBSERVABILITY_AVAILABLE:
        features.append("observability")
    return features


def main() -> None:
    """Entry point для CLI"""
    try:
        from pipeline_core.cli.main import app

        app()
    except ImportError:
        print("CLI module не найден")


__all__.extend(["get_available_features", "main"])


# Безопасная автоинициализация
def _auto_initialize():
    """Автоматическая инициализация framework'а"""
    try:
        if OBSERVABILITY_AVAILABLE:
            setup_logging()

        if REGISTRY_AVAILABLE:
            registry = ComponentRegistry()
            try:
                if hasattr(registry, "discover_components"):
                    registry.discover_components()
            except Exception as e:
                import warnings

                warnings.warn(f"Component discovery failed: {e}")
    except Exception as e:
        import warnings

        warnings.warn(f"Auto-initialization failed: {e}")


# Выполняем автоинициализацию только если есть базовые компоненты
if CORE_AVAILABLE or REGISTRY_AVAILABLE:
    _auto_initialize()
