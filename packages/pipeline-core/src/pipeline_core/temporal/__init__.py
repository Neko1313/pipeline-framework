"""
Temporal integration package для pipeline framework

Предоставляет интеграцию с Temporal для оркестрации data workflows:
- Client для подключения к Temporal Server
- Activities для выполнения компонентов
- Workflows для управления pipeline
- Error handling и retry policies
"""

# Lazy imports для избежания circular dependencies
__all__ = [
    "TemporalClient",
    "TemporalClientError",
    "create_temporal_client",
    "ComponentActivity",
    "QualityCheckActivity",
    "PipelineStateActivity",
    "DataPipelineWorkflow",
]


def __getattr__(name: str):
    """Lazy loading для избежания circular imports"""
    if name == "TemporalClient":
        from pipeline_core.temporal.client import TemporalClient

        return TemporalClient

    elif name == "TemporalClientError":
        from pipeline_core.temporal.client import TemporalClientError

        return TemporalClientError

    elif name == "create_temporal_client":
        from pipeline_core.temporal.client import create_temporal_client

        return create_temporal_client

    elif name == "ComponentActivity":
        from pipeline_core.temporal.activities import ComponentActivity

        return ComponentActivity

    elif name == "QualityCheckActivity":
        from pipeline_core.temporal.activities import QualityCheckActivity

        return QualityCheckActivity

    elif name == "PipelineStateActivity":
        from pipeline_core.temporal.activities import PipelineStateActivity

        return PipelineStateActivity

    elif name == "DataPipelineWorkflow":
        from pipeline_core.temporal.workflows import DataPipelineWorkflow

        return DataPipelineWorkflow

    else:
        raise AttributeError(f"module '{__name__}' has no attribute '{name}'")


# Проверка доступности Temporal
try:
    import temporalio

    TEMPORAL_AVAILABLE = True
except ImportError:
    TEMPORAL_AVAILABLE = False


def is_temporal_available() -> bool:
    """Проверка доступности Temporal библиотеки"""
    return TEMPORAL_AVAILABLE


# Утилитарные функции для быстрого создания компонентов
def create_temporal_client(
    server_address: str = "localhost:7233", namespace: str = "default"
):
    """Быстрое создание Temporal client с базовыми настройками"""
    if not TEMPORAL_AVAILABLE:
        raise ImportError("temporalio library not available")

    from pipeline_core.config import TemporalConfig
    from pipeline_core.temporal.client import TemporalClient

    config = TemporalConfig(
        server_address=server_address,
        namespace=namespace,
    )

    return TemporalClient(config)
