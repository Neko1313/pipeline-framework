"""
Pipeline Core - Ядро фреймворка для создания data pipeline
"""

__version__ = "0.1.0"
__author__ = "Pipeline Framework Team"

# Импорты основных классов и функций
from .models.component import (
    BaseComponent,
    ComponentConfig,
    ExecutionContext,
    ExecutionResult,
    ExecutionStatus,
    ComponentType,
    ComponentMetadata,
    ComponentInfo,
)

from .models.pipeline import (
    PipelineConfig,
    PipelineMetadata,
    StageConfig,
    ScheduleConfig,
    ResourceLimits,
    PipelineExecution,
)

from .parser.yaml_parser import PipelineYAMLParser, YAMLTemplateProcessor

from .registry.component_registry import (
    ComponentRegistry,
    get_registry,
    register_component,
)

from .exceptions.errors import (
    PipelineError,
    PipelineConfigError,
    PipelineValidationError,
    PipelineComponentError,
    PipelineExecutionError,
    PipelineDependencyError,
    PipelineTimeoutError,
    PipelineRetryExhaustedError,
    PipelineResourceError,
    PipelineDataError,
    PipelineConnectionError,
)

from .utils.logging import (
    setup_logging,
    get_pipeline_logger,
    log_execution_start,
    log_execution_success,
    log_execution_failure,
    log_pipeline_start,
    log_pipeline_completion,
    with_log_context,
)

# Экспорт всех публичных API
__all__ = [
    # Версия
    "__version__",
    "__author__",
    # Базовые модели
    "BaseComponent",
    "ComponentConfig",
    "ExecutionContext",
    "ExecutionResult",
    "ExecutionStatus",
    "ComponentType",
    "ComponentMetadata",
    "ComponentInfo",
    # Pipeline модели
    "PipelineConfig",
    "PipelineMetadata",
    "StageConfig",
    "ScheduleConfig",
    "ResourceLimits",
    "PipelineExecution",
    # Парсер
    "PipelineYAMLParser",
    "YAMLTemplateProcessor",
    # Реестр компонентов
    "ComponentRegistry",
    "get_registry",
    "register_component",
    # Исключения
    "PipelineError",
    "PipelineConfigError",
    "PipelineValidationError",
    "PipelineComponentError",
    "PipelineExecutionError",
    "PipelineDependencyError",
    "PipelineTimeoutError",
    "PipelineRetryExhaustedError",
    "PipelineResourceError",
    "PipelineDataError",
    "PipelineConnectionError",
    # Утилиты логирования
    "setup_logging",
    "get_pipeline_logger",
    "log_execution_start",
    "log_execution_success",
    "log_execution_failure",
    "log_pipeline_start",
    "log_pipeline_completion",
    "with_log_context",
]


def get_version() -> str:
    """Получить версию библиотеки"""
    return __version__


def create_simple_component(component_type: str, execute_func):
    """
    Утилита для создания простого компонента из функции

    Args:
        component_type: Тип компонента
        execute_func: Функция выполнения, принимающая context и возвращающая результат

    Returns:
        Type[BaseComponent]: Класс компонента

    Example:
        def my_transform(context):
            data = context.get_stage_data("extract")
            # обработка данных
            return processed_data

        MyComponent = create_simple_component("my-transform", my_transform)
        register_component("my-transform")(MyComponent)
    """

    class SimpleComponent(BaseComponent):
        def get_config_model(self):
            return ComponentConfig

        def execute(self, context: ExecutionContext) -> ExecutionResult:
            import time

            start_time = time.time()

            try:
                result_data = execute_func(context)
                execution_time = time.time() - start_time

                # Подсчет записей более точно
                processed_records = 1
                if hasattr(result_data, "__len__"):
                    try:
                        # Проверяем что это не строка (у строк тоже есть __len__)
                        if not isinstance(result_data, str):
                            processed_records = len(result_data)
                    except (TypeError, AttributeError):
                        processed_records = 1

                return ExecutionResult(
                    status=ExecutionStatus.SUCCESS,
                    data=result_data,
                    execution_time=execution_time,
                    processed_records=processed_records,
                )
            except Exception as e:
                execution_time = time.time() - start_time
                return ExecutionResult(
                    status=ExecutionStatus.FAILED,
                    error_message=str(e),
                    execution_time=execution_time,
                )

    SimpleComponent.__name__ = f"SimpleComponent_{component_type.replace('-', '_')}"
    return SimpleComponent


# Настройка базового логирования при импорте
import logging

logging.getLogger("pipeline").addHandler(logging.NullHandler())
