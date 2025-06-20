"""
Базовые компоненты для pipeline framework

Предоставляет абстрактные базовые классы для всех типов компонентов:
- BaseComponent: Базовый класс для всех компонентов
- BaseExtractor: Для извлечения данных
- BaseTransformer: Для трансформации данных
- BaseLoader: Для загрузки данных
- BaseValidator: Для валидации данных
- BaseStage: Для произвольных операций
"""

import asyncio
import traceback
import uuid
from abc import ABC, abstractmethod
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum
from typing import Any, Generic, TypeVar

# Imports для observability
import structlog

# Pydantic для конфигурации и валидации
from pydantic import BaseModel, ConfigDict, Field, field_validator

# Проверяем доступность prometheus_client
try:
    from prometheus_client import Counter, Gauge, Histogram
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False
    # Создаем заглушки
    class Counter:
        def __init__(self, *args, **kwargs): pass
        def inc(self, *args, **kwargs): pass
        def labels(self, *args, **kwargs): return self

    class Histogram:
        def __init__(self, *args, **kwargs): pass
        def observe(self, *args, **kwargs): pass
        def labels(self, *args, **kwargs): return self
        def time(self):
            return lambda: None

    class Gauge:
        def __init__(self, *args, **kwargs): pass
        def set(self, *args, **kwargs): pass
        def inc(self, *args, **kwargs): pass
        def dec(self, *args, **kwargs): pass
        def labels(self, *args, **kwargs): return self

# Type variables для generic typing
DataType = TypeVar("DataType")
ConfigType = TypeVar("ConfigType")

logger = structlog.get_logger(__name__)


class ComponentType(Enum):
    """Типы компонентов в pipeline"""

    EXTRACTOR = "extractor"
    TRANSFORMER = "transformer"
    LOADER = "loader"
    VALIDATOR = "validator"
    STAGE = "stage"


class ExecutionStatus(Enum):
    """Статусы выполнения компонента"""

    PENDING = "pending"
    INITIALIZING = "initializing"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    CANCELLED = "cancelled"
    RETRYING = "retrying"
    SKIPPED = "skipped"


class CheckpointStrategy(Enum):
    """Стратегии сохранения checkpoint'ов"""

    NONE = "none"
    MEMORY = "memory"
    DISK = "disk"
    TEMPORAL = "temporal"


@dataclass
class ExecutionMetadata:
    """Метаданные выполнения компонента"""

    execution_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    started_at: datetime | None = None
    finished_at: datetime | None = None
    duration_seconds: float | None = None

    # Data processing metrics
    rows_processed: int | None = None
    bytes_processed: int | None = None
    files_processed: int | None = None

    # Performance metrics
    memory_peak_mb: float | None = None
    cpu_time_seconds: float | None = None

    # Checkpoints and recovery
    checkpoints: dict[str, Any] = field(default_factory=dict)
    last_checkpoint_at: datetime | None = None

    # Custom metrics
    custom_metrics: dict[str, Any] = field(default_factory=dict)

    # Error information
    error_count: int = 0
    last_error: str | None = None
    error_details: list[str] = field(default_factory=list)

    def add_error(self, error: Exception, context: str | None = None):
        """Добавление информации об ошибке"""
        self.error_count += 1
        error_msg = str(error)
        if context:
            error_msg = f"{context}: {error_msg}"

        self.last_error = error_msg
        self.error_details.append(error_msg)

        # Ограничиваем количество сохраняемых ошибок
        if len(self.error_details) > 10:
            self.error_details = self.error_details[-10:]

    def to_dict(self) -> dict[str, Any]:
        """Сериализация метаданных"""
        return {
            "execution_id": self.execution_id,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "finished_at": self.finished_at.isoformat() if self.finished_at else None,
            "duration_seconds": self.duration_seconds,
            "rows_processed": self.rows_processed,
            "bytes_processed": self.bytes_processed,
            "files_processed": self.files_processed,
            "memory_peak_mb": self.memory_peak_mb,
            "cpu_time_seconds": self.cpu_time_seconds,
            "checkpoints": self.checkpoints,
            "last_checkpoint_at": self.last_checkpoint_at.isoformat()
            if self.last_checkpoint_at
            else None,
            "custom_metrics": self.custom_metrics,
            "error_count": self.error_count,
            "last_error": self.last_error,
            "error_details": self.error_details,
        }


@dataclass
class ExecutionContext:
    """Контекст выполнения компонента"""

    pipeline_id: str
    stage_name: str
    run_id: str | None = None
    attempt_number: int = 1
    is_retry: bool = False

    # Previous stage results
    previous_results: dict[str, Any] = field(default_factory=dict)
    shared_state: dict[str, Any] = field(default_factory=dict)

    # Runtime configuration
    dry_run: bool = False
    debug_mode: bool = False
    max_memory_mb: int | None = None

    # Temporal workflow context (если используется)
    temporal_activity_id: str | None = None
    temporal_workflow_id: str | None = None

    # Environment variables и secrets
    environment_vars: dict[str, str] = field(default_factory=dict)
    secrets: dict[str, str] = field(default_factory=dict)

    def get_previous_result(self, stage_name: str) -> Any | None:
        """Получить результат предыдущего этапа"""
        return self.previous_results.get(stage_name)

    def set_shared_value(self, key: str, value: Any):
        """Установить значение в shared state"""
        self.shared_state[key] = value

    def get_shared_value(self, key: str, default: Any = None) -> Any:
        """Получить значение из shared state"""
        return self.shared_state.get(key, default)

    def to_dict(self) -> dict[str, Any]:
        """Сериализация контекста"""
        return {
            "pipeline_id": self.pipeline_id,
            "stage_name": self.stage_name,
            "run_id": self.run_id,
            "attempt_number": self.attempt_number,
            "is_retry": self.is_retry,
            "previous_results_keys": list(self.previous_results.keys()),
            "shared_state_keys": list(self.shared_state.keys()),
            "dry_run": self.dry_run,
            "debug_mode": self.debug_mode,
            "max_memory_mb": self.max_memory_mb,
            "temporal_activity_id": self.temporal_activity_id,
            "temporal_workflow_id": self.temporal_workflow_id,
        }


@dataclass
class ExecutionResult:
    """Результат выполнения компонента"""

    success: bool
    data: Any = None
    error: str | None = None
    metadata: ExecutionMetadata = field(default_factory=ExecutionMetadata)

    # Output artifacts
    artifacts: dict[str, Any] = field(default_factory=dict)

    # Data quality metrics
    data_quality_score: float | None = None
    data_quality_issues: list[str] = field(default_factory=list)

    # Checkpoint information
    checkpoint_data: dict[str, Any] | None = None
    requires_checkpoint: bool = False

    def add_artifact(self, name: str, value: Any):
        """Добавить артефакт к результату"""
        self.artifacts[name] = value

    def get_artifact(self, name: str, default: Any = None) -> Any:
        """Получить артефакт"""
        return self.artifacts.get(name, default)

    def to_dict(self) -> dict[str, Any]:
        """Сериализация результата"""
        return {
            "success": self.success,
            "error": self.error,
            "metadata": self.metadata.to_dict(),
            "artifacts_keys": list(self.artifacts.keys()),
            "data_quality_score": self.data_quality_score,
            "data_quality_issues": self.data_quality_issues,
            "requires_checkpoint": self.requires_checkpoint,
            "has_checkpoint_data": self.checkpoint_data is not None,
        }

    @classmethod
    def success_result(
            cls,
            data: Any,
            metadata: ExecutionMetadata | None = None,
            **kwargs
    ) -> "ExecutionResult":
        """Создать успешный результат"""
        return cls(
            success=True,
            data=data,
            metadata=metadata or ExecutionMetadata(),
            **kwargs
        )

    @classmethod
    def failure_result(
            cls,
            error: str,
            metadata: ExecutionMetadata | None = None,
            **kwargs
    ) -> "ExecutionResult":
        """Создать результат с ошибкой"""
        return cls(
            success=False,
            error=error,
            metadata=metadata or ExecutionMetadata(),
            **kwargs
        )


class ComponentConfig(BaseModel):
    """Базовая конфигурация компонента"""

    model_config = ConfigDict(
        extra="allow",  # Разрешаем дополнительные поля
        validate_assignment=True,  # Валидируем при присвоении
        use_enum_values=True,  # Используем значения enum'ов
    )

    # Базовые настройки
    enabled: bool = Field(True, description="Включен ли компонент")
    timeout_seconds: int | None = Field(None, ge=1, description="Timeout выполнения")
    retry_attempts: int = Field(0, ge=0, le=10, description="Количество повторных попыток")

    # Checkpoint settings
    checkpoint_enabled: bool = Field(False, description="Включить checkpoint'ы")
    checkpoint_strategy: CheckpointStrategy = Field(
        CheckpointStrategy.NONE, description="Стратегия checkpoint'ов"
    )

    # Observability
    log_level: str = Field("INFO", description="Уровень логирования")
    metrics_enabled: bool = Field(True, description="Включить метрики")

    @field_validator("log_level")
    @classmethod
    def validate_log_level(cls, v):
        """Валидация уровня логирования"""
        valid_levels = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
        if v.upper() not in valid_levels:
            raise ValueError(f"log_level must be one of: {valid_levels}")
        return v.upper()


class BaseComponent(Generic[DataType, ConfigType], ABC):
    """
    Абстрактный базовый класс для всех компонентов pipeline

    Предоставляет:
    - Унифицированный интерфейс выполнения
    - Observability (логирование, метрики)
    - Checkpoint/restore функциональность
    - Lifecycle management
    - Error handling и retry logic
    """

    def __init__(self, config: dict[str, Any] | ConfigType):
        """Инициализация компонента"""
        if isinstance(config, dict):
            self.config = self._create_config(config)
        else:
            self.config = config

        self.logger = structlog.get_logger(self.__class__.__name__)
        self._initialized = False
        self._execution_count = 0

        # Метрики
        self._setup_metrics()

    @property
    @abstractmethod
    def component_type(self) -> ComponentType:
        """Тип компонента"""
        pass

    @property
    @abstractmethod
    def name(self) -> str:
        """Уникальное имя компонента"""
        pass

    @property
    def version(self) -> str:
        """Версия компонента"""
        return "1.0.0"

    @property
    def description(self) -> str:
        """Описание компонента"""
        return self.__doc__ or ""

    @property
    def dependencies(self) -> list[str]:
        """Зависимости компонента"""
        return []

    @property
    def input_schema(self) -> dict[str, Any] | None:
        """JSON схема входных данных"""
        return None

    @property
    def output_schema(self) -> dict[str, Any] | None:
        """JSON схема выходных данных"""
        return None

    def _create_config(self, config_dict: dict[str, Any]) -> ConfigType:
        """Создание объекта конфигурации из словаря"""
        # По умолчанию используем ComponentConfig
        return ComponentConfig(**config_dict)

    def _setup_metrics(self):
        """Настройка метрик Prometheus"""
        if not PROMETHEUS_AVAILABLE or not getattr(self.config, 'metrics_enabled', True):
            return

        component_name = f"{self.component_type.value}_{self.name}"

        self.execution_counter = Counter(
            f"{component_name}_executions_total",
            "Total number of component executions",
            ["status", "component_type", "component_name"]
        )

        self.execution_duration = Histogram(
            f"{component_name}_execution_duration_seconds",
            "Component execution duration",
            ["component_type", "component_name"]
        )

        self.active_executions = Gauge(
            f"{component_name}_active_executions",
            "Number of active executions",
            ["component_type", "component_name"]
        )

    async def initialize(self) -> None:
        """Инициализация компонента (переопределяется в подклассах)"""
        self._initialized = True
        self.logger.info("Component initialized", component=self.name)

    async def cleanup(self) -> None:
        """Очистка ресурсов (переопределяется в подклассах)"""
        self.logger.info("Component cleanup", component=self.name)

    @abstractmethod
    async def _execute_impl(self, context: ExecutionContext) -> DataType:
        """
        Основная логика выполнения компонента

        Args:
            context: Контекст выполнения

        Returns:
            Результат обработки данных
        """
        pass

    async def execute(self, context: ExecutionContext) -> ExecutionResult:
        """
        Основной метод выполнения компонента

        Обеспечивает:
        - Инициализацию если необходимо
        - Логирование и метрики
        - Обработку ошибок
        - Checkpoint'инг
        """
        execution_metadata = ExecutionMetadata()
        execution_metadata.started_at = datetime.now(UTC)

        # Обновляем метрики
        if PROMETHEUS_AVAILABLE and hasattr(self, 'active_executions'):
            self.active_executions.labels(
                component_type=self.component_type.value,
                component_name=self.name
            ).inc()

        try:
            # Инициализация если нужно
            if not self._initialized:
                await self.initialize()

            self.logger.info(
                "Component execution started",
                component=self.name,
                execution_id=execution_metadata.execution_id,
                pipeline_id=context.pipeline_id,
                stage_name=context.stage_name,
            )

            # Выполнение основной логики
            with self._measure_duration():
                result_data = await self._execute_impl(context)

            execution_metadata.finished_at = datetime.now(UTC)
            execution_metadata.duration_seconds = (
                    execution_metadata.finished_at - execution_metadata.started_at
            ).total_seconds()

            self._execution_count += 1

            # Создаем успешный результат
            result = ExecutionResult.success_result(
                data=result_data,
                metadata=execution_metadata
            )

            # Обновляем метрики
            if PROMETHEUS_AVAILABLE and hasattr(self, 'execution_counter'):
                self.execution_counter.labels(
                    status="success",
                    component_type=self.component_type.value,
                    component_name=self.name
                ).inc()

            self.logger.info(
                "Component execution completed",
                component=self.name,
                execution_id=execution_metadata.execution_id,
                duration_seconds=execution_metadata.duration_seconds,
            )

            return result

        except Exception as e:
            execution_metadata.finished_at = datetime.now(UTC)
            execution_metadata.duration_seconds = (
                    execution_metadata.finished_at - execution_metadata.started_at
            ).total_seconds()
            execution_metadata.add_error(e)

            # Обновляем метрики
            if PROMETHEUS_AVAILABLE and hasattr(self, 'execution_counter'):
                self.execution_counter.labels(
                    status="failed",
                    component_type=self.component_type.value,
                    component_name=self.name
                ).inc()

            self.logger.error(
                "Component execution failed",
                component=self.name,
                execution_id=execution_metadata.execution_id,
                error=str(e),
                traceback=traceback.format_exc(),
            )

            return ExecutionResult.failure_result(
                error=str(e),
                metadata=execution_metadata
            )

        finally:
            # Уменьшаем счетчик активных выполнений
            if PROMETHEUS_AVAILABLE and hasattr(self, 'active_executions'):
                self.active_executions.labels(
                    component_type=self.component_type.value,
                    component_name=self.name
                ).dec()

    @asynccontextmanager
    async def _measure_duration(self):
        """Контекстный менеджер для измерения времени выполнения"""
        start_time = asyncio.get_event_loop().time()
        try:
            yield
        finally:
            duration = asyncio.get_event_loop().time() - start_time
            if PROMETHEUS_AVAILABLE and hasattr(self, 'execution_duration'):
                self.execution_duration.labels(
                    component_type=self.component_type.value,
                    component_name=self.name
                ).observe(duration)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(name='{self.name}', type='{self.component_type.value}')"


# Специализированные базовые классы
class BaseExtractor(BaseComponent[DataType, ConfigType]):
    """Базовый класс для экстракторов данных"""

    @property
    def component_type(self) -> ComponentType:
        return ComponentType.EXTRACTOR


class BaseTransformer(BaseComponent[DataType, ConfigType]):
    """Базовый класс для трансформеров данных"""

    @property
    def component_type(self) -> ComponentType:
        return ComponentType.TRANSFORMER


class BaseLoader(BaseComponent[DataType, ConfigType]):
    """Базовый класс для загрузчиков данных"""

    @property
    def component_type(self) -> ComponentType:
        return ComponentType.LOADER


class BaseValidator(BaseComponent[DataType, ConfigType]):
    """Базовый класс для валидаторов данных"""

    @property
    def component_type(self) -> ComponentType:
        return ComponentType.VALIDATOR


class BaseStage(BaseComponent[DataType, ConfigType]):
    """Базовый класс для произвольных этапов pipeline"""

    @property
    def component_type(self) -> ComponentType:
        return ComponentType.STAGE
