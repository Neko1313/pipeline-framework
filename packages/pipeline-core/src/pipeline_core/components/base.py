# packages/pipeline-core/src/pipeline_core/components/base.py

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
from typing import Any, Generic, TypeVar, Optional, Dict, List

# Imports для observability
import structlog

# Pydantic для конфигурации и валидации
from pydantic import BaseModel, ConfigDict, Field, field_validator

# Проверяем доступность prometheus_client
try:
    from prometheus_client import Counter, Gauge, Histogram, CollectorRegistry

    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

    # Создаем заглушки
    class Counter:
        def __init__(self, *args, **kwargs):
            pass

        def inc(self, *args):
            pass

        def labels(self, *args, **kwargs):
            return self

    class Histogram:
        def __init__(self, *args, **kwargs):
            pass

        def observe(self, *args):
            pass

        def labels(self, *args, **kwargs):
            return self

        def time(self):
            return lambda: None

    class Gauge:
        def __init__(self, *args, **kwargs):
            pass

        def set(self, *args):
            pass

        def inc(self, *args):
            pass

        def dec(self, *args):
            pass

        def labels(self, *args, **kwargs):
            return self

    class CollectorRegistry:
        def __init__(self):
            pass


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
    DATABASE = "database"
    REMOTE_STORAGE = "remote_storage"


@dataclass
class ExecutionMetadata:
    """Метаданные выполнения компонента"""

    execution_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    duration_seconds: Optional[float] = None
    rows_processed: Optional[int] = None
    bytes_processed: Optional[int] = None
    checkpoints: Dict[str, Any] = field(default_factory=dict)
    custom_metrics: Dict[str, Any] = field(default_factory=dict)
    error_count: int = 0
    last_error: Optional[str] = None

    def mark_started(self) -> None:
        """Отметка начала выполнения"""
        self.started_at = datetime.now(UTC)

    def mark_finished(self) -> None:
        """Отметка завершения выполнения"""
        self.finished_at = datetime.now(UTC)
        if self.started_at:
            self.duration_seconds = (self.finished_at - self.started_at).total_seconds()

    def record_error(self, error: str) -> None:
        """Запись ошибки"""
        self.error_count += 1
        self.last_error = error


@dataclass
class ExecutionContext:
    """Контекст выполнения компонента"""

    pipeline_id: str
    stage_name: str
    run_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    execution_time: datetime = field(default_factory=lambda: datetime.now(UTC))
    config: Dict[str, Any] = field(default_factory=dict)
    global_variables: Dict[str, Any] = field(default_factory=dict)
    previous_results: List[Any] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def get_previous_result(self, stage_name: str) -> Optional[Any]:
        """Получение результата предыдущего этапа по имени"""
        for result in self.previous_results:
            if hasattr(result, "stage_name") and result.stage_name == stage_name:
                return result
        return None

    def get_global_variable(self, key: str, default: Any = None) -> Any:
        """Получение глобальной переменной"""
        return self.global_variables.get(key, default)

    def set_global_variable(self, key: str, value: Any) -> None:
        """Установка глобальной переменной"""
        self.global_variables[key] = value


class ExecutionResult(Generic[DataType]):
    """Результат выполнения компонента"""

    def __init__(
        self,
        data: Optional[DataType] = None,
        status: ExecutionStatus = ExecutionStatus.SUCCESS,
        error: Optional[str] = None,
        metadata: Optional[ExecutionMetadata] = None,
        checkpoints: Optional[Dict[str, Any]] = None,
    ):
        self.data = data
        self.status = status
        self.error = error
        self.metadata = metadata or ExecutionMetadata()
        self.checkpoints = checkpoints or {}

    @property
    def success(self) -> bool:
        """Проверка успешности выполнения"""
        return self.status == ExecutionStatus.SUCCESS

    @property
    def failed(self) -> bool:
        """Проверка неуспешности выполнения"""
        return self.status == ExecutionStatus.FAILED

    @classmethod
    def success_result(
        cls,
        data: DataType,
        metadata: Optional[ExecutionMetadata] = None,
        checkpoints: Optional[Dict[str, Any]] = None,
    ) -> "ExecutionResult[DataType]":
        """Создание успешного результата"""
        return cls(
            data=data,
            status=ExecutionStatus.SUCCESS,
            metadata=metadata,
            checkpoints=checkpoints,
        )

    @classmethod
    def failure_result(
        cls,
        error: str,
        metadata: Optional[ExecutionMetadata] = None,
        data: Optional[DataType] = None,
    ) -> "ExecutionResult[DataType]":
        """Создание результата с ошибкой"""
        return cls(
            data=data,
            status=ExecutionStatus.FAILED,
            error=error,
            metadata=metadata,
        )


class ComponentConfig(BaseModel):
    """Базовая конфигурация компонента"""

    model_config = ConfigDict(
        extra="allow",  # Разрешаем дополнительные поля
        validate_assignment=True,
    )

    # Общие настройки
    enabled: bool = Field(default=True, description="Включен ли компонент")
    timeout: Optional[float] = Field(
        default=None, ge=0, description="Timeout в секундах"
    )
    retry_attempts: int = Field(
        default=0, ge=0, le=10, description="Количество повторов"
    )

    # Checkpoint настройки
    checkpoint_strategy: CheckpointStrategy = Field(
        default=CheckpointStrategy.NONE, description="Стратегия checkpoint'ов"
    )
    checkpoint_interval: Optional[int] = Field(
        default=None, ge=1, description="Интервал checkpoint'ов"
    )

    # Observability
    metrics_enabled: bool = Field(default=True, description="Включены ли метрики")
    logging_level: str = Field(default="INFO", description="Уровень логирования")


class BaseComponent(ABC, Generic[DataType, ConfigType]):
    """
    Базовый абстрактный класс для всех компонентов pipeline

    Обеспечивает:
    - Стандартный интерфейс выполнения
    - Управление конфигурацией
    - Обработку ошибок
    - Checkpoint функциональность
    - Метрики и логирование
    """

    def __init__(self, config: ConfigType):
        self.config = config
        self._initialized = False
        self._metrics_registry = None

        # Настраиваем логирование
        self.logger = structlog.get_logger(
            component=self.__class__.__name__,
            component_type=self.component_type.value,
            component_name=self.name,
        )

        # Настраиваем метрики только если они включены
        if PROMETHEUS_AVAILABLE and getattr(self.config, "metrics_enabled", True):
            self._setup_metrics()

    @property
    @abstractmethod
    def name(self) -> str:
        """Уникальное имя компонента"""
        pass

    @property
    @abstractmethod
    def component_type(self) -> ComponentType:
        """Тип компонента"""
        pass

    @classmethod
    def from_config_dict(cls, config_dict: Dict[str, Any]) -> ConfigType:
        """Создание объекта конфигурации из словаря"""
        # По умолчанию используем ComponentConfig
        return ComponentConfig(**config_dict)

    def _setup_metrics(self):
        """Настройка метрик Prometheus"""
        if not PROMETHEUS_AVAILABLE or not getattr(
            self.config, "metrics_enabled", True
        ):
            return

        # Создаем отдельный registry для каждого компонента
        self._metrics_registry = CollectorRegistry()

        component_name = f"{self.component_type.value}_{self.name.replace('-', '_')}"

        try:
            self.execution_counter = Counter(
                f"{component_name}_executions_total",
                "Total number of component executions",
                ["status", "component_type", "component_name"],
                registry=self._metrics_registry,
            )

            self.execution_duration = Histogram(
                f"{component_name}_execution_duration_seconds",
                "Component execution duration",
                ["component_type", "component_name"],
                registry=self._metrics_registry,
            )

            self.active_executions = Gauge(
                f"{component_name}_active_executions",
                "Number of active executions",
                ["component_type", "component_name"],
                registry=self._metrics_registry,
            )
        except Exception as e:
            self.logger.warning("Failed to setup metrics", error=str(e))
            # Создаем заглушки
            self.execution_counter = Counter("dummy", "dummy")
            self.execution_duration = Histogram("dummy", "dummy")
            self.active_executions = Gauge("dummy", "dummy")

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
        execution_metadata.mark_started()

        # Обновляем метрики
        if PROMETHEUS_AVAILABLE and hasattr(self, "active_executions"):
            self.active_executions.labels(
                component_type=self.component_type.value, component_name=self.name
            ).inc()

        try:
            # Инициализируем компонент если необходимо
            if not self._initialized:
                await self.initialize()

            # Логируем начало выполнения
            self.logger.info(
                "Starting component execution",
                pipeline_id=context.pipeline_id,
                stage_name=context.stage_name,
                execution_id=execution_metadata.execution_id,
            )

            # Измеряем время выполнения
            async with self._measure_duration():
                # Выполняем основную логику
                result_data = await self._execute_impl(context)

            # Завершаем метаданные
            execution_metadata.mark_finished()

            # Логируем успешное завершение
            self.logger.info(
                "Component execution completed successfully",
                execution_id=execution_metadata.execution_id,
                duration=execution_metadata.duration_seconds,
            )

            # Обновляем метрики успеха
            if PROMETHEUS_AVAILABLE and hasattr(self, "execution_counter"):
                self.execution_counter.labels(
                    status="success",
                    component_type=self.component_type.value,
                    component_name=self.name,
                ).inc()

            return ExecutionResult.success_result(
                data=result_data, metadata=execution_metadata
            )

        except Exception as e:
            # Завершаем метаданные
            execution_metadata.mark_finished()
            execution_metadata.record_error(str(e))

            # Логируем ошибку
            self.logger.error(
                "Component execution failed",
                execution_id=execution_metadata.execution_id,
                error=str(e),
                traceback=traceback.format_exc(),
            )

            # Обновляем метрики ошибок
            if PROMETHEUS_AVAILABLE and hasattr(self, "execution_counter"):
                self.execution_counter.labels(
                    status="failed",
                    component_type=self.component_type.value,
                    component_name=self.name,
                ).inc()

            return ExecutionResult.failure_result(
                error=str(e), metadata=execution_metadata
            )

        finally:
            # Уменьшаем счетчик активных выполнений
            if PROMETHEUS_AVAILABLE and hasattr(self, "active_executions"):
                self.active_executions.labels(
                    component_type=self.component_type.value, component_name=self.name
                ).dec()

    @asynccontextmanager
    async def _measure_duration(self):
        """Контекстный менеджер для измерения времени выполнения"""
        start_time = asyncio.get_event_loop().time()
        try:
            yield
        finally:
            duration = asyncio.get_event_loop().time() - start_time
            if PROMETHEUS_AVAILABLE and hasattr(self, "execution_duration"):
                self.execution_duration.labels(
                    component_type=self.component_type.value, component_name=self.name
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
