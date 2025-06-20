from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, TypeVar, Generic, Protocol, Union
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
import asyncio
import uuid
import json
import traceback
from contextlib import asynccontextmanager

# Imports для observability
import structlog
from prometheus_client import Counter, Histogram, Gauge

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
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    duration_seconds: Optional[float] = None

    # Data processing metrics
    rows_processed: Optional[int] = None
    bytes_processed: Optional[int] = None
    files_processed: Optional[int] = None

    # Performance metrics
    memory_peak_mb: Optional[float] = None
    cpu_time_seconds: Optional[float] = None

    # Checkpoints and recovery
    checkpoints: Dict[str, Any] = field(default_factory=dict)
    last_checkpoint_at: Optional[datetime] = None

    # Custom metrics
    custom_metrics: Dict[str, Any] = field(default_factory=dict)

    # Error information
    error_count: int = 0
    last_error: Optional[str] = None
    error_details: List[str] = field(default_factory=list)

    def add_error(self, error: Exception, context: Optional[str] = None):
        """Добавление информации об ошибке"""
        self.error_count += 1
        error_msg = str(error)
        if context:
            error_msg = f"{context}: {error_msg}"

        self.last_error = error_msg
        self.error_details.append(
            {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "error": error_msg,
                "type": type(error).__name__,
                "context": context,
            }
        )

    def calculate_duration(self):
        """Вычисление продолжительности выполнения"""
        if self.started_at and self.finished_at:
            self.duration_seconds = (self.finished_at - self.started_at).total_seconds()

    def to_dict(self) -> Dict[str, Any]:
        """Сериализация в словарь"""
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
            "custom_metrics": self.custom_metrics,
            "error_count": self.error_count,
            "last_error": self.last_error,
            "error_details": self.error_details,
        }


@dataclass
class ExecutionContext:
    """Контекст выполнения для передачи между компонентами"""

    # Pipeline identification
    pipeline_id: str
    run_id: str
    stage_name: str

    # Configuration
    config: Dict[str, Any] = field(default_factory=dict)
    global_variables: Dict[str, Any] = field(default_factory=dict)

    # Data flow
    previous_results: List["ExecutionResult"] = field(default_factory=list)
    input_data: Optional[Any] = None

    # Execution metadata
    metadata: ExecutionMetadata = field(default_factory=ExecutionMetadata)

    # Environment context
    environment: str = "development"
    dry_run: bool = False

    # Temporal context (если используется)
    temporal_workflow_id: Optional[str] = None
    temporal_run_id: Optional[str] = None

    def get_previous_result(self, stage_name: str) -> Optional["ExecutionResult"]:
        """Получить результат предыдущего этапа по имени"""
        for result in self.previous_results:
            if result.stage_name == stage_name:
                return result
        return None

    def get_previous_data(self, stage_name: str) -> Optional[Any]:
        """Получить данные предыдущего этапа по имени"""
        result = self.get_previous_result(stage_name)
        return result.data if result else None

    def get_config_value(self, key: str, default: Any = None) -> Any:
        """Получить значение из конфигурации с fallback"""
        return self.config.get(key, self.global_variables.get(key, default))

    def add_checkpoint(self, key: str, value: Any):
        """Добавить checkpoint"""
        self.metadata.checkpoints[key] = value
        self.metadata.last_checkpoint_at = datetime.now(timezone.utc)

    def get_checkpoint(self, key: str, default: Any = None) -> Any:
        """Получить checkpoint"""
        return self.metadata.checkpoints.get(key, default)


@dataclass
class ExecutionResult:
    """Результат выполнения компонента"""

    # Identification
    stage_name: str
    status: ExecutionStatus

    # Data and artifacts
    data: Optional[Any] = None
    artifacts: Dict[str, str] = field(default_factory=dict)  # пути к файлам/объектам

    # Error information
    error: Optional[str] = None
    error_type: Optional[str] = None
    traceback: Optional[str] = None

    # Metadata
    metadata: ExecutionMetadata = field(default_factory=ExecutionMetadata)

    # Output schema (для валидации следующих этапов)
    output_schema: Optional[Dict[str, Any]] = None

    def is_success(self) -> bool:
        return self.status == ExecutionStatus.SUCCESS

    def is_failed(self) -> bool:
        return self.status == ExecutionStatus.FAILED

    def is_terminal(self) -> bool:
        """Проверка, что состояние терминальное"""
        return self.status in {
            ExecutionStatus.SUCCESS,
            ExecutionStatus.FAILED,
            ExecutionStatus.CANCELLED,
        }

    def set_error(self, error: Exception, context: Optional[str] = None):
        """Установка информации об ошибке"""
        self.status = ExecutionStatus.FAILED
        self.error = str(error)
        self.error_type = type(error).__name__
        self.traceback = traceback.format_exc()
        self.metadata.add_error(error, context)

    def to_dict(self) -> Dict[str, Any]:
        """Сериализация в словарь"""
        return {
            "stage_name": self.stage_name,
            "status": self.status.value,
            "data": self.data,
            "artifacts": self.artifacts,
            "error": self.error,
            "error_type": self.error_type,
            "traceback": self.traceback,
            "metadata": self.metadata.to_dict(),
            "output_schema": self.output_schema,
        }


# Protocol для конфигурации компонентов
class ComponentConfig(Protocol):
    """Протокол для конфигурации компонентов"""

    def validate(self) -> bool:
        """Валидация конфигурации"""
        ...

    def to_dict(self) -> Dict[str, Any]:
        """Сериализация в словарь"""
        ...


# Metrics для observability
COMPONENT_EXECUTIONS = Counter(
    "pipeline_component_executions_total",
    "Total component executions",
    ["component_type", "component_name", "status", "pipeline_id"],
)

COMPONENT_DURATION = Histogram(
    "pipeline_component_duration_seconds",
    "Component execution duration",
    ["component_type", "component_name", "pipeline_id"],
)

COMPONENT_DATA_PROCESSED = Counter(
    "pipeline_component_data_processed_total",
    "Total data processed by components",
    ["component_type", "component_name", "data_type"],
)


class BaseComponent(ABC, Generic[DataType, ConfigType]):
    """
    Базовый класс для всех компонентов data pipeline

    Обеспечивает:
    - Стандартизированный интерфейс выполнения
    - Управление жизненным циклом
    - Логирование и метрики
    - Error handling и recovery
    - Checkpoint/resume функциональность
    - Temporal integration готовность
    """

    def __init__(self, config: Union[ConfigType, Dict[str, Any]]):
        self.config = config
        self._is_initialized = False
        self._logger = structlog.get_logger(
            component_type=self.component_type.value, component_name=self.name
        )

        # Checkpoint strategy
        self._checkpoint_strategy = CheckpointStrategy.MEMORY
        self._checkpoints: Dict[str, Any] = {}

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
        return f"{self.component_type.value} component: {self.name}"

    @property
    def input_schema(self) -> Optional[Dict[str, Any]]:
        """Схема входных данных (для валидации)"""
        return None

    @property
    def output_schema(self) -> Optional[Dict[str, Any]]:
        """Схема выходных данных"""
        return None

    @property
    def dependencies(self) -> List[str]:
        """Список зависимостей компонента (внешние библиотеки, сервисы)"""
        return []

    # === Lifecycle методы ===

    async def initialize(self, context: ExecutionContext) -> None:
        """Инициализация компонента перед выполнением"""
        if self._is_initialized:
            return

        self._logger.info("Initializing component", stage=context.stage_name)

        try:
            # Валидация конфигурации
            await self._validate_config(context)

            # Проверка зависимостей
            await self._check_dependencies(context)

            # Настройка ресурсов
            await self._setup_resources(context)

            # Пользовательская инициализация
            await self._initialize_internal(context)

            self._is_initialized = True
            self._logger.info(
                "Component initialized successfully", stage=context.stage_name
            )

        except Exception as e:
            self._logger.error(
                "Component initialization failed",
                error=str(e),
                stage=context.stage_name,
            )
            raise

    async def cleanup(self, context: ExecutionContext) -> None:
        """Очистка ресурсов после выполнения"""
        if not self._is_initialized:
            return

        self._logger.info("Cleaning up component", stage=context.stage_name)

        try:
            # Пользовательская очистка
            await self._cleanup_internal(context)

            # Очистка ресурсов
            await self._cleanup_resources(context)

            self._is_initialized = False
            self._logger.info("Component cleanup completed", stage=context.stage_name)

        except Exception as e:
            self._logger.warning(
                "Component cleanup failed", error=str(e), stage=context.stage_name
            )

    # === Основной метод выполнения ===

    async def execute(self, context: ExecutionContext) -> ExecutionResult:
        """
        Главный метод выполнения компонента

        Обеспечивает:
        - Lifecycle management
        - Error handling с retry logic
        - Метрики и логирование
        - Checkpoint support
        - Temporal integration
        """
        result = ExecutionResult(
            stage_name=context.stage_name,
            status=ExecutionStatus.PENDING,
            metadata=context.metadata,
        )

        # Метрики
        component_key = f"{self.component_type.value}:{self.name}"

        self._logger.info(
            "Starting component execution",
            stage=context.stage_name,
            pipeline_id=context.pipeline_id,
            run_id=context.run_id,
            dry_run=context.dry_run,
        )

        try:
            # Инициализация
            result.status = ExecutionStatus.INITIALIZING
            await self.initialize(context)

            # Проверка возможности восстановления с checkpoint
            if await self._can_resume_from_checkpoint(context):
                self._logger.info("Resuming from checkpoint", stage=context.stage_name)
                return await self._resume_from_checkpoint(context)

            # Валидация входных данных
            await self._validate_input_data(context)

            # Обновляем статус и время начала
            result.status = ExecutionStatus.RUNNING
            result.metadata.started_at = datetime.now(timezone.utc)

            # Dry run проверка
            if context.dry_run:
                self._logger.info(
                    "Dry run mode - skipping actual execution", stage=context.stage_name
                )
                result.data = await self._dry_run_execute(context)
                result.status = ExecutionStatus.SUCCESS
            else:
                # Основная логика выполнения с метриками
                with self._track_execution_metrics(context):
                    result.data = await self._execute_internal(context)

                result.status = ExecutionStatus.SUCCESS

            # Успешное завершение
            result.metadata.finished_at = datetime.now(timezone.utc)
            result.metadata.calculate_duration()
            result.output_schema = self.output_schema

            # Сохраняем checkpoint для возможности повторного использования
            await self._save_checkpoint(context, result)

            # Валидация выходных данных
            await self._validate_output_data(result)

            self._logger.info(
                "Component execution completed successfully",
                stage=context.stage_name,
                duration=result.metadata.duration_seconds,
                rows_processed=result.metadata.rows_processed,
            )

            # Обновляем метрики
            COMPONENT_EXECUTIONS.labels(
                component_type=self.component_type.value,
                component_name=self.name,
                status="success",
                pipeline_id=context.pipeline_id,
            ).inc()

            return result

        except Exception as e:
            # Обработка ошибок
            result.set_error(e, f"Component {self.name} execution failed")
            result.metadata.finished_at = datetime.now(timezone.utc)
            result.metadata.calculate_duration()

            self._logger.error(
                "Component execution failed",
                stage=context.stage_name,
                error=str(e),
                error_type=type(e).__name__,
                duration=result.metadata.duration_seconds,
            )

            # Обновляем метрики
            COMPONENT_EXECUTIONS.labels(
                component_type=self.component_type.value,
                component_name=self.name,
                status="failed",
                pipeline_id=context.pipeline_id,
            ).inc()

            await self._handle_error(context, e)
            return result

        finally:
            # Cleanup в любом случае
            try:
                await self.cleanup(context)
            except Exception as cleanup_error:
                self._logger.warning("Cleanup error", error=str(cleanup_error))

    # === Абстрактные методы для реализации в наследниках ===

    @abstractmethod
    async def _execute_internal(self, context: ExecutionContext) -> DataType:
        """Основная логика компонента - должна быть реализована в наследниках"""
        pass

    # === Опциональные методы для переопределения ===

    async def _initialize_internal(self, context: ExecutionContext) -> None:
        """Пользовательская инициализация - может быть переопределена"""
        pass

    async def _cleanup_internal(self, context: ExecutionContext) -> None:
        """Пользовательская очистка - может быть переопределена"""
        pass

    async def _setup_resources(self, context: ExecutionContext) -> None:
        """Настройка ресурсов (подключения к БД, файлы и т.д.)"""
        pass

    async def _cleanup_resources(self, context: ExecutionContext) -> None:
        """Очистка ресурсов"""
        pass

    async def _validate_config(self, context: ExecutionContext) -> None:
        """Валидация конфигурации"""
        if hasattr(self.config, "validate"):
            if not self.config.validate():
                raise ValueError(f"Invalid configuration for component {self.name}")

    async def _check_dependencies(self, context: ExecutionContext) -> None:
        """Проверка зависимостей"""
        for dependency in self.dependencies:
            try:
                __import__(dependency)
            except ImportError as e:
                raise ImportError(
                    f"Missing dependency for {self.name}: {dependency}"
                ) from e

    async def _validate_input_data(self, context: ExecutionContext) -> None:
        """Валидация входных данных"""
        if self.input_schema and context.input_data is not None:
            # Здесь можно добавить JSON Schema валидацию
            pass

    async def _validate_output_data(self, result: ExecutionResult) -> None:
        """Валидация выходных данных"""
        if self.output_schema and result.data is not None:
            # Здесь можно добавить JSON Schema валидацию
            pass

    async def _dry_run_execute(self, context: ExecutionContext) -> DataType:
        """Выполнение в dry run режиме"""
        # По умолчанию возвращаем None, наследники могут переопределить
        return None

    async def _handle_error(self, context: ExecutionContext, error: Exception) -> None:
        """Обработка ошибок"""
        # Логирование уже выполнено в execute(), здесь можно добавить
        # дополнительную обработку (отправка уведомлений, cleanup и т.д.)
        pass

    # === Checkpoint функциональность ===

    async def _can_resume_from_checkpoint(self, context: ExecutionContext) -> bool:
        """Проверка возможности восстановления с checkpoint"""
        checkpoint_key = f"{context.pipeline_id}:{context.stage_name}:result"
        return checkpoint_key in self._checkpoints

    async def _resume_from_checkpoint(
        self, context: ExecutionContext
    ) -> ExecutionResult:
        """Восстановление выполнения с checkpoint"""
        checkpoint_key = f"{context.pipeline_id}:{context.stage_name}:result"
        checkpoint_data = self._checkpoints.get(checkpoint_key)

        if checkpoint_data:
            self._logger.info("Resumed from checkpoint", stage=context.stage_name)
            return ExecutionResult(**checkpoint_data)

        raise RuntimeError("Checkpoint data not found")

    async def _save_checkpoint(
        self, context: ExecutionContext, result: ExecutionResult
    ) -> None:
        """Сохранение checkpoint"""
        if self._checkpoint_strategy == CheckpointStrategy.MEMORY:
            checkpoint_key = f"{context.pipeline_id}:{context.stage_name}:result"
            self._checkpoints[checkpoint_key] = result.to_dict()

            self._logger.debug(
                "Checkpoint saved", stage=context.stage_name, strategy="memory"
            )

    # === Metrics tracking ===

    @asynccontextmanager
    async def _track_execution_metrics(self, context: ExecutionContext):
        """Context manager для отслеживания метрик выполнения"""
        start_time = datetime.now(timezone.utc)

        try:
            yield
        finally:
            duration = (datetime.now(timezone.utc) - start_time).total_seconds()

            COMPONENT_DURATION.labels(
                component_type=self.component_type.value,
                component_name=self.name,
                pipeline_id=context.pipeline_id,
            ).observe(duration)


# === Специализированные базовые классы ===


class BaseExtractor(BaseComponent[DataType, ConfigType]):
    """Базовый класс для extractors"""

    @property
    def component_type(self) -> ComponentType:
        return ComponentType.EXTRACTOR


class BaseTransformer(BaseComponent[DataType, ConfigType]):
    """Базовый класс для transformers"""

    @property
    def component_type(self) -> ComponentType:
        return ComponentType.TRANSFORMER


class BaseLoader(BaseComponent[DataType, ConfigType]):
    """Базовый класс для loaders"""

    @property
    def component_type(self) -> ComponentType:
        return ComponentType.LOADER


class BaseValidator(BaseComponent[DataType, ConfigType]):
    """Базовый класс для validators"""

    @property
    def component_type(self) -> ComponentType:
        return ComponentType.VALIDATOR


class BaseStage(BaseComponent[DataType, ConfigType]):
    """Базовый класс для generic stages"""

    @property
    def component_type(self) -> ComponentType:
        return ComponentType.STAGE
