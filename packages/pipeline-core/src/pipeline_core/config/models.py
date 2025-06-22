# packages/pipeline-core/src/pipeline_core/config/models.py

"""
Pydantic модели для конфигурации pipeline framework

Включает:
- PipelineConfig: Основная конфигурация pipeline
- StageConfig: Конфигурация этапа pipeline
- RetryPolicy: Политика повторов
- QualityCheck: Проверки качества данных
- TemporalConfig: Настройки Temporal
- RuntimeConfig: Runtime конфигурация
"""

from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Union, Literal
from enum import Enum

from pydantic import BaseModel, Field, field_validator, computed_field, ConfigDict
import structlog

logger = structlog.get_logger(__name__)


class LogLevel(str, Enum):
    """Уровни логирования"""

    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class ScheduleType(str, Enum):
    """Типы расписания"""

    CRON = "cron"
    INTERVAL = "interval"
    MANUAL = "manual"
    EVENT_DRIVEN = "event_driven"


class WriteMode(str, Enum):
    """Режимы записи данных"""

    APPEND = "append"
    OVERWRITE = "overwrite"
    UPSERT = "upsert"
    INSERT = "insert"
    MERGE = "merge"


class RetryPolicy(BaseModel):
    """Конфигурация политики повторов"""

    model_config = ConfigDict(extra="forbid")

    maximum_attempts: int = Field(
        default=3, ge=1, le=10, description="Максимальное количество попыток"
    )
    initial_interval: str = Field(
        default="30s", description="Начальный интервал ожидания"
    )
    maximum_interval: str = Field(
        default="5m", description="Максимальный интервал ожидания"
    )
    backoff_coefficient: float = Field(
        default=2.0, ge=1.0, le=10.0, description="Коэффициент экспоненциального роста"
    )

    # Дополнительные настройки
    jitter: bool = Field(default=True, description="Добавлять случайный jitter")
    non_retryable_error_types: List[str] = Field(
        default_factory=lambda: ["ConfigurationError", "AuthenticationError"],
        description="Типы ошибок, которые не нужно повторять",
    )

    @field_validator("initial_interval", "maximum_interval")
    @classmethod
    def validate_duration(cls, v: str) -> str:
        """Валидация строк длительности"""
        if not v:
            raise ValueError("Duration cannot be empty")

        # Простая валидация формата (например, "30s", "5m", "1h")
        import re

        pattern = r"^\d+[smhd]$"
        if not re.match(pattern, v):
            raise ValueError("Duration must be in format like '30s', '5m', '1h', '2d'")

        return v

    def to_seconds(self, duration_str: str) -> float:
        """Конвертация строки длительности в секунды"""
        import re

        match = re.match(r"^(\d+)([smhd])$", duration_str)
        if not match:
            raise ValueError(f"Invalid duration format: {duration_str}")

        value, unit = match.groups()
        value = int(value)

        multipliers = {
            "s": 1,
            "m": 60,
            "h": 3600,
            "d": 86400,
        }

        return value * multipliers[unit]

    @property
    def initial_interval_seconds(self) -> float:
        """Начальный интервал в секундах"""
        return self.to_seconds(self.initial_interval)

    @property
    def maximum_interval_seconds(self) -> float:
        """Максимальный интервал в секундах"""
        return self.to_seconds(self.maximum_interval)


class QualityCheck(BaseModel):
    """Конфигурация проверки качества данных"""

    model_config = ConfigDict(extra="allow")

    name: str = Field(..., description="Имя проверки качества")
    type: str = Field(
        ..., description="Тип проверки (count_validator, schema_validator, etc.)"
    )
    config: Dict[str, Any] = Field(
        default_factory=dict, description="Конфигурация проверки"
    )

    # Опциональные параметры
    applies_to: Optional[List[str]] = Field(
        default=None, description="К каким этапам применяется"
    )
    fail_on_error: bool = Field(
        default=True, description="Останавливать pipeline при ошибке"
    )
    warning_threshold: Optional[float] = Field(
        default=None, description="Порог для предупреждений"
    )

    enabled: bool = Field(default=True, description="Включена ли проверка")
    timeout: Optional[str] = Field(default="5m", description="Timeout для проверки")

    @field_validator("name")
    @classmethod
    def validate_name(cls, v: str) -> str:
        """Валидация имени проверки"""
        if not v or not v.strip():
            raise ValueError("Quality check name cannot be empty")

        # Проверяем что имя содержит только допустимые символы
        import re

        if not re.match(r"^[a-zA-Z][a-zA-Z0-9_-]*$", v):
            raise ValueError(
                "Quality check name must start with letter and contain only letters, numbers, underscores and hyphens"
            )

        return v.strip()


class ParallelConfig(BaseModel):
    """Конфигурация параллельного выполнения"""

    model_config = ConfigDict(extra="forbid")

    enabled: bool = Field(
        default=False, description="Включено ли параллельное выполнение"
    )
    max_workers: int = Field(
        default=4, ge=1, le=100, description="Максимальное количество воркеров"
    )
    chunk_size: Optional[int] = Field(
        default=None, ge=1, description="Размер чанка для обработки"
    )

    # Настройки ресурсов
    memory_limit_mb: Optional[int] = Field(
        default=None, ge=100, description="Лимит памяти в МБ"
    )
    cpu_limit: Optional[float] = Field(
        default=None, ge=0.1, le=16.0, description="Лимит CPU"
    )


class ErrorHandling(BaseModel):
    """Конфигурация обработки ошибок"""

    model_config = ConfigDict(extra="allow")

    on_error: Literal["fail", "skip", "continue", "retry"] = Field(
        default="fail", description="Действие при ошибке"
    )

    max_errors: Optional[int] = Field(
        default=None, ge=0, description="Максимальное количество ошибок"
    )
    error_threshold_percent: Optional[float] = Field(
        default=None, ge=0.0, le=100.0, description="Порог ошибок в процентах"
    )

    # Действия при ошибках
    notify_on_error: bool = Field(
        default=True, description="Отправлять уведомления об ошибках"
    )
    save_error_data: bool = Field(
        default=False, description="Сохранять данные с ошибками"
    )
    error_data_path: Optional[str] = Field(
        default=None, description="Путь для сохранения данных с ошибками"
    )


class StageConfig(BaseModel):
    """Конфигурация этапа pipeline"""

    model_config = ConfigDict(extra="allow")

    # Основные параметры
    name: str = Field(..., description="Уникальное имя этапа")
    component: str = Field(
        ..., description="Тип/имя компонента (например, 'extractor/sql')"
    )
    description: Optional[str] = Field(default="", description="Описание этапа")

    # Конфигурация компонента
    config: Dict[str, Any] = Field(
        default_factory=dict, description="Конфигурация компонента"
    )

    # Зависимости и порядок выполнения
    depends_on: List[str] = Field(
        default_factory=list, description="Зависимости от других этапов"
    )
    execution_order: Optional[int] = Field(
        default=None, description="Порядок выполнения"
    )

    # Управление выполнением
    enabled: bool = Field(default=True, description="Включен ли этап")
    timeout: Optional[str] = Field(default="30m", description="Timeout для этапа")

    # Retry и обработка ошибок
    retry_policy: Optional[RetryPolicy] = Field(
        default=None, description="Политика повторов"
    )
    error_handling: Optional[ErrorHandling] = Field(
        default=None, description="Обработка ошибок"
    )

    # Параллельное выполнение
    parallel: Optional[ParallelConfig] = Field(
        default=None, description="Настройки параллельного выполнения"
    )

    # Проверки качества для этапа
    quality_checks: List[QualityCheck] = Field(
        default_factory=list, description="Проверки качества"
    )

    # Метаданные
    tags: List[str] = Field(default_factory=list, description="Теги этапа")
    metadata: Dict[str, Any] = Field(
        default_factory=dict, description="Дополнительные метаданные"
    )

    @field_validator("name")
    @classmethod
    def validate_name(cls, v: str) -> str:
        """Валидация имени этапа"""
        if not v or not v.strip():
            raise ValueError("Stage name cannot be empty")

        # Проверяем формат имени
        import re

        if not re.match(r"^[a-zA-Z][a-zA-Z0-9_-]*$", v):
            raise ValueError(
                "Stage name must start with letter and contain only letters, numbers, underscores and hyphens"
            )

        return v.strip()

    @field_validator("component")
    @classmethod
    def validate_component(cls, v: str) -> str:
        """Валидация компонента"""
        if not v or not v.strip():
            raise ValueError("Component cannot be empty")

        # Проверяем формат компонента (тип/имя)
        if "/" not in v:
            raise ValueError(
                "Component must be in format 'type/name' (e.g., 'extractor/sql')"
            )

        parts = v.split("/")
        if len(parts) != 2:
            raise ValueError("Component must have exactly one '/' separator")

        component_type, component_name = parts
        if not component_type or not component_name:
            raise ValueError("Both component type and name must be non-empty")

        return v.strip()

    @field_validator("depends_on")
    @classmethod
    def validate_dependencies(cls, v: List[str]) -> List[str]:
        """Валидация зависимостей"""
        # Проверяем что нет пустых строк
        filtered = [dep.strip() for dep in v if dep.strip()]

        # Проверяем на дублирование
        if len(filtered) != len(set(filtered)):
            raise ValueError("Dependencies cannot contain duplicates")

        return filtered

    @computed_field
    @property
    def component_type(self) -> str:
        """Тип компонента"""
        return self.component.split("/")[0]

    @computed_field
    @property
    def component_name(self) -> str:
        """Имя компонента"""
        return self.component.split("/")[1]


class AlertConfig(BaseModel):
    """Конфигурация алертов"""

    model_config = ConfigDict(extra="allow")

    name: str = Field(..., description="Имя алерта")
    condition: str = Field(..., description="Условие срабатывания алерта")
    channels: List[str] = Field(default_factory=list, description="Каналы уведомлений")
    recipients: List[str] = Field(
        default_factory=list, description="Получатели уведомлений"
    )

    enabled: bool = Field(default=True, description="Включен ли алерт")
    severity: Literal["info", "warning", "error", "critical"] = Field(
        default="error", description="Уровень важности"
    )

    # Настройки throttling
    throttle_minutes: Optional[int] = Field(
        default=None, ge=1, description="Интервал throttling в минутах"
    )
    max_alerts_per_hour: Optional[int] = Field(
        default=None, ge=1, description="Максимум алертов в час"
    )


class SLAConfig(BaseModel):
    """Конфигурация SLA"""

    model_config = ConfigDict(extra="forbid")

    max_duration_minutes: Optional[int] = Field(
        default=None, ge=1, description="Максимальная длительность в минутах"
    )
    max_memory_mb: Optional[int] = Field(
        default=None, ge=100, description="Максимальное использование памяти"
    )
    min_success_rate_percent: float = Field(
        default=95.0,
        ge=0.0,
        le=100.0,
        description="Минимальный процент успешных выполнений",
    )

    alert_on_breach: bool = Field(
        default=True, description="Алертить при нарушении SLA"
    )
    escalation_after_minutes: Optional[int] = Field(
        default=None, ge=1, description="Эскалация через X минут"
    )


class MonitoringConfig(BaseModel):
    """Конфигурация мониторинга"""

    model_config = ConfigDict(extra="forbid")

    metrics_enabled: bool = Field(default=True, description="Включены ли метрики")
    log_level: LogLevel = Field(
        default=LogLevel.INFO, description="Уровень логирования"
    )
    heartbeat_interval: int = Field(
        default=30, ge=1, le=300, description="Интервал heartbeat в секундах"
    )

    # Настройки метрик
    metrics_export_interval: int = Field(
        default=60, ge=10, description="Интервал экспорта метрик в секундах"
    )
    custom_metrics: Dict[str, Any] = Field(
        default_factory=dict, description="Кастомные метрики"
    )

    # Трейсинг
    tracing_enabled: bool = Field(default=False, description="Включен ли трейсинг")
    trace_sample_rate: float = Field(
        default=0.1, ge=0.0, le=1.0, description="Частота сэмплирования трейсов"
    )


class PipelineMetadata(BaseModel):
    """Метаданные pipeline"""

    model_config = ConfigDict(extra="allow")

    # Основная информация
    owner: Optional[str] = Field(default=None, description="Владелец pipeline")
    team: Optional[str] = Field(default=None, description="Команда")
    environment: str = Field(default="development", description="Окружение")

    # Расписание
    schedule: Optional[str] = Field(default=None, description="Cron расписание")
    schedule_type: ScheduleType = Field(
        default=ScheduleType.MANUAL, description="Тип расписания"
    )
    timezone: str = Field(default="UTC", description="Временная зона")

    # Классификация
    tags: List[str] = Field(default_factory=list, description="Теги pipeline")
    category: Optional[str] = Field(default=None, description="Категория pipeline")
    priority: Literal["low", "medium", "high", "critical"] = Field(
        default="medium", description="Приоритет"
    )

    # SLA и мониторинг
    sla: Optional[SLAConfig] = Field(default=None, description="Настройки SLA")
    monitoring: MonitoringConfig = Field(
        default_factory=MonitoringConfig, description="Настройки мониторинга"
    )

    # Контакты
    on_call: Optional[str] = Field(default=None, description="Дежурный контакт")
    documentation_url: Optional[str] = Field(
        default=None, description="Ссылка на документацию"
    )

    @field_validator("schedule")
    @classmethod
    def validate_schedule(cls, v: Optional[str]) -> Optional[str]:
        """Валидация cron расписания"""
        if v is None:
            return v

        # Простая валидация cron формата
        import re

        cron_pattern = r"^(\*|[0-5]?\d|\*\/\d+)(\s+(\*|[0-5]?\d|\*\/\d+)){4}$"
        if not re.match(cron_pattern, v.strip()):
            raise ValueError("Invalid cron schedule format")

        return v.strip()


class TemporalConfig(BaseModel):
    """Конфигурация Temporal"""

    model_config = ConfigDict(extra="forbid")

    # Подключение
    server_address: str = Field(
        default="localhost:7233", description="Адрес Temporal сервера"
    )
    namespace: str = Field(default="default", description="Namespace в Temporal")

    # Workflow настройки
    workflow_id_template: str = Field(
        default="{pipeline_name}-{date}-{run_id}", description="Шаблон для workflow ID"
    )
    task_queue: str = Field(
        default="data-pipeline-queue", description="Task queue для Activities"
    )

    # Timeouts
    workflow_timeout: str = Field(default="2h", description="Timeout для Workflow")
    activity_timeout: str = Field(default="30m", description="Timeout для Activity")

    # Retry policies
    activity_retry_policy: Optional[RetryPolicy] = Field(
        default_factory=lambda: RetryPolicy(
            maximum_attempts=3,
            initial_interval="1s",
            maximum_interval="1m",
            backoff_coefficient=2.0,
        ),
        description="Retry policy для Activities",
    )

    # TLS
    tls_enabled: bool = Field(default=False, description="Включен ли TLS")
    cert_path: Optional[str] = Field(default=None, description="Путь к сертификату")
    key_path: Optional[str] = Field(default=None, description="Путь к приватному ключу")


class RuntimeConfig(BaseModel):
    """Runtime конфигурация pipeline"""

    model_config = ConfigDict(extra="allow")

    # Выполнение
    dry_run: bool = Field(default=False, description="Режим dry run")
    debug_mode: bool = Field(default=False, description="Режим отладки")

    # Ресурсы
    max_memory_mb: Optional[int] = Field(
        default=None, ge=100, description="Лимит памяти"
    )
    max_cpu_cores: Optional[float] = Field(
        default=None, ge=0.1, description="Лимит CPU"
    )

    # Параллелизм
    max_parallel_stages: int = Field(
        default=4, ge=1, le=50, description="Максимум параллельных этапов"
    )
    enable_stage_parallelism: bool = Field(
        default=True, description="Включить параллелизм этапов"
    )

    # Checkpoint и persistence
    enable_checkpoints: bool = Field(default=True, description="Включить checkpoint'ы")
    checkpoint_interval: int = Field(
        default=300, ge=60, description="Интервал checkpoint'ов в секундах"
    )
    checkpoint_storage: Literal["memory", "disk", "database"] = Field(
        default="disk", description="Хранилище checkpoint'ов"
    )

    # Логирование и метрики
    log_format: Literal["json", "text", "structured"] = Field(
        default="structured", description="Формат логов"
    )
    metrics_export_enabled: bool = Field(default=True, description="Экспорт метрик")

    # Временные директории
    temp_dir: Optional[str] = Field(default=None, description="Временная директория")
    cleanup_temp_files: bool = Field(
        default=True, description="Очищать временные файлы"
    )


class ComponentSettings(BaseModel):
    """Базовые настройки для компонентов"""

    model_config = ConfigDict(extra="allow")

    # Основные настройки
    enabled: bool = Field(default=True, description="Включен ли компонент")
    timeout: Optional[float] = Field(
        default=None, ge=0, description="Timeout в секундах"
    )

    # Retry
    retry_attempts: int = Field(
        default=0, ge=0, le=10, description="Количество повторов"
    )
    retry_delay: float = Field(
        default=1.0, ge=0, description="Задержка между повторами"
    )

    # Мониторинг
    metrics_enabled: bool = Field(default=True, description="Включены ли метрики")
    logging_enabled: bool = Field(default=True, description="Включено ли логирование")
    log_level: LogLevel = Field(
        default=LogLevel.INFO, description="Уровень логирования"
    )

    # Безопасность
    mask_sensitive_data: bool = Field(
        default=True, description="Маскировать чувствительные данные"
    )

    # Кастомные настройки (будут переданы в компонент как есть)
    custom_config: Dict[str, Any] = Field(
        default_factory=dict, description="Кастомные настройки"
    )


class PipelineConfig(BaseModel):
    """Основная конфигурация pipeline"""

    model_config = ConfigDict(extra="allow")

    # Основная информация
    name: str = Field(..., description="Имя pipeline")
    version: str = Field(default="1.0.0", description="Версия pipeline")
    description: str = Field(default="", description="Описание pipeline")

    # Метаданные
    metadata: PipelineMetadata = Field(
        default_factory=PipelineMetadata, description="Метаданные pipeline"
    )

    # Глобальные переменные
    variables: Dict[str, Any] = Field(
        default_factory=dict, description="Глобальные переменные"
    )

    # Этапы pipeline
    stages: List[StageConfig] = Field(..., min_length=1, description="Этапы pipeline")

    # Политики по умолчанию
    default_retry_policy: Optional[RetryPolicy] = Field(
        default=None, description="Retry policy по умолчанию"
    )
    default_timeout: str = Field(default="30m", description="Timeout по умолчанию")

    # Проверки качества на уровне pipeline
    quality_checks: List[QualityCheck] = Field(
        default_factory=list, description="Глобальные проверки качества"
    )

    # Алерты
    alerts: List[AlertConfig] = Field(
        default_factory=list, description="Настройки алертов"
    )

    # Конфигурации интеграций
    temporal: Optional[TemporalConfig] = Field(
        default=None, description="Настройки Temporal"
    )

    # Runtime настройки
    runtime: RuntimeConfig = Field(
        default_factory=RuntimeConfig, description="Runtime настройки"
    )

    @field_validator("name")
    @classmethod
    def validate_name(cls, v: str) -> str:
        """Валидация имени pipeline"""
        if not v or not v.strip():
            raise ValueError("Pipeline name cannot be empty")

        # Проверяем формат имени
        import re

        if not re.match(r"^[a-zA-Z][a-zA-Z0-9_-]*$", v):
            raise ValueError(
                "Pipeline name must start with letter and contain only letters, numbers, underscores and hyphens"
            )

        return v.strip()

    @field_validator("version")
    @classmethod
    def validate_version(cls, v: str) -> str:
        """Валидация версии (семантическое версионирование)"""
        import re

        # Проверяем семантическое версионирование (major.minor.patch)
        pattern = r"^\d+\.\d+\.\d+(-[a-zA-Z0-9]+(\.[a-zA-Z0-9]+)*)?(\+[a-zA-Z0-9]+(\.[a-zA-Z0-9]+)*)?$"
        if not re.match(pattern, v):
            raise ValueError(
                "Version must follow semantic versioning format (e.g., '1.0.0', '1.2.3-alpha.1')"
            )

        return v

    @field_validator("stages")
    @classmethod
    def validate_stages(cls, v: List[StageConfig]) -> List[StageConfig]:
        """Валидация этапов pipeline"""
        if not v:
            raise ValueError("Pipeline must have at least one stage")

        # Проверяем уникальность имен этапов
        stage_names = [stage.name for stage in v]
        if len(stage_names) != len(set(stage_names)):
            raise ValueError("Stage names must be unique")

        # Проверяем валидность зависимостей
        for stage in v:
            for dependency in stage.depends_on:
                if dependency not in stage_names:
                    raise ValueError(
                        f"Stage '{stage.name}' depends on non-existent stage '{dependency}'"
                    )

                if dependency == stage.name:
                    raise ValueError(f"Stage '{stage.name}' cannot depend on itself")

        # Проверяем на циклические зависимости
        cls._check_circular_dependencies(v)

        return v

    @staticmethod
    def _check_circular_dependencies(stages: List[StageConfig]) -> None:
        """Проверка на циклические зависимости"""
        # Строим граф зависимостей
        graph = {stage.name: stage.depends_on for stage in stages}

        # DFS для обнаружения циклов
        def has_cycle(node: str, visited: set, rec_stack: set) -> bool:
            visited.add(node)
            rec_stack.add(node)

            for neighbor in graph.get(node, []):
                if neighbor not in visited:
                    if has_cycle(neighbor, visited, rec_stack):
                        return True
                elif neighbor in rec_stack:
                    return True

            rec_stack.remove(node)
            return False

        visited = set()
        for stage_name in graph:
            if stage_name not in visited:
                if has_cycle(stage_name, visited, set()):
                    raise ValueError(
                        f"Circular dependency detected involving stage '{stage_name}'"
                    )

    @computed_field
    @property
    def stage_names(self) -> List[str]:
        """Список имен этапов"""
        return [stage.name for stage in self.stages]

    @computed_field
    @property
    def total_stages(self) -> int:
        """Общее количество этапов"""
        return len(self.stages)

    def get_stage(self, name: str) -> Optional[StageConfig]:
        """Получение этапа по имени"""
        for stage in self.stages:
            if stage.name == name:
                return stage
        return None

    def get_stage_dependencies(self, stage_name: str) -> List[str]:
        """Получение зависимостей этапа"""
        stage = self.get_stage(stage_name)
        return stage.depends_on if stage else []

    def get_execution_order(self) -> List[List[str]]:
        """
        Получение порядка выполнения этапов с учетом зависимостей

        Returns:
            Список групп этапов, где каждая группа может выполняться параллельно
        """
        # Строим граф зависимостей
        graph = {stage.name: stage.depends_on for stage in self.stages}
        in_degree = {stage.name: 0 for stage in self.stages}

        # Подсчитываем входящие ребра
        for stage_name, dependencies in graph.items():
            for dep in dependencies:
                in_degree[stage_name] += 1

        # Топологическая сортировка с группировкой
        result = []
        remaining = set(stage.name for stage in self.stages)

        while remaining:
            # Находим этапы без зависимостей (могут выполняться параллельно)
            current_batch = [
                stage_name for stage_name in remaining if in_degree[stage_name] == 0
            ]

            if not current_batch:
                raise ValueError("Circular dependency detected in pipeline stages")

            result.append(current_batch)

            # Удаляем выполненные этапы и обновляем in_degree
            for stage_name in current_batch:
                remaining.remove(stage_name)
                for other_stage in remaining:
                    if stage_name in graph[other_stage]:
                        in_degree[other_stage] -= 1

        return result
