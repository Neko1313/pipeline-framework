"""
Pydantic модели для конфигурации pipeline

Предоставляет:
- Валидацию конфигурации
- Type safety
- JSON Schema generation
- Environment variables substitution
- Configuration inheritance
"""

from typing import Dict, List, Any, Optional, Union, Literal
from pydantic import BaseModel, Field, field_validator, model_validator, ConfigDict
from enum import Enum
from datetime import datetime, timedelta
import re
import os


class RetryPolicyType(str, Enum):
    """Типы политик повторных попыток"""

    EXPONENTIAL_BACKOFF = "exponential_backoff"
    FIXED_INTERVAL = "fixed_interval"
    LINEAR_BACKOFF = "linear_backoff"
    IMMEDIATE = "immediate"
    NONE = "none"


class WriteMode(str, Enum):
    """Режимы записи данных"""

    INSERT = "insert"
    UPSERT = "upsert"
    REPLACE = "replace"
    APPEND = "append"
    OVERWRITE = "overwrite"


class DataFormat(str, Enum):
    """Форматы данных"""

    JSON = "json"
    CSV = "csv"
    PARQUET = "parquet"
    AVRO = "avro"
    XML = "xml"
    EXCEL = "excel"


class LogLevel(str, Enum):
    """Уровни логирования"""

    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class Environment(str, Enum):
    """Окружения выполнения"""

    DEVELOPMENT = "development"
    TESTING = "testing"
    STAGING = "staging"
    PRODUCTION = "production"


class RetryPolicy(BaseModel):
    """Политика повторных попыток"""

    maximum_attempts: int = Field(
        3, ge=1, le=10, description="Максимальное количество попыток"
    )
    initial_interval_seconds: float = Field(
        30.0, ge=0.1, description="Начальный интервал в секундах"
    )
    backoff_coefficient: float = Field(
        2.0, ge=1.0, le=10.0, description="Коэффициент увеличения интервала"
    )
    max_interval_seconds: float = Field(
        300.0, ge=1.0, description="Максимальный интервал в секундах"
    )
    policy_type: RetryPolicyType = Field(
        RetryPolicyType.EXPONENTIAL_BACKOFF, description="Тип политики"
    )

    @field_validator("max_interval_seconds")
    @classmethod
    def max_interval_must_be_greater_than_initial(cls, v, info):
        if (
            hasattr(info, 'data') and "initial_interval_seconds" in info.data
            and v < info.data["initial_interval_seconds"]
        ):
            raise ValueError("max_interval_seconds must be >= initial_interval_seconds")
        return v


class ParallelConfig(BaseModel):
    """Конфигурация параллельного выполнения"""

    enabled: bool = Field(False, description="Включить параллельное выполнение")
    max_workers: int = Field(
        4, ge=1, le=32, description="Максимальное количество worker'ов"
    )
    batch_size: int = Field(1000, ge=1, description="Размер batch для обработки")
    timeout_seconds: Optional[int] = Field(
        None, ge=1, description="Timeout для каждого worker'а"
    )


class ResourceLimits(BaseModel):
    """Ограничения ресурсов"""

    memory_limit_mb: Optional[int] = Field(None, ge=64, description="Лимит памяти в MB")
    cpu_limit_cores: Optional[float] = Field(None, ge=0.1, description="Лимит CPU ядер")
    disk_limit_mb: Optional[int] = Field(
        None, ge=100, description="Лимит дискового пространства в MB"
    )
    network_limit_mbps: Optional[int] = Field(
        None, ge=1, description="Лимит сети в Mbps"
    )


class StageConfig(BaseModel):
    """Конфигурация отдельного этапа pipeline"""

    name: str = Field(..., min_length=1, description="Уникальное имя этапа")
    component: str = Field(
        ...,
        pattern=r"^(extractor|transformer|loader|validator|stage)/[\w-]+$",
        description="Компонент в формате 'type/name'",
    )
    config: Dict[str, Any] = Field(
        default_factory=dict, description="Конфигурация компонента"
    )

    # Dependencies
    depends_on: List[str] = Field(
        default_factory=list, description="Зависимости от других этапов"
    )

    # Execution settings
    enabled: bool = Field(True, description="Включен ли этап")
    retry_policy: RetryPolicy = Field(
        default_factory=RetryPolicy, description="Политика повторных попыток"
    )
    timeout_seconds: int = Field(3600, ge=1, description="Timeout в секундах")

    # Performance settings
    parallel: Optional[ParallelConfig] = Field(
        None, description="Настройки параллельного выполнения"
    )
    resource_limits: Optional[ResourceLimits] = Field(
        None, description="Ограничения ресурсов"
    )

    # Conditional execution
    condition: Optional[str] = Field(
        None, description="Условие выполнения (Python expression)"
    )

    # Output settings
    cache_result: bool = Field(True, description="Кэшировать результат этапа")
    checkpoint_enabled: bool = Field(True, description="Включить checkpoint'ы")

    @property
    def component_type(self) -> str:
        """Тип компонента"""
        return self.component.split("/")[0]

    @property
    def component_name(self) -> str:
        """Имя компонента"""
        return self.component.split("/")[1]

    @field_validator("component")
    @classmethod
    def validate_component_format(cls, v):
        """Валидация формата компонента"""
        parts = v.split("/")
        if len(parts) != 2:
            raise ValueError("Component must be in format 'type/name'")

        valid_types = {"extractor", "transformer", "loader", "validator", "stage"}
        if parts[0] not in valid_types:
            raise ValueError(f"Component type must be one of: {valid_types}")

        return v


class QualityCheck(BaseModel):
    """Конфигурация проверки качества данных"""

    name: str = Field(..., min_length=1, description="Имя проверки")
    component: str = Field(
        "validator/data-quality", description="Компонент для проверки"
    )
    applies_to: List[str] = Field(
        default_factory=list, description="Этапы, к которым применяется"
    )
    config: Dict[str, Any] = Field(
        default_factory=dict, description="Конфигурация проверки"
    )

    # Execution settings
    fail_pipeline_on_error: bool = Field(
        True, description="Остановить pipeline при ошибке"
    )
    warning_only: bool = Field(False, description="Только предупреждение, не ошибка")
    retry_policy: Optional[RetryPolicy] = Field(
        None, description="Политика повторных попыток"
    )

    @property
    def component_type(self) -> str:
        return self.component.split("/")[0]

    @property
    def component_name(self) -> str:
        return self.component.split("/")[1]


class NotificationConfig(BaseModel):
    """Конфигурация уведомлений"""

    enabled: bool = Field(False, description="Включить уведомления")
    on_success: bool = Field(False, description="Уведомления при успехе")
    on_failure: bool = Field(True, description="Уведомления при ошибке")
    on_retry: bool = Field(False, description="Уведомления при повторной попытке")

    # Channels
    email: List[str] = Field(default_factory=list, description="Email адреса")
    slack_webhook: Optional[str] = Field(None, description="Slack webhook URL")
    teams_webhook: Optional[str] = Field(None, description="Teams webhook URL")

    # Templates
    success_template: Optional[str] = Field(None, description="Шаблон для успеха")
    failure_template: Optional[str] = Field(None, description="Шаблон для ошибки")


class EncryptionConfig(BaseModel):
    """Конфигурация шифрования"""

    enabled: bool = Field(False, description="Включить шифрование")
    algorithm: str = Field("AES-256-GCM", description="Алгоритм шифрования")
    key_source: str = Field("env", description="Источник ключа (env, file, vault)")
    key_path: Optional[str] = Field(None, description="Путь к ключу")


class PipelineMetadata(BaseModel):
    """Метаданные pipeline"""

    owner: str = Field("", description="Владелец pipeline")
    description: str = Field("", description="Описание pipeline")

    # Scheduling
    schedule: Optional[str] = Field(None, description="Cron расписание")
    timezone: str = Field("UTC", description="Временная зона")

    # Categorization
    tags: List[str] = Field(default_factory=list, description="Теги для категоризации")
    category: Optional[str] = Field(None, description="Категория pipeline")
    priority: int = Field(5, ge=1, le=10, description="Приоритет (1-10)")

    # Environment
    environment: Environment = Field(Environment.DEVELOPMENT, description="Окружение")

    # Documentation
    documentation_url: Optional[str] = Field(None, description="Ссылка на документацию")
    repository_url: Optional[str] = Field(None, description="Ссылка на репозиторий")

    @field_validator("schedule")
    @classmethod
    def validate_cron_schedule(cls, v):
        """Валидация cron расписания"""
        if v is not None:
            # Простая проверка формата cron (5 или 6 полей)
            fields = v.split()
            if len(fields) not in [5, 6]:
                raise ValueError("Cron schedule must have 5 or 6 fields")
        return v


class PipelineConfig(BaseModel):
    """Полная конфигурация pipeline"""

    # Basic info
    name: str = Field(
        ..., min_length=1, pattern=r"^[a-zA-Z0-9-_]+$", description="Имя pipeline"
    )
    version: str = Field("1.0.0", pattern=r"^\d+\.\d+\.\d+", description="Версия")

    # Metadata
    metadata: PipelineMetadata = Field(
        default_factory=PipelineMetadata, description="Метаданные"
    )

    # Pipeline structure
    stages: List[StageConfig] = Field(..., min_length=1, description="Этапы pipeline")
    quality_checks: List[QualityCheck] = Field(
        default_factory=list, description="Проверки качества"
    )

    # Global configuration
    global_config: Dict[str, Any] = Field(
        default_factory=dict, description="Глобальная конфигурация"
    )

    # Execution settings
    continue_on_failure: bool = Field(
        False, description="Продолжить выполнение при ошибке"
    )
    max_parallel_stages: int = Field(
        4, ge=1, le=16, description="Максимум параллельных этапов"
    )

    # Monitoring and notifications
    notifications: NotificationConfig = Field(
        default_factory=NotificationConfig, description="Настройки уведомлений"
    )

    # Security
    encryption: EncryptionConfig = Field(
        default_factory=EncryptionConfig, description="Настройки шифрования"
    )

    # Advanced
    allow_dynamic_config: bool = Field(
        False, description="Разрешить динамическую конфигурацию"
    )
    validate_data_schema: bool = Field(
        True, description="Валидировать схему данных между этапами"
    )

    def substitute_environment_variables(self):
        """Подстановка переменных окружения в конфигурацию"""

        def substitute_value(value):
            if isinstance(value, str):
                # Поддержка различных форматов переменных
                # ${VAR}, ${VAR:default}, $VAR
                pattern = r"\$\{([^}:]+)(?::([^}]*))?\}"

                def replace_var(match):
                    var_name = match.group(1)
                    default_value = match.group(2) if match.group(2) is not None else ""
                    return os.getenv(var_name, default_value)

                return re.sub(pattern, replace_var, value)
            elif isinstance(value, dict):
                return {k: substitute_value(v) for k, v in value.items()}
            elif isinstance(value, list):
                return [substitute_value(v) for v in value]
            return value

        # Применяем подстановку ко всем полям
        for stage in self.stages:
            stage.config = substitute_value(stage.config)

        for check in self.quality_checks:
            check.config = substitute_value(check.config)

        self.global_config = substitute_value(self.global_config)


class RuntimeConfig(BaseModel):
    """Конфигурация времени выполнения"""

    run_id: str = Field(..., description="Идентификатор запуска")
    dry_run: bool = Field(False, description="Режим сухого запуска")
    resume_from_stage: Optional[str] = Field(None, description="Возобновить с этапа")
    skip_stages: List[str] = Field(default_factory=list, description="Пропустить этапы")
    override_config: Dict[str, Any] = Field(
        default_factory=dict, description="Переопределение конфигурации"
    )

    # Execution context
    triggered_by: str = Field("manual", description="Кто/что запустило pipeline")
    trigger_time: datetime = Field(
        default_factory=datetime.now, description="Время запуска"
    )

    # Environment overrides
    environment_overrides: Dict[str, str] = Field(
        default_factory=dict, description="Переопределение переменных окружения"
    )


# Модель для полной конфигурации с runtime
class FullPipelineConfig(BaseModel):
    """Полная конфигурация включая runtime"""

    pipeline: PipelineConfig
    runtime: Optional[RuntimeConfig] = None

    model_config = ConfigDict(
        extra="allow",
        validate_assignment=True,
        use_enum_values=True,
    )

class TemporalConfig(BaseModel):
    """Конфигурация Temporal"""

    enabled: bool = Field(False, description="Использовать Temporal для оркестрации")

    # Connection
    server_address: str = Field("localhost:7233", description="Адрес Temporal сервера")
    namespace: str = Field("default", description="Temporal namespace")

    # Workflow settings
    task_queue: str = Field("pipeline-tasks", description="Task queue для pipeline")
    workflow_id_prefix: str = Field("pipeline", description="Префикс для Workflow ID")

    # Timeouts
    workflow_execution_timeout: Optional[int] = Field(
        None, ge=1, description="Timeout для workflow"
    )
    workflow_run_timeout: Optional[int] = Field(
        None, ge=1, description="Run timeout для workflow"
    )
    workflow_task_timeout: Optional[int] = Field(
        None, ge=1, description="Task timeout для workflow"
    )

    # Security
    tls_enabled: bool = Field(False, description="Использовать TLS")
    cert_path: Optional[str] = Field(None, description="Путь к сертификату")
    key_path: Optional[str] = Field(None, description="Путь к ключу")