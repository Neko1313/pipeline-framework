"""
Базовые настройки для компонентов pipeline
"""

from enum import Enum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator


class LogLevel(str, Enum):
    """Уровни логирования"""

    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class ComponentSettings(BaseModel):
    """Базовые настройки для всех компонентов pipeline"""

    model_config = ConfigDict(
        extra="allow",
        validate_assignment=True,
        use_enum_values=True,
    )

    # Основные настройки
    enabled: bool = Field(True, description="Включен ли компонент")
    timeout_seconds: int | None = Field(None, ge=1, description="Timeout выполнения")
    retry_attempts: int = Field(
        0, ge=0, le=10, description="Количество повторных попыток"
    )

    # Логирование
    log_level: LogLevel = Field(LogLevel.INFO, description="Уровень логирования")
    metrics_enabled: bool = Field(True, description="Включить метрики")

    # Дополнительная конфигурация
    custom_config: dict[str, Any] = Field(
        default_factory=dict, description="Дополнительная конфигурация"
    )

    @field_validator("log_level")
    @classmethod
    def validate_log_level(cls, v):
        if isinstance(v, str):
            valid_levels = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
            if v.upper() not in valid_levels:
                raise ValueError(f"log_level must be one of: {valid_levels}")
            return v.upper()
        return v
