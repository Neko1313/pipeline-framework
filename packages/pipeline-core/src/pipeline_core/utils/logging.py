"""
Утилиты для структурированного логирования
"""

import logging
import logging.config
import json
import sys
from typing import Dict, Any, Optional
from datetime import datetime
from pathlib import Path
import structlog


class JSONFormatter(logging.Formatter):
    """Форматтер для JSON логирования"""

    def format(self, record: logging.LogRecord) -> str:
        log_data = {
            "timestamp": datetime.fromtimestamp(record.created).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }

        # Добавляем дополнительные поля если есть
        if hasattr(record, "pipeline_id"):
            log_data["pipeline_id"] = record.pipeline_id
        if hasattr(record, "stage_name"):
            log_data["stage_name"] = record.stage_name
        if hasattr(record, "component_type"):
            log_data["component_type"] = record.component_type
        if hasattr(record, "execution_id"):
            log_data["execution_id"] = record.execution_id

        # Добавляем stack trace для ошибок
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_data, ensure_ascii=False)


class PipelineLoggerAdapter(logging.LoggerAdapter):
    """Адаптер логгера с контекстом pipeline"""

    def __init__(self, logger: logging.Logger, extra: Dict[str, Any]):
        super().__init__(logger, extra)

    def process(self, msg: str, kwargs: Dict[str, Any]) -> tuple:
        # Добавляем контекст pipeline в каждое сообщение
        return f"[{self.extra.get('pipeline_id', 'unknown')}] {msg}", kwargs


def setup_logging(
    level: str = "INFO",
    format_type: str = "json",
    log_file: Optional[Path] = None,
    enable_console: bool = True,
    pipeline_id: Optional[str] = None,
) -> None:
    """
    Настройка системы логирования

    Args:
        level: Уровень логирования (DEBUG, INFO, WARNING, ERROR)
        format_type: Формат логов ("json" или "text")
        log_file: Путь к файлу логов (опционально)
        enable_console: Включить вывод в консоль
        pipeline_id: ID pipeline для контекста
    """

    # Базовая конфигурация
    config = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "json": {
                "()": JSONFormatter,
            },
            "text": {
                "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                "datefmt": "%Y-%m-%d %H:%M:%S",
            },
        },
        "handlers": {},
        "loggers": {
            "pipeline": {
                "level": level,
                "handlers": [],
                "propagate": False,
            },
            "pipeline.core": {
                "level": level,
                "handlers": [],
                "propagate": True,
            },
            "pipeline.component": {
                "level": level,
                "handlers": [],
                "propagate": True,
            },
        },
        "root": {
            "level": level,
            "handlers": [],
        },
    }

    # Консольный handler
    if enable_console:
        config["handlers"]["console"] = {
            "class": "logging.StreamHandler",
            "stream": sys.stdout,
            "formatter": format_type,
            "level": level,
        }
        config["loggers"]["pipeline"]["handlers"].append("console")
        config["root"]["handlers"].append("console")

    # Файловый handler
    if log_file:
        log_file.parent.mkdir(parents=True, exist_ok=True)

        config["handlers"]["file"] = {
            "class": "logging.handlers.RotatingFileHandler",
            "filename": str(log_file),
            "maxBytes": 50 * 1024 * 1024,  # 50MB
            "backupCount": 5,
            "formatter": format_type,
            "level": level,
        }
        config["loggers"]["pipeline"]["handlers"].append("file")
        config["root"]["handlers"].append("file")

    # Применяем конфигурацию
    logging.config.dictConfig(config)

    # Настраиваем structlog если используется
    if format_type == "json":
        structlog.configure(
            processors=[
                structlog.stdlib.filter_by_level,
                structlog.stdlib.add_logger_name,
                structlog.stdlib.add_log_level,
                structlog.stdlib.PositionalArgumentsFormatter(),
                structlog.processors.TimeStamper(fmt="ISO"),
                structlog.processors.StackInfoRenderer(),
                structlog.processors.format_exc_info,
                structlog.processors.UnicodeDecoder(),
                structlog.processors.JSONRenderer(),
            ],
            context_class=dict,
            logger_factory=structlog.stdlib.LoggerFactory(),
            wrapper_class=structlog.stdlib.BoundLogger,
            cache_logger_on_first_use=True,
        )


def get_pipeline_logger(
    name: str,
    pipeline_id: Optional[str] = None,
    stage_name: Optional[str] = None,
    component_type: Optional[str] = None,
) -> PipelineLoggerAdapter:
    """
    Получение логгера с контекстом pipeline

    Args:
        name: Имя логгера
        pipeline_id: ID pipeline
        stage_name: Имя стадии
        component_type: Тип компонента

    Returns:
        PipelineLoggerAdapter: Логгер с контекстом
    """
    logger = logging.getLogger(f"pipeline.{name}")

    extra = {}
    if pipeline_id:
        extra["pipeline_id"] = pipeline_id
    if stage_name:
        extra["stage_name"] = stage_name
    if component_type:
        extra["component_type"] = component_type

    return PipelineLoggerAdapter(logger, extra)


def log_execution_start(
    logger: logging.Logger, pipeline_id: str, stage_name: str, component_type: str
) -> None:
    """Логирование начала выполнения стадии"""
    logger.info(
        "Начало выполнения стадии",
        extra={
            "pipeline_id": pipeline_id,
            "stage_name": stage_name,
            "component_type": component_type,
            "event_type": "execution_start",
        },
    )


def log_execution_success(
    logger: logging.Logger,
    pipeline_id: str,
    stage_name: str,
    component_type: str,
    execution_time: float,
    processed_records: int,
) -> None:
    """Логирование успешного завершения стадии"""
    logger.info(
        "Стадия выполнена успешно",
        extra={
            "pipeline_id": pipeline_id,
            "stage_name": stage_name,
            "component_type": component_type,
            "execution_time": execution_time,
            "processed_records": processed_records,
            "event_type": "execution_success",
        },
    )


def log_execution_failure(
    logger: logging.Logger,
    pipeline_id: str,
    stage_name: str,
    component_type: str,
    error: Exception,
    execution_time: float,
) -> None:
    """Логирование ошибки выполнения стадии"""
    logger.error(
        f"Ошибка выполнения стадии: {error}",
        extra={
            "pipeline_id": pipeline_id,
            "stage_name": stage_name,
            "component_type": component_type,
            "error_type": type(error).__name__,
            "error_message": str(error),
            "execution_time": execution_time,
            "event_type": "execution_failure",
        },
        exc_info=True,
    )


def log_pipeline_start(
    logger: logging.Logger, pipeline_id: str, pipeline_name: str, total_stages: int
) -> None:
    """Логирование начала выполнения pipeline"""
    logger.info(
        "Начало выполнения pipeline",
        extra={
            "pipeline_id": pipeline_id,
            "pipeline_name": pipeline_name,
            "total_stages": total_stages,
            "event_type": "pipeline_start",
        },
    )


def log_pipeline_completion(
    logger: logging.Logger,
    pipeline_id: str,
    pipeline_name: str,
    execution_time: float,
    successful_stages: int,
    failed_stages: int,
    total_records: int,
) -> None:
    """Логирование завершения выполнения pipeline"""
    status = (
        "success"
        if failed_stages == 0
        else "partial_failure"
        if successful_stages > 0
        else "failure"
    )

    logger.info(
        f"Pipeline завершен со статусом: {status}",
        extra={
            "pipeline_id": pipeline_id,
            "pipeline_name": pipeline_name,
            "execution_time": execution_time,
            "successful_stages": successful_stages,
            "failed_stages": failed_stages,
            "total_records": total_records,
            "status": status,
            "event_type": "pipeline_completion",
        },
    )


class LogContext:
    """Контекстный менеджер для логирования с дополнительными полями"""

    def __init__(self, logger: logging.Logger, **context):
        self.logger = logger
        self.context = context
        self.old_extra = getattr(logger, "_extra", {})

    def __enter__(self):
        # Добавляем контекст в логгер
        if hasattr(self.logger, "_extra"):
            self.logger._extra.update(self.context)
        else:
            self.logger._extra = self.context.copy()
        return self.logger

    def __exit__(self, exc_type, exc_val, exc_tb):
        # Восстанавливаем предыдущий контекст
        if hasattr(self.logger, "_extra"):
            self.logger._extra = self.old_extra


def with_log_context(logger: logging.Logger, **context):
    """
    Создание контекстного менеджера для логирования

    Usage:
        with with_log_context(logger, stage_name="extract", component_type="sql"):
            logger.info("Начало извлечения данных")
    """
    return LogContext(logger, **context)
