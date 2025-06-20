"""
Структурированное логирование для pipeline framework

Обеспечивает:
- Структурированные логи в JSON формате
- Контекстное логирование
- Корреляционные ID
- Интеграция с различными backend'ами
- Производительность и безопасность
"""

import os
import sys
import logging
from pathlib import Path
from typing import Any, Dict, Optional, Union, TextIO
from datetime import datetime

import structlog
from structlog.stdlib import BoundLogger


class PipelineLoggerConfig:
    """Конфигурация логгера pipeline"""

    def __init__(
        self,
        level: str = "INFO",
        format: str = "json",  # json, console, text
        output: Union[str, TextIO] = sys.stdout,
        include_caller: bool = True,
        include_timestamp: bool = True,
        include_level: bool = True,
        correlation_id_header: str = "x-correlation-id",
        max_string_length: int = 2000,
        sanitize_keys: bool = True,
    ):
        self.level = level.upper()
        self.format = format.lower()
        self.output = output
        self.include_caller = include_caller
        self.include_timestamp = include_timestamp
        self.include_level = include_level
        self.correlation_id_header = correlation_id_header
        self.max_string_length = max_string_length
        self.sanitize_keys = sanitize_keys


def sanitize_sensitive_data(logger, method_name, event_dict):
    """Процессор для очистки чувствительных данных"""

    sensitive_keys = {
        "password",
        "passwd",
        "secret",
        "token",
        "key",
        "auth",
        "authorization",
        "credentials",
        "connection_string",
        "database_url",
        "api_key",
        "private_key",
    }

    def sanitize_value(key: str, value: Any) -> Any:
        if isinstance(key, str) and any(
            sensitive in key.lower() for sensitive in sensitive_keys
        ):
            if isinstance(value, str):
                return "***REDACTED***" if value else ""
            return "***REDACTED***"
        return value

    def sanitize_dict(data: Dict[str, Any]) -> Dict[str, Any]:
        result = {}
        for k, v in data.items():
            if isinstance(v, dict):
                result[k] = sanitize_dict(v)
            elif isinstance(v, list):
                result[k] = [
                    sanitize_dict(item)
                    if isinstance(item, dict)
                    else sanitize_value(k, item)
                    for item in v
                ]
            else:
                result[k] = sanitize_value(k, v)
        return result

    return sanitize_dict(event_dict)


def truncate_long_values(logger, method_name, event_dict):
    """Процессор для обрезания длинных значений"""

    max_length = getattr(logger, "_max_string_length", 2000)

    def truncate_value(value: Any) -> Any:
        if isinstance(value, str) and len(value) > max_length:
            return value[:max_length] + "... [TRUNCATED]"
        elif isinstance(value, dict):
            return {k: truncate_value(v) for k, v in value.items()}
        elif isinstance(value, list):
            return [truncate_value(item) for item in value]
        return value

    return {k: truncate_value(v) for k, v in event_dict.items()}


def add_pipeline_context(logger, method_name, event_dict):
    """Процессор для добавления контекста pipeline"""

    # Добавляем информацию о процессе
    event_dict["process_id"] = os.getpid()

    # Добавляем hostname
    import socket

    try:
        event_dict["hostname"] = socket.gethostname()
    except Exception:
        event_dict["hostname"] = "unknown"

    # Добавляем версию framework'а
    event_dict["framework_version"] = "0.1.0"

    return event_dict


def setup_logging(config: Optional[PipelineLoggerConfig] = None) -> None:
    """
    Настройка системы логирования для pipeline framework

    Args:
        config: Конфигурация логгера, если None - используются значения по умолчанию
    """

    if config is None:
        # Определяем настройки из переменных окружения
        log_level = os.getenv("PIPELINE_LOG_LEVEL", "INFO")
        log_format = os.getenv("PIPELINE_LOG_FORMAT", "json")
        log_file = os.getenv("PIPELINE_LOG_FILE")

        output = sys.stdout
        if log_file:
            try:
                output = open(log_file, "a", encoding="utf-8")
            except Exception:
                output = sys.stdout

        config = PipelineLoggerConfig(level=log_level, format=log_format, output=output)

    # Базовые процессоры
    processors = [
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        add_pipeline_context,
    ]

    # Добавляем уровень логирования
    if config.include_level:
        processors.append(structlog.stdlib.add_log_level)

    # Добавляем timestamp
    if config.include_timestamp:
        processors.append(structlog.processors.TimeStamper(fmt="iso"))

    # Добавляем информацию о вызывающем коде
    if config.include_caller:
        processors.append(
            structlog.processors.CallsiteParameterAdder(
                parameters=[
                    structlog.processors.CallsiteParameter.FUNC_NAME,
                    structlog.processors.CallsiteParameter.LINENO,
                ]
            )
        )

    # Обработка исключений
    processors.append(structlog.processors.format_exc_info)

    # Очистка чувствительных данных
    if config.sanitize_keys:
        processors.append(sanitize_sensitive_data)

    # Обрезание длинных значений
    processors.append(truncate_long_values)

    # Финальный рендерер в зависимости от формата
    if config.format == "json":
        processors.append(structlog.processors.JSONRenderer())
    elif config.format == "console":
        processors.append(structlog.dev.ConsoleRenderer(colors=True))
    else:  # text format
        processors.append(structlog.processors.LogfmtRenderer())

    # Настраиваем structlog
    structlog.configure(
        processors=processors,
        wrapper_class=structlog.stdlib.BoundLogger,
        logger_factory=structlog.stdlib.LoggerFactory(),
        context_class=dict,
        cache_logger_on_first_use=True,
    )

    # Настраиваем стандартный logging
    logging.basicConfig(
        level=getattr(logging, config.level), stream=config.output, format="%(message)s"
    )

    # Устанавливаем максимальную длину строк для процессора
    root_logger = structlog.get_logger()
    if hasattr(root_logger, "_context"):
        root_logger._context["_max_string_length"] = config.max_string_length


def get_logger(name: Optional[str] = None, **initial_values) -> BoundLogger:
    """
    Получение логгера с начальным контекстом

    Args:
        name: Имя логгера
        **initial_values: Начальные значения контекста

    Returns:
        Настроенный BoundLogger
    """

    logger = structlog.get_logger(name)

    if initial_values:
        logger = logger.bind(**initial_values)

    return logger


class CorrelationContextManager:
    """Context manager для работы с correlation ID"""

    def __init__(self, correlation_id: str):
        self.correlation_id = correlation_id
        self.logger = get_logger()
        self._original_context = None

    def __enter__(self):
        self.logger = self.logger.bind(correlation_id=self.correlation_id)
        return self.logger

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


def with_correlation_id(correlation_id: str) -> CorrelationContextManager:
    """
    Создание контекста с correlation ID

    Args:
        correlation_id: Уникальный идентификатор для отслеживания запросов

    Returns:
        Context manager с логгером, содержащим correlation ID

    Example:
        with with_correlation_id("123-456-789") as logger:
            logger.info("Processing request")
    """
    return CorrelationContextManager(correlation_id)


class ComponentLogger:
    """Специализированный логгер для компонентов pipeline"""

    def __init__(
        self,
        component_type: str,
        component_name: str,
        stage_name: Optional[str] = None,
        pipeline_id: Optional[str] = None,
        run_id: Optional[str] = None,
    ):
        context = {"component_type": component_type, "component_name": component_name}

        if stage_name:
            context["stage_name"] = stage_name
        if pipeline_id:
            context["pipeline_id"] = pipeline_id
        if run_id:
            context["run_id"] = run_id

        self.logger = get_logger().bind(**context)

    def info(self, message: str, **kwargs):
        self.logger.info(message, **kwargs)

    def debug(self, message: str, **kwargs):
        self.logger.debug(message, **kwargs)

    def warning(self, message: str, **kwargs):
        self.logger.warning(message, **kwargs)

    def error(self, message: str, **kwargs):
        self.logger.error(message, **kwargs)

    def critical(self, message: str, **kwargs):
        self.logger.critical(message, **kwargs)

    def bind(self, **kwargs):
        """Создание нового логгера с дополнительным контекстом"""
        return ComponentLogger.__new__(ComponentLogger)


class PipelineLogger:
    """Специализированный логгер для pipeline"""

    def __init__(self, pipeline_id: str, run_id: Optional[str] = None):
        context = {"pipeline_id": pipeline_id}
        if run_id:
            context["run_id"] = run_id

        self.logger = get_logger().bind(**context)
        self._stage_loggers = {}

    def get_stage_logger(self, stage_name: str) -> BoundLogger:
        """Получение логгера для конкретного этапа"""
        if stage_name not in self._stage_loggers:
            self._stage_loggers[stage_name] = self.logger.bind(stage_name=stage_name)
        return self._stage_loggers[stage_name]

    def info(self, message: str, **kwargs):
        self.logger.info(message, **kwargs)

    def debug(self, message: str, **kwargs):
        self.logger.debug(message, **kwargs)

    def warning(self, message: str, **kwargs):
        self.logger.warning(message, **kwargs)

    def error(self, message: str, **kwargs):
        self.logger.error(message, **kwargs)

    def critical(self, message: str, **kwargs):
        self.logger.critical(message, **kwargs)


class AsyncLoggingHandler:
    """Асинхронный handler для высокопроизводительного логирования"""

    def __init__(
        self,
        buffer_size: int = 1000,
        flush_interval: float = 5.0,
        output_handler: Optional[logging.Handler] = None,
    ):
        self.buffer_size = buffer_size
        self.flush_interval = flush_interval
        self.output_handler = output_handler or logging.StreamHandler()

        self._buffer = []
        self._last_flush = datetime.now()

    async def log(self, record: logging.LogRecord):
        """Асинхронное логирование записи"""
        self._buffer.append(record)

        # Проверяем условия для flush
        now = datetime.now()
        should_flush = (
            len(self._buffer) >= self.buffer_size
            or (now - self._last_flush).total_seconds() >= self.flush_interval
        )

        if should_flush:
            await self._flush()

    async def _flush(self):
        """Сброс буфера в output handler"""
        if not self._buffer:
            return

        records_to_flush = self._buffer.copy()
        self._buffer.clear()
        self._last_flush = datetime.now()

        # Отправляем записи в handler (в отдельном потоке если нужно)
        for record in records_to_flush:
            try:
                self.output_handler.emit(record)
            except Exception:
                # Игнорируем ошибки логирования чтобы не нарушить основной поток
                pass


# Фабричные функции для удобства


def create_component_logger(
    component_type: str, component_name: str, **kwargs
) -> ComponentLogger:
    """Создание логгера для компонента"""
    return ComponentLogger(component_type, component_name, **kwargs)


def create_pipeline_logger(
    pipeline_id: str, run_id: Optional[str] = None
) -> PipelineLogger:
    """Создание логгера для pipeline"""
    return PipelineLogger(pipeline_id, run_id)


def configure_for_development():
    """Быстрая настройка для разработки"""
    config = PipelineLoggerConfig(level="DEBUG", format="console", output=sys.stdout)
    setup_logging(config)


def configure_for_production(log_file: Optional[str] = None):
    """Быстрая настройка для production"""
    output = sys.stdout
    if log_file:
        try:
            output = open(log_file, "a", encoding="utf-8")
        except Exception:
            output = sys.stdout

    config = PipelineLoggerConfig(
        level="INFO", format="json", output=output, include_caller=False
    )
    setup_logging(config)
