# packages/components/extractors/extractor_sql/src/extractor_sql/exceptions.py

"""
Кастомные исключения для SQL Extractor

Этот модуль содержит специализированные исключения для обработки ошибок
при работе с SQL extraction компонентами.
"""

from typing import Any


class SQLExtractorError(Exception):
    """Базовое исключение для всех ошибок SQL Extractor"""

    def __init__(
        self,
        message: str,
        error_code: str | None = None,
        context: dict[str, Any] | None = None,
        original_error: Exception | None = None,
    ):
        super().__init__(message)
        self.message = message
        self.error_code = error_code or self.__class__.__name__
        self.context = context or {}
        self.original_error = original_error

    def __str__(self) -> str:
        base_msg = f"[{self.error_code}] {self.message}"
        if self.context:
            context_str = ", ".join(f"{k}={v}" for k, v in self.context.items())
            base_msg += f" (Context: {context_str})"
        if self.original_error:
            base_msg += f" (Caused by: {self.original_error})"
        return base_msg

    def to_dict(self) -> dict[str, Any]:
        """Сериализация исключения в словарь"""
        return {
            "error_type": self.__class__.__name__,
            "error_code": self.error_code,
            "message": self.message,
            "context": self.context,
            "original_error": str(self.original_error) if self.original_error else None,
        }


class ConnectionError(SQLExtractorError):
    """Ошибки подключения к базе данных"""

    def __init__(
        self,
        message: str = "Failed to connect to database",
        connection_string: str | None = None,
        dialect: str | None = None,
        **kwargs,
    ):
        context = kwargs.get("context", {})
        if connection_string:
            # Маскируем чувствительную информацию
            masked_conn = self._mask_connection_string(connection_string)
            context["connection_string"] = masked_conn
        if dialect:
            context["dialect"] = dialect

        super().__init__(
            message=message, error_code="CONNECTION_ERROR", context=context, **kwargs
        )

    @staticmethod
    def _mask_connection_string(conn_str: str) -> str:
        """Маскирование чувствительной информации в connection string"""
        import re

        # Маскируем пароль в connection string
        return re.sub(r"://([^:]+):([^@]+)@", r"://\1:***@", conn_str)


class QueryExecutionError(SQLExtractorError):
    """Ошибки выполнения SQL запросов"""

    def __init__(
        self,
        message: str = "Failed to execute SQL query",
        query: str | None = None,
        parameters: dict[str, Any] | None = None,
        **kwargs,
    ):
        context = kwargs.get("context", {})
        if query:
            # Ограничиваем длину запроса для логирования
            context["query"] = query[:500] + "..." if len(query) > 500 else query
        if parameters:
            context["parameters"] = parameters

        super().__init__(
            message=message,
            error_code="QUERY_EXECUTION_ERROR",
            context=context,
            **kwargs,
        )


class ConfigurationError(SQLExtractorError):
    """Ошибки конфигурации SQL Extractor"""

    def __init__(
        self,
        message: str = "Invalid configuration",
        config_field: str | None = None,
        config_value: Any | None = None,
        **kwargs,
    ):
        context = kwargs.get("context", {})
        if config_field:
            context["config_field"] = config_field
        if config_value is not None:
            context["config_value"] = str(config_value)

        super().__init__(
            message=message, error_code="CONFIGURATION_ERROR", context=context, **kwargs
        )


class DataFormatError(SQLExtractorError):
    """Ошибки преобразования данных"""

    def __init__(
        self,
        message: str = "Failed to format data",
        source_format: str | None = None,
        target_format: str | None = None,
        **kwargs,
    ):
        context = kwargs.get("context", {})
        if source_format:
            context["source_format"] = source_format
        if target_format:
            context["target_format"] = target_format

        super().__init__(
            message=message, error_code="DATA_FORMAT_ERROR", context=context, **kwargs
        )


class TimeoutError(SQLExtractorError):
    """Ошибки превышения времени ожидания"""

    def __init__(
        self,
        message: str = "Operation timed out",
        timeout_seconds: float | None = None,
        operation: str | None = None,
        **kwargs,
    ):
        context = kwargs.get("context", {})
        if timeout_seconds is not None:
            context["timeout_seconds"] = timeout_seconds
        if operation:
            context["operation"] = operation

        super().__init__(
            message=message, error_code="TIMEOUT_ERROR", context=context, **kwargs
        )


class AuthenticationError(SQLExtractorError):
    """Ошибки аутентификации в базе данных"""

    def __init__(
        self,
        message: str = "Authentication failed",
        username: str | None = None,
        database: str | None = None,
        **kwargs,
    ):
        context = kwargs.get("context", {})
        if username:
            context["username"] = username
        if database:
            context["database"] = database

        super().__init__(
            message=message,
            error_code="AUTHENTICATION_ERROR",
            context=context,
            **kwargs,
        )


class PermissionError(SQLExtractorError):
    """Ошибки прав доступа к базе данных или таблицам"""

    def __init__(
        self,
        message: str = "Permission denied",
        resource: str | None = None,
        required_permission: str | None = None,
        **kwargs,
    ):
        context = kwargs.get("context", {})
        if resource:
            context["resource"] = resource
        if required_permission:
            context["required_permission"] = required_permission

        super().__init__(
            message=message, error_code="PERMISSION_ERROR", context=context, **kwargs
        )


class ResourceNotFoundError(SQLExtractorError):
    """Ошибки отсутствия ресурсов (таблицы, колонки и т.д.)"""

    def __init__(
        self,
        message: str = "Resource not found",
        resource_type: str | None = None,
        resource_name: str | None = None,
        **kwargs,
    ):
        context = kwargs.get("context", {})
        if resource_type:
            context["resource_type"] = resource_type
        if resource_name:
            context["resource_name"] = resource_name

        super().__init__(
            message=message,
            error_code="RESOURCE_NOT_FOUND_ERROR",
            context=context,
            **kwargs,
        )


class DataValidationError(SQLExtractorError):
    """Ошибки валидации извлеченных данных"""

    def __init__(
        self,
        message: str = "Data validation failed",
        validation_rule: str | None = None,
        invalid_data_sample: Any | None = None,
        **kwargs,
    ):
        context = kwargs.get("context", {})
        if validation_rule:
            context["validation_rule"] = validation_rule
        if invalid_data_sample is not None:
            context["invalid_data_sample"] = str(invalid_data_sample)[:200]

        super().__init__(
            message=message,
            error_code="DATA_VALIDATION_ERROR",
            context=context,
            **kwargs,
        )


class RetryExhaustedError(SQLExtractorError):
    """Ошибка исчерпания всех попыток retry"""

    def __init__(
        self,
        message: str = "All retry attempts exhausted",
        max_attempts: int | None = None,
        last_error: Exception | None = None,
        **kwargs,
    ):
        context = kwargs.get("context", {})
        if max_attempts is not None:
            context["max_attempts"] = max_attempts
        if last_error:
            context["last_error"] = str(last_error)

        super().__init__(
            message=message,
            error_code="RETRY_EXHAUSTED_ERROR",
            context=context,
            original_error=last_error,
            **kwargs,
        )


class MemoryError(SQLExtractorError):
    """Ошибки превышения лимитов памяти"""

    def __init__(
        self,
        message: str = "Memory limit exceeded",
        memory_usage_mb: float | None = None,
        memory_limit_mb: float | None = None,
        **kwargs,
    ):
        context = kwargs.get("context", {})
        if memory_usage_mb is not None:
            context["memory_usage_mb"] = memory_usage_mb
        if memory_limit_mb is not None:
            context["memory_limit_mb"] = memory_limit_mb

        super().__init__(
            message=message, error_code="MEMORY_ERROR", context=context, **kwargs
        )


# ================================
# Exception Utilities
# ================================


def wrap_sqlalchemy_error(
    error: Exception, context: dict[str, Any] | None = None
) -> SQLExtractorError:
    """
    Оборачивание SQLAlchemy исключений в наши кастомные исключения

    Args:
        error: Исходное SQLAlchemy исключение
        context: Дополнительный контекст

    Returns:
        Соответствующее кастомное исключение
    """
    from sqlalchemy.exc import (
        DataError,
        DisconnectionError,
        IntegrityError,
        OperationalError,
        ProgrammingError,
        SQLAlchemyError,
    )
    from sqlalchemy.exc import (
        TimeoutError as SQLTimeoutError,
    )

    error_msg = str(error)
    error_context = context or {}

    # Определяем тип ошибки и создаем соответствующее исключение
    if isinstance(error, DisconnectionError):
        return ConnectionError(
            message=f"Database connection lost: {error_msg}",
            context=error_context,
            original_error=error,
        )
    elif isinstance(error, SQLTimeoutError):
        return TimeoutError(
            message=f"SQL operation timed out: {error_msg}",
            context=error_context,
            original_error=error,
        )
    elif isinstance(error, OperationalError):
        # Может быть ошибкой подключения или выполнения
        if "authentication" in error_msg.lower() or "login" in error_msg.lower():
            return AuthenticationError(
                message=f"Authentication failed: {error_msg}",
                context=error_context,
                original_error=error,
            )
        elif "permission" in error_msg.lower() or "access" in error_msg.lower():
            return PermissionError(
                message=f"Permission denied: {error_msg}",
                context=error_context,
                original_error=error,
            )
        else:
            return QueryExecutionError(
                message=f"SQL execution failed: {error_msg}",
                context=error_context,
                original_error=error,
            )
    elif isinstance(error, ProgrammingError):
        return QueryExecutionError(
            message=f"SQL syntax or logic error: {error_msg}",
            context=error_context,
            original_error=error,
        )
    elif isinstance(error, IntegrityError):
        return DataValidationError(
            message=f"Data integrity constraint violated: {error_msg}",
            context=error_context,
            original_error=error,
        )
    elif isinstance(error, DataError):
        return DataFormatError(
            message=f"Data format error: {error_msg}",
            context=error_context,
            original_error=error,
        )
    elif isinstance(error, SQLAlchemyError):
        return SQLExtractorError(
            message=f"SQLAlchemy error: {error_msg}",
            context=error_context,
            original_error=error,
        )
    else:
        # Для не-SQLAlchemy ошибок возвращаем базовое исключение
        return SQLExtractorError(
            message=f"Unexpected error: {error_msg}",
            context=error_context,
            original_error=error,
        )


def handle_exception(
    error: Exception, context: dict[str, Any] | None = None
) -> SQLExtractorError:
    """
    Обработчик исключений с логированием и преобразованием

    Args:
        error: Исходное исключение
        context: Дополнительный контекст

    Returns:
        Обработанное кастомное исключение
    """
    import structlog

    logger = structlog.get_logger(__name__)

    # Если это уже наше исключение, просто возвращаем
    if isinstance(error, SQLExtractorError):
        return error

    # Обрабатываем SQLAlchemy ошибки
    try:
        from sqlalchemy.exc import SQLAlchemyError

        if isinstance(error, SQLAlchemyError):
            wrapped_error = wrap_sqlalchemy_error(error, context)
            logger.error(
                "SQLAlchemy error occurred",
                error_type=type(error).__name__,
                message=str(error),
                context=context,
            )
            return wrapped_error
    except ImportError:
        pass

    # Обрабатываем стандартные Python исключения
    if isinstance(error, ConnectionError):
        return ConnectionError(
            message=f"Connection error: {error}",
            context=context,
            original_error=error,
        )
    elif isinstance(error, TimeoutError):
        return TimeoutError(
            message=f"Timeout error: {error}",
            context=context,
            original_error=error,
        )
    elif isinstance(error, MemoryError):
        return MemoryError(
            message=f"Memory error: {error}",
            context=context,
            original_error=error,
        )
    elif isinstance(error, ValueError):
        return ConfigurationError(
            message=f"Configuration error: {error}",
            context=context,
            original_error=error,
        )
    else:
        # Общий случай
        logger.error(
            "Unexpected error occurred",
            error_type=type(error).__name__,
            message=str(error),
            context=context,
        )
        return SQLExtractorError(
            message=f"Unexpected error: {error}",
            context=context,
            original_error=error,
        )
