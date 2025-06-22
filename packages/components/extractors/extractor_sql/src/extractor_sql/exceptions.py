# packages/components/extractors/extractor_sql/src/extractor_sql/exceptions.py

"""
Исключения для SQL Extractor

Иерархия исключений:
- SQLExtractorError (базовое)
  - ConfigurationError (ошибки конфигурации)
  - ConnectionError (ошибки подключения)
  - QueryExecutionError (ошибки выполнения запросов)
  - DataFormatError (ошибки форматирования данных)
  - ValidationError (ошибки валидации)
"""

from typing import Any


class SQLExtractorError(Exception):
    """Базовое исключение для SQL Extractor"""

    def __init__(
        self,
        message: str,
        error_code: str | None = None,
        details: dict[str, Any] | None = None,
        original_error: Exception | None = None,
    ):
        super().__init__(message)
        self.message = message
        self.error_code = error_code or self.__class__.__name__
        self.details = details or {}
        self.original_error = original_error

    def __str__(self) -> str:
        error_parts = [self.message]

        if self.error_code:
            error_parts.append(f"[{self.error_code}]")

        if self.details:
            details_str = ", ".join(f"{k}={v}" for k, v in self.details.items())
            error_parts.append(f"Details: {details_str}")

        if self.original_error:
            error_parts.append(f"Caused by: {self.original_error}")

        return " | ".join(error_parts)

    def to_dict(self) -> dict[str, Any]:
        """Конвертация исключения в словарь для логирования/сериализации"""
        return {
            "error_type": self.__class__.__name__,
            "error_code": self.error_code,
            "message": self.message,
            "details": self.details,
            "original_error": str(self.original_error) if self.original_error else None,
        }


class ConfigurationError(SQLExtractorError):
    """Ошибки конфигурации SQL Extractor"""

    def __init__(
        self,
        message: str,
        config_field: str | None = None,
        config_value: Any | None = None,
        **kwargs,
    ):
        details = kwargs.pop("details", {})
        if config_field:
            details["config_field"] = config_field
        if config_value is not None:
            details["config_value"] = config_value

        super().__init__(
            message=message, error_code="CONFIG_ERROR", details=details, **kwargs
        )
        self.config_field = config_field
        self.config_value = config_value


class ConnectionError(SQLExtractorError):
    """Ошибки подключения к базе данных"""

    def __init__(
        self,
        message: str,
        connection_string: str | None = None,
        dialect: str | None = None,
        host: str | None = None,
        database: str | None = None,
        **kwargs,
    ):
        details = kwargs.pop("details", {})
        if connection_string:
            # Маскируем пароль в connection string
            details["connection_string"] = _mask_password(connection_string)
        if dialect:
            details["dialect"] = dialect
        if host:
            details["host"] = host
        if database:
            details["database"] = database

        super().__init__(
            message=message, error_code="CONNECTION_ERROR", details=details, **kwargs
        )
        self.connection_string = connection_string
        self.dialect = dialect
        self.host = host
        self.database = database


class QueryExecutionError(SQLExtractorError):
    """Ошибки выполнения SQL запросов"""

    def __init__(
        self,
        message: str,
        query: str | None = None,
        parameters: dict[str, Any] | None = None,
        sql_state: str | None = None,
        error_position: int | None = None,
        **kwargs,
    ):
        details = kwargs.pop("details", {})
        if query:
            # Обрезаем длинные запросы для логирования
            details["query"] = query[:500] + "..." if len(query) > 500 else query
        if parameters:
            details["parameters"] = parameters
        if sql_state:
            details["sql_state"] = sql_state
        if error_position is not None:
            details["error_position"] = error_position

        super().__init__(
            message=message, error_code="QUERY_ERROR", details=details, **kwargs
        )
        self.query = query
        self.parameters = parameters
        self.sql_state = sql_state
        self.error_position = error_position


class DataFormatError(SQLExtractorError):
    """Ошибки форматирования данных"""

    def __init__(
        self,
        message: str,
        output_format: str | None = None,
        data_type: str | None = None,
        conversion_target: str | None = None,
        **kwargs,
    ):
        details = kwargs.pop("details", {})
        if output_format:
            details["output_format"] = output_format
        if data_type:
            details["data_type"] = data_type
        if conversion_target:
            details["conversion_target"] = conversion_target

        super().__init__(
            message=message, error_code="FORMAT_ERROR", details=details, **kwargs
        )
        self.output_format = output_format
        self.data_type = data_type
        self.conversion_target = conversion_target


class ValidationError(SQLExtractorError):
    """Ошибки валидации данных или параметров"""

    def __init__(
        self,
        message: str,
        validation_type: str | None = None,
        field_name: str | None = None,
        field_value: Any | None = None,
        constraints: dict[str, Any] | None = None,
        **kwargs,
    ):
        details = kwargs.pop("details", {})
        if validation_type:
            details["validation_type"] = validation_type
        if field_name:
            details["field_name"] = field_name
        if field_value is not None:
            details["field_value"] = field_value
        if constraints:
            details["constraints"] = constraints

        super().__init__(
            message=message, error_code="VALIDATION_ERROR", details=details, **kwargs
        )
        self.validation_type = validation_type
        self.field_name = field_name
        self.field_value = field_value
        self.constraints = constraints


class RetryableError(SQLExtractorError):
    """Ошибки, которые можно повторить"""

    def __init__(
        self,
        message: str,
        retry_after: float | None = None,
        max_retries: int | None = None,
        current_attempt: int | None = None,
        **kwargs,
    ):
        details = kwargs.pop("details", {})
        if retry_after is not None:
            details["retry_after"] = retry_after
        if max_retries is not None:
            details["max_retries"] = max_retries
        if current_attempt is not None:
            details["current_attempt"] = current_attempt

        super().__init__(
            message=message, error_code="RETRYABLE_ERROR", details=details, **kwargs
        )
        self.retry_after = retry_after
        self.max_retries = max_retries
        self.current_attempt = current_attempt


class TimeoutError(SQLExtractorError):
    """Ошибки timeout"""

    def __init__(
        self,
        message: str,
        timeout_duration: float | None = None,
        operation_type: str | None = None,
        **kwargs,
    ):
        details = kwargs.pop("details", {})
        if timeout_duration is not None:
            details["timeout_duration"] = timeout_duration
        if operation_type:
            details["operation_type"] = operation_type

        super().__init__(
            message=message, error_code="TIMEOUT_ERROR", details=details, **kwargs
        )
        self.timeout_duration = timeout_duration
        self.operation_type = operation_type


class AuthenticationError(SQLExtractorError):
    """Ошибки аутентификации"""

    def __init__(
        self,
        message: str,
        username: str | None = None,
        auth_method: str | None = None,
        **kwargs,
    ):
        details = kwargs.pop("details", {})
        if username:
            details["username"] = username
        if auth_method:
            details["auth_method"] = auth_method

        super().__init__(
            message=message, error_code="AUTH_ERROR", details=details, **kwargs
        )
        self.username = username
        self.auth_method = auth_method


class PermissionError(SQLExtractorError):
    """Ошибки прав доступа"""

    def __init__(
        self,
        message: str,
        required_permission: str | None = None,
        resource: str | None = None,
        **kwargs,
    ):
        details = kwargs.pop("details", {})
        if required_permission:
            details["required_permission"] = required_permission
        if resource:
            details["resource"] = resource

        super().__init__(
            message=message, error_code="PERMISSION_ERROR", details=details, **kwargs
        )
        self.required_permission = required_permission
        self.resource = resource


class SchemaError(SQLExtractorError):
    """Ошибки схемы данных"""

    def __init__(
        self,
        message: str,
        table_name: str | None = None,
        column_name: str | None = None,
        schema_name: str | None = None,
        expected_type: str | None = None,
        actual_type: str | None = None,
        **kwargs,
    ):
        details = kwargs.pop("details", {})
        if table_name:
            details["table_name"] = table_name
        if column_name:
            details["column_name"] = column_name
        if schema_name:
            details["schema_name"] = schema_name
        if expected_type:
            details["expected_type"] = expected_type
        if actual_type:
            details["actual_type"] = actual_type

        super().__init__(
            message=message, error_code="SCHEMA_ERROR", details=details, **kwargs
        )
        self.table_name = table_name
        self.column_name = column_name
        self.schema_name = schema_name
        self.expected_type = expected_type
        self.actual_type = actual_type


class ResourceError(SQLExtractorError):
    """Ошибки ресурсов (память, диск, и т.д.)"""

    def __init__(
        self,
        message: str,
        resource_type: str | None = None,
        current_usage: Any | None = None,
        limit: Any | None = None,
        **kwargs,
    ):
        details = kwargs.pop("details", {})
        if resource_type:
            details["resource_type"] = resource_type
        if current_usage is not None:
            details["current_usage"] = current_usage
        if limit is not None:
            details["limit"] = limit

        super().__init__(
            message=message, error_code="RESOURCE_ERROR", details=details, **kwargs
        )
        self.resource_type = resource_type
        self.current_usage = current_usage
        self.limit = limit


# Утилитарные функции для работы с исключениями


def _mask_password(connection_string: str) -> str:
    """Маскирование пароля в строке подключения"""
    try:
        from urllib.parse import urlparse

        parsed = urlparse(connection_string)
        if parsed.password:
            masked_password = "*" * len(parsed.password)
            netloc = parsed.netloc.replace(parsed.password, masked_password)
            masked_url = parsed._replace(netloc=netloc).geturl()
            return masked_url
        return connection_string
    except Exception:
        return connection_string


def wrap_sqlalchemy_error(
    error: Exception, context: dict[str, Any] | None = None
) -> SQLExtractorError:
    """
    Обертка SQLAlchemy исключений в исключения SQL Extractor

    Args:
        error: Исходное исключение SQLAlchemy
        context: Дополнительный контекст

    Returns:
        Соответствующее исключение SQL Extractor
    """
    context = context or {}
    error_msg = str(error)
    original_error = error

    # Импорты SQLAlchemy исключений
    try:
        from sqlalchemy.exc import (
            DisconnectionError,
            IntegrityError,
            InvalidRequestError,
            OperationalError,
            ProgrammingError,
            SQLAlchemyError,
        )
        from sqlalchemy.exc import (
            TimeoutError as SQLTimeoutError,
        )

        if isinstance(error, DisconnectionError):
            return ConnectionError(
                message=f"Database connection lost: {error_msg}",
                original_error=original_error,
                details=context,
            )

        elif isinstance(error, SQLTimeoutError):
            return TimeoutError(
                message=f"Database operation timed out: {error_msg}",
                original_error=original_error,
                details=context,
            )

        elif isinstance(error, OperationalError):
            # Анализируем сообщение для определения типа ошибки
            if "authentication" in error_msg.lower() or "login" in error_msg.lower():
                return AuthenticationError(
                    message=f"Database authentication failed: {error_msg}",
                    original_error=original_error,
                    details=context,
                )
            elif "permission" in error_msg.lower() or "access" in error_msg.lower():
                return PermissionError(
                    message=f"Database access denied: {error_msg}",
                    original_error=original_error,
                    details=context,
                )
            else:
                return ConnectionError(
                    message=f"Database operational error: {error_msg}",
                    original_error=original_error,
                    details=context,
                )

        elif isinstance(error, ProgrammingError):
            return QueryExecutionError(
                message=f"SQL syntax or programming error: {error_msg}",
                original_error=original_error,
                details=context,
            )

        elif isinstance(error, IntegrityError):
            return ValidationError(
                message=f"Data integrity constraint violation: {error_msg}",
                original_error=original_error,
                details=context,
            )

        elif isinstance(error, InvalidRequestError):
            return ConfigurationError(
                message=f"Invalid SQLAlchemy request: {error_msg}",
                original_error=original_error,
                details=context,
            )

        elif isinstance(error, SQLAlchemyError):
            return SQLExtractorError(
                message=f"SQLAlchemy error: {error_msg}",
                original_error=original_error,
                details=context,
            )

    except ImportError:
        # SQLAlchemy не доступна, возвращаем базовое исключение
        pass

    # Обработка стандартных Python исключений
    if isinstance(error, ConnectionRefusedError):
        return ConnectionError(
            message=f"Connection refused: {error_msg}",
            original_error=original_error,
            details=context,
        )

    elif isinstance(error, TimeoutError):
        return TimeoutError(
            message=f"Operation timed out: {error_msg}",
            original_error=original_error,
            details=context,
        )

    elif isinstance(error, PermissionError):
        return PermissionError(
            message=f"Permission denied: {error_msg}",
            original_error=original_error,
            details=context,
        )

    elif isinstance(error, ValueError):
        return ValidationError(
            message=f"Value error: {error_msg}",
            original_error=original_error,
            details=context,
        )

    elif isinstance(error, TypeError):
        return DataFormatError(
            message=f"Type error: {error_msg}",
            original_error=original_error,
            details=context,
        )

    # Для всех остальных случаев возвращаем базовое исключение
    return SQLExtractorError(
        message=f"Unexpected error: {error_msg}",
        original_error=original_error,
        details=context,
    )


def create_error_context(
    extractor_name: str | None = None,
    pipeline_id: str | None = None,
    stage_name: str | None = None,
    **additional_context: Any,
) -> dict[str, Any]:
    """
    Создание контекста для исключений

    Args:
        extractor_name: Имя extractor'а
        pipeline_id: ID pipeline
        stage_name: Имя этапа
        **additional_context: Дополнительный контекст

    Returns:
        Словарь с контекстом
    """
    context = {}

    if extractor_name:
        context["extractor_name"] = extractor_name
    if pipeline_id:
        context["pipeline_id"] = pipeline_id
    if stage_name:
        context["stage_name"] = stage_name

    context.update(additional_context)

    return context


# Предопределенные исключения для частых случаев


class EmptyResultError(QueryExecutionError):
    """Исключение для случаев, когда запрос не вернул данных"""

    def __init__(self, query: str | None = None, **kwargs):
        super().__init__(
            message="Query returned no data",
            query=query,
            error_code="EMPTY_RESULT",
            **kwargs,
        )


class InvalidConnectionStringError(ConfigurationError):
    """Исключение для невалидных строк подключения"""

    def __init__(self, connection_string: str, **kwargs):
        super().__init__(
            message="Invalid connection string format",
            config_field="connection_string",
            config_value=_mask_password(connection_string),
            error_code="INVALID_CONNECTION_STRING",
            **kwargs,
        )


class UnsupportedDialectError(ConfigurationError):
    """Исключение для неподдерживаемых диалектов БД"""

    def __init__(self, dialect: str, supported_dialects: list | None = None, **kwargs):
        details = kwargs.pop("details", {})
        if supported_dialects:
            details["supported_dialects"] = supported_dialects

        super().__init__(
            message=f"Unsupported database dialect: {dialect}",
            config_field="dialect",
            config_value=dialect,
            error_code="UNSUPPORTED_DIALECT",
            details=details,
            **kwargs,
        )


class UnsupportedOutputFormatError(ConfigurationError):
    """Исключение для неподдерживаемых форматов вывода"""

    def __init__(
        self, output_format: str, supported_formats: list | None = None, **kwargs
    ):
        details = kwargs.pop("details", {})
        if supported_formats:
            details["supported_formats"] = supported_formats

        super().__init__(
            message=f"Unsupported output format: {output_format}",
            config_field="output_format",
            config_value=output_format,
            error_code="UNSUPPORTED_OUTPUT_FORMAT",
            details=details,
            **kwargs,
        )
