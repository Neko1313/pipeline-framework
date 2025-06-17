"""
Исключения для pipeline фреймворка
"""

from typing import Optional, Dict, Any, List


class PipelineError(Exception):
    """Базовое исключение для всех ошибок pipeline"""

    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        super().__init__(message)
        self.message = message
        self.details = details or {}

    def __str__(self) -> str:
        if self.details:
            details_str = ", ".join(f"{k}={v}" for k, v in self.details.items())
            return f"{self.message} ({details_str})"
        return self.message


class PipelineConfigError(PipelineError):
    """Ошибка в конфигурации pipeline"""

    pass


class PipelineValidationError(PipelineError):
    """Ошибка валидации pipeline"""

    def __init__(
        self,
        message: str,
        validation_errors: Optional[List[str]] = None,
        details: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(message, details)
        self.validation_errors = validation_errors or []


class PipelineComponentError(PipelineError):
    """Ошибка компонента pipeline"""

    def __init__(
        self,
        message: str,
        component_type: Optional[str] = None,
        component_name: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(message, details)
        self.component_type = component_type
        self.component_name = component_name


class PipelineRegistrationError(PipelineError):
    """Ошибка регистрации компонента"""

    pass


class PipelineExecutionError(PipelineError):
    """Ошибка выполнения pipeline"""

    def __init__(
        self,
        message: str,
        stage_name: Optional[str] = None,
        execution_id: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(message, details)
        self.stage_name = stage_name
        self.execution_id = execution_id


class PipelineDependencyError(PipelineError):
    """Ошибка зависимостей между стадиями"""

    def __init__(
        self,
        message: str,
        stage_name: Optional[str] = None,
        missing_dependencies: Optional[List[str]] = None,
        details: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(message, details)
        self.stage_name = stage_name
        self.missing_dependencies = missing_dependencies or []


class PipelineTimeoutError(PipelineError):
    """Ошибка таймаута выполнения"""

    def __init__(
        self,
        message: str,
        timeout_seconds: Optional[int] = None,
        stage_name: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(message, details)
        self.timeout_seconds = timeout_seconds
        self.stage_name = stage_name


class PipelineRetryExhaustedError(PipelineError):
    """Ошибка исчерпания попыток повтора"""

    def __init__(
        self,
        message: str,
        attempts: Optional[int] = None,
        last_error: Optional[Exception] = None,
        details: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(message, details)
        self.attempts = attempts
        self.last_error = last_error


class PipelineResourceError(PipelineError):
    """Ошибка ресурсов (память, CPU, диск)"""

    def __init__(
        self,
        message: str,
        resource_type: Optional[str] = None,
        current_usage: Optional[str] = None,
        limit: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(message, details)
        self.resource_type = resource_type
        self.current_usage = current_usage
        self.limit = limit


class PipelineDataError(PipelineError):
    """Ошибка в данных (валидация, формат, схема)"""

    def __init__(
        self,
        message: str,
        data_type: Optional[str] = None,
        validation_errors: Optional[List[str]] = None,
        details: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(message, details)
        self.data_type = data_type
        self.validation_errors = validation_errors or []


class PipelineConnectionError(PipelineError):
    """Ошибка подключения к внешним системам"""

    def __init__(
        self,
        message: str,
        connection_type: Optional[str] = None,
        endpoint: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(message, details)
        self.connection_type = connection_type
        self.endpoint = endpoint


# Utility функции для создания исключений


def create_component_error(
    component_type: str, component_name: str, error: Exception
) -> PipelineComponentError:
    """Создание ошибки компонента из обычного исключения"""
    return PipelineComponentError(
        f"Ошибка в компоненте {component_type}: {error}",
        component_type=component_type,
        component_name=component_name,
        details={"original_error": str(error), "error_type": type(error).__name__},
    )


def create_validation_error(message: str, errors: List[str]) -> PipelineValidationError:
    """Создание ошибки валидации с детальным списком ошибок"""
    return PipelineValidationError(
        message, validation_errors=errors, details={"error_count": len(errors)}
    )


def create_dependency_error(
    stage_name: str, missing_deps: List[str]
) -> PipelineDependencyError:
    """Создание ошибки зависимостей"""
    return PipelineDependencyError(
        f"Стадия '{stage_name}' имеет неразрешенные зависимости: {', '.join(missing_deps)}",
        stage_name=stage_name,
        missing_dependencies=missing_deps,
        details={"dependency_count": len(missing_deps)},
    )
