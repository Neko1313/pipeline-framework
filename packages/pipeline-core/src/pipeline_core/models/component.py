"""
Базовые модели и интерфейсы для компонентов pipeline
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Type, Union
from pydantic import BaseModel, Field, ConfigDict
from enum import Enum
import logging


class ComponentType(str, Enum):
    """Типы компонентов pipeline"""

    EXTRACTOR = "extractor"
    TRANSFORMER = "transformer"
    VALIDATOR = "validator"
    LOADER = "loader"
    UTILITY = "utility"


class ExecutionStatus(str, Enum):
    """Статусы выполнения компонента"""

    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"


class ComponentConfig(BaseModel):
    """Базовая конфигурация компонента"""

    model_config = ConfigDict(extra="forbid")

    type: str = Field(..., description="Тип компонента")
    name: Optional[str] = Field(None, description="Имя компонента")
    timeout: int = Field(300, description="Таймаут выполнения в секундах")
    retry_attempts: int = Field(3, description="Количество попыток повтора")
    retry_delay: float = Field(1.0, description="Задержка между повторами")
    allow_failure: bool = Field(
        False, description="Разрешить сбой без остановки pipeline"
    )

    def get_component_name(self) -> str:
        """Получить имя компонента (type если name не задано)"""
        return self.name or self.type


class ExecutionResult(BaseModel):
    """Результат выполнения компонента"""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    status: ExecutionStatus
    data: Any = None
    metadata: Dict[str, Any] = Field(default_factory=dict)
    error_message: Optional[str] = None
    execution_time: float = 0.0
    processed_records: int = 0
    artifacts: List[str] = Field(default_factory=list)


class ExecutionContext:
    """Контекст выполнения с доступом к результатам других стадий"""

    def __init__(self):
        self.stage_results: Dict[str, ExecutionResult] = {}
        self.global_variables: Dict[str, Any] = {}
        self.pipeline_metadata: Dict[str, Any] = {}
        self.logger = logging.getLogger("pipeline.context")

    def set_stage_result(self, stage_name: str, result: ExecutionResult) -> None:
        """Сохранить результат выполнения стадии"""
        self.stage_results[stage_name] = result
        self.logger.debug(f"Сохранен результат стадии {stage_name}: {result.status}")

    def get_stage_result(self, stage_name: str) -> Optional[ExecutionResult]:
        """Получить результат выполнения стадии"""
        return self.stage_results.get(stage_name)

    def get_stage_data(self, stage_name: str) -> Any:
        """Получить данные от стадии"""
        result = self.get_stage_result(stage_name)
        return result.data if result else None

    def has_stage_succeeded(self, stage_name: str) -> bool:
        """Проверить успешность выполнения стадии"""
        result = self.get_stage_result(stage_name)
        return result is not None and result.status == ExecutionStatus.SUCCESS

    def set_global_variable(self, key: str, value: Any) -> None:
        """Установить глобальную переменную"""
        self.global_variables[key] = value

    def get_global_variable(self, key: str, default: Any = None) -> Any:
        """Получить глобальную переменную"""
        return self.global_variables.get(key, default)

    def get_execution_summary(self) -> Dict[str, Any]:
        """Получить сводку выполнения"""
        return {
            "total_stages": len(self.stage_results),
            "successful_stages": sum(
                1
                for r in self.stage_results.values()
                if r.status == ExecutionStatus.SUCCESS
            ),
            "failed_stages": sum(
                1
                for r in self.stage_results.values()
                if r.status == ExecutionStatus.FAILED
            ),
            "total_processed_records": sum(
                r.processed_records for r in self.stage_results.values()
            ),
            "total_execution_time": sum(
                r.execution_time for r in self.stage_results.values()
            ),
        }


class BaseComponent(ABC):
    """Базовый компонент pipeline"""

    def __init__(self, config: Dict[str, Any]):
        self.config = self.get_config_model()(**config)
        self.logger = logging.getLogger(f"pipeline.component.{self.config.type}")

    @abstractmethod
    def get_config_model(self) -> Type[ComponentConfig]:
        """Возвращает модель конфигурации компонента"""
        pass

    @abstractmethod
    def execute(self, context: ExecutionContext) -> ExecutionResult:
        """
        Выполнение компонента

        Args:
            context: Контекст выполнения с доступом к результатам других стадий

        Returns:
            ExecutionResult: Результат выполнения
        """
        pass

    def validate_dependencies(self, available_stages: List[str]) -> List[str]:
        """
        Валидация зависимостей компонента

        Args:
            available_stages: Список доступных стадий

        Returns:
            List[str]: Список отсутствующих зависимостей
        """
        return []

    def get_component_type(self) -> ComponentType:
        """Получить тип компонента"""
        return ComponentType.UTILITY

    def setup(self) -> None:
        """Инициализация компонента (вызывается один раз)"""
        self.logger.debug(f"Инициализация компонента {self.config.type}")

    def teardown(self) -> None:
        """Очистка ресурсов компонента"""
        self.logger.debug(f"Очистка ресурсов компонента {self.config.type}")

    def health_check(self) -> bool:
        """Проверка состояния компонента"""
        return True

    def get_schema(self) -> Dict[str, Any]:
        """Получить JSON схему конфигурации компонента"""
        return self.get_config_model().model_json_schema()


class ComponentMetadata(BaseModel):
    """Метаданные компонента"""

    name: str
    version: str
    description: str
    component_type: ComponentType
    author: Optional[str] = None
    tags: List[str] = Field(default_factory=list)
    input_schema: Optional[Dict[str, Any]] = None
    output_schema: Optional[Dict[str, Any]] = None
    dependencies: List[str] = Field(default_factory=list)


class ComponentInfo(BaseModel):
    """Информация о зарегистрированном компоненте"""

    component_class: Type[BaseComponent]
    metadata: ComponentMetadata
    entry_point: str
