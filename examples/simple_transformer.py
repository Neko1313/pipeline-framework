"""
Пример простого компонента для трансформации данных
"""

from typing import Any, Dict, List, Type
from pydantic import BaseModel, Field
import time

from pipeline_core import (
    BaseComponent,
    ComponentConfig,
    ExecutionContext,
    ExecutionResult,
    ExecutionStatus,
    ComponentType,
    register_component,
)


class SimpleTransformConfig(ComponentConfig):
    """Конфигурация для простого трансформатора"""

    type: str = Field(default="simple-transform", const=True)

    # Источник данных
    source_stage: str = Field(..., description="Имя стадии-источника данных")

    # Параметры трансформации
    operations: List[str] = Field(default_factory=list, description="Список операций")
    multiplier: float = Field(default=1.0, description="Множитель для числовых значений")
    prefix: str = Field(default="", description="Префикс для строковых значений")

    # Настройки
    batch_size: int = Field(default=1000, description="Размер batch для обработки")
    validate_output: bool = Field(default=True, description="Валидировать выходные данные")


@register_component("simple-transform")
class SimpleTransformComponent(BaseComponent):
    """
    Простой компонент для демонстрации трансформации данных

    Поддерживаемые операции:
    - multiply: умножение числовых значений на multiplier
    - add_prefix: добавление префикса к строковым значениям
    - uppercase: перевод строк в верхний регистр
    - filter_positive: фильтрация только положительных чисел
    """

    def get_config_model(self) -> Type[ComponentConfig]:
        return SimpleTransformConfig

    def get_component_type(self) -> ComponentType:
        return ComponentType.TRANSFORMER

    def validate_dependencies(self, available_stages: List[str]) -> List[str]:
        """Проверяем что источник данных существует"""
        config: SimpleTransformConfig = self.config
        missing = []

        if config.source_stage not in available_stages:
            missing.append(config.source_stage)

        return missing

    def execute(self, context: ExecutionContext) -> ExecutionResult:
        """Выполнение трансформации данных"""
        config: SimpleTransformConfig = self.config
        start_time = time.time()

        try:
            # Получаем данные от источника
            source_data = context.get_stage_data(config.source_stage)
            if source_data is None:
                raise ValueError(f"Данные от стадии '{config.source_stage}' не найдены")

            self.logger.info(f"Начало трансформации данных от стадии '{config.source_stage}'")

            # Выполняем трансформации
            transformed_data = self._transform_data(source_data, config)

            # Валидируем результат если нужно
            if config.validate_output:
                self._validate_output(transformed_data)

            execution_time = time.time() - start_time
            processed_records = len(transformed_data) if hasattr(transformed_data, "__len__") else 1

            self.logger.info(
                f"Трансформация завершена успешно. "
                f"Обработано {processed_records} записей за {execution_time:.2f} сек"
            )

            return ExecutionResult(
                status=ExecutionStatus.SUCCESS,
                data=transformed_data,
                execution_time=execution_time,
                processed_records=processed_records,
                metadata={
                    "operations_applied": config.operations,
                    "source_stage": config.source_stage,
                    "batch_size": config.batch_size,
                },
            )

        except Exception as e:
            execution_time = time.time() - start_time
            self.logger.error(f"Ошибка трансформации: {e}", exc_info=True)

            return ExecutionResult(
                status=ExecutionStatus.FAILED,
                error_message=str(e),
                execution_time=execution_time,
                metadata={
                    "source_stage": config.source_stage,
                    "failed_operations": config.operations,
                },
            )

    def _transform_data(self, data: Any, config: SimpleTransformConfig) -> Any:
        """Применение трансформаций к данным"""

        # Обрабатываем разные типы данных
        if isinstance(data, list):
            return self._transform_list(data, config)
        elif isinstance(data, dict):
            return self._transform_dict(data, config)
        else:
            return self._transform_value(data, config)

    def _transform_list(self, data_list: List[Any], config: SimpleTransformConfig) -> List[Any]:
        """Трансформация списка данных"""
        result = []

        # Обрабатываем батчами для больших списков
        batch_size = config.batch_size
        for i in range(0, len(data_list), batch_size):
            batch = data_list[i : i + batch_size]
            transformed_batch = [self._transform_value(item, config) for item in batch]
            result.extend(transformed_batch)

            # Логируем прогресс для больших datasets
            if len(data_list) > 10000 and i % (batch_size * 10) == 0:
                self.logger.debug(f"Обработано {i}/{len(data_list)} записей")

        # Применяем операции фильтрации к результату
        if "filter_positive" in config.operations:
            result = [item for item in result if self._is_positive(item)]

        return result

    def _transform_dict(
        self, data_dict: Dict[str, Any], config: SimpleTransformConfig
    ) -> Dict[str, Any]:
        """Трансформация словаря данных"""
        result = {}

        for key, value in data_dict.items():
            # Трансформируем ключ если нужно
            new_key = key
            if "add_prefix" in config.operations and isinstance(key, str):
                new_key = f"{config.prefix}{key}"
            if "uppercase" in config.operations and isinstance(key, str):
                new_key = new_key.upper()

            # Трансформируем значение
            new_value = self._transform_value(value, config)
            result[new_key] = new_value

        return result

    def _transform_value(self, value: Any, config: SimpleTransformConfig) -> Any:
        """Трансформация отдельного значения"""
        result = value

        for operation in config.operations:
            if operation == "multiply" and isinstance(result, (int, float)):
                result = result * config.multiplier

            elif operation == "add_prefix" and isinstance(result, str):
                result = f"{config.prefix}{result}"

            elif operation == "uppercase" and isinstance(result, str):
                result = result.upper()

            elif operation == "filter_positive":
                # Фильтрация применяется на уровне списка
                pass

            else:
                self.logger.warning(f"Неизвестная операция: {operation}")

        return result

    def _is_positive(self, value: Any) -> bool:
        """Проверка что значение положительное"""
        if isinstance(value, (int, float)):
            return value > 0
        elif isinstance(value, dict):
            # Для словарей проверяем есть ли положительные числовые значения
            return any(isinstance(v, (int, float)) and v > 0 for v in value.values())
        return True  # Не числовые значения пропускаем

    def _validate_output(self, data: Any) -> None:
        """Валидация выходных данных"""
        if data is None:
            raise ValueError("Результат трансформации не может быть None")

        if isinstance(data, list) and len(data) == 0:
            self.logger.warning("Результат трансформации - пустой список")

        # Дополнительные проверки можно добавить здесь


# Пример создания компонента через простую функцию
def simple_multiplier_transform(context: ExecutionContext) -> List[float]:
    """
    Простая функция трансформации - умножает все числа на 2
    """
    data = context.get_stage_data("extract")
    if not isinstance(data, list):
        raise ValueError("Ожидается список чисел")

    result = []
    for item in data:
        if isinstance(item, (int, float)):
            result.append(item * 2)
        else:
            result.append(item)

    return result


# Создаем компонент из функции и регистрируем
from pipeline_core import create_simple_component

SimpleMultiplierComponent = create_simple_component(
    "simple-multiplier", simple_multiplier_transform
)

register_component("simple-multiplier", SimpleMultiplierComponent)
