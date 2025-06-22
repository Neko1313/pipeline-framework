# packages/pipeline-core/src/pipeline_core/config/__init__.py

"""
Configuration package для pipeline framework

Включает:
- Pydantic модели для валидации конфигурации
- YAML загрузчик с поддержкой template и includes
- Утилиты для работы с конфигурацией
"""

# Основные модели конфигурации
from pipeline_core.config.models import (
    # Основные конфигурации
    PipelineConfig,
    StageConfig,
    PipelineMetadata,
    ComponentSettings,
    # Политики и настройки
    RetryPolicy,
    QualityCheck,
    ParallelConfig,
    ErrorHandling,
    # Мониторинг и алерты
    AlertConfig,
    SLAConfig,
    MonitoringConfig,
    # Интеграции
    TemporalConfig,
    RuntimeConfig,
    # Енумы
    LogLevel,
    ScheduleType,
    WriteMode,
)

# YAML загрузчик
try:
    from pipeline_core.config.yaml_loader import (
        YAMLConfigLoader,
        YAMLConfigError,
        load_pipeline_config,
        validate_pipeline_config,
        create_config_template,
    )

    YAML_LOADER_AVAILABLE = True
except ImportError:
    YAML_LOADER_AVAILABLE = False

    # Создаем заглушки
    class YAMLConfigLoader:
        def __init__(self, *args, **kwargs):
            raise ImportError("YAML loader dependencies not available")

    class YAMLConfigError(Exception):
        pass

    def load_pipeline_config(*args, **kwargs):
        raise ImportError("YAML loader dependencies not available")

    def validate_pipeline_config(*args, **kwargs):
        raise ImportError("YAML loader dependencies not available")

    def create_config_template(*args, **kwargs):
        raise ImportError("YAML loader dependencies not available")


# Экспортируемые элементы
__all__ = [
    # Основные модели
    "PipelineConfig",
    "StageConfig",
    "PipelineMetadata",
    "ComponentSettings",
    # Политики и настройки
    "RetryPolicy",
    "QualityCheck",
    "ParallelConfig",
    "ErrorHandling",
    # Мониторинг и алерты
    "AlertConfig",
    "SLAConfig",
    "MonitoringConfig",
    # Интеграции
    "TemporalConfig",
    "RuntimeConfig",
    # Енумы
    "LogLevel",
    "ScheduleType",
    "WriteMode",
    # YAML загрузчик
    "YAMLConfigLoader",
    "YAMLConfigError",
    "load_pipeline_config",
    "validate_pipeline_config",
    "create_config_template",
]


# Информация о доступности
def is_yaml_loader_available() -> bool:
    """Проверка доступности YAML загрузчика"""
    return YAML_LOADER_AVAILABLE


# Утилитарные функции для быстрого создания конфигураций
def create_basic_pipeline_config(
    name: str,
    stages: list,
    description: str = "",
    version: str = "1.0.0",
) -> PipelineConfig:
    """
    Создание базовой конфигурации pipeline

    Args:
        name: Имя pipeline
        stages: Список конфигураций этапов
        description: Описание
        version: Версия

    Returns:
        Объект PipelineConfig
    """
    return PipelineConfig(
        name=name,
        version=version,
        description=description,
        stages=stages,
    )


def create_stage_config(
    name: str,
    component: str,
    config: dict = None,
    depends_on: list = None,
    description: str = "",
) -> StageConfig:
    """
    Создание конфигурации этапа

    Args:
        name: Имя этапа
        component: Компонент (например, "extractor/sql")
        config: Конфигурация компонента
        depends_on: Зависимости
        description: Описание

    Returns:
        Объект StageConfig
    """
    return StageConfig(
        name=name,
        component=component,
        config=config or {},
        depends_on=depends_on or [],
        description=description,
    )


def create_retry_policy(
    max_attempts: int = 3,
    initial_interval: str = "30s",
    max_interval: str = "5m",
    backoff_coefficient: float = 2.0,
) -> RetryPolicy:
    """
    Создание политики повторов

    Args:
        max_attempts: Максимальное количество попыток
        initial_interval: Начальный интервал
        max_interval: Максимальный интервал
        backoff_coefficient: Коэффициент роста

    Returns:
        Объект RetryPolicy
    """
    return RetryPolicy(
        maximum_attempts=max_attempts,
        initial_interval=initial_interval,
        maximum_interval=max_interval,
        backoff_coefficient=backoff_coefficient,
    )


def create_quality_check(
    name: str,
    check_type: str,
    config: dict = None,
    fail_on_error: bool = True,
) -> QualityCheck:
    """
    Создание проверки качества

    Args:
        name: Имя проверки
        check_type: Тип проверки
        config: Конфигурация проверки
        fail_on_error: Останавливать при ошибке

    Returns:
        Объект QualityCheck
    """
    return QualityCheck(
        name=name,
        type=check_type,
        config=config or {},
        fail_on_error=fail_on_error,
    )


# Дополнительные утилиты для валидации
def validate_config_dict(config_dict: dict) -> tuple[bool, list[str]]:
    """
    Валидация словаря конфигурации без создания объекта

    Args:
        config_dict: Словарь конфигурации

    Returns:
        Кортеж (валидна ли конфигурация, список ошибок)
    """
    errors = []

    try:
        # Пытаемся создать PipelineConfig
        if "pipeline" in config_dict:
            PipelineConfig(**config_dict["pipeline"])
        else:
            PipelineConfig(**config_dict)
        return True, []

    except Exception as e:
        errors.append(str(e))
        return False, errors


def merge_configs(base_config: dict, override_config: dict) -> dict:
    """
    Слияние конфигураций с перезаписью

    Args:
        base_config: Базовая конфигурация
        override_config: Переопределяющая конфигурация

    Returns:
        Объединенная конфигурация
    """

    def deep_merge(base: dict, override: dict) -> dict:
        result = base.copy()

        for key, value in override.items():
            if (
                key in result
                and isinstance(result[key], dict)
                and isinstance(value, dict)
            ):
                result[key] = deep_merge(result[key], value)
            else:
                result[key] = value

        return result

    return deep_merge(base_config, override_config)


def extract_variables(config_dict: dict) -> set[str]:
    """
    Извлечение всех переменных из конфигурации

    Args:
        config_dict: Словарь конфигурации

    Returns:
        Множество имен переменных
    """
    import re

    variables = set()

    def scan_dict(obj):
        if isinstance(obj, dict):
            for value in obj.values():
                scan_dict(value)
        elif isinstance(obj, list):
            for item in obj:
                scan_dict(item)
        elif isinstance(obj, str):
            # Ищем переменные в формате ${VAR_NAME}
            matches = re.findall(r"\$\{([^}:]+)(?::[^}]*)?\}", obj)
            variables.update(matches)

    scan_dict(config_dict)
    return variables


def substitute_variables(config_dict: dict, variables: dict[str, str]) -> dict:
    """
    Подстановка переменных в конфигурацию

    Args:
        config_dict: Словарь конфигурации
        variables: Словарь переменных

    Returns:
        Конфигурация с подставленными переменными
    """
    import re
    import copy

    result = copy.deepcopy(config_dict)

    def substitute_in_obj(obj):
        if isinstance(obj, dict):
            return {key: substitute_in_obj(value) for key, value in obj.items()}
        elif isinstance(obj, list):
            return [substitute_in_obj(item) for item in obj]
        elif isinstance(obj, str):
            # Заменяем переменные
            def replace_var(match):
                var_name = match.group(1)
                default_value = match.group(2) if match.group(2) is not None else ""
                return variables.get(var_name, default_value)

            pattern = r"\$\{([^}:]+)(?::([^}]*))?\}"
            return re.sub(pattern, replace_var, obj)
        else:
            return obj

    return substitute_in_obj(result)


# Добавляем утилиты в __all__
__all__.extend(
    [
        "is_yaml_loader_available",
        "create_basic_pipeline_config",
        "create_stage_config",
        "create_retry_policy",
        "create_quality_check",
        "validate_config_dict",
        "merge_configs",
        "extract_variables",
        "substitute_variables",
    ]
)
