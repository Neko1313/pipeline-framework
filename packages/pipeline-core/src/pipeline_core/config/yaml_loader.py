# packages/pipeline-core/src/pipeline_core/config/yaml_loader.py

"""
YAML Configuration Loader для pipeline framework

Поддерживает:
- Загрузку YAML конфигураций pipeline
- Подстановку переменных окружения
- Template рендеринг с Jinja2
- Валидацию конфигурации
- Include/extend механизм
- Schema versioning
"""

import os
import re
from pathlib import Path
from typing import Dict, Any, List, Optional, Union
from datetime import datetime

import yaml
import structlog
from pydantic import ValidationError

try:
    from jinja2 import Environment, FileSystemLoader, Template, TemplateSyntaxError

    JINJA2_AVAILABLE = True
except ImportError:
    JINJA2_AVAILABLE = False

from pipeline_core.config.models import PipelineConfig, StageConfig

logger = structlog.get_logger(__name__)


class YAMLConfigError(Exception):
    """Ошибка загрузки YAML конфигурации"""

    pass


class YAMLConfigLoader:
    """
    Загрузчик YAML конфигураций для pipeline

    Обеспечивает:
    - Парсинг YAML файлов
    - Подстановку переменных
    - Template рендеринг
    - Валидацию схемы
    - Include/extend поддержку
    """

    def __init__(
        self,
        base_path: Optional[Path] = None,
        environment: str = "development",
        enable_templating: bool = True,
        enable_includes: bool = True,
    ):
        self.base_path = Path(base_path) if base_path else Path.cwd()
        self.environment = environment
        self.enable_templating = enable_templating
        self.enable_includes = enable_includes

        # Template engine
        self._jinja_env = None
        if JINJA2_AVAILABLE and enable_templating:
            self._setup_jinja_environment()

        # Кэш для загруженных файлов
        self._file_cache: Dict[str, Dict[str, Any]] = {}

        self.logger = structlog.get_logger(
            component="yaml_config_loader",
            environment=environment,
        )

    def _setup_jinja_environment(self):
        """Настройка Jinja2 окружения"""
        if not JINJA2_AVAILABLE:
            return

        # Создаем loader для поиска template файлов
        template_paths = [
            str(self.base_path),
            str(self.base_path / "templates"),
            str(self.base_path / "configs"),
        ]

        file_loader = FileSystemLoader(template_paths)

        self._jinja_env = Environment(
            loader=file_loader,
            # Настройки безопасности
            autoescape=False,
            trim_blocks=True,
            lstrip_blocks=True,
        )

        # Добавляем custom функции
        self._jinja_env.globals.update(
            {
                "env": self._get_env_var,
                "now": datetime.now,
                "date_format": self._format_date,
            }
        )

    def _get_env_var(self, name: str, default: str = "") -> str:
        """Получение переменной окружения для Jinja2"""
        return os.getenv(name, default)

    def _format_date(self, date_obj: datetime, format_str: str = "%Y-%m-%d") -> str:
        """Форматирование даты для Jinja2"""
        return date_obj.strftime(format_str)

    def load_config(self, config_path: Union[str, Path]) -> PipelineConfig:
        """
        Загрузка конфигурации pipeline из YAML файла

        Args:
            config_path: Путь к YAML файлу

        Returns:
            Объект PipelineConfig

        Raises:
            YAMLConfigError: При ошибках загрузки или валидации
        """
        config_path = Path(config_path)

        if not config_path.is_absolute():
            config_path = self.base_path / config_path

        try:
            self.logger.info("Loading pipeline configuration", path=str(config_path))

            # Загружаем и обрабатываем YAML
            raw_config = self._load_yaml_file(config_path)

            # Обрабатываем includes
            if self.enable_includes:
                raw_config = self._process_includes(raw_config, config_path.parent)

            # Подстановка переменных
            raw_config = self._substitute_variables(raw_config)

            # Template рендеринг
            if self.enable_templating and JINJA2_AVAILABLE:
                raw_config = self._render_templates(raw_config)

            # Применяем environment-specific настройки
            raw_config = self._apply_environment_config(raw_config)

            # Валидация и создание объекта конфигурации
            pipeline_config = self._validate_and_create_config(raw_config)

            self.logger.info(
                "Pipeline configuration loaded successfully",
                pipeline_name=pipeline_config.name,
                stages_count=len(pipeline_config.stages),
            )

            return pipeline_config

        except Exception as e:
            self.logger.error(
                "Failed to load pipeline configuration",
                error=str(e),
                path=str(config_path),
            )
            raise YAMLConfigError(
                f"Failed to load config from {config_path}: {e}"
            ) from e

    def _load_yaml_file(self, file_path: Path) -> Dict[str, Any]:
        """Загрузка YAML файла"""
        if not file_path.exists():
            raise FileNotFoundError(f"Config file not found: {file_path}")

        # Проверяем кэш
        cache_key = str(file_path.absolute())
        file_stat = file_path.stat()

        if cache_key in self._file_cache:
            cached_data, cached_mtime = self._file_cache[cache_key]
            if cached_mtime >= file_stat.st_mtime:
                return cached_data

        try:
            with open(file_path, "r", encoding="utf-8") as f:
                content = f.read()

            # Парсим YAML
            data = yaml.safe_load(content)

            # Сохраняем в кэш
            self._file_cache[cache_key] = (data, file_stat.st_mtime)

            return data

        except yaml.YAMLError as e:
            raise YAMLConfigError(f"Invalid YAML syntax in {file_path}: {e}") from e

    def _process_includes(
        self, config: Dict[str, Any], base_dir: Path
    ) -> Dict[str, Any]:
        """Обработка include директив"""
        if not isinstance(config, dict):
            return config

        # Ищем include директивы
        if "include" in config:
            includes = config.pop("include")
            if isinstance(includes, str):
                includes = [includes]

            # Загружаем каждый include файл
            for include_path in includes:
                include_file = base_dir / include_path
                included_config = self._load_yaml_file(include_file)

                # Рекурсивно обрабатываем includes во включенных файлах
                included_config = self._process_includes(
                    included_config, include_file.parent
                )

                # Мержим конфигурации (текущая перезаписывает включенную)
                config = self._deep_merge(included_config, config)

        # Рекурсивно обрабатываем вложенные объекты
        for key, value in config.items():
            if isinstance(value, dict):
                config[key] = self._process_includes(value, base_dir)
            elif isinstance(value, list):
                config[key] = [
                    self._process_includes(item, base_dir)
                    if isinstance(item, dict)
                    else item
                    for item in value
                ]

        return config

    def _deep_merge(
        self, base: Dict[str, Any], override: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Глубокое слияние словарей"""
        result = base.copy()

        for key, value in override.items():
            if (
                key in result
                and isinstance(result[key], dict)
                and isinstance(value, dict)
            ):
                result[key] = self._deep_merge(result[key], value)
            else:
                result[key] = value

        return result

    def _substitute_variables(self, config: Any) -> Any:
        """Подстановка переменных окружения"""
        if isinstance(config, str):
            # Паттерн для переменных: ${VAR_NAME} или ${VAR_NAME:default_value}
            pattern = r"\$\{([^}:]+)(?::([^}]*))?\}"

            def replace_var(match):
                var_name = match.group(1)
                default_value = match.group(2) if match.group(2) is not None else ""
                return os.getenv(var_name, default_value)

            return re.sub(pattern, replace_var, config)

        elif isinstance(config, dict):
            return {
                key: self._substitute_variables(value) for key, value in config.items()
            }

        elif isinstance(config, list):
            return [self._substitute_variables(item) for item in config]

        else:
            return config

    def _render_templates(self, config: Any) -> Any:
        """Рендеринг Jinja2 templates"""
        if not self._jinja_env:
            return config

        # Контекст для рендеринга
        template_context = {
            "environment": self.environment,
            "ds": datetime.now().strftime("%Y-%m-%d"),  # Airflow-style date
            "ts": datetime.now().isoformat(),
        }

        if isinstance(config, str):
            # Проверяем наличие Jinja2 синтаксиса
            if "{{" in config or "{%" in config:
                try:
                    template = self._jinja_env.from_string(config)
                    return template.render(**template_context)
                except TemplateSyntaxError as e:
                    self.logger.warning(
                        "Template syntax error", template=config, error=str(e)
                    )
                    return config
            return config

        elif isinstance(config, dict):
            return {key: self._render_templates(value) for key, value in config.items()}

        elif isinstance(config, list):
            return [self._render_templates(item) for item in config]

        else:
            return config

    def _apply_environment_config(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Применение environment-specific настроек"""
        if "environments" not in config:
            return config

        environments = config.get("environments", {})
        env_config = environments.get(self.environment, {})

        if env_config:
            self.logger.debug(
                "Applying environment configuration", environment=self.environment
            )
            # Мержим environment-specific настройки
            config = self._deep_merge(config, env_config)

        # Удаляем секцию environments из итоговой конфигурации
        config.pop("environments", None)

        return config

    def _validate_and_create_config(self, raw_config: Dict[str, Any]) -> PipelineConfig:
        """Валидация и создание объекта PipelineConfig"""
        try:
            # Извлекаем секцию pipeline
            if "pipeline" not in raw_config:
                raise YAMLConfigError("Missing 'pipeline' section in configuration")

            pipeline_data = raw_config["pipeline"]

            # Преобразуем stages в объекты StageConfig
            stages_data = pipeline_data.get("stages", [])
            stages = []

            for i, stage_data in enumerate(stages_data):
                try:
                    # Добавляем индекс для порядка выполнения
                    stage_data["execution_order"] = i

                    stage_config = StageConfig(**stage_data)
                    stages.append(stage_config)

                except ValidationError as e:
                    raise YAMLConfigError(
                        f"Invalid stage configuration at index {i}: {e}"
                    ) from e

            # Заменяем stages на валидированные объекты
            pipeline_data["stages"] = stages

            # Создаем и валидируем PipelineConfig
            return PipelineConfig(**pipeline_data)

        except ValidationError as e:
            raise YAMLConfigError(
                f"Pipeline configuration validation failed: {e}"
            ) from e

    def validate_config_file(self, config_path: Union[str, Path]) -> Dict[str, Any]:
        """
        Валидация YAML файла без создания объекта конфигурации

        Args:
            config_path: Путь к файлу

        Returns:
            Результат валидации
        """
        try:
            config = self.load_config(config_path)
            return {
                "valid": True,
                "errors": [],
                "warnings": [],
                "pipeline_name": config.name,
                "stages_count": len(config.stages),
            }

        except Exception as e:
            return {
                "valid": False,
                "errors": [str(e)],
                "warnings": [],
                "pipeline_name": None,
                "stages_count": 0,
            }

    def get_available_templates(self) -> List[str]:
        """Получение списка доступных template файлов"""
        if not self._jinja_env:
            return []

        templates = []
        for loader in self._jinja_env.loaders:
            if hasattr(loader, "list_templates"):
                templates.extend(loader.list_templates())

        return sorted(set(templates))

    def render_template_string(
        self, template_str: str, context: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        Рендеринг строки как Jinja2 template

        Args:
            template_str: Template строка
            context: Контекст для рендеринга

        Returns:
            Отрендеренная строка
        """
        if not self._jinja_env:
            return template_str

        context = context or {}
        default_context = {
            "environment": self.environment,
            "ds": datetime.now().strftime("%Y-%m-%d"),
            "ts": datetime.now().isoformat(),
        }
        default_context.update(context)

        try:
            template = self._jinja_env.from_string(template_str)
            return template.render(**default_context)
        except Exception as e:
            self.logger.error(
                "Template rendering failed", template=template_str, error=str(e)
            )
            return template_str

    def export_config_as_dict(self, config: PipelineConfig) -> Dict[str, Any]:
        """Экспорт конфигурации обратно в словарь"""
        return {
            "pipeline": config.model_dump(
                exclude_none=True,
                exclude_unset=False,
            )
        }

    def save_config(
        self, config: PipelineConfig, output_path: Union[str, Path]
    ) -> None:
        """
        Сохранение конфигурации в YAML файл

        Args:
            config: Объект конфигурации
            output_path: Путь для сохранения
        """
        output_path = Path(output_path)

        config_dict = self.export_config_as_dict(config)

        try:
            with open(output_path, "w", encoding="utf-8") as f:
                yaml.dump(
                    config_dict,
                    f,
                    default_flow_style=False,
                    allow_unicode=True,
                    sort_keys=False,
                    indent=2,
                )

            self.logger.info("Configuration saved", path=str(output_path))

        except Exception as e:
            raise YAMLConfigError(f"Failed to save config to {output_path}: {e}") from e


# Утилитарные функции


def load_pipeline_config(
    config_path: Union[str, Path],
    environment: str = "development",
    base_path: Optional[Path] = None,
) -> PipelineConfig:
    """
    Быстрая загрузка конфигурации pipeline

    Args:
        config_path: Путь к YAML файлу
        environment: Окружение
        base_path: Базовый путь для поиска файлов

    Returns:
        Объект PipelineConfig
    """
    loader = YAMLConfigLoader(
        base_path=base_path,
        environment=environment,
    )
    return loader.load_config(config_path)


def validate_pipeline_config(config_path: Union[str, Path]) -> Dict[str, Any]:
    """
    Валидация YAML файла конфигурации

    Args:
        config_path: Путь к файлу

    Returns:
        Результат валидации
    """
    loader = YAMLConfigLoader()
    return loader.validate_config_file(config_path)


def create_config_template(
    output_path: Union[str, Path],
    pipeline_name: str = "my-pipeline",
    include_examples: bool = True,
) -> None:
    """
    Создание template файла конфигурации

    Args:
        output_path: Путь для сохранения template
        pipeline_name: Имя pipeline
        include_examples: Включать ли примеры этапов
    """
    template_config = {
        "pipeline": {
            "name": pipeline_name,
            "version": "1.0.0",
            "description": f"Pipeline configuration for {pipeline_name}",
            "metadata": {
                "owner": "data-team@company.com",
                "schedule": "0 6 * * *",  # Daily at 6 AM
                "environment": "development",
                "tags": ["example"],
            },
            "variables": {
                "database_url": "${DATABASE_URL}",
                "batch_size": 1000,
            },
            "stages": [],
        }
    }

    if include_examples:
        template_config["pipeline"]["stages"] = [
            {
                "name": "extract-data",
                "component": "extractor/sql",
                "description": "Extract data from source database",
                "config": {
                    "connection_string": "${database_url}",
                    "query": "SELECT * FROM source_table",
                    "output_format": "pandas",
                },
                "timeout": "10m",
            },
            {
                "name": "transform-data",
                "component": "transformer/pandas",
                "description": "Transform and clean data",
                "depends_on": ["extract-data"],
                "config": {
                    "script_path": "transforms/clean_data.py",
                },
                "timeout": "15m",
            },
            {
                "name": "load-data",
                "component": "loader/sql",
                "description": "Load data to target system",
                "depends_on": ["transform-data"],
                "config": {
                    "connection_string": "${warehouse_url}",
                    "target_table": "processed_data",
                    "write_mode": "append",
                },
                "timeout": "10m",
            },
        ]

    output_path = Path(output_path)
    with open(output_path, "w", encoding="utf-8") as f:
        yaml.dump(
            template_config,
            f,
            default_flow_style=False,
            allow_unicode=True,
            sort_keys=False,
            indent=2,
        )

    logger.info("Pipeline configuration template created", path=str(output_path))
