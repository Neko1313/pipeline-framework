"""
YAML парсер для конфигурации pipeline
"""

import yaml
from typing import Any, Dict, List, Optional
from pathlib import Path
import re
import os
from datetime import datetime
from enum import Enum
from pydantic import BaseModel, Field, model_validator

from ..models.pipeline import (
    PipelineConfig,
    PipelineMetadata,
    StageConfig,
    ScheduleConfig,
    ResourceLimits,
)
from ..exceptions.errors import PipelineConfigError, PipelineValidationError


class YAMLTemplateProcessor:
    """Обработчик шаблонов в YAML конфигурации"""

    def __init__(self, variables: Optional[Dict[str, Any]] = None):
        self.variables = variables or {}
        # Добавляем системные переменные
        self.variables.update(
            {
                "NOW": datetime.now().isoformat(),
                "DATE": datetime.now().strftime("%Y-%m-%d"),
                "TIME": datetime.now().strftime("%H:%M:%S"),
            }
        )
        # Добавляем переменные окружения
        self.variables.update(os.environ)

    def process_template(self, content: str) -> str:
        """
        Обработка шаблонов в строке
        Поддерживает:
        - ${VAR} - обязательная переменная
        - ${VAR:-default} - переменная с значением по умолчанию
        - ${VAR:?error} - переменная с ошибкой если не найдена
        """

        def replace_var(match):
            var_expr = match.group(1)

            # Проверяем синтаксис ${VAR:-default}
            if ":-" in var_expr:
                var_name, default_value = var_expr.split(":-", 1)
                return str(self.variables.get(var_name.strip(), default_value))

            # Проверяем синтаксис ${VAR:?error}
            elif ":?" in var_expr:
                var_name, error_msg = var_expr.split(":?", 1)
                var_name = var_name.strip()
                if var_name not in self.variables:
                    raise PipelineConfigError(
                        f"Обязательная переменная '{var_name}' не найдена: {error_msg}"
                    )
                return str(self.variables[var_name])

            # Обычный синтаксис ${VAR}
            else:
                var_name = var_expr.strip()
                if var_name not in self.variables:
                    raise PipelineConfigError(f"Переменная '{var_name}' не найдена")
                return str(self.variables[var_name])

        # Регулярное выражение для поиска ${...}
        pattern = r"\$\{([^}]+)\}"
        return re.sub(pattern, replace_var, content)

    def process_dict(self, data: Any) -> Any:
        """Рекурсивная обработка словарей и списков"""
        if isinstance(data, dict):
            return {key: self.process_dict(value) for key, value in data.items()}
        elif isinstance(data, list):
            return [self.process_dict(item) for item in data]
        elif isinstance(data, str):
            return self.process_template(data)
        else:
            return data


class PipelineYAMLParser:
    """Парсер YAML конфигурации pipeline"""

    def __init__(self):
        self.template_processor: Optional[YAMLTemplateProcessor] = None

    def parse_file(
        self, file_path: Path, variables: Optional[Dict[str, Any]] = None
    ) -> PipelineConfig:
        """
        Парсинг YAML файла с конфигурацией pipeline

        Args:
            file_path: Путь к YAML файлу
            variables: Дополнительные переменные для шаблонизации

        Returns:
            PipelineConfig: Распарсенная конфигурация
        """
        if not file_path.exists():
            raise PipelineConfigError(f"Файл конфигурации не найден: {file_path}")

        try:
            with open(file_path, "r", encoding="utf-8") as f:
                content = f.read()

            return self.parse_string(content, variables)

        except yaml.YAMLError as e:
            raise PipelineConfigError(f"Ошибка парсинга YAML: {e}")
        except Exception as e:
            raise PipelineConfigError(f"Ошибка чтения файла {file_path}: {e}")

    def parse_string(
        self, yaml_content: str, variables: Optional[Dict[str, Any]] = None
    ) -> PipelineConfig:
        """
        Парсинг строки с YAML конфигурацией

        Args:
            yaml_content: YAML контент как строка
            variables: Переменные для шаблонизации

        Returns:
            PipelineConfig: Распарсенная конфигурация
        """
        try:
            # Инициализируем процессор шаблонов только с внешними переменными
            self.template_processor = YAMLTemplateProcessor(variables)

            # Сначала парсим YAML без обработки переменных из файла
            raw_data = yaml.safe_load(yaml_content)
            if not raw_data:
                raise PipelineConfigError("YAML файл пуст")

            # Добавляем переменные из YAML в процессор
            yaml_variables = raw_data.get("variables", {})
            if yaml_variables:
                self.template_processor.variables.update(yaml_variables)

            # Теперь обрабатываем шаблоны с полным набором переменных
            processed_content = self.template_processor.process_template(yaml_content)

            # Парсим обработанный YAML
            processed_data = yaml.safe_load(processed_content)
            if not processed_data:
                raise PipelineConfigError("Обработанный YAML файл пуст")

            # Конвертируем в структурированную конфигурацию
            return self._convert_to_pipeline_config(processed_data)

        except yaml.YAMLError as e:
            raise PipelineConfigError(f"Ошибка парсинга YAML: {e}")
        except Exception as e:
            if isinstance(e, (PipelineConfigError, PipelineValidationError)):
                raise
            raise PipelineConfigError(f"Неожиданная ошибка при парсинге: {e}")

    def _convert_to_pipeline_config(self, data: Dict[str, Any]) -> PipelineConfig:
        """Конвертация сырых данных в PipelineConfig"""

        # Извлекаем метаданные
        metadata_data = data.get("metadata", {})
        if "name" not in metadata_data and "name" in data:
            metadata_data["name"] = data["name"]

        metadata = PipelineMetadata(**metadata_data)

        # Извлекаем глобальные переменные
        variables = data.get("variables", {})

        # Извлекаем конфигурацию расписания
        schedule = None
        if "schedule" in data:
            schedule = ScheduleConfig(**data["schedule"])

        # Извлекаем ограничения ресурсов
        resources = ResourceLimits()
        if "resources" in data:
            resources = ResourceLimits(**data["resources"])

        # Обрабатываем стадии
        stages = self._parse_stages(data)

        return PipelineConfig(
            metadata=metadata,
            variables=variables,
            schedule=schedule,
            resources=resources,
            stages=stages,
        )

    def _parse_stages(self, data: Dict[str, Any]) -> Dict[str, StageConfig]:
        """Парсинг конфигурации стадий"""
        stages = {}

        # Поддерживаем разные форматы описания стадий
        if "stages" in data:
            # Формат с явным разделом stages
            stage_data = data["stages"]
        else:
            # Ищем стадии в корне документа (исключая служебные секции)
            excluded_keys = {
                "metadata",
                "name",
                "variables",
                "schedule",
                "resources",
                "version",
                "description",
            }
            stage_data = {k: v for k, v in data.items() if k not in excluded_keys}

        for stage_name, stage_config in stage_data.items():
            if not isinstance(stage_config, dict):
                raise PipelineConfigError(
                    f"Конфигурация стадии '{stage_name}' должна быть объектом"
                )

            # Извлекаем тип компонента
            if "type" not in stage_config:
                raise PipelineConfigError(
                    f"Стадия '{stage_name}' должна содержать поле 'type'"
                )

            component_type = stage_config["type"]

            # Извлекаем зависимости
            depends_on = self._extract_dependencies(stage_config, stage_name)

            # Извлекаем условие выполнения
            condition = stage_config.get("condition")

            # Флаг параллельного выполнения
            parallel = stage_config.get("parallel", False)

            # Остальная конфигурация передается в config
            # Важно: исключаем служебные поля, но сохраняем все остальное
            config = {
                k: v
                for k, v in stage_config.items()
                if k not in ["type", "depends_on", "condition", "parallel"]
            }

            stages[stage_name] = StageConfig(
                type=component_type,
                config=config,
                depends_on=depends_on,
                condition=condition,
                parallel=parallel,
            )

        if not stages:
            raise PipelineConfigError("Pipeline должен содержать хотя бы одну стадию")

        return stages

    def _extract_dependencies(
        self, stage_config: Dict[str, Any], stage_name: str
    ) -> List[str]:
        """Извлечение зависимостей стадии"""
        dependencies = []

        # Явные зависимости через depends_on
        if "depends_on" in stage_config:
            deps = stage_config["depends_on"]
            if isinstance(deps, str):
                dependencies.append(deps)
            elif isinstance(deps, list):
                dependencies.extend(deps)
            else:
                raise PipelineConfigError(
                    f"depends_on для стадии '{stage_name}' должно быть строкой или списком"
                )

        # Неявные зависимости через ссылки на другие стадии
        for key, value in stage_config.items():
            if key.startswith("stage-") or key.endswith("-stage"):
                # Найдена ссылка на стадию
                referenced_stage = value
                if (
                    isinstance(referenced_stage, str)
                    and referenced_stage not in dependencies
                ):
                    dependencies.append(referenced_stage)

        return dependencies

    def validate_config(self, config: PipelineConfig) -> List[str]:
        """
        Валидация конфигурации pipeline

        Returns:
            List[str]: Список предупреждений (пустой если все ОК)
        """
        warnings = []

        # Проверяем что все зависимости существуют
        stage_names = set(config.stages.keys())
        for stage_name, stage_config in config.stages.items():
            for dep in stage_config.depends_on:
                if dep not in stage_names:
                    raise PipelineValidationError(
                        f"Стадия '{stage_name}' зависит от несуществующей стадии '{dep}'"
                    )

        # Проверяем на потенциальные проблемы производительности
        execution_levels = config.get_execution_order()
        max_parallel = (
            max(len(level) for level in execution_levels) if execution_levels else 0
        )

        if max_parallel > config.resources.max_parallel_stages:
            warnings.append(
                f"Максимальное количество параллельных стадий ({max_parallel}) "
                f"превышает лимит ({config.resources.max_parallel_stages})"
            )

        # Проверяем на стадии без зависимостей (кроме первого уровня)
        if len(execution_levels) > 1:
            isolated_stages = []
            for level in execution_levels[1:]:
                for stage in level:
                    if not config.stages[stage].depends_on:
                        isolated_stages.append(stage)

            if isolated_stages:
                warnings.append(
                    f"Стадии без зависимостей (возможно независимые): {', '.join(isolated_stages)}"
                )

        return warnings
