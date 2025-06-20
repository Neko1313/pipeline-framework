"""
YAML Configuration Loader с расширенными возможностями

Поддерживает:
- Environment variables substitution
- File inclusion (include/extends)
- Template inheritance
- Validation с помощью Pydantic
- Multiple format support (YAML, JSON)
"""

import os
import re
import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Union
from urllib.parse import urlparse

import yaml
import structlog
from pydantic import ValidationError

from pipeline_core.config.models import (
    PipelineConfig,
    FullPipelineConfig,
    RuntimeConfig,
)

logger = structlog.get_logger(__name__)


class YAMLLoaderError(Exception):
    """Ошибка загрузки YAML конфигурации"""

    pass


class ConfigValidator:
    """Валидатор конфигураций"""

    @staticmethod
    def validate_pipeline_config(data: Dict[str, Any]) -> PipelineConfig:
        """Валидация конфигурации pipeline"""
        try:
            return PipelineConfig(**data)
        except ValidationError as e:
            raise YAMLLoaderError(f"Pipeline configuration validation failed: {e}")

    @staticmethod
    def validate_full_config(data: Dict[str, Any]) -> FullPipelineConfig:
        """Валидация полной конфигурации"""
        try:
            return FullPipelineConfig(**data)
        except ValidationError as e:
            raise YAMLLoaderError(f"Full configuration validation failed: {e}")


class EnvironmentSubstitution:
    """Обработчик подстановки переменных окружения"""

    # Паттерны для различных форматов переменных
    PATTERNS = {
        "simple": re.compile(r"\$([A-Za-z_][A-Za-z0-9_]*)"),  # $VAR
        "braced": re.compile(r"\$\{([^}:]+)\}"),  # ${VAR}
        "default": re.compile(r"\$\{([^}:]+):([^}]*)\}"),  # ${VAR:default}
    }

    @classmethod
    def substitute(
        cls, value: Any, env_overrides: Optional[Dict[str, str]] = None
    ) -> Any:
        """Подстановка переменных окружения"""
        if isinstance(value, str):
            return cls._substitute_string(value, env_overrides or {})
        elif isinstance(value, dict):
            return {k: cls.substitute(v, env_overrides) for k, v in value.items()}
        elif isinstance(value, list):
            return [cls.substitute(v, env_overrides) for v in value]
        else:
            return value

    @classmethod
    def _substitute_string(cls, text: str, env_overrides: Dict[str, str]) -> str:
        """Подстановка переменных в строке"""
        # Сначала подставляем переменные с default значениями
        text = cls.PATTERNS["default"].sub(
            lambda m: cls._get_env_var(m.group(1), m.group(2), env_overrides), text
        )

        # Затем обычные braced переменные
        text = cls.PATTERNS["braced"].sub(
            lambda m: cls._get_env_var(m.group(1), "", env_overrides), text
        )

        # Наконец простые переменные
        text = cls.PATTERNS["simple"].sub(
            lambda m: cls._get_env_var(m.group(1), "", env_overrides), text
        )

        return text

    @staticmethod
    def _get_env_var(var_name: str, default: str, env_overrides: Dict[str, str]) -> str:
        """Получение значения переменной окружения"""
        # Сначала проверяем overrides, затем os.environ, затем default
        return env_overrides.get(var_name, os.getenv(var_name, default))


class TemplateProcessor:
    """Обработчик шаблонов и наследования"""

    def __init__(self, base_path: Optional[Path] = None):
        self.base_path = base_path or Path.cwd()
        self._loaded_files: Set[Path] = set()

    def process_includes(
        self, data: Dict[str, Any], current_file: Optional[Path] = None
    ) -> Dict[str, Any]:
        """Обработка include директив"""
        if not isinstance(data, dict):
            return data

        # Обрабатываем include на верхнем уровне
        if "include" in data:
            included_data = self._process_include_directive(
                data["include"], current_file
            )
            # Удаляем include директиву
            data = {k: v for k, v in data.items() if k != "include"}
            # Мержим данные (текущие данные имеют приоритет)
            data = self._deep_merge(included_data, data)

        # Обрабатываем extends на верхнем уровне
        if "extends" in data:
            base_data = self._process_extends_directive(data["extends"], current_file)
            # Удаляем extends директиву
            data = {k: v for k, v in data.items() if k != "extends"}
            # Мержим данные (текущие данные переопределяют базовые)
            data = self._deep_merge(base_data, data)

        # Рекурсивно обрабатываем вложенные структуры
        for key, value in data.items():
            if isinstance(value, dict):
                data[key] = self.process_includes(value, current_file)
            elif isinstance(value, list):
                data[key] = [
                    self.process_includes(item, current_file)
                    if isinstance(item, dict)
                    else item
                    for item in value
                ]

        return data

    def _process_include_directive(
        self, include_spec: Union[str, List[str]], current_file: Optional[Path]
    ) -> Dict[str, Any]:
        """Обработка include директивы"""
        if isinstance(include_spec, str):
            include_files = [include_spec]
        else:
            include_files = include_spec

        result = {}

        for include_file in include_files:
            file_path = self._resolve_file_path(include_file, current_file)

            if file_path in self._loaded_files:
                logger.warning("Circular include detected", file=str(file_path))
                continue

            try:
                self._loaded_files.add(file_path)
                included_data = self._load_file(file_path)
                included_data = self.process_includes(included_data, file_path)
                result = self._deep_merge(result, included_data)
            finally:
                self._loaded_files.discard(file_path)

        return result

    def _process_extends_directive(
        self, extends_spec: str, current_file: Optional[Path]
    ) -> Dict[str, Any]:
        """Обработка extends директивы"""
        file_path = self._resolve_file_path(extends_spec, current_file)

        if file_path in self._loaded_files:
            raise YAMLLoaderError(f"Circular extends detected: {file_path}")

        try:
            self._loaded_files.add(file_path)
            base_data = self._load_file(file_path)
            return self.process_includes(base_data, file_path)
        finally:
            self._loaded_files.discard(file_path)

    def _resolve_file_path(self, file_spec: str, current_file: Optional[Path]) -> Path:
        """Разрешение пути к файлу"""
        # Проверяем на URL
        if self._is_url(file_spec):
            raise YAMLLoaderError("URL includes not yet supported")

        file_path = Path(file_spec)

        # Если путь не абсолютный, делаем его относительным к текущему файлу или base_path
        if not file_path.is_absolute():
            if current_file:
                file_path = current_file.parent / file_path
            else:
                file_path = self.base_path / file_path

        # Нормализуем путь
        file_path = file_path.resolve()

        if not file_path.exists():
            raise YAMLLoaderError(f"Include file not found: {file_path}")

        return file_path

    def _load_file(self, file_path: Path) -> Dict[str, Any]:
        """Загрузка файла"""
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                if file_path.suffix.lower() in [".yml", ".yaml"]:
                    return yaml.safe_load(f) or {}
                elif file_path.suffix.lower() == ".json":
                    return json.load(f)
                else:
                    raise YAMLLoaderError(
                        f"Unsupported file format: {file_path.suffix}"
                    )
        except Exception as e:
            raise YAMLLoaderError(f"Failed to load file {file_path}: {e}")

    @staticmethod
    def _is_url(spec: str) -> bool:
        """Проверка, является ли строка URL"""
        parsed = urlparse(spec)
        return parsed.scheme in ["http", "https", "ftp", "ftps"]

    @staticmethod
    def _deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
        """Глубокое слияние словарей"""
        result = base.copy()

        for key, value in override.items():
            if (
                key in result
                and isinstance(result[key], dict)
                and isinstance(value, dict)
            ):
                result[key] = TemplateProcessor._deep_merge(result[key], value)
            else:
                result[key] = value

        return result


class YAMLConfigLoader:
    """Основной загрузчик YAML конфигураций"""

    def __init__(
        self,
        base_path: Optional[Path] = None,
        enable_env_substitution: bool = True,
        enable_template_processing: bool = True,
    ):
        self.base_path = base_path or Path.cwd()
        self.enable_env_substitution = enable_env_substitution
        self.enable_template_processing = enable_template_processing

        self.validator = ConfigValidator()
        self.env_substitution = EnvironmentSubstitution()
        self.template_processor = TemplateProcessor(base_path)

    def load_pipeline_config(
        self,
        config_path: Union[str, Path],
        env_overrides: Optional[Dict[str, str]] = None,
        runtime_config: Optional[RuntimeConfig] = None,
    ) -> FullPipelineConfig:
        """Загрузка конфигурации pipeline"""

        config_path = Path(config_path)
        logger.info("Loading pipeline configuration", path=str(config_path))

        try:
            # 1. Загружаем базовый YAML
            raw_data = self._load_yaml_file(config_path)

            # 2. Обрабатываем шаблоны и include
            if self.enable_template_processing:
                raw_data = self.template_processor.process_includes(
                    raw_data, config_path
                )

            # 3. Подставляем переменные окружения
            if self.enable_env_substitution:
                raw_data = self.env_substitution.substitute(raw_data, env_overrides)

            # 4. Извлекаем секцию pipeline
            if "pipeline" not in raw_data:
                # Если нет секции pipeline, считаем что весь файл - это конфигурация pipeline
                pipeline_data = raw_data
            else:
                pipeline_data = raw_data["pipeline"]

            # 5. Валидируем конфигурацию pipeline
            pipeline_config = self.validator.validate_pipeline_config(pipeline_data)

            # 6. Создаем полную конфигурацию
            full_config_data = {
                "pipeline": pipeline_config.dict(),
                "runtime": runtime_config.dict() if runtime_config else None,
            }

            # Добавляем дополнительные секции из исходного файла
            for key, value in raw_data.items():
                if key not in ["pipeline", "runtime"]:
                    full_config_data[key] = value

            full_config = self.validator.validate_full_config(full_config_data)

            logger.info(
                "Pipeline configuration loaded successfully",
                pipeline_name=pipeline_config.name,
                stages_count=len(pipeline_config.stages),
            )

            return full_config

        except Exception as e:
            logger.error(
                "Failed to load pipeline configuration",
                path=str(config_path),
                error=str(e),
            )
            raise YAMLLoaderError(
                f"Failed to load configuration from {config_path}: {e}"
            )

    def load_from_dict(
        self, data: Dict[str, Any], env_overrides: Optional[Dict[str, str]] = None
    ) -> FullPipelineConfig:
        """Загрузка конфигурации из словаря"""

        logger.debug("Loading pipeline configuration from dict")

        try:
            # Обрабатываем подстановку переменных
            if self.enable_env_substitution:
                data = self.env_substitution.substitute(data, env_overrides)

            # Валидируем
            if "pipeline" in data:
                full_config = self.validator.validate_full_config(data)
            else:
                # Весь словарь - конфигурация pipeline
                pipeline_config = self.validator.validate_pipeline_config(data)
                full_config = FullPipelineConfig(pipeline=pipeline_config)

            return full_config

        except Exception as e:
            logger.error(
                "Failed to load pipeline configuration from dict", error=str(e)
            )
            raise YAMLLoaderError(f"Failed to load configuration from dict: {e}")

    def load_from_string(
        self, yaml_content: str, env_overrides: Optional[Dict[str, str]] = None
    ) -> FullPipelineConfig:
        """Загрузка конфигурации из строки"""

        logger.debug("Loading pipeline configuration from string")

        try:
            # Парсим YAML
            data = yaml.safe_load(yaml_content)
            if not data:
                raise YAMLLoaderError("Empty YAML content")

            return self.load_from_dict(data, env_overrides)

        except yaml.YAMLError as e:
            raise YAMLLoaderError(f"YAML parsing error: {e}")

    def validate_config_file(self, config_path: Union[str, Path]) -> List[str]:
        """Валидация файла конфигурации без загрузки"""

        errors = []

        try:
            self.load_pipeline_config(config_path)
        except YAMLLoaderError as e:
            errors.append(str(e))
        except Exception as e:
            errors.append(f"Unexpected error: {e}")

        return errors

    def _load_yaml_file(self, file_path: Path) -> Dict[str, Any]:
        """Загрузка YAML файла"""

        if not file_path.exists():
            raise YAMLLoaderError(f"Configuration file not found: {file_path}")

        try:
            with open(file_path, "r", encoding="utf-8") as f:
                data = yaml.safe_load(f)

            if not data:
                raise YAMLLoaderError("Empty configuration file")

            if not isinstance(data, dict):
                raise YAMLLoaderError("Configuration must be a YAML object/dict")

            return data

        except yaml.YAMLError as e:
            raise YAMLLoaderError(f"YAML parsing error in {file_path}: {e}")
        except Exception as e:
            raise YAMLLoaderError(f"Failed to read file {file_path}: {e}")


# Factory functions для удобства
def load_pipeline_config(config_path: Union[str, Path], **kwargs) -> FullPipelineConfig:
    """Быстрая загрузка конфигурации pipeline"""
    loader = YAMLConfigLoader()
    return loader.load_pipeline_config(config_path, **kwargs)


def load_config_from_dict(data: Dict[str, Any], **kwargs) -> FullPipelineConfig:
    """Быстрая загрузка из словаря"""
    loader = YAMLConfigLoader()
    return loader.load_from_dict(data, **kwargs)


def validate_config_file(config_path: Union[str, Path]) -> List[str]:
    """Быстрая валидация файла конфигурации"""
    loader = YAMLConfigLoader()
    return loader.validate_config_file(config_path)
