# packages/pipeline-core/src/pipeline_core/registry/component_registry.py

"""
Централизованный реестр компонентов с автоматическим обнаружением

Поддерживает:
- Автоматическое обнаружение через entry points
- Регистрацию через декораторы
- Directory-based discovery
- Валидацию компонентов
- Управление версиями
- Hot reload в development режиме
"""

import importlib
import importlib.metadata
import importlib.util
import inspect
import pkgutil
import sys
import warnings
from datetime import datetime
from pathlib import Path
from typing import Dict, Type, Optional, List, Set, Any, Callable, Union
from dataclasses import dataclass
from enum import Enum
from threading import Lock

import structlog

# Безопасный импорт базовых компонентов
try:
    from pipeline_core.components.base import BaseComponent, ComponentType

    BASE_COMPONENTS_AVAILABLE = True
except ImportError as e:
    warnings.warn(f"Base components not available: {e}")
    BASE_COMPONENTS_AVAILABLE = False

    # Создаем заглушки
    class BaseComponent:
        component_type = "unknown"
        name = "unknown"

    class ComponentType:
        EXTRACTOR = "extractor"
        TRANSFORMER = "transformer"
        LOADER = "loader"
        VALIDATOR = "validator"
        STAGE = "stage"


logger = structlog.get_logger(__name__)


class DiscoverySource(Enum):
    """Источники обнаружения компонентов"""

    ENTRY_POINT = "entry_point"
    DECORATOR = "decorator"
    AUTO_DISCOVERY = "auto_discovery"
    MANUAL = "manual"
    DIRECTORY_SCAN = "directory_scan"


@dataclass
class ComponentInfo:
    """Информация о зарегистрированном компоненте"""

    component_class: Type[BaseComponent]
    component_type: str
    name: str
    version: str
    description: str
    discovery_source: DiscoverySource
    module_path: str
    registered_at: datetime
    metadata: Dict[str, Any]

    @property
    def full_name(self) -> str:
        """Полное имя компонента"""
        return f"{self.component_type}/{self.name}"

    def to_dict(self) -> Dict[str, Any]:
        """Конвертация в словарь"""
        return {
            "component_type": self.component_type,
            "name": self.name,
            "version": self.version,
            "description": self.description,
            "discovery_source": self.discovery_source.value,
            "module_path": self.module_path,
            "registered_at": self.registered_at.isoformat(),
            "metadata": self.metadata,
        }


class RegistryError(Exception):
    """Ошибка реестра компонентов"""

    pass


class ComponentRegistry:
    """
    Централизованный реестр компонентов pipeline framework

    Обеспечивает:
    - Автоматическое обнаружение компонентов
    - Регистрацию и валидацию
    - Получение компонентов по типу и имени
    - Hot reload в development режиме
    """

    _instance: Optional["ComponentRegistry"] = None
    _lock = Lock()

    def __new__(cls) -> "ComponentRegistry":
        """Singleton pattern"""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        """Инициализация реестра"""
        if hasattr(self, "_initialized"):
            return

        self._components: Dict[str, ComponentInfo] = {}
        self._discovery_hooks: List[Callable] = []
        self._auto_discovery_enabled = True
        self._development_mode = False

        self.logger = structlog.get_logger(component="component_registry")

        # Автоматическое обнаружение при инициализации
        if BASE_COMPONENTS_AVAILABLE:
            try:
                self.discover_components()
            except Exception as e:
                self.logger.warning("Failed to auto-discover components", error=str(e))

        self._initialized = True

    def register_component(
        self,
        component_class: Type[BaseComponent],
        component_type: Optional[str] = None,
        name: Optional[str] = None,
        version: str = "1.0.0",
        description: str = "",
        discovery_source: DiscoverySource = DiscoverySource.MANUAL,
        metadata: Optional[Dict[str, Any]] = None,
        override: bool = False,
    ) -> ComponentInfo:
        """
        Регистрация компонента в реестре

        Args:
            component_class: Класс компонента
            component_type: Тип компонента (если не указан, берется из класса)
            name: Имя компонента (если не указано, берется из класса)
            version: Версия компонента
            description: Описание компонента
            discovery_source: Источник обнаружения
            metadata: Дополнительные метаданные
            override: Перезаписать существующий компонент

        Returns:
            Информация о зарегистрированном компоненте

        Raises:
            RegistryError: При ошибках регистрации
        """
        # Валидация класса компонента
        if not BASE_COMPONENTS_AVAILABLE:
            # В режиме без базовых компонентов пропускаем валидацию
            pass
        elif not issubclass(component_class, BaseComponent):
            raise RegistryError(
                f"Component class must inherit from BaseComponent: {component_class}"
            )

        # Получаем тип и имя компонента
        if component_type is None:
            if hasattr(component_class, "component_type"):
                component_type = getattr(
                    component_class.component_type,
                    "value",
                    str(component_class.component_type),
                )
            else:
                raise RegistryError(
                    f"Component type not specified and not found in class: {component_class}"
                )

        if name is None:
            if hasattr(component_class, "name"):
                name = component_class.name
            else:
                # Генерируем имя из класса
                name = (
                    component_class.__name__.lower()
                    .replace("component", "")
                    .replace("extractor", "")
                    .replace("transformer", "")
                    .replace("loader", "")
                )

        full_name = f"{component_type}/{name}"

        # Проверяем на дублирование
        if full_name in self._components and not override:
            existing = self._components[full_name]
            raise RegistryError(
                f"Component {full_name} already registered from {existing.discovery_source.value}. "
                f"Use override=True to replace."
            )

        # Получаем модуль компонента
        module_path = getattr(component_class, "__module__", "unknown")

        # Получаем описание из docstring если не указано
        if not description and component_class.__doc__:
            description = component_class.__doc__.split("\n")[0].strip()

        # Создаем информацию о компоненте
        component_info = ComponentInfo(
            component_class=component_class,
            component_type=component_type,
            name=name,
            version=version,
            description=description,
            discovery_source=discovery_source,
            module_path=module_path,
            registered_at=datetime.now(),
            metadata=metadata or {},
        )

        # Регистрируем компонент
        self._components[full_name] = component_info

        self.logger.info(
            "Component registered",
            component_type=component_type,
            name=name,
            source=discovery_source.value,
            module=module_path,
        )

        return component_info

    def get_component(
        self, component_type: str, name: str
    ) -> Optional[Type[BaseComponent]]:
        """
        Получение класса компонента по типу и имени

        Args:
            component_type: Тип компонента
            name: Имя компонента

        Returns:
            Класс компонента или None если не найден
        """
        full_name = f"{component_type}/{name}"
        component_info = self._components.get(full_name)

        if component_info:
            return component_info.component_class

        return None

    def get_component_info(
        self, component_type: str, name: str
    ) -> Optional[ComponentInfo]:
        """
        Получение информации о компоненте

        Args:
            component_type: Тип компонента
            name: Имя компонента

        Returns:
            Информация о компоненте или None если не найден
        """
        full_name = f"{component_type}/{name}"
        return self._components.get(full_name)

    def list_components(
        self,
        component_type: Optional[str] = None,
        name_pattern: Optional[str] = None,
        discovery_source: Optional[DiscoverySource] = None,
    ) -> List[ComponentInfo]:
        """
        Получение списка зарегистрированных компонентов

        Args:
            component_type: Фильтр по типу компонента
            name_pattern: Паттерн для фильтрации по имени
            discovery_source: Фильтр по источнику обнаружения

        Returns:
            Список информации о компонентах
        """
        result = list(self._components.values())

        # Фильтрация по типу
        if component_type:
            result = [c for c in result if c.component_type == component_type]

        # Фильтрация по имени
        if name_pattern:
            import re

            pattern = re.compile(name_pattern, re.IGNORECASE)
            result = [c for c in result if pattern.search(c.name)]

        # Фильтрация по источнику
        if discovery_source:
            result = [c for c in result if c.discovery_source == discovery_source]

        # Сортировка по типу и имени
        result.sort(key=lambda x: (x.component_type, x.name))

        return result

    def discover_components(self) -> int:
        """
        Автоматическое обнаружение компонентов

        Returns:
            Количество обнаруженных компонентов
        """
        if not self._auto_discovery_enabled:
            return 0

        discovered_count = 0

        try:
            # 1. Entry points discovery
            discovered_count += self._discover_from_entry_points()

            # 2. Package scanning
            discovered_count += self._discover_from_packages()

            # 3. Выполняем дополнительные hooks
            for hook in self._discovery_hooks:
                try:
                    hook_count = hook(self)
                    if isinstance(hook_count, int):
                        discovered_count += hook_count
                except Exception as e:
                    self.logger.warning(
                        "Discovery hook failed", hook=hook.__name__, error=str(e)
                    )

            self.logger.info(
                "Component discovery completed", discovered_count=discovered_count
            )

        except Exception as e:
            self.logger.error("Component discovery failed", error=str(e))
            raise RegistryError(f"Component discovery failed: {e}") from e

        return discovered_count

    def _discover_from_entry_points(self) -> int:
        """Обнаружение компонентов через entry points"""
        discovered = 0

        try:
            # Ищем entry points с группой 'pipeline_framework.components'
            for entry_point in importlib.metadata.entry_points(
                group="pipeline_framework.components"
            ):
                try:
                    component_class = entry_point.load()

                    # Валидируем что это действительно компонент
                    if BASE_COMPONENTS_AVAILABLE and not issubclass(
                        component_class, BaseComponent
                    ):
                        self.logger.warning(
                            "Entry point is not a valid component",
                            entry_point=entry_point.name,
                            class_name=component_class.__name__,
                        )
                        continue

                    # Регистрируем компонент
                    self.register_component(
                        component_class=component_class,
                        discovery_source=DiscoverySource.ENTRY_POINT,
                        override=True,  # Entry points имеют приоритет
                    )
                    discovered += 1

                except Exception as e:
                    self.logger.warning(
                        "Failed to load component from entry point",
                        entry_point=entry_point.name,
                        error=str(e),
                    )

        except Exception as e:
            self.logger.debug("Entry points discovery failed", error=str(e))

        return discovered

    def _discover_from_packages(self) -> int:
        """Обнаружение компонентов путем сканирования пакетов"""
        discovered = 0

        # Список пакетов для сканирования
        packages_to_scan = [
            "extractor_sql",
            "transformer_pandas",
            "loader_sql",
            "pipeline_core.components",
        ]

        for package_name in packages_to_scan:
            try:
                discovered += self._scan_package(package_name)
            except ImportError:
                self.logger.debug(f"Package {package_name} not available for scanning")
            except Exception as e:
                self.logger.warning(
                    f"Failed to scan package {package_name}", error=str(e)
                )

        return discovered

    def _scan_package(self, package_name: str) -> int:
        """Сканирование конкретного пакета"""
        discovered = 0

        try:
            package = importlib.import_module(package_name)

            # Если пакет имеет __path__, сканируем подмодули
            if hasattr(package, "__path__"):
                for importer, modname, ispkg in pkgutil.iter_modules(
                    package.__path__, package_name + "."
                ):
                    try:
                        module = importlib.import_module(modname)
                        discovered += self._scan_module(module)
                    except Exception as e:
                        self.logger.debug(
                            f"Failed to scan module {modname}", error=str(e)
                        )
            else:
                discovered += self._scan_module(package)

        except ImportError:
            self.logger.debug(f"Package {package_name} not found")

        return discovered

    def _scan_module(self, module) -> int:
        """Сканирование модуля на наличие компонентов"""
        discovered = 0

        for name in dir(module):
            obj = getattr(module, name)

            # Проверяем что это класс
            if not inspect.isclass(obj):
                continue

            # Пропускаем базовые классы и импорты из других модулей
            if obj.__module__ != module.__name__:
                continue

            # Проверяем наследование от BaseComponent
            if BASE_COMPONENTS_AVAILABLE:
                try:
                    if issubclass(obj, BaseComponent) and obj is not BaseComponent:
                        # Регистрируем компонент
                        self.register_component(
                            component_class=obj,
                            discovery_source=DiscoverySource.AUTO_DISCOVERY,
                            override=False,  # Не перезаписываем уже зарегистрированные
                        )
                        discovered += 1
                except (TypeError, RegistryError):
                    # TypeError - если obj не является классом
                    # RegistryError - если компонент уже зарегистрирован
                    pass

        return discovered

    def add_discovery_hook(self, hook: Callable[["ComponentRegistry"], int]) -> None:
        """
        Добавление hook'а для дополнительного обнаружения компонентов

        Args:
            hook: Функция, принимающая registry и возвращающая количество найденных компонентов
        """
        self._discovery_hooks.append(hook)

    def unregister_component(self, component_type: str, name: str) -> bool:
        """
        Удаление компонента из реестра

        Args:
            component_type: Тип компонента
            name: Имя компонента

        Returns:
            True если компонент был удален, False если не найден
        """
        full_name = f"{component_type}/{name}"

        if full_name in self._components:
            component_info = self._components.pop(full_name)
            self.logger.info(
                "Component unregistered",
                component_type=component_type,
                name=name,
                source=component_info.discovery_source.value,
            )
            return True

        return False

    def clear_registry(self) -> None:
        """Очистка всего реестра"""
        count = len(self._components)
        self._components.clear()
        self.logger.info("Registry cleared", components_removed=count)

    def set_development_mode(self, enabled: bool) -> None:
        """Включение/выключение режима разработки"""
        self._development_mode = enabled
        if enabled:
            self.logger.info("Development mode enabled - hot reload available")

    def reload_component(self, component_type: str, name: str) -> bool:
        """
        Перезагрузка компонента в development режиме

        Args:
            component_type: Тип компонента
            name: Имя компонента

        Returns:
            True если компонент был перезагружен
        """
        if not self._development_mode:
            raise RegistryError("Component reload only available in development mode")

        component_info = self.get_component_info(component_type, name)
        if not component_info:
            return False

        try:
            # Перезагружаем модуль
            module = sys.modules.get(component_info.module_path)
            if module:
                importlib.reload(module)

            # Повторно сканируем модуль
            self._scan_module(module)

            self.logger.info(
                "Component reloaded",
                component_type=component_type,
                name=name,
                module=component_info.module_path,
            )
            return True

        except Exception as e:
            self.logger.error(
                "Component reload failed",
                component_type=component_type,
                name=name,
                error=str(e),
            )
            return False

    def validate_registry(self) -> Dict[str, Any]:
        """
        Валидация реестра компонентов

        Returns:
            Результат валидации
        """
        issues = []
        warnings_list = []

        for full_name, component_info in self._components.items():
            # Проверяем доступность класса
            try:
                # Пытаемся создать экземпляр (если есть конструктор по умолчанию)
                if BASE_COMPONENTS_AVAILABLE:
                    component_class = component_info.component_class
                    # Базовая проверка что класс можно импортировать
                    module = sys.modules.get(component_info.module_path)
                    if not module:
                        warnings_list.append(
                            f"Module {component_info.module_path} not loaded for {full_name}"
                        )

            except Exception as e:
                issues.append(f"Component {full_name} validation failed: {e}")

        return {
            "valid": len(issues) == 0,
            "total_components": len(self._components),
            "issues": issues,
            "warnings": warnings_list,
            "components_by_type": self._get_components_by_type(),
            "discovery_sources": self._get_discovery_sources(),
        }

    def _get_components_by_type(self) -> Dict[str, int]:
        """Подсчет компонентов по типам"""
        counts = {}
        for component_info in self._components.values():
            component_type = component_info.component_type
            counts[component_type] = counts.get(component_type, 0) + 1
        return counts

    def _get_discovery_sources(self) -> Dict[str, int]:
        """Подсчет компонентов по источникам обнаружения"""
        counts = {}
        for component_info in self._components.values():
            source = component_info.discovery_source.value
            counts[source] = counts.get(source, 0) + 1
        return counts

    def export_registry(self) -> Dict[str, Any]:
        """Экспорт реестра в словарь"""
        return {
            "components": {
                full_name: component_info.to_dict()
                for full_name, component_info in self._components.items()
            },
            "statistics": {
                "total_components": len(self._components),
                "components_by_type": self._get_components_by_type(),
                "discovery_sources": self._get_discovery_sources(),
            },
            "exported_at": datetime.now().isoformat(),
        }


# Декоратор для регистрации компонентов
def register_component(
    component_type: Optional[str] = None,
    name: Optional[str] = None,
    version: str = "1.0.0",
    description: str = "",
    metadata: Optional[Dict[str, Any]] = None,
):
    """
    Декоратор для автоматической регистрации компонентов

    Args:
        component_type: Тип компонента
        name: Имя компонента
        version: Версия
        description: Описание
        metadata: Метаданные

    Example:
        @register_component("extractor", "my_extractor", "1.0.0", "My custom extractor")
        class MyExtractor(BaseExtractor):
            pass
    """

    def decorator(cls: Type[BaseComponent]) -> Type[BaseComponent]:
        # Регистрируем компонент при импорте класса
        try:
            registry = ComponentRegistry()
            registry.register_component(
                component_class=cls,
                component_type=component_type,
                name=name,
                version=version,
                description=description,
                discovery_source=DiscoverySource.DECORATOR,
                metadata=metadata,
                override=True,
            )
        except Exception as e:
            logger.warning(f"Failed to register component {cls.__name__}", error=str(e))

        return cls

    return decorator


# Глобальный экземпляр реестра
_global_registry: Optional[ComponentRegistry] = None


def get_global_registry() -> ComponentRegistry:
    """Получение глобального экземпляра реестра"""
    global _global_registry
    if _global_registry is None:
        _global_registry = ComponentRegistry()
    return _global_registry


# Утилитарные функции
def list_available_components(
    component_type: Optional[str] = None,
) -> List[ComponentInfo]:
    """Получение списка доступных компонентов"""
    registry = get_global_registry()
    return registry.list_components(component_type=component_type)


def get_component_class(
    component_type: str, name: str
) -> Optional[Type[BaseComponent]]:
    """Получение класса компонента"""
    registry = get_global_registry()
    return registry.get_component(component_type, name)


def discover_all_components() -> int:
    """Запуск автоматического обнаружения компонентов"""
    registry = get_global_registry()
    return registry.discover_components()
