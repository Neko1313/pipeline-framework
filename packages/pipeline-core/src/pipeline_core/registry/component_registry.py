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
from datetime import datetime
from pathlib import Path
from typing import Dict, Type, Optional, List, Set, Any, Callable
from dataclasses import dataclass
from enum import Enum

import structlog

from pipeline_core.components import BaseComponent, ComponentType

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
    component_type: ComponentType
    name: str
    version: str
    description: str
    source: DiscoverySource
    module_path: str
    dependencies: List[str]
    input_schema: Optional[Dict[str, Any]] = None
    output_schema: Optional[Dict[str, Any]] = None

    @property
    def key(self) -> str:
        """Уникальный ключ компонента"""
        return f"{self.component_type.value}:{self.name}"

    def to_dict(self) -> Dict[str, Any]:
        """Сериализация в словарь"""
        return {
            "type": self.component_type.value,
            "name": self.name,
            "version": self.version,
            "description": self.description,
            "source": self.source.value,
            "module_path": self.module_path,
            "dependencies": self.dependencies,
            "input_schema": self.input_schema,
            "output_schema": self.output_schema,
        }


class ComponentRegistry:
    """
    Централизованный реестр компонентов с автоматическим обнаружением

    Singleton pattern - один экземпляр на приложение
    """

    _instance: Optional["ComponentRegistry"] = None
    _components: Dict[str, ComponentInfo] = {}
    _discovery_paths: Set[Path] = set()
    _hooks: Dict[str, List[Callable]] = {"register": [], "unregister": []}

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        if not hasattr(self, "_initialized"):
            logger.info("Initializing ComponentRegistry")
            self._discover_all_components()
            self._initialized = True
            logger.info(
                "ComponentRegistry initialized", total_components=len(self._components)
            )

    # === Discovery methods ===

    def _discover_all_components(self):
        """Полное обнаружение всех компонентов"""

        # 1. Entry points discovery
        self._discover_via_entry_points()

        # 2. Workspace packages discovery
        self._discover_workspace_packages()

        # 3. Directory-based discovery
        self._discover_from_directories()

        # 4. Current module discovery
        self._discover_current_modules()

    def _discover_via_entry_points(self):
        """Обнаружение через entry points"""
        try:
            # Ищем entry points для компонентов
            entry_point_groups = [
                "pipeline_framework.components",
                "pipeline_framework.extractors",
                "pipeline_framework.transformers",
                "pipeline_framework.loaders",
                "pipeline_framework.validators",
            ]

            for group in entry_point_groups:
                try:
                    for entry_point in importlib.metadata.entry_points(group=group):
                        try:
                            component_class = entry_point.load()
                            self._register_component_class(
                                component_class, source=DiscoverySource.ENTRY_POINT
                            )
                            logger.debug(
                                "Loaded component from entry point",
                                component=entry_point.name,
                                group=group,
                            )
                        except Exception as e:
                            logger.warning(
                                "Failed to load component from entry point",
                                entry_point=entry_point.name,
                                error=str(e),
                            )
                except Exception:
                    # Entry point group может не существовать
                    continue

        except Exception as e:
            logger.warning("Entry points discovery failed", error=str(e))

    def _discover_workspace_packages(self):
        """Обнаружение компонентов в workspace packages"""

        # Известные пакеты workspace
        workspace_packages = ["extractor_sql", "stages", "loaders_sql", "polars"]

        for package_name in workspace_packages:
            try:
                if package_name in sys.modules:
                    module = sys.modules[package_name]
                else:
                    try:
                        module = importlib.import_module(package_name)
                    except ImportError:
                        logger.debug(
                            "Workspace package not found", package=package_name
                        )
                        continue

                self._scan_module_for_components(module, DiscoverySource.AUTO_DISCOVERY)
                logger.debug("Scanned workspace package", package=package_name)

            except Exception as e:
                logger.warning(
                    "Failed to scan workspace package",
                    package=package_name,
                    error=str(e),
                )

    def _discover_from_directories(self):
        """Обнаружение компонентов в директориях"""

        # Добавляем стандартные пути для поиска
        current_dir = Path(__file__).parent.parent
        potential_paths = [
            current_dir / "components",
            current_dir.parent / "components",
            Path.cwd() / "packages" / "components",
            Path.cwd() / "components",
        ]

        for path in potential_paths:
            if path.exists():
                self._discovery_paths.add(path)
                self._scan_directory_for_components(path)

    def _discover_current_modules(self):
        """Обнаружение компонентов в уже загруженных модулях"""

        for module_name, module in sys.modules.items():
            if module and hasattr(module, "__file__"):
                # Сканируем только модули, которые выглядят как наши
                if any(
                    keyword in module_name.lower()
                    for keyword in [
                        "pipeline",
                        "extractor",
                        "transformer",
                        "loader",
                        "component",
                    ]
                ):
                    self._scan_module_for_components(
                        module, DiscoverySource.AUTO_DISCOVERY
                    )

    def _scan_directory_for_components(self, directory: Path):
        """Сканирование директории на предмет компонентов"""

        logger.debug("Scanning directory for components", path=str(directory))

        for py_file in directory.rglob("*.py"):
            if py_file.name.startswith("__"):
                continue

            try:
                # Загружаем модуль из файла
                spec = importlib.util.spec_from_file_location(
                    f"dynamic_{py_file.stem}", py_file
                )
                if spec and spec.loader:
                    module = importlib.util.module_from_spec(spec)
                    spec.loader.exec_module(module)

                    self._scan_module_for_components(
                        module, DiscoverySource.DIRECTORY_SCAN
                    )

            except Exception as e:
                logger.debug(
                    "Failed to load module from file", file=str(py_file), error=str(e)
                )

    def _scan_module_for_components(self, module, source: DiscoverySource):
        """Сканирование модуля на предмет компонентов"""

        if not module:
            return

        for name, obj in inspect.getmembers(module):
            if (
                inspect.isclass(obj)
                and issubclass(obj, BaseComponent)
                and obj != BaseComponent
                and not inspect.isabstract(obj)
            ):
                self._register_component_class(obj, source=source)

    # === Registration methods ===

    def _register_component_class(
        self,
        component_class: Type[BaseComponent],
        source: DiscoverySource = DiscoverySource.MANUAL,
    ) -> bool:
        """Регистрация класса компонента"""

        if not self._validate_component_class(component_class):
            return False

        try:
            # Получаем информацию о компоненте
            component_info = self._create_component_info(component_class, source)

            # Проверяем на дубликаты
            if component_info.key in self._components:
                existing = self._components[component_info.key]
                logger.debug(
                    "Component already registered",
                    key=component_info.key,
                    existing_source=existing.source.value,
                    new_source=source.value,
                )
                return False

            # Регистрируем компонент
            self._components[component_info.key] = component_info

            logger.debug(
                "Component registered successfully",
                key=component_info.key,
                source=source.value,
                module=component_info.module_path,
            )

            # Вызываем hooks
            self._call_hooks("register", component_info)

            return True

        except Exception as e:
            logger.error(
                "Failed to register component",
                component=component_class.__name__,
                error=str(e),
            )
            return False

    def _create_component_info(
        self, component_class: Type[BaseComponent], source: DiscoverySource
    ) -> ComponentInfo:
        """Создание информации о компоненте"""

        # Создаем временный экземпляр для получения метаданных
        # (если у класса нет property без инициализации)
        try:
            temp_instance = component_class({})
            component_type = temp_instance.component_type
            name = temp_instance.name
            version = temp_instance.version
            description = temp_instance.description
            dependencies = temp_instance.dependencies
            input_schema = temp_instance.input_schema
            output_schema = temp_instance.output_schema
        except Exception:
            # Fallback к class attributes
            component_type = getattr(
                component_class, "component_type", ComponentType.STAGE
            )
            name = getattr(component_class, "name", component_class.__name__.lower())
            version = getattr(component_class, "version", "1.0.0")
            description = getattr(
                component_class, "description", component_class.__doc__ or ""
            )
            dependencies = getattr(component_class, "dependencies", [])
            input_schema = getattr(component_class, "input_schema", None)
            output_schema = getattr(component_class, "output_schema", None)

        return ComponentInfo(
            component_class=component_class,
            component_type=component_type,
            name=name,
            version=version,
            description=description.strip(),
            source=source,
            module_path=component_class.__module__,
            dependencies=dependencies,
            input_schema=input_schema,
            output_schema=output_schema,
        )

    def _validate_component_class(self, component_class: Type[BaseComponent]) -> bool:
        """Валидация класса компонента"""

        try:
            # Проверяем, что это подкласс BaseComponent
            if not issubclass(component_class, BaseComponent):
                return False

            # Проверяем, что это не абстрактный класс
            if inspect.isabstract(component_class):
                return False

            # Проверяем наличие обязательных атрибутов/методов
            required_attrs = ["component_type", "name"]
            for attr in required_attrs:
                if not hasattr(component_class, attr):
                    logger.warning(
                        "Component missing required attribute",
                        component=component_class.__name__,
                        attribute=attr,
                    )
                    return False

            return True

        except Exception as e:
            logger.warning(
                "Component validation failed",
                component=component_class.__name__,
                error=str(e),
            )
            return False

    # === Public API methods ===

    @classmethod
    def register(cls, component_type: Optional[str] = None):
        """Декоратор для регистрации компонентов"""

        def decorator(component_class):
            registry = cls()
            registry._register_component_class(
                component_class, source=DiscoverySource.DECORATOR
            )
            return component_class

        return decorator

    def register_component(self, component_class: Type[BaseComponent]) -> bool:
        """Ручная регистрация компонента"""
        return self._register_component_class(
            component_class, source=DiscoverySource.MANUAL
        )

    def unregister_component(self, component_type: str, name: str) -> bool:
        """Отмена регистрации компонента"""
        key = f"{component_type}:{name}"

        if key in self._components:
            component_info = self._components.pop(key)
            logger.info("Component unregistered", key=key)

            # Вызываем hooks
            self._call_hooks("unregister", component_info)
            return True

        return False

    def get_component(
        self, component_type: str, name: str
    ) -> Optional[Type[BaseComponent]]:
        """Получение класса компонента по типу и имени"""
        key = f"{component_type}:{name}"
        component_info = self._components.get(key)
        return component_info.component_class if component_info else None

    def get_component_info(
        self, component_type: str, name: str
    ) -> Optional[ComponentInfo]:
        """Получение информации о компоненте"""
        key = f"{component_type}:{name}"
        return self._components.get(key)

    def list_components(
        self, component_type: Optional[str] = None
    ) -> List[ComponentInfo]:
        """Получение списка зарегистрированных компонентов"""
        components = list(self._components.values())

        if component_type:
            components = [
                c for c in components if c.component_type.value == component_type
            ]

        # Сортируем по типу, затем по имени
        return sorted(components, key=lambda c: (c.component_type.value, c.name))

    def list_component_types(self) -> List[str]:
        """Получение списка типов компонентов"""
        types = {info.component_type.value for info in self._components.values()}
        return sorted(list(types))

    def search_components(self, query: str) -> List[ComponentInfo]:
        """Поиск компонентов по запросу"""
        query_lower = query.lower()
        results = []

        for component_info in self._components.values():
            # Поиск в имени, описании, типе
            searchable_text = " ".join(
                [
                    component_info.name,
                    component_info.description,
                    component_info.component_type.value,
                    component_info.module_path,
                ]
            ).lower()

            if query_lower in searchable_text:
                results.append(component_info)

        return results

    def reload_component(self, component_type: str, name: str) -> bool:
        """Перезагрузка компонента (для development)"""
        key = f"{component_type}:{name}"
        component_info = self._components.get(key)

        if not component_info:
            return False

        try:
            # Перезагружаем модуль
            module = importlib.reload(sys.modules[component_info.module_path])

            # Удаляем старую регистрацию
            self.unregister_component(component_type, name)

            # Сканируем модуль заново
            self._scan_module_for_components(module, DiscoverySource.AUTO_DISCOVERY)

            logger.info("Component reloaded", key=key)
            return True

        except Exception as e:
            logger.error("Failed to reload component", key=key, error=str(e))
            return False

    def add_discovery_path(self, path: Path):
        """Добавление пути для поиска компонентов"""
        if path.exists():
            self._discovery_paths.add(path)
            self._scan_directory_for_components(path)
            logger.info("Added discovery path", path=str(path))

    def rescan_all(self):
        """Повторное сканирование всех источников"""
        logger.info("Rescanning all component sources")
        old_count = len(self._components)

        # Очищаем текущие компоненты (кроме manually registered)
        manual_components = {
            k: v
            for k, v in self._components.items()
            if v.source == DiscoverySource.MANUAL
        }
        self._components = manual_components

        # Заново обнаруживаем
        self._discover_all_components()

        new_count = len(self._components)
        logger.info("Rescan completed", old_count=old_count, new_count=new_count)

    # === Hooks system ===

    def add_hook(self, event: str, callback: Callable):
        """Добавление hook для событий регистрации"""
        if event in self._hooks:
            self._hooks[event].append(callback)

    def remove_hook(self, event: str, callback: Callable):
        """Удаление hook"""
        if event in self._hooks and callback in self._hooks[event]:
            self._hooks[event].remove(callback)

    def _call_hooks(self, event: str, component_info: ComponentInfo):
        """Вызов hooks для события"""
        for callback in self._hooks.get(event, []):
            try:
                callback(component_info)
            except Exception as e:
                logger.warning("Hook execution failed", event=event, error=str(e))

    # === Statistics and debugging ===

    def get_stats(self) -> Dict[str, Any]:
        """Получение статистики реестра"""
        stats = {
            "total_components": len(self._components),
            "by_type": {},
            "by_source": {},
            "discovery_paths": [str(p) for p in self._discovery_paths],
        }

        for component_info in self._components.values():
            # По типам
            type_name = component_info.component_type.value
            stats["by_type"][type_name] = stats["by_type"].get(type_name, 0) + 1

            # По источникам
            source_name = component_info.source.value
            stats["by_source"][source_name] = stats["by_source"].get(source_name, 0) + 1

        return stats

    def validate_all_components(self) -> Dict[str, List[str]]:
        """Валидация всех зарегистрированных компонентов"""
        results = {"valid": [], "invalid": []}

        for key, component_info in self._components.items():
            try:
                # Попытка создания экземпляра
                component_info.component_class({})
                results["valid"].append(key)
            except Exception as e:
                results["invalid"].append(f"{key}: {str(e)}")

        return results

    def export_registry(self) -> Dict[str, Any]:
        """Экспорт реестра в формате JSON"""
        return {
            "components": {
                key: info.to_dict() for key, info in self._components.items()
            },
            "stats": self.get_stats(),
            "timestamp": str(datetime.now()),
        }
