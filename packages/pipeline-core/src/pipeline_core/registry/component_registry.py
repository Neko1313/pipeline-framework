"""
Реестр компонентов pipeline с поддержкой автоматического обнаружения
"""

import importlib
import importlib.metadata
import logging
from typing import Dict, List, Type, Optional, Any
from dataclasses import dataclass

from ..models.component import (
    BaseComponent,
    ComponentMetadata,
    ComponentInfo,
    ExecutionContext,
    ComponentType,
)
from ..exceptions.errors import PipelineComponentError, PipelineRegistrationError


@dataclass
class PluginInfo:
    """Информация о плагине"""

    name: str
    entry_point: str
    module_name: str
    is_loaded: bool = False
    error: Optional[str] = None


class ComponentRegistry:
    """Реестр компонентов pipeline"""

    def __init__(self):
        self._components: Dict[str, ComponentInfo] = {}
        self._plugins: Dict[str, PluginInfo] = {}
        self.logger = logging.getLogger("pipeline.registry")

        # Автоматически загружаем компоненты при инициализации
        self._discover_components()

    def _discover_components(self) -> None:
        """Автоматическое обнаружение компонентов через entry points"""
        try:
            # Ищем entry points с группой 'pipeline.components'
            entry_points = importlib.metadata.entry_points(group="pipeline.components")

            for entry_point in entry_points:
                plugin_info = PluginInfo(
                    name=entry_point.name,
                    entry_point=str(entry_point),
                    module_name=entry_point.module,
                )

                self._plugins[entry_point.name] = plugin_info

                try:
                    # Пытаемся загрузить компонент
                    component_class = entry_point.load()
                    self._register_component(
                        entry_point.name, component_class, str(entry_point)
                    )
                    plugin_info.is_loaded = True

                    self.logger.info(f"Загружен компонент: {entry_point.name}")

                except Exception as e:
                    plugin_info.error = str(e)
                    self.logger.error(
                        f"Ошибка загрузки компонента {entry_point.name}: {e}"
                    )

        except Exception as e:
            self.logger.error(f"Ошибка обнаружения компонентов: {e}")

    def _register_component(
        self, name: str, component_class: Type[BaseComponent], entry_point: str
    ) -> None:
        """Регистрация компонента в реестре"""

        # Проверяем что класс наследуется от BaseComponent
        if not issubclass(component_class, BaseComponent):
            raise PipelineRegistrationError(
                f"Компонент {name} должен наследоваться от BaseComponent"
            )

        # Пытаемся создать экземпляр для получения метаданных
        try:
            # Создаем временный экземпляр с минимальной конфигурацией
            temp_config = {"type": name}
            temp_instance = component_class(temp_config)

            # Получаем метаданные
            metadata = self._extract_metadata(temp_instance, name)

        except Exception as e:
            # Если не можем создать экземпляр, создаем базовые метаданные
            self.logger.warning(
                f"Не удалось создать экземпляр {name} для извлечения метаданных: {e}"
            )
            metadata = ComponentMetadata(
                name=name,
                version="unknown",
                description="Описание недоступно",
                component_type="utility",  # Используем фиксированное значение
            )

        component_info = ComponentInfo(
            component_class=component_class, metadata=metadata, entry_point=entry_point
        )

        self._components[name] = component_info
        self.logger.debug(f"Зарегистрирован компонент: {name}")

    def _extract_metadata(
        self, component_instance: BaseComponent, name: str
    ) -> ComponentMetadata:
        """Извлечение метаданных из экземпляра компонента"""

        # Пытаемся получить метаданные разными способами
        if hasattr(component_instance, "get_metadata"):
            try:
                return component_instance.get_metadata()
            except Exception as e:
                self.logger.warning(f"Ошибка получения метаданных от {name}: {e}")

        # Извлекаем информацию из класса
        component_class = component_instance.__class__

        # Безопасное получение описания
        doc = getattr(component_class, "__doc__", None)
        description = doc.strip() if doc else f"Компонент {name}"

        return ComponentMetadata(
            name=name,
            version=getattr(component_class, "__version__", "1.0.0"),
            description=description,
            component_type=component_instance.get_component_type(),
            author=getattr(component_class, "__author__", None),
            tags=getattr(component_class, "__tags__", []),
            dependencies=getattr(component_class, "__dependencies__", []),
        )

    def register(self, name: str, component_class: Type[BaseComponent]) -> None:
        """
        Ручная регистрация компонента

        Args:
            name: Имя компонента
            component_class: Класс компонента
        """
        self._register_component(name, component_class, f"manual:{name}")
        self.logger.info(f"Вручную зарегистрирован компонент: {name}")

    def get_component_class(self, component_type: str) -> Type[BaseComponent]:
        """
        Получение класса компонента по типу

        Args:
            component_type: Тип компонента

        Returns:
            Type[BaseComponent]: Класс компонента

        Raises:
            PipelineComponentError: Если компонент не найден
        """
        if component_type not in self._components:
            available = ", ".join(self._components.keys())
            raise PipelineComponentError(
                f"Компонент типа '{component_type}' не найден. "
                f"Доступные типы: {available}"
            )

        return self._components[component_type].component_class

    def create_component(
        self, component_type: str, config: Dict[str, Any]
    ) -> BaseComponent:
        """
        Создание экземпляра компонента

        Args:
            component_type: Тип компонента
            config: Конфигурация компонента

        Returns:
            BaseComponent: Экземпляр компонента
        """
        component_class = self.get_component_class(component_type)

        try:
            # Добавляем тип в конфигурацию если его нет
            if "type" not in config:
                config = dict(config)
                config["type"] = component_type

            return component_class(config)

        except Exception as e:
            raise PipelineComponentError(
                f"Ошибка создания компонента '{component_type}': {e}"
            )

    def list_components(self) -> List[str]:
        """Получение списка доступных типов компонентов"""
        return list(self._components.keys())

    def get_component_info(self, component_type: str) -> Optional[ComponentInfo]:
        """Получение информации о компоненте"""
        return self._components.get(component_type)

    def get_component_metadata(
        self, component_type: str
    ) -> Optional[ComponentMetadata]:
        """Получение метаданных компонента"""
        info = self.get_component_info(component_type)
        return info.metadata if info else None

    def get_component_schema(self, component_type: str) -> Optional[Dict[str, Any]]:
        """Получение JSON схемы конфигурации компонента"""
        try:
            component_class = self.get_component_class(component_type)
            temp_instance = component_class({"type": component_type})
            return temp_instance.get_schema()
        except Exception as e:
            self.logger.error(f"Ошибка получения схемы для {component_type}: {e}")
            return None

    def validate_component_config(
        self, component_type: str, config: Dict[str, Any]
    ) -> List[str]:
        """
        Валидация конфигурации компонента

        Returns:
            List[str]: Список ошибок валидации (пустой если все ОК)
        """
        errors = []

        try:
            # Пытаемся создать компонент с данной конфигурацией
            self.create_component(component_type, config)
        except Exception as e:
            errors.append(f"Ошибка валидации конфигурации: {e}")

        return errors

    def get_plugins_info(self) -> Dict[str, PluginInfo]:
        """Получение информации о плагинах"""
        return self._plugins.copy()

    def reload_plugin(self, plugin_name: str) -> bool:
        """
        Перезагрузка плагина

        Args:
            plugin_name: Имя плагина

        Returns:
            bool: True если успешно перезагружен
        """
        if plugin_name not in self._plugins:
            self.logger.error(f"Плагин {plugin_name} не найден")
            return False

        plugin_info = self._plugins[plugin_name]

        try:
            # Удаляем из реестра
            if plugin_name in self._components:
                del self._components[plugin_name]

            # Перезагружаем модуль
            module = importlib.import_module(plugin_info.module_name)
            importlib.reload(module)

            # Загружаем заново через entry point
            entry_points = importlib.metadata.entry_points(group="pipeline.components")
            for entry_point in entry_points:
                if entry_point.name == plugin_name:
                    component_class = entry_point.load()
                    self._register_component(
                        plugin_name, component_class, str(entry_point)
                    )
                    plugin_info.is_loaded = True
                    plugin_info.error = None

                    self.logger.info(f"Плагин {plugin_name} успешно перезагружен")
                    return True

            self.logger.error(f"Entry point для плагина {plugin_name} не найден")
            return False

        except Exception as e:
            plugin_info.error = str(e)
            plugin_info.is_loaded = False
            self.logger.error(f"Ошибка перезагрузки плагина {plugin_name}: {e}")
            return False

    def health_check(self) -> Dict[str, Any]:
        """Проверка состояния реестра"""
        total_plugins = len(self._plugins)
        loaded_plugins = sum(1 for p in self._plugins.values() if p.is_loaded)
        failed_plugins = [
            name for name, p in self._plugins.items() if not p.is_loaded and p.error
        ]

        return {
            "total_components": len(self._components),
            "total_plugins": total_plugins,
            "loaded_plugins": loaded_plugins,
            "failed_plugins": failed_plugins,
            "component_types": list(self._components.keys()),
        }


# Глобальный экземпляр реестра
_global_registry: Optional[ComponentRegistry] = None


def get_registry() -> ComponentRegistry:
    """Получение глобального экземпляра реестра"""
    global _global_registry
    if _global_registry is None:
        _global_registry = ComponentRegistry()
    return _global_registry


def register_component(name: str):
    """
    Декоратор для регистрации компонента

    Usage:
        @register_component("my-component")
        class MyComponent(BaseComponent):
            ...
    """

    def decorator(component_class: Type[BaseComponent]):
        get_registry().register(name, component_class)
        return component_class

    return decorator
