"""
Модели для описания pipeline конфигурации
"""

from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field, field_validator, model_validator
from datetime import datetime
import re


class StageConfig(BaseModel):
    """Конфигурация отдельной стадии"""

    type: str = Field(..., description="Тип компонента стадии")
    config: Dict[str, Any] = Field(
        default_factory=dict, description="Конфигурация компонента"
    )
    depends_on: List[str] = Field(
        default_factory=list, description="Зависимости стадии"
    )
    condition: Optional[str] = Field(None, description="Условие выполнения стадии")
    parallel: bool = Field(False, description="Можно ли выполнять параллельно")

    @field_validator("type")
    @classmethod
    def validate_type(cls, v: str) -> str:
        if not v or not isinstance(v, str):
            raise ValueError("Тип стадии должен быть непустой строкой")
        return v.strip()

    @field_validator("depends_on")
    @classmethod
    def validate_dependencies(cls, v: List[str]) -> List[str]:
        if not isinstance(v, list):
            return []
        return [dep.strip() for dep in v if isinstance(dep, str) and dep.strip()]


class PipelineMetadata(BaseModel):
    """Метаданные pipeline"""

    name: str = Field(..., description="Имя pipeline")
    description: Optional[str] = Field(None, description="Описание pipeline")
    version: str = Field("1.0.0", description="Версия pipeline")
    author: Optional[str] = Field(None, description="Автор pipeline")
    tags: List[str] = Field(default_factory=list, description="Теги pipeline")
    created_at: Optional[datetime] = Field(None, description="Дата создания")

    @field_validator("name")
    @classmethod
    def validate_name(cls, v: str) -> str:
        if not v or not isinstance(v, str):
            raise ValueError("Имя pipeline обязательно")
        # Проверяем что имя содержит только допустимые символы
        if not re.match(r"^[a-zA-Z0-9_-]+$", v):
            raise ValueError("Имя pipeline может содержать только буквы, цифры, _ и -")
        return v.strip()

    @field_validator("version")
    @classmethod
    def validate_version(cls, v: str) -> str:
        # Простая проверка семантической версии
        if not re.match(r"^\d+\.\d+\.\d+$", v):
            raise ValueError(
                "Версия должна быть в формате семантического версионирования (x.y.z)"
            )
        return v


class ScheduleConfig(BaseModel):
    """Конфигурация расписания pipeline"""

    cron: Optional[str] = Field(None, description="Cron выражение для расписания")
    timezone: str = Field("UTC", description="Часовой пояс")
    enabled: bool = Field(True, description="Включено ли расписание")

    @field_validator("cron")
    @classmethod
    def validate_cron(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return v
        # Базовая проверка cron выражения (5 или 6 полей)
        fields = v.strip().split()
        if len(fields) not in [5, 6]:
            raise ValueError("Cron выражение должно содержать 5 или 6 полей")
        return v.strip()


class ResourceLimits(BaseModel):
    """Ограничения ресурсов для pipeline"""

    cpu_limit: Optional[float] = Field(None, description="Лимит CPU")
    memory_limit: Optional[str] = Field(
        None, description="Лимит памяти (например: 2Gi)"
    )
    timeout: int = Field(3600, description="Общий таймаут pipeline в секундах")
    max_parallel_stages: int = Field(
        10, description="Максимальное количество параллельных стадий"
    )

    @field_validator("timeout")
    @classmethod
    def validate_timeout(cls, v: int) -> int:
        if v <= 0:
            raise ValueError("Таймаут должен быть положительным числом")
        return v

    @field_validator("max_parallel_stages")
    @classmethod
    def validate_max_parallel(cls, v: int) -> int:
        if v <= 0:
            raise ValueError(
                "Максимальное количество параллельных стадий должно быть положительным"
            )
        return v


class PipelineConfig(BaseModel):
    """Полная конфигурация pipeline"""

    metadata: PipelineMetadata
    variables: Dict[str, Any] = Field(
        default_factory=dict, description="Глобальные переменные"
    )
    schedule: Optional[ScheduleConfig] = Field(
        None, description="Конфигурация расписания"
    )
    resources: ResourceLimits = Field(
        default_factory=ResourceLimits, description="Ограничения ресурсов"
    )
    stages: Dict[str, StageConfig] = Field(..., description="Конфигурация стадий")

    @field_validator("stages")
    @classmethod
    def validate_stages(cls, v: Dict[str, StageConfig]) -> Dict[str, StageConfig]:
        if not v:
            raise ValueError("Pipeline должен содержать хотя бы одну стадию")

        # Проверяем что все зависимости существуют
        stage_names = set(v.keys())
        for stage_name, stage_config in v.items():
            for dep in stage_config.depends_on:
                if dep not in stage_names:
                    raise ValueError(
                        f"Стадия '{stage_name}' зависит от несуществующей стадии '{dep}'"
                    )

        return v

    @model_validator(mode="after")
    def validate_no_circular_dependencies(self) -> "PipelineConfig":
        """Проверка на циклические зависимости"""
        stages = self.stages
        if not stages:
            return self

        # Алгоритм обнаружения циклов через DFS
        def has_cycle(graph: Dict[str, List[str]]) -> bool:
            WHITE, GRAY, BLACK = 0, 1, 2
            color = {node: WHITE for node in graph}

            def dfs(node: str) -> bool:
                if color[node] == GRAY:
                    return True  # Найден цикл
                if color[node] == BLACK:
                    return False

                color[node] = GRAY
                for neighbor in graph.get(node, []):
                    if dfs(neighbor):
                        return True
                color[node] = BLACK
                return False

            for node in graph:
                if color[node] == WHITE:
                    if dfs(node):
                        return True
            return False

        # Строим граф зависимостей
        dependency_graph: Dict[str, List[str]] = {}
        for stage_name, stage_config in stages.items():
            dependency_graph[stage_name] = stage_config.depends_on

        if has_cycle(dependency_graph):
            raise ValueError("Обнаружены циклические зависимости между стадиями")

        return self

    def get_execution_order(self) -> List[List[str]]:
        """
        Получить порядок выполнения стадий с учетом зависимостей
        Возвращает список уровней, где каждый уровень содержит стадии,
        которые могут выполняться параллельно
        """
        stages = self.stages
        in_degree = {stage: 0 for stage in stages}

        # Подсчитываем входящие ребра (зависимости)
        for stage_config in stages.values():
            for dep in stage_config.depends_on:
                in_degree[dep] = in_degree.get(dep, 0)

        for stage_name, stage_config in stages.items():
            for dep in stage_config.depends_on:
                in_degree[stage_name] += 1

        # Топологическая сортировка по уровням
        levels: List[List[str]] = []
        remaining = set(stages.keys())

        while remaining:
            # Найти стадии без зависимостей на текущем уровне
            current_level = []
            for stage in list(remaining):
                if in_degree[stage] == 0:
                    current_level.append(stage)
                    remaining.remove(stage)

            if not current_level:
                raise ValueError("Невозможно определить порядок выполнения стадий")

            levels.append(current_level)

            # Обновить счетчики зависимостей
            for stage in current_level:
                for other_stage, other_config in stages.items():
                    if stage in other_config.depends_on:
                        in_degree[other_stage] -= 1

        return levels

    def get_stage_dependencies(self, stage_name: str) -> List[str]:
        """Получить все зависимости стадии (включая транзитивные)"""
        if stage_name not in self.stages:
            return []

        visited = set()
        dependencies = []

        def collect_deps(current_stage: str) -> None:
            if current_stage in visited:
                return
            visited.add(current_stage)

            stage_config = self.stages.get(current_stage)
            if stage_config:
                for dep in stage_config.depends_on:
                    dependencies.append(dep)
                    collect_deps(dep)

        collect_deps(stage_name)
        return list(set(dependencies))  # Убираем дубли


class PipelineExecution(BaseModel):
    """Информация о выполнении pipeline"""

    execution_id: str
    pipeline_config: PipelineConfig
    status: str
    started_at: datetime
    finished_at: Optional[datetime] = None
    stage_executions: Dict[str, Dict[str, Any]] = Field(default_factory=dict)

    def get_duration(self) -> Optional[float]:
        """Получить длительность выполнения в секундах"""
        if self.finished_at and self.started_at:
            return (self.finished_at - self.started_at).total_seconds()
        return None
