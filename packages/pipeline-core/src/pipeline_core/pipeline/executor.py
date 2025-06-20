"""
Pipeline Executor - центральный orchestrator для выполнения data workflows

Обеспечивает:
- Построение и выполнение dependency graph
- Параллельное выполнение независимых этапов
- Error handling и retry логику
- Checkpoint/resume функциональность
- Temporal integration
- Quality checks выполнение
- Monitoring и metrics
"""

import asyncio
import uuid
from datetime import datetime, timezone
from typing import Dict, List, Optional, Set, Any, Tuple
from dataclasses import dataclass, field
from enum import Enum

import structlog
from prometheus_client import Counter, Histogram, Gauge

from pipeline_core.components import (
    BaseComponent,
    ExecutionContext,
    ExecutionResult,
    ExecutionStatus,
    ExecutionMetadata,
)
from pipeline_core.config import (
    PipelineConfig,
    StageConfig,
    QualityCheck,
    RuntimeConfig,
)
from pipeline_core.registry import ComponentRegistry
from pipeline_core.observability import MetricsCollector

logger = structlog.get_logger(__name__)


class PipelineStatus(Enum):
    """Статусы выполнения pipeline"""

    PENDING = "pending"
    INITIALIZING = "initializing"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    CANCELLED = "cancelled"
    PAUSED = "paused"


class StageStatus(Enum):
    """Статусы выполнения этапа"""

    PENDING = "pending"
    READY = "ready"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"
    CANCELLED = "cancelled"


@dataclass
class PipelineExecutionMetrics:
    """Метрики выполнения pipeline"""

    total_stages: int = 0
    completed_stages: int = 0
    failed_stages: int = 0
    skipped_stages: int = 0

    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    duration_seconds: Optional[float] = None

    total_rows_processed: int = 0
    total_bytes_processed: int = 0
    peak_memory_mb: Optional[float] = None

    retry_count: int = 0
    checkpoint_count: int = 0

    def calculate_duration(self):
        if self.started_at and self.finished_at:
            self.duration_seconds = (self.finished_at - self.started_at).total_seconds()

    def to_dict(self) -> Dict[str, Any]:
        return {
            "total_stages": self.total_stages,
            "completed_stages": self.completed_stages,
            "failed_stages": self.failed_stages,
            "skipped_stages": self.skipped_stages,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "finished_at": self.finished_at.isoformat() if self.finished_at else None,
            "duration_seconds": self.duration_seconds,
            "total_rows_processed": self.total_rows_processed,
            "total_bytes_processed": self.total_bytes_processed,
            "peak_memory_mb": self.peak_memory_mb,
            "retry_count": self.retry_count,
            "checkpoint_count": self.checkpoint_count,
        }


@dataclass
class PipelineExecutionResult:
    """Результат выполнения pipeline"""

    pipeline_id: str
    run_id: str
    status: PipelineStatus

    stage_results: Dict[str, ExecutionResult] = field(default_factory=dict)
    quality_check_results: Dict[str, ExecutionResult] = field(default_factory=dict)

    metrics: PipelineExecutionMetrics = field(default_factory=PipelineExecutionMetrics)
    error: Optional[str] = None
    error_stage: Optional[str] = None

    artifacts: Dict[str, str] = field(default_factory=dict)

    def is_success(self) -> bool:
        return self.status == PipelineStatus.SUCCESS

    def is_failed(self) -> bool:
        return self.status == PipelineStatus.FAILED

    def get_failed_stages(self) -> List[str]:
        return [
            name
            for name, result in self.stage_results.items()
            if result.status == ExecutionStatus.FAILED
        ]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "pipeline_id": self.pipeline_id,
            "run_id": self.run_id,
            "status": self.status.value,
            "stage_results": {k: v.to_dict() for k, v in self.stage_results.items()},
            "quality_check_results": {
                k: v.to_dict() for k, v in self.quality_check_results.items()
            },
            "metrics": self.metrics.to_dict(),
            "error": self.error,
            "error_stage": self.error_stage,
            "artifacts": self.artifacts,
        }


class PipelineExecutionError(Exception):
    """Ошибка выполнения pipeline"""

    def __init__(
        self,
        message: str,
        stage_name: Optional[str] = None,
        original_error: Optional[Exception] = None,
    ):
        super().__init__(message)
        self.stage_name = stage_name
        self.original_error = original_error


class DependencyGraph:
    """Граф зависимостей для определения порядка выполнения"""

    def __init__(self, stages: List[StageConfig]):
        self.stages = {stage.name: stage for stage in stages if stage.enabled}
        self.dependencies = {
            stage.name: set(stage.depends_on) for stage in stages if stage.enabled
        }
        self.execution_plan: List[List[str]] = []
        self._build_execution_plan()

    def _build_execution_plan(self):
        """Построение плана выполнения (топологическая сортировка)"""
        visited = set()
        self.execution_plan = []

        while len(visited) < len(self.stages):
            # Находим этапы, готовые к выполнению
            ready_stages = []
            for stage_name in self.stages:
                if stage_name not in visited:
                    deps_satisfied = all(
                        dep in visited for dep in self.dependencies[stage_name]
                    )
                    if deps_satisfied:
                        ready_stages.append(stage_name)

            if not ready_stages:
                remaining = set(self.stages.keys()) - visited
                raise PipelineExecutionError(
                    f"Circular dependency detected in stages: {remaining}"
                )

            self.execution_plan.append(ready_stages)
            visited.update(ready_stages)

        logger.debug("Execution plan built", plan=self.execution_plan)

    def get_execution_plan(self) -> List[List[str]]:
        """Получение плана выполнения"""
        return self.execution_plan

    def get_dependencies(self, stage_name: str) -> Set[str]:
        """Получение зависимостей этапа"""
        return self.dependencies.get(stage_name, set())

    def get_dependents(self, stage_name: str) -> Set[str]:
        """Получение этапов, зависящих от данного"""
        dependents = set()
        for name, deps in self.dependencies.items():
            if stage_name in deps:
                dependents.add(name)
        return dependents


class Pipeline:
    """
    Основной класс для выполнения data pipeline

    Функциональность:
    - Построение dependency graph
    - Параллельное выполнение этапов
    - Error handling и retry
    - Checkpoint/resume
    - Quality checks
    - Monitoring
    """

    def __init__(self, config: PipelineConfig):
        self.config = config
        self.registry = ComponentRegistry()
        self.metrics_collector = MetricsCollector()

        # Execution state
        self._execution_result: Optional[PipelineExecutionResult] = None
        self._dependency_graph: Optional[DependencyGraph] = None
        self._stage_statuses: Dict[str, StageStatus] = {}
        self._cancelled = False

        # Logging
        self._logger = structlog.get_logger(pipeline_id=config.name)

    async def execute(
        self,
        runtime_config: Optional[RuntimeConfig] = None,
        validate_components: bool = True,
    ) -> PipelineExecutionResult:
        """Выполнение pipeline"""

        # Генерируем run_id если не указан
        run_id = (
            runtime_config.run_id
            if runtime_config
            else f"run_{datetime.utcnow().strftime('%Y%m%d_%H%M%S_%f')}"
        )

        # Инициализируем результат выполнения
        self._execution_result = PipelineExecutionResult(
            pipeline_id=self.config.name, run_id=run_id, status=PipelineStatus.PENDING
        )

        self._logger.info(
            "Starting pipeline execution",
            run_id=run_id,
            stages_count=len(self.config.stages),
            dry_run=runtime_config.dry_run if runtime_config else False,
        )

        try:
            # 1. Инициализация
            await self._initialize_execution(runtime_config, validate_components)

            # 2. Выполнение этапов
            await self._execute_stages(runtime_config)

            # 3. Выполнение quality checks
            await self._execute_quality_checks(runtime_config)

            # 4. Финализация
            await self._finalize_execution()

            self._execution_result.status = PipelineStatus.SUCCESS
            self._logger.info(
                "Pipeline execution completed successfully",
                run_id=run_id,
                duration=self._execution_result.metrics.duration_seconds,
                stages_completed=self._execution_result.metrics.completed_stages,
            )

        except Exception as e:
            await self._handle_pipeline_error(e)

        finally:
            # Cleanup
            await self._cleanup_execution()

        return self._execution_result

    async def _initialize_execution(
        self, runtime_config: Optional[RuntimeConfig], validate_components: bool
    ):
        """Инициализация выполнения"""
        self._execution_result.status = PipelineStatus.INITIALIZING
        self._execution_result.metrics.started_at = datetime.now(timezone.utc)

        # Валидация конфигурации
        validation_errors = (
            self.config.validate_components_exist(self.registry)
            if validate_components
            else []
        )
        if validation_errors:
            raise PipelineExecutionError(
                f"Component validation failed: {validation_errors}"
            )

        # Построение dependency graph
        self._dependency_graph = DependencyGraph(self.config.stages)

        # Инициализация статусов этапов
        self._stage_statuses = {
            stage.name: StageStatus.PENDING
            for stage in self.config.stages
            if stage.enabled
        }

        # Применение runtime конфигурации
        if runtime_config:
            await self._apply_runtime_config(runtime_config)

        # Инициализация метрик
        self._execution_result.metrics.total_stages = len(
            [s for s in self.config.stages if s.enabled]
        )

        self._logger.info("Pipeline initialization completed")

    async def _apply_runtime_config(self, runtime_config: RuntimeConfig):
        """Применение runtime конфигурации"""
        # Пропуск этапов
        for stage_name in runtime_config.skip_stages:
            if stage_name in self._stage_statuses:
                self._stage_statuses[stage_name] = StageStatus.SKIPPED
                self._execution_result.metrics.skipped_stages += 1

        # Resume from stage (пропускаем предыдущие этапы)
        if runtime_config.resume_from_stage:
            stages_before_resume = []
            for stage_group in self._dependency_graph.get_execution_plan():
                if runtime_config.resume_from_stage in stage_group:
                    break
                stages_before_resume.extend(stage_group)

            for stage_name in stages_before_resume:
                if stage_name in self._stage_statuses:
                    self._stage_statuses[stage_name] = StageStatus.SKIPPED
                    self._execution_result.metrics.skipped_stages += 1

    async def _execute_stages(self, runtime_config: Optional[RuntimeConfig]):
        """Выполнение этапов согласно dependency graph"""
        self._execution_result.status = PipelineStatus.RUNNING

        execution_plan = self._dependency_graph.get_execution_plan()

        for stage_group in execution_plan:
            if self._cancelled:
                break

            # Фильтруем этапы для выполнения
            stages_to_execute = [
                name
                for name in stage_group
                if self._stage_statuses.get(name) == StageStatus.PENDING
            ]

            if not stages_to_execute:
                continue

            # Помечаем этапы как готовые к выполнению
            for stage_name in stages_to_execute:
                self._stage_statuses[stage_name] = StageStatus.READY

            # Параллельное выполнение этапов в группе
            await self._execute_stage_group(stages_to_execute, runtime_config)

            # Проверяем, нужно ли останавливать pipeline при ошибках
            if not self.config.continue_on_failure:
                failed_stages = [
                    name
                    for name in stages_to_execute
                    if self._stage_statuses.get(name) == StageStatus.FAILED
                ]
                if failed_stages:
                    raise PipelineExecutionError(
                        f"Pipeline stopped due to failed stages: {failed_stages}",
                        stage_name=failed_stages[0],
                    )

    async def _execute_stage_group(
        self, stage_names: List[str], runtime_config: Optional[RuntimeConfig]
    ):
        """Параллельное выполнение группы этапов"""

        # Ограничиваем количество параллельных задач
        semaphore = asyncio.Semaphore(self.config.max_parallel_stages)

        async def execute_stage_with_semaphore(stage_name: str):
            async with semaphore:
                return await self._execute_single_stage(stage_name, runtime_config)

        # Создаем задачи для всех этапов группы
        tasks = [
            asyncio.create_task(execute_stage_with_semaphore(stage_name))
            for stage_name in stage_names
        ]

        # Ждем завершения всех задач
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Обрабатываем результаты
        for i, result in enumerate(results):
            stage_name = stage_names[i]

            if isinstance(result, Exception):
                self._logger.error(
                    "Stage execution failed", stage=stage_name, error=str(result)
                )
                self._stage_statuses[stage_name] = StageStatus.FAILED
                self._execution_result.metrics.failed_stages += 1

                # Создаем результат ошибки
                error_result = ExecutionResult(
                    stage_name=stage_name,
                    status=ExecutionStatus.FAILED,
                    error=str(result),
                )
                self._execution_result.stage_results[stage_name] = error_result
            else:
                self._stage_statuses[stage_name] = StageStatus.SUCCESS
                self._execution_result.metrics.completed_stages += 1

    async def _execute_single_stage(
        self, stage_name: str, runtime_config: Optional[RuntimeConfig]
    ) -> ExecutionResult:
        """Выполнение отдельного этапа"""

        stage_config = next(s for s in self.config.stages if s.name == stage_name)
        self._stage_statuses[stage_name] = StageStatus.RUNNING

        stage_logger = self._logger.bind(stage=stage_name)
        stage_logger.info("Starting stage execution")

        try:
            # Получаем компонент из реестра
            component_class = self.registry.get_component(
                stage_config.component_type, stage_config.component_name
            )

            if not component_class:
                raise PipelineExecutionError(
                    f"Component not found: {stage_config.component}",
                    stage_name=stage_name,
                )

            # Создаем экземпляр компонента
            component = component_class(stage_config.config)

            # Подготавливаем контекст выполнения
            context = await self._build_execution_context(stage_config, runtime_config)

            # Выполняем с retry логикой
            result = await self._execute_with_retry(component, context, stage_config)

            # Сохраняем результат
            self._execution_result.stage_results[stage_name] = result

            # Обновляем метрики
            self._update_metrics_from_result(result)

            stage_logger.info(
                "Stage execution completed",
                status=result.status.value,
                duration=result.metadata.duration_seconds,
            )

            return result

        except Exception as e:
            stage_logger.error("Stage execution failed", error=str(e))

            # Создаем результат ошибки
            error_result = ExecutionResult(
                stage_name=stage_name, status=ExecutionStatus.FAILED
            )
            error_result.set_error(e, f"Stage {stage_name} execution failed")

            self._execution_result.stage_results[stage_name] = error_result
            raise PipelineExecutionError(
                f"Stage {stage_name} failed: {str(e)}",
                stage_name=stage_name,
                original_error=e,
            )

    async def _build_execution_context(
        self, stage_config: StageConfig, runtime_config: Optional[RuntimeConfig]
    ) -> ExecutionContext:
        """Построение контекста выполнения для этапа"""

        # Получаем результаты зависимых этапов
        previous_results = []
        for dep_stage in stage_config.depends_on:
            if dep_stage in self._execution_result.stage_results:
                previous_results.append(self._execution_result.stage_results[dep_stage])

        # Создаем контекст
        context = ExecutionContext(
            pipeline_id=self.config.name,
            run_id=self._execution_result.run_id,
            stage_name=stage_config.name,
            config=stage_config.config,
            global_variables=self.config.global_config,
            previous_results=previous_results,
            environment=self.config.metadata.environment.value,
            dry_run=runtime_config.dry_run if runtime_config else False,
        )

        return context

    async def _execute_with_retry(
        self,
        component: BaseComponent,
        context: ExecutionContext,
        stage_config: StageConfig,
    ) -> ExecutionResult:
        """Выполнение компонента с retry логикой"""

        retry_policy = stage_config.retry_policy
        last_error = None

        for attempt in range(retry_policy.maximum_attempts):
            try:
                # Добавляем timeout
                result = await asyncio.wait_for(
                    component.execute(context), timeout=stage_config.timeout_seconds
                )

                if result.is_success():
                    if attempt > 0:
                        self._execution_result.metrics.retry_count += 1
                    return result
                else:
                    last_error = result.error

            except asyncio.TimeoutError:
                last_error = (
                    f"Stage timeout after {stage_config.timeout_seconds} seconds"
                )
            except Exception as e:
                last_error = str(e)

            # Если не последняя попытка - ждем перед повтором
            if attempt < retry_policy.maximum_attempts - 1:
                delay = self._calculate_retry_delay(attempt, retry_policy)
                self._logger.info(
                    "Retrying stage execution",
                    stage=context.stage_name,
                    attempt=attempt + 1,
                    delay=delay,
                )
                await asyncio.sleep(delay)

        # Все попытки исчерпаны
        error_result = ExecutionResult(
            stage_name=context.stage_name,
            status=ExecutionStatus.FAILED,
            error=f"Failed after {retry_policy.maximum_attempts} attempts. Last error: {last_error}",
        )

        return error_result

    def _calculate_retry_delay(self, attempt: int, retry_policy) -> float:
        """Вычисление задержки для retry"""
        if retry_policy.policy_type.value == "exponential_backoff":
            delay = retry_policy.initial_interval_seconds * (
                retry_policy.backoff_coefficient**attempt
            )
        elif retry_policy.policy_type.value == "linear_backoff":
            delay = retry_policy.initial_interval_seconds * (attempt + 1)
        else:  # fixed_interval
            delay = retry_policy.initial_interval_seconds

        return min(delay, retry_policy.max_interval_seconds)

    async def _execute_quality_checks(self, runtime_config: Optional[RuntimeConfig]):
        """Выполнение проверок качества данных"""

        if not self.config.quality_checks:
            return

        self._logger.info(
            "Starting quality checks", count=len(self.config.quality_checks)
        )

        for check in self.config.quality_checks:
            try:
                result = await self._execute_quality_check(check, runtime_config)
                self._execution_result.quality_check_results[check.name] = result

                # Проверяем, нужно ли останавливать pipeline
                if not result.is_success() and check.fail_pipeline_on_error:
                    raise PipelineExecutionError(
                        f"Quality check '{check.name}' failed: {result.error}"
                    )

            except Exception as e:
                if check.fail_pipeline_on_error:
                    raise
                else:
                    self._logger.warning(
                        "Quality check failed but pipeline continues",
                        check=check.name,
                        error=str(e),
                    )

    async def _execute_quality_check(
        self, check: QualityCheck, runtime_config: Optional[RuntimeConfig]
    ) -> ExecutionResult:
        """Выполнение отдельной проверки качества"""

        # Получаем компонент
        component_class = self.registry.get_component(
            check.component_type, check.component_name
        )

        if not component_class:
            raise PipelineExecutionError(
                f"Quality check component not found: {check.component}"
            )

        # Создаем компонент
        component = component_class(check.config)

        # Подготавливаем контекст
        relevant_results = [
            result
            for stage_name, result in self._execution_result.stage_results.items()
            if stage_name in check.applies_to
        ]

        context = ExecutionContext(
            pipeline_id=self.config.name,
            run_id=self._execution_result.run_id,
            stage_name=f"quality_check_{check.name}",
            config=check.config,
            previous_results=relevant_results,
            dry_run=runtime_config.dry_run if runtime_config else False,
        )

        # Выполняем проверку
        return await component.execute(context)

    def _update_metrics_from_result(self, result: ExecutionResult):
        """Обновление метрик на основе результата выполнения"""
        if result.metadata.rows_processed:
            self._execution_result.metrics.total_rows_processed += (
                result.metadata.rows_processed
            )

        if result.metadata.bytes_processed:
            self._execution_result.metrics.total_bytes_processed += (
                result.metadata.bytes_processed
            )

        if result.metadata.checkpoints:
            self._execution_result.metrics.checkpoint_count += len(
                result.metadata.checkpoints
            )

    async def _finalize_execution(self):
        """Финализация выполнения"""
        self._execution_result.metrics.finished_at = datetime.now(timezone.utc)
        self._execution_result.metrics.calculate_duration()

        # Собираем artifacts
        for stage_name, result in self._execution_result.stage_results.items():
            for artifact_name, artifact_path in result.artifacts.items():
                self._execution_result.artifacts[f"{stage_name}_{artifact_name}"] = (
                    artifact_path
                )

        self._logger.info("Pipeline execution finalized")

    async def _handle_pipeline_error(self, error: Exception):
        """Обработка ошибок pipeline"""
        self._execution_result.status = PipelineStatus.FAILED
        self._execution_result.error = str(error)

        if isinstance(error, PipelineExecutionError) and error.stage_name:
            self._execution_result.error_stage = error.stage_name

        self._execution_result.metrics.finished_at = datetime.now(timezone.utc)
        self._execution_result.metrics.calculate_duration()

        self._logger.error(
            "Pipeline execution failed",
            error=str(error),
            error_stage=self._execution_result.error_stage,
            duration=self._execution_result.metrics.duration_seconds,
        )

    async def _cleanup_execution(self):
        """Очистка ресурсов после выполнения"""
        # Здесь можно добавить cleanup логику
        pass

    # === Public API ===

    def cancel(self):
        """Отмена выполнения pipeline"""
        self._cancelled = True
        if self._execution_result:
            self._execution_result.status = PipelineStatus.CANCELLED
        self._logger.info("Pipeline execution cancelled")

    def get_status(self) -> Optional[PipelineStatus]:
        """Получение текущего статуса"""
        return self._execution_result.status if self._execution_result else None

    def get_stage_status(self, stage_name: str) -> Optional[StageStatus]:
        """Получение статуса этапа"""
        return self._stage_statuses.get(stage_name)

    def get_execution_result(self) -> Optional[PipelineExecutionResult]:
        """Получение результата выполнения"""
        return self._execution_result


class PipelineBuilder:
    """Builder для создания Pipeline из различных источников"""

    @staticmethod
    def from_config(config: PipelineConfig) -> Pipeline:
        """Создание pipeline из конфигурации"""
        return Pipeline(config)

    @staticmethod
    def from_yaml(yaml_path: str) -> Pipeline:
        """Создание pipeline из YAML файла"""
        from ..config.yaml_loader import load_pipeline_config

        full_config = load_pipeline_config(yaml_path)
        return Pipeline(full_config.pipeline)

    @staticmethod
    def from_dict(config_dict: Dict[str, Any]) -> Pipeline:
        """Создание pipeline из словаря"""
        from ..config.yaml_loader import load_config_from_dict

        full_config = load_config_from_dict(config_dict)
        return Pipeline(full_config.pipeline)
