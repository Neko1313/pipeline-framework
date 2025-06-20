# src/pipeline_core/temporal/workflows.py

"""
Temporal Workflows для оркестрации data pipelines

Workflow управляет выполнением всего pipeline:
- Координирует выполнение Activities
- Управляет dependency graph
- Обрабатывает ошибки и retry
- Поддерживает checkpoint/resume
- Обеспечивает observability
"""

import asyncio
from typing import Dict, List, Any, Optional
from datetime import datetime, timezone, timedelta

import structlog
from temporalio import workflow
from temporalio.common import RetryPolicy as TemporalRetryPolicy
from temporalio.exceptions import ActivityError, ApplicationError

from pipeline_core.temporal import (
    ComponentActivity,
    QualityCheckActivity,
    PipelineStateActivity,
)
from pipeline_core.pipeline import DependencyGraph

logger = structlog.get_logger(__name__)


@workflow.defn
class DataPipelineWorkflow:
    """
    Главный Workflow для выполнения data pipeline

    Обеспечивает:
    - Оркестрацию выполнения всех этапов pipeline
    - Управление зависимостями между этапами
    - Error handling и retry на уровне Workflow
    - Checkpoint/resume functionality
    - Quality checks выполнение
    """

    def __init__(self):
        self._cancelled = False
        self._stage_results: Dict[str, Dict[str, Any]] = {}
        self._quality_check_results: Dict[str, Dict[str, Any]] = {}
        self._execution_metadata = {}

    @workflow.run
    async def run_pipeline(self, workflow_input: Dict[str, Any]) -> Dict[str, Any]:
        """
        Основной метод выполнения pipeline workflow

        Args:
            workflow_input: Входные данные содержащие:
                - pipeline_config: Конфигурация pipeline
                - runtime_config: Runtime конфигурация (опционально)
                - execution_metadata: Метаданные выполнения

        Returns:
            Результат выполнения pipeline
        """

        pipeline_config = workflow_input["pipeline_config"]
        runtime_config = workflow_input.get("runtime_config")
        execution_metadata = workflow_input.get("execution_metadata", {})

        workflow_logger = logger.bind(
            pipeline_name=pipeline_config["name"],
            workflow_id=workflow.info().workflow_id,
            run_id=workflow.info().workflow_run_id,
        )

        workflow_logger.info("Starting pipeline workflow execution")

        try:
            # Инициализируем метаданные выполнения
            self._execution_metadata = {
                "pipeline_name": pipeline_config["name"],
                "workflow_id": workflow.info().workflow_id,
                "run_id": workflow.info().workflow_run_id,
                "started_at": datetime.now(timezone.utc).isoformat(),
                "stages_total": len(
                    [s for s in pipeline_config["stages"] if s.get("enabled", True)]
                ),
                "stages_completed": 0,
                "stages_failed": 0,
                **execution_metadata,
            }

            # Выполняем этапы pipeline
            await self._execute_pipeline_stages(pipeline_config, runtime_config)

            # Выполняем quality checks
            await self._execute_quality_checks(pipeline_config, runtime_config)

            # Финализируем выполнение
            self._execution_metadata["finished_at"] = datetime.now(
                timezone.utc
            ).isoformat()
            self._execution_metadata["status"] = "success"

            workflow_logger.info(
                "Pipeline workflow completed successfully",
                stages_completed=self._execution_metadata["stages_completed"],
                duration_seconds=self._calculate_duration(),
            )

            return {
                "status": "success",
                "metadata": self._execution_metadata,
                "stage_results": self._stage_results,
                "quality_check_results": self._quality_check_results,
            }

        except Exception as e:
            # Обрабатываем ошибки workflow
            await self._handle_workflow_error(e, workflow_logger)

            return {
                "status": "failed",
                "error": str(e),
                "metadata": self._execution_metadata,
                "stage_results": self._stage_results,
                "quality_check_results": self._quality_check_results,
            }

    async def _execute_pipeline_stages(
        self, pipeline_config: Dict[str, Any], runtime_config: Optional[Dict[str, Any]]
    ):
        """Выполнение этапов pipeline согласно dependency graph"""

        # Строим dependency graph
        enabled_stages = [
            s for s in pipeline_config["stages"] if s.get("enabled", True)
        ]

        # Применяем runtime конфигурацию для пропуска этапов
        if runtime_config:
            skip_stages = runtime_config.get("skip_stages", [])
            resume_from_stage = runtime_config.get("resume_from_stage")

            if resume_from_stage:
                # Пропускаем этапы до resume_from_stage
                stages_to_skip = []
                for stage in enabled_stages:
                    if stage["name"] == resume_from_stage:
                        break
                    stages_to_skip.append(stage["name"])
                skip_stages.extend(stages_to_skip)

            enabled_stages = [s for s in enabled_stages if s["name"] not in skip_stages]

        # Создаем объекты StageConfig для dependency graph
        from ..config.models import StageConfig

        stage_configs = []
        for stage_data in enabled_stages:
            stage_config = StageConfig(**stage_data)
            stage_configs.append(stage_config)

        # Строим план выполнения
        dependency_graph = DependencyGraph(stage_configs)
        execution_plan = dependency_graph.get_execution_plan()

        workflow.logger.info(f"Execution plan: {execution_plan}")

        # Выполняем этапы группами согласно dependency graph
        for stage_group in execution_plan:
            if self._cancelled:
                break

            # Параллельное выполнение этапов в группе
            stage_tasks = []
            for stage_name in stage_group:
                stage_config = next(
                    s for s in enabled_stages if s["name"] == stage_name
                )
                task = asyncio.create_task(
                    self._execute_single_stage(
                        stage_config, pipeline_config, runtime_config
                    )
                )
                stage_tasks.append((stage_name, task))

            # Ждем завершения всех этапов в группе
            for stage_name, task in stage_tasks:
                try:
                    result = await task
                    self._stage_results[stage_name] = result

                    if result["status"] == "success":
                        self._execution_metadata["stages_completed"] += 1
                    else:
                        self._execution_metadata["stages_failed"] += 1

                        # Проверяем, нужно ли останавливать pipeline
                        if not pipeline_config.get("continue_on_failure", False):
                            raise ApplicationError(
                                f"Pipeline stopped due to failed stage: {stage_name}",
                                f"Stage {stage_name} failed: {result.get('error', 'Unknown error')}",
                            )

                except Exception as e:
                    self._execution_metadata["stages_failed"] += 1
                    workflow.logger.error(f"Stage {stage_name} failed: {str(e)}")

                    # Сохраняем результат ошибки
                    self._stage_results[stage_name] = {
                        "status": "failed",
                        "error": str(e),
                        "stage_name": stage_name,
                    }

                    if not pipeline_config.get("continue_on_failure", False):
                        raise

    async def _execute_single_stage(
        self,
        stage_config: Dict[str, Any],
        pipeline_config: Dict[str, Any],
        runtime_config: Optional[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """Выполнение отдельного этапа как Temporal Activity"""

        stage_name = stage_config["name"]
        component_parts = stage_config["component"].split("/")
        component_type = component_parts[0]
        component_name = component_parts[1]

        workflow.logger.info(f"Starting stage: {stage_name}")

        # Подготавливаем контекст выполнения
        execution_context = self._build_execution_context(
            stage_config, pipeline_config, runtime_config
        )

        # Настройки retry policy для Activity
        retry_policy = self._build_temporal_retry_policy(
            stage_config.get("retry_policy", {})
        )

        # Timeout для Activity
        timeout = timedelta(seconds=stage_config.get("timeout_seconds", 3600))

        try:
            # Выполняем компонент как Temporal Activity
            result = await workflow.execute_activity(
                ComponentActivity.execute_component,
                args=[component_type, component_name, stage_config, execution_context],
                start_to_close_timeout=timeout,
                retry_policy=retry_policy,
                heartbeat_timeout=timedelta(seconds=60),  # Heartbeat timeout
            )

            workflow.logger.info(f"Stage {stage_name} completed successfully")
            return result

        except ActivityError as e:
            workflow.logger.error(f"Stage {stage_name} failed: {str(e)}")
            raise ApplicationError(f"Stage {stage_name} failed", str(e))

    def _build_execution_context(
        self,
        stage_config: Dict[str, Any],
        pipeline_config: Dict[str, Any],
        runtime_config: Optional[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """Построение контекста выполнения для этапа"""

        # Получаем результаты зависимых этапов
        previous_results = []
        for dep_stage in stage_config.get("depends_on", []):
            if dep_stage in self._stage_results:
                previous_results.append(self._stage_results[dep_stage])

        return {
            "pipeline_id": pipeline_config["name"],
            "run_id": workflow.info().workflow_run_id,
            "stage_name": stage_config["name"],
            "config": stage_config.get("config", {}),
            "global_variables": pipeline_config.get("global_config", {}),
            "previous_results": previous_results,
            "environment": pipeline_config.get("metadata", {}).get(
                "environment", "development"
            ),
            "dry_run": runtime_config.get("dry_run", False)
            if runtime_config
            else False,
            "metadata": {
                "execution_id": f"{workflow.info().workflow_id}_{stage_config['name']}",
                "started_at": datetime.now(timezone.utc).isoformat(),
            },
        }

    def _build_temporal_retry_policy(
        self, retry_config: Dict[str, Any]
    ) -> TemporalRetryPolicy:
        """Построение Temporal RetryPolicy из конфигурации"""

        return TemporalRetryPolicy(
            initial_interval=timedelta(
                seconds=retry_config.get("initial_interval_seconds", 30)
            ),
            maximum_interval=timedelta(
                seconds=retry_config.get("max_interval_seconds", 300)
            ),
            backoff_coefficient=retry_config.get("backoff_coefficient", 2.0),
            maximum_attempts=retry_config.get("maximum_attempts", 3),
        )

    async def _execute_quality_checks(
        self, pipeline_config: Dict[str, Any], runtime_config: Optional[Dict[str, Any]]
    ):
        """Выполнение quality checks"""

        quality_checks = pipeline_config.get("quality_checks", [])
        if not quality_checks:
            return

        workflow.logger.info(f"Starting quality checks: {len(quality_checks)} checks")

        for check in quality_checks:
            try:
                result = await self._execute_quality_check(
                    check, pipeline_config, runtime_config
                )
                self._quality_check_results[check["name"]] = result

                # Проверяем, нужно ли останавливать pipeline
                if (
                    result["status"] != "success"
                    and check.get("fail_pipeline_on_error", True)
                    and not check.get("warning_only", False)
                ):
                    raise ApplicationError(
                        f"Quality check '{check['name']}' failed",
                        result.get("error", "Quality check failed"),
                    )

            except Exception as e:
                workflow.logger.error(f"Quality check {check['name']} failed: {str(e)}")

                self._quality_check_results[check["name"]] = {
                    "status": "failed",
                    "error": str(e),
                    "check_name": check["name"],
                }

                if check.get("fail_pipeline_on_error", True) and not check.get(
                    "warning_only", False
                ):
                    raise

    async def _execute_quality_check(
        self,
        check_config: Dict[str, Any],
        pipeline_config: Dict[str, Any],
        runtime_config: Optional[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """Выполнение отдельной quality check"""

        check_name = check_config["name"]
        component_parts = check_config["component"].split("/")
        component_type = component_parts[0]
        component_name = component_parts[1]

        workflow.logger.info(f"Starting quality check: {check_name}")

        # Получаем результаты этапов, к которым применяется проверка
        applies_to = check_config.get("applies_to", [])
        applies_to_results = []
        for stage_name in applies_to:
            if stage_name in self._stage_results:
                applies_to_results.append(self._stage_results[stage_name])

        # Контекст выполнения для quality check
        execution_context = {
            "pipeline_id": pipeline_config["name"],
            "run_id": workflow.info().workflow_run_id,
            "stage_name": f"quality_check_{check_name}",
            "config": check_config.get("config", {}),
            "global_variables": pipeline_config.get("global_config", {}),
            "previous_results": applies_to_results,
            "environment": pipeline_config.get("metadata", {}).get(
                "environment", "development"
            ),
            "dry_run": runtime_config.get("dry_run", False)
            if runtime_config
            else False,
            "metadata": {
                "execution_id": f"{workflow.info().workflow_id}_qc_{check_name}",
                "started_at": datetime.now(timezone.utc).isoformat(),
            },
        }

        # Retry policy для quality check
        retry_policy = self._build_temporal_retry_policy(
            check_config.get("retry_policy", {})
        )

        try:
            result = await workflow.execute_activity(
                QualityCheckActivity.execute_quality_check,
                args=[
                    check_name,
                    component_type,
                    component_name,
                    check_config.get("config", {}),
                    execution_context,
                    applies_to_results,
                ],
                start_to_close_timeout=timedelta(
                    seconds=600
                ),  # 10 минут для quality check
                retry_policy=retry_policy,
            )

            workflow.logger.info(f"Quality check {check_name} completed")
            return result

        except ActivityError as e:
            workflow.logger.error(f"Quality check {check_name} failed: {str(e)}")
            raise ApplicationError(f"Quality check {check_name} failed", str(e))

    async def _handle_workflow_error(self, error: Exception, workflow_logger):
        """Обработка ошибок workflow"""

        self._execution_metadata["finished_at"] = datetime.now(timezone.utc).isoformat()
        self._execution_metadata["status"] = "failed"
        self._execution_metadata["error"] = str(error)

        workflow_logger.error(
            "Pipeline workflow failed",
            error=str(error),
            duration_seconds=self._calculate_duration(),
        )

        # Можно добавить сохранение checkpoint'а при ошибке
        try:
            await workflow.execute_activity(
                PipelineStateActivity.save_pipeline_checkpoint,
                args=[
                    self._execution_metadata["pipeline_name"],
                    self._execution_metadata["run_id"],
                    {
                        "stage_results": self._stage_results,
                        "execution_metadata": self._execution_metadata,
                        "error_info": {
                            "error": str(error),
                            "error_type": type(error).__name__,
                            "failed_at": datetime.now(timezone.utc).isoformat(),
                        },
                    },
                ],
                start_to_close_timeout=timedelta(seconds=60),
                retry_policy=TemporalRetryPolicy(maximum_attempts=1),
            )
        except Exception as checkpoint_error:
            workflow_logger.warning(
                "Failed to save error checkpoint", error=str(checkpoint_error)
            )

    def _calculate_duration(self) -> Optional[float]:
        """Вычисление продолжительности выполнения"""
        started_at = self._execution_metadata.get("started_at")
        finished_at = self._execution_metadata.get("finished_at")

        if started_at and finished_at:
            start_time = datetime.fromisoformat(started_at.replace("Z", "+00:00"))
            end_time = datetime.fromisoformat(finished_at.replace("Z", "+00:00"))
            return (end_time - start_time).total_seconds()

        return None

    @workflow.signal
    async def cancel_pipeline(self):
        """Signal для отмены выполнения pipeline"""
        workflow.logger.info("Received cancel signal")
        self._cancelled = True

    @workflow.query
    def get_execution_status(self) -> Dict[str, Any]:
        """Query для получения текущего статуса выполнения"""
        return {
            "metadata": self._execution_metadata,
            "stage_results": self._stage_results,
            "quality_check_results": self._quality_check_results,
            "cancelled": self._cancelled,
        }
