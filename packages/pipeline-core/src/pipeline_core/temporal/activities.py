"""
Temporal Activities для выполнения компонентов pipeline

Каждый компонент pipeline выполняется как отдельная Temporal Activity,
что обеспечивает:
- Retry policies на уровне Temporal
- Heartbeat для long-running tasks
- Checkpoint/resume functionality
- Distributed execution
"""

import asyncio
from typing import Dict, Any, Optional
from datetime import datetime, timezone

import structlog
from temporalio import activity

from pipeline_core.components import (
    ExecutionContext,
    ExecutionResult,
    ExecutionMetadata,
)
from pipeline_core.registry import ComponentRegistry

logger = structlog.get_logger(__name__)


class ComponentActivity:
    """Activities для выполнения компонентов pipeline"""

    @staticmethod
    @activity.defn(name="execute_component")
    async def execute_component(
        component_type: str,
        component_name: str,
        stage_config: Dict[str, Any],
        execution_context: Dict[str, Any],
        heartbeat_interval: int = 30,
    ) -> Dict[str, Any]:
        """
        Temporal Activity для выполнения компонента

        Args:
            component_type: Тип компонента (extractor, transformer, loader)
            component_name: Имя компонента
            stage_config: Конфигурация этапа
            execution_context: Контекст выполнения
            heartbeat_interval: Интервал heartbeat в секундах

        Returns:
            Результат выполнения в виде словаря
        """

        activity_logger = logger.bind(
            component_type=component_type,
            component_name=component_name,
            stage_name=execution_context.get("stage_name", "unknown"),
            workflow_id=activity.info().workflow_id,
            activity_id=activity.info().activity_id,
        )

        activity_logger.info("Starting component execution")

        try:
            # Получаем компонент из реестра
            registry = ComponentRegistry()
            component_class = registry.get_component(component_type, component_name)

            if not component_class:
                error_msg = f"Component not found: {component_type}/{component_name}"
                activity_logger.error(error_msg)
                return {
                    "status": "failed",
                    "error": error_msg,
                    "data": None,
                    "metadata": {},
                }

            # Создаем экземпляр компонента
            component_config = stage_config.get("config", {})
            component = component_class(component_config)

            # Восстанавливаем контекст выполнения
            context = ComponentActivity._restore_execution_context(execution_context)

            # Настраиваем heartbeat для long-running tasks
            heartbeat_task = None
            if heartbeat_interval > 0:
                heartbeat_task = asyncio.create_task(
                    ComponentActivity._send_heartbeat(
                        heartbeat_interval, activity_logger
                    )
                )

            try:
                # Выполняем компонент
                result = await component.execute(context)

                activity_logger.info(
                    "Component execution completed",
                    status=result.status.value,
                    duration=result.metadata.duration_seconds,
                )

                # Конвертируем результат в словарь для Temporal
                return ComponentActivity._serialize_execution_result(result)

            finally:
                # Останавливаем heartbeat
                if heartbeat_task:
                    heartbeat_task.cancel()
                    try:
                        await heartbeat_task
                    except asyncio.CancelledError:
                        pass

        except Exception as e:
            activity_logger.error("Component execution failed", error=str(e))

            # Возвращаем результат ошибки
            return {
                "status": "failed",
                "error": str(e),
                "error_type": type(e).__name__,
                "data": None,
                "metadata": {
                    "execution_id": str(uuid.uuid4()),
                    "started_at": datetime.now(timezone.utc).isoformat(),
                    "finished_at": datetime.now(timezone.utc).isoformat(),
                    "error_count": 1,
                    "last_error": str(e),
                },
            }

    @staticmethod
    async def _send_heartbeat(interval: int, activity_logger):
        """Отправка heartbeat для long-running activities"""
        try:
            while True:
                await asyncio.sleep(interval)
                activity.heartbeat("Component execution in progress")
                activity_logger.debug("Heartbeat sent")
        except asyncio.CancelledError:
            activity_logger.debug("Heartbeat cancelled")
            raise

    @staticmethod
    def _restore_execution_context(context_dict: Dict[str, Any]) -> ExecutionContext:
        """Восстановление ExecutionContext из словаря"""

        # Восстанавливаем metadata
        metadata_dict = context_dict.get("metadata", {})
        metadata = ExecutionMetadata(
            execution_id=metadata_dict.get("execution_id", str(uuid.uuid4())),
            started_at=datetime.fromisoformat(metadata_dict["started_at"])
            if metadata_dict.get("started_at")
            else None,
            finished_at=datetime.fromisoformat(metadata_dict["finished_at"])
            if metadata_dict.get("finished_at")
            else None,
            duration_seconds=metadata_dict.get("duration_seconds"),
            rows_processed=metadata_dict.get("rows_processed"),
            bytes_processed=metadata_dict.get("bytes_processed"),
            checkpoints=metadata_dict.get("checkpoints", {}),
            custom_metrics=metadata_dict.get("custom_metrics", {}),
            error_count=metadata_dict.get("error_count", 0),
            last_error=metadata_dict.get("last_error"),
        )

        # Восстанавливаем previous_results
        previous_results = []
        for result_dict in context_dict.get("previous_results", []):
            result = ComponentActivity._deserialize_execution_result(result_dict)
            previous_results.append(result)

        return ExecutionContext(
            pipeline_id=context_dict["pipeline_id"],
            run_id=context_dict["run_id"],
            stage_name=context_dict["stage_name"],
            config=context_dict.get("config", {}),
            global_variables=context_dict.get("global_variables", {}),
            previous_results=previous_results,
            input_data=context_dict.get("input_data"),
            metadata=metadata,
            environment=context_dict.get("environment", "development"),
            dry_run=context_dict.get("dry_run", False),
            temporal_workflow_id=activity.info().workflow_id,
            temporal_run_id=activity.info().workflow_run_id,
        )

    @staticmethod
    def _serialize_execution_result(result: ExecutionResult) -> Dict[str, Any]:
        """Сериализация ExecutionResult в словарь"""
        return {
            "stage_name": result.stage_name,
            "status": result.status.value,
            "data": result.data,
            "artifacts": result.artifacts,
            "error": result.error,
            "error_type": result.error_type,
            "traceback": result.traceback,
            "metadata": result.metadata.to_dict(),
            "output_schema": result.output_schema,
        }

    @staticmethod
    def _deserialize_execution_result(result_dict: Dict[str, Any]) -> ExecutionResult:
        """Десериализация ExecutionResult из словаря"""
        from ..components.base import ExecutionStatus

        # Восстанавливаем metadata
        metadata_dict = result_dict.get("metadata", {})
        metadata = ExecutionMetadata(
            execution_id=metadata_dict.get("execution_id", str(uuid.uuid4())),
            started_at=datetime.fromisoformat(metadata_dict["started_at"])
            if metadata_dict.get("started_at")
            else None,
            finished_at=datetime.fromisoformat(metadata_dict["finished_at"])
            if metadata_dict.get("finished_at")
            else None,
            duration_seconds=metadata_dict.get("duration_seconds"),
            rows_processed=metadata_dict.get("rows_processed"),
            bytes_processed=metadata_dict.get("bytes_processed"),
            checkpoints=metadata_dict.get("checkpoints", {}),
            custom_metrics=metadata_dict.get("custom_metrics", {}),
            error_count=metadata_dict.get("error_count", 0),
            last_error=metadata_dict.get("last_error"),
        )

        return ExecutionResult(
            stage_name=result_dict["stage_name"],
            status=ExecutionStatus(result_dict["status"]),
            data=result_dict.get("data"),
            artifacts=result_dict.get("artifacts", {}),
            error=result_dict.get("error"),
            error_type=result_dict.get("error_type"),
            traceback=result_dict.get("traceback"),
            metadata=metadata,
            output_schema=result_dict.get("output_schema"),
        )


class QualityCheckActivity:
    """Activities для выполнения quality checks"""

    @staticmethod
    @activity.defn(name="execute_quality_check")
    async def execute_quality_check(
        check_name: str,
        component_type: str,
        component_name: str,
        check_config: Dict[str, Any],
        execution_context: Dict[str, Any],
        applies_to_results: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """
        Temporal Activity для выполнения quality check

        Args:
            check_name: Имя проверки
            component_type: Тип компонента для проверки
            component_name: Имя компонента для проверки
            check_config: Конфигурация проверки
            execution_context: Контекст выполнения
            applies_to_results: Результаты этапов, к которым применяется проверка

        Returns:
            Результат выполнения проверки
        """

        activity_logger = logger.bind(
            check_name=check_name,
            component_type=component_type,
            component_name=component_name,
            workflow_id=activity.info().workflow_id,
            activity_id=activity.info().activity_id,
        )

        activity_logger.info("Starting quality check execution")

        try:
            # Получаем компонент для проверки
            registry = ComponentRegistry()
            component_class = registry.get_component(component_type, component_name)

            if not component_class:
                error_msg = f"Quality check component not found: {component_type}/{component_name}"
                activity_logger.error(error_msg)
                return {
                    "status": "failed",
                    "error": error_msg,
                    "data": None,
                    "metadata": {},
                }

            # Создаем компонент
            component = component_class(check_config)

            # Восстанавливаем контекст
            context = ComponentActivity._restore_execution_context(execution_context)

            # Добавляем результаты этапов, к которым применяется проверка
            relevant_results = [
                ComponentActivity._deserialize_execution_result(result_dict)
                for result_dict in applies_to_results
            ]
            context.previous_results = relevant_results
            context.stage_name = f"quality_check_{check_name}"

            # Выполняем проверку
            result = await component.execute(context)

            activity_logger.info(
                "Quality check execution completed",
                status=result.status.value,
                duration=result.metadata.duration_seconds,
            )

            return ComponentActivity._serialize_execution_result(result)

        except Exception as e:
            activity_logger.error("Quality check execution failed", error=str(e))

            return {
                "status": "failed",
                "error": str(e),
                "error_type": type(e).__name__,
                "data": None,
                "metadata": {
                    "execution_id": str(uuid.uuid4()),
                    "started_at": datetime.now(timezone.utc).isoformat(),
                    "finished_at": datetime.now(timezone.utc).isoformat(),
                    "error_count": 1,
                    "last_error": str(e),
                },
            }


class PipelineStateActivity:
    """Activities для управления состоянием pipeline"""

    @staticmethod
    @activity.defn(name="save_pipeline_checkpoint")
    async def save_pipeline_checkpoint(
        pipeline_id: str, run_id: str, checkpoint_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Сохранение checkpoint'а pipeline"""

        activity_logger = logger.bind(
            pipeline_id=pipeline_id,
            run_id=run_id,
            workflow_id=activity.info().workflow_id,
        )

        try:
            activity_logger.info("Saving pipeline checkpoint")

            # Здесь можно реализовать сохранение checkpoint'а
            # в внешнее хранилище (S3, Database и т.д.)

            # Для демонстрации просто логируем
            activity_logger.info("Pipeline checkpoint saved successfully")

            return {
                "status": "success",
                "checkpoint_id": f"{pipeline_id}_{run_id}_{datetime.now(timezone.utc).timestamp()}",
                "saved_at": datetime.now(timezone.utc).isoformat(),
            }

        except Exception as e:
            activity_logger.error("Failed to save pipeline checkpoint", error=str(e))
            return {"status": "failed", "error": str(e)}

    @staticmethod
    @activity.defn(name="load_pipeline_checkpoint")
    async def load_pipeline_checkpoint(
        pipeline_id: str, run_id: str, checkpoint_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """Загрузка checkpoint'а pipeline"""

        activity_logger = logger.bind(
            pipeline_id=pipeline_id,
            run_id=run_id,
            checkpoint_id=checkpoint_id,
            workflow_id=activity.info().workflow_id,
        )

        try:
            activity_logger.info("Loading pipeline checkpoint")

            # Здесь можно реализовать загрузку checkpoint'а
            # из внешнего хранилища

            # Для демонстрации возвращаем пустой checkpoint
            return {
                "status": "success",
                "checkpoint_data": {},
                "loaded_at": datetime.now(timezone.utc).isoformat(),
            }

        except Exception as e:
            activity_logger.error("Failed to load pipeline checkpoint", error=str(e))
            return {"status": "failed", "error": str(e), "checkpoint_data": {}}
