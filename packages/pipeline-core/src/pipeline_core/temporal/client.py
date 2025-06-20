"""
Temporal Client для интеграции с Temporal Workflow Service

Обеспечивает:
- Подключение к Temporal Server
- Управление Workers
- Запуск Workflows
- Мониторинг выполнения
- Error handling
"""

import asyncio
import uuid
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Union

import structlog
from temporalio.client import Client, WorkflowHandle
from temporalio.worker import Worker
from temporalio.common import RetryPolicy as TemporalRetryPolicy
from temporalio.exceptions import WorkflowAlreadyStartedError

from pipeline_core.config import PipelineConfig, TemporalConfig, RuntimeConfig
from pipeline_core.pipeline import PipelineExecutionResult, PipelineStatus
from pipeline_core.temporal import ComponentActivity, QualityCheckActivity
from pipeline_core.temporal import DataPipelineWorkflow

logger = structlog.get_logger(__name__)


class TemporalClientError(Exception):
    """Ошибка Temporal клиента"""

    pass


class TemporalClient:
    """
    Клиент для работы с Temporal Workflow Service

    Предоставляет высокоуровневый API для:
    - Выполнения pipeline через Temporal
    - Управления Workers
    - Мониторинга Workflows
    """

    def __init__(self, temporal_config: TemporalConfig):
        self.config = temporal_config
        self._client: Optional[Client] = None
        self._workers: List[Worker] = []
        self._is_connected = False

        self._logger = structlog.get_logger(
            component="temporal_client",
            server=temporal_config.server_address,
            namespace=temporal_config.namespace,
        )

    async def connect(self) -> None:
        """Подключение к Temporal Server"""
        if self._is_connected:
            return

        try:
            self._logger.info("Connecting to Temporal server")

            # Настройки подключения
            connect_kwargs = {
                "target_host": self.config.server_address,
                "namespace": self.config.namespace,
            }

            # TLS конфигурация
            if self.config.tls_enabled:
                if self.config.cert_path and self.config.key_path:
                    # Здесь должна быть настройка TLS
                    # connect_kwargs["tls"] = ...
                    pass

            self._client = await Client.connect(**connect_kwargs)
            self._is_connected = True

            self._logger.info("Successfully connected to Temporal server")

        except Exception as e:
            self._logger.error("Failed to connect to Temporal server", error=str(e))
            raise TemporalClientError(f"Failed to connect to Temporal: {e}")

    async def disconnect(self) -> None:
        """Отключение от Temporal Server"""
        if not self._is_connected:
            return

        try:
            # Останавливаем всех workers
            await self.stop_all_workers()

            # Закрываем соединение
            if self._client:
                await self._client.close()
                self._client = None

            self._is_connected = False
            self._logger.info("Disconnected from Temporal server")

        except Exception as e:
            self._logger.error("Error during disconnect", error=str(e))

    async def start_worker(
        self,
        task_queue: Optional[str] = None,
        max_concurrent_activities: int = 100,
        max_concurrent_workflows: int = 100,
    ) -> Worker:
        """Запуск Temporal Worker"""

        if not self._is_connected:
            await self.connect()

        task_queue = task_queue or self.config.task_queue

        try:
            self._logger.info("Starting Temporal worker", task_queue=task_queue)

            worker = Worker(
                self._client,
                task_queue=task_queue,
                activities=[
                    ComponentActivity.execute_component,
                    QualityCheckActivity.execute_quality_check,
                ],
                workflows=[DataPipelineWorkflow],
                max_concurrent_activities=max_concurrent_activities,
                max_concurrent_workflows=max_concurrent_workflows,
            )

            # Запускаем worker в background
            asyncio.create_task(worker.run())

            self._workers.append(worker)

            self._logger.info(
                "Temporal worker started successfully",
                task_queue=task_queue,
                total_workers=len(self._workers),
            )

            return worker

        except Exception as e:
            self._logger.error("Failed to start Temporal worker", error=str(e))
            raise TemporalClientError(f"Failed to start worker: {e}")

    async def stop_all_workers(self) -> None:
        """Остановка всех workers"""
        if not self._workers:
            return

        self._logger.info("Stopping all Temporal workers", count=len(self._workers))

        # Останавливаем всех workers
        stop_tasks = []
        for worker in self._workers:
            stop_tasks.append(asyncio.create_task(worker.shutdown()))

        try:
            await asyncio.gather(*stop_tasks, return_exceptions=True)
            self._workers.clear()
            self._logger.info("All Temporal workers stopped")
        except Exception as e:
            self._logger.error("Error stopping workers", error=str(e))

    async def execute_pipeline(
        self,
        pipeline_config: PipelineConfig,
        runtime_config: Optional[RuntimeConfig] = None,
        workflow_id: Optional[str] = None,
        task_queue: Optional[str] = None,
    ) -> WorkflowHandle:
        """Запуск pipeline через Temporal Workflow"""

        if not self._is_connected:
            await self.connect()

        # Генерируем workflow_id если не указан
        if not workflow_id:
            timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
            workflow_id = (
                f"{self.config.workflow_id_prefix}_{pipeline_config.name}_{timestamp}"
            )

        task_queue = task_queue or self.config.task_queue

        try:
            self._logger.info(
                "Starting pipeline workflow",
                pipeline_name=pipeline_config.name,
                workflow_id=workflow_id,
                task_queue=task_queue,
            )

            # Подготавливаем входные данные для workflow
            workflow_input = {
                "pipeline_config": pipeline_config.dict(),
                "runtime_config": runtime_config.dict() if runtime_config else None,
                "execution_metadata": {
                    "workflow_id": workflow_id,
                    "started_at": datetime.utcnow().isoformat(),
                    "client_version": "1.0.0",
                },
            }

            # Настройки timeout'ов
            workflow_options = {}

            if self.config.workflow_execution_timeout:
                workflow_options["execution_timeout"] = timedelta(
                    seconds=self.config.workflow_execution_timeout
                )

            if self.config.workflow_run_timeout:
                workflow_options["run_timeout"] = timedelta(
                    seconds=self.config.workflow_run_timeout
                )

            if self.config.workflow_task_timeout:
                workflow_options["task_timeout"] = timedelta(
                    seconds=self.config.workflow_task_timeout
                )

            # Запускаем workflow
            handle = await self._client.start_workflow(
                DataPipelineWorkflow.run_pipeline,
                workflow_input,
                id=workflow_id,
                task_queue=task_queue,
                **workflow_options,
            )

            self._logger.info(
                "Pipeline workflow started successfully",
                workflow_id=workflow_id,
                run_id=handle.result_run_id,
            )

            return handle

        except WorkflowAlreadyStartedError:
            # Workflow уже запущен, получаем handle
            handle = self._client.get_workflow_handle(workflow_id)
            self._logger.warning(
                "Workflow already started, returning existing handle",
                workflow_id=workflow_id,
            )
            return handle

        except Exception as e:
            self._logger.error(
                "Failed to start pipeline workflow",
                pipeline_name=pipeline_config.name,
                workflow_id=workflow_id,
                error=str(e),
            )
            raise TemporalClientError(f"Failed to start workflow: {e}")

    async def wait_for_pipeline_completion(
        self, workflow_handle: WorkflowHandle, timeout_seconds: Optional[int] = None
    ) -> Dict[str, Any]:
        """Ожидание завершения pipeline workflow"""

        try:
            self._logger.info(
                "Waiting for pipeline workflow completion",
                workflow_id=workflow_handle.id,
            )

            if timeout_seconds:
                result = await asyncio.wait_for(
                    workflow_handle.result(), timeout=timeout_seconds
                )
            else:
                result = await workflow_handle.result()

            self._logger.info(
                "Pipeline workflow completed successfully",
                workflow_id=workflow_handle.id,
                status=result.get("status", "unknown"),
            )

            return result

        except asyncio.TimeoutError:
            self._logger.error(
                "Pipeline workflow timeout",
                workflow_id=workflow_handle.id,
                timeout=timeout_seconds,
            )
            raise TemporalClientError(
                f"Workflow timeout after {timeout_seconds} seconds"
            )

        except Exception as e:
            self._logger.error(
                "Pipeline workflow failed", workflow_id=workflow_handle.id, error=str(e)
            )
            raise TemporalClientError(f"Workflow failed: {e}")

    async def cancel_pipeline(self, workflow_handle: WorkflowHandle) -> None:
        """Отмена выполнения pipeline"""

        try:
            self._logger.info(
                "Cancelling pipeline workflow", workflow_id=workflow_handle.id
            )

            await workflow_handle.cancel()

            self._logger.info(
                "Pipeline workflow cancelled", workflow_id=workflow_handle.id
            )

        except Exception as e:
            self._logger.error(
                "Failed to cancel pipeline workflow",
                workflow_id=workflow_handle.id,
                error=str(e),
            )
            raise TemporalClientError(f"Failed to cancel workflow: {e}")

    async def get_workflow_status(self, workflow_id: str) -> Dict[str, Any]:
        """Получение статуса workflow"""

        try:
            handle = self._client.get_workflow_handle(workflow_id)

            # Получаем описание workflow
            description = await handle.describe()

            status_info = {
                "workflow_id": workflow_id,
                "run_id": handle.result_run_id,
                "status": description.status.name,
                "start_time": description.start_time.isoformat()
                if description.start_time
                else None,
                "close_time": description.close_time.isoformat()
                if description.close_time
                else None,
                "execution_time": None,
                "task_queue": description.task_queue,
            }

            # Вычисляем время выполнения
            if description.start_time and description.close_time:
                execution_time = (
                    description.close_time - description.start_time
                ).total_seconds()
                status_info["execution_time"] = execution_time

            return status_info

        except Exception as e:
            self._logger.error(
                "Failed to get workflow status", workflow_id=workflow_id, error=str(e)
            )
            raise TemporalClientError(f"Failed to get workflow status: {e}")

    async def list_workflows(
        self,
        pipeline_name: Optional[str] = None,
        status_filter: Optional[str] = None,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """Получение списка workflows"""

        try:
            # Строим запрос для поиска
            query_parts = []

            if pipeline_name:
                query_parts.append(f"WorkflowType='DataPipelineWorkflow'")
                # Можно добавить фильтрацию по имени pipeline через SearchAttributes

            if status_filter:
                query_parts.append(f"ExecutionStatus='{status_filter}'")

            query = (
                " AND ".join(query_parts)
                if query_parts
                else "WorkflowType='DataPipelineWorkflow'"
            )

            # Выполняем поиск
            workflows = []
            async for workflow in self._client.list_workflows(query):
                workflow_info = {
                    "workflow_id": workflow.id,
                    "run_id": workflow.run_id,
                    "status": workflow.status.name,
                    "start_time": workflow.start_time.isoformat()
                    if workflow.start_time
                    else None,
                    "close_time": workflow.close_time.isoformat()
                    if workflow.close_time
                    else None,
                    "task_queue": workflow.task_queue,
                }
                workflows.append(workflow_info)

                if len(workflows) >= limit:
                    break

            return workflows

        except Exception as e:
            self._logger.error("Failed to list workflows", error=str(e))
            raise TemporalClientError(f"Failed to list workflows: {e}")

    def is_connected(self) -> bool:
        """Проверка статуса подключения"""
        return self._is_connected

    def get_worker_count(self) -> int:
        """Получение количества активных workers"""
        return len(self._workers)


# Factory function для создания клиента
async def create_temporal_client(temporal_config: TemporalConfig) -> TemporalClient:
    """Создание и подключение Temporal клиента"""
    client = TemporalClient(temporal_config)
    if temporal_config.enabled:
        await client.connect()
    return client
