"""
Система метрик для pipeline framework

Обеспечивает:
- Prometheus метрики
- Кастомные метрики компонентов
- Агрегация данных
- Health checks
- Performance monitoring
"""

import time
import threading
from contextlib import contextmanager
from typing import Dict, List, Optional, Any, Union
from datetime import datetime, timezone
from dataclasses import dataclass, field
from enum import Enum

from prometheus_client import (
    Counter,
    Histogram,
    Gauge,
    Summary,
    Info,
    CollectorRegistry,
    generate_latest,
    CONTENT_TYPE_LATEST,
    start_http_server,
    MetricsHandler,
)
import structlog

logger = structlog.get_logger(__name__)


class MetricType(Enum):
    """Типы метрик"""

    COUNTER = "counter"
    HISTOGRAM = "histogram"
    GAUGE = "gauge"
    SUMMARY = "summary"
    INFO = "info"


@dataclass
class MetricDefinition:
    """Определение метрики"""

    name: str
    help: str
    metric_type: MetricType
    labels: List[str] = field(default_factory=list)
    buckets: Optional[List[float]] = None  # Для histogram
    objectives: Optional[Dict[float, float]] = None  # Для summary


class MetricsCollector:
    """
    Централизованный сборщик метрик для pipeline framework

    Предоставляет:
    - Стандартные метрики для всех компонентов
    - Кастомные метрики
    - Prometheus экспорт
    - Агрегацию данных
    """

    def __init__(self, registry: Optional[CollectorRegistry] = None):
        self.registry = registry or CollectorRegistry()
        self._metrics: Dict[str, Any] = {}
        self._metric_definitions: Dict[str, MetricDefinition] = {}
        self._lock = threading.Lock()

        # Инициализируем стандартные метрики
        self._init_standard_metrics()

        self._logger = structlog.get_logger(component="metrics_collector")

    def _init_standard_metrics(self):
        """Инициализация стандартных метрик pipeline"""

        # Метрики выполнения компонентов
        self.register_metric(
            MetricDefinition(
                name="pipeline_component_executions_total",
                help="Total number of component executions",
                metric_type=MetricType.COUNTER,
                labels=["component_type", "component_name", "status", "pipeline_id"],
            )
        )

        self.register_metric(
            MetricDefinition(
                name="pipeline_component_duration_seconds",
                help="Component execution duration in seconds",
                metric_type=MetricType.HISTOGRAM,
                labels=["component_type", "component_name", "pipeline_id"],
                buckets=[
                    0.1,
                    0.5,
                    1.0,
                    5.0,
                    10.0,
                    30.0,
                    60.0,
                    300.0,
                    600.0,
                    1800.0,
                    3600.0,
                ],
            )
        )

        self.register_metric(
            MetricDefinition(
                name="pipeline_component_memory_usage_bytes",
                help="Component memory usage in bytes",
                metric_type=MetricType.GAUGE,
                labels=["component_type", "component_name", "pipeline_id"],
            )
        )

        # Метрики данных
        self.register_metric(
            MetricDefinition(
                name="pipeline_data_processed_rows_total",
                help="Total number of data rows processed",
                metric_type=MetricType.COUNTER,
                labels=["component_type", "component_name", "pipeline_id"],
            )
        )

        self.register_metric(
            MetricDefinition(
                name="pipeline_data_processed_bytes_total",
                help="Total bytes of data processed",
                metric_type=MetricType.COUNTER,
                labels=["component_type", "component_name", "pipeline_id"],
            )
        )

        # Метрики pipeline
        self.register_metric(
            MetricDefinition(
                name="pipeline_executions_total",
                help="Total number of pipeline executions",
                metric_type=MetricType.COUNTER,
                labels=["pipeline_id", "status", "environment"],
            )
        )

        self.register_metric(
            MetricDefinition(
                name="pipeline_duration_seconds",
                help="Pipeline execution duration in seconds",
                metric_type=MetricType.HISTOGRAM,
                labels=["pipeline_id", "environment"],
                buckets=[
                    10.0,
                    30.0,
                    60.0,
                    300.0,
                    600.0,
                    1800.0,
                    3600.0,
                    7200.0,
                    14400.0,
                ],
            )
        )

        self.register_metric(
            MetricDefinition(
                name="pipeline_active_executions",
                help="Number of currently active pipeline executions",
                metric_type=MetricType.GAUGE,
                labels=["pipeline_id"],
            )
        )

        # Метрики ошибок
        self.register_metric(
            MetricDefinition(
                name="pipeline_errors_total",
                help="Total number of pipeline errors",
                metric_type=MetricType.COUNTER,
                labels=["pipeline_id", "stage_name", "error_type"],
            )
        )

        self.register_metric(
            MetricDefinition(
                name="pipeline_retries_total",
                help="Total number of retries",
                metric_type=MetricType.COUNTER,
                labels=["pipeline_id", "stage_name", "attempt"],
            )
        )

        # Метрики качества данных
        self.register_metric(
            MetricDefinition(
                name="pipeline_quality_checks_total",
                help="Total number of quality checks executed",
                metric_type=MetricType.COUNTER,
                labels=["pipeline_id", "check_name", "status"],
            )
        )

        # Temporal метрики (если используется)
        self.register_metric(
            MetricDefinition(
                name="temporal_workflow_executions_total",
                help="Total number of Temporal workflow executions",
                metric_type=MetricType.COUNTER,
                labels=["workflow_type", "status"],
            )
        )

        self.register_metric(
            MetricDefinition(
                name="temporal_activity_executions_total",
                help="Total number of Temporal activity executions",
                metric_type=MetricType.COUNTER,
                labels=["activity_type", "status"],
            )
        )

        # Framework метрики
        self.register_metric(
            MetricDefinition(
                name="pipeline_framework_info",
                help="Pipeline framework information",
                metric_type=MetricType.INFO,
                labels=["version", "python_version", "platform"],
            )
        )

        # Системные метрики
        self.register_metric(
            MetricDefinition(
                name="pipeline_system_cpu_usage_percent",
                help="System CPU usage percentage",
                metric_type=MetricType.GAUGE,
            )
        )

        self.register_metric(
            MetricDefinition(
                name="pipeline_system_memory_usage_bytes",
                help="System memory usage in bytes",
                metric_type=MetricType.GAUGE,
            )
        )

    def register_metric(self, definition: MetricDefinition) -> None:
        """Регистрация новой метрики"""

        with self._lock:
            if definition.name in self._metrics:
                self._logger.warning(
                    "Metric already registered", metric=definition.name
                )
                return

            # Создаем метрику в зависимости от типа
            if definition.metric_type == MetricType.COUNTER:
                metric = Counter(
                    definition.name,
                    definition.help,
                    definition.labels,
                    registry=self.registry,
                )
            elif definition.metric_type == MetricType.HISTOGRAM:
                metric = Histogram(
                    definition.name,
                    definition.help,
                    definition.labels,
                    buckets=definition.buckets,
                    registry=self.registry,
                )
            elif definition.metric_type == MetricType.GAUGE:
                metric = Gauge(
                    definition.name,
                    definition.help,
                    definition.labels,
                    registry=self.registry,
                )
            elif definition.metric_type == MetricType.SUMMARY:
                metric = Summary(
                    definition.name,
                    definition.help,
                    definition.labels,
                    objectives=definition.objectives,
                    registry=self.registry,
                )
            elif definition.metric_type == MetricType.INFO:
                metric = Info(definition.name, definition.help, registry=self.registry)
            else:
                raise ValueError(f"Unsupported metric type: {definition.metric_type}")

            self._metrics[definition.name] = metric
            self._metric_definitions[definition.name] = definition

            self._logger.debug(
                "Metric registered",
                metric=definition.name,
                type=definition.metric_type.value,
            )

    def get_metric(self, name: str) -> Optional[Any]:
        """Получение метрики по имени"""
        return self._metrics.get(name)

    def increment_counter(
        self, name: str, labels: Optional[Dict[str, str]] = None, value: float = 1.0
    ):
        """Увеличение счетчика"""
        metric = self.get_metric(name)
        if metric and hasattr(metric, "inc"):
            if labels:
                metric.labels(**labels).inc(value)
            else:
                metric.inc(value)

    def observe_histogram(
        self, name: str, value: float, labels: Optional[Dict[str, str]] = None
    ):
        """Добавление наблюдения в гистограмму"""
        metric = self.get_metric(name)
        if metric and hasattr(metric, "observe"):
            if labels:
                metric.labels(**labels).observe(value)
            else:
                metric.observe(value)

    def set_gauge(
        self, name: str, value: float, labels: Optional[Dict[str, str]] = None
    ):
        """Установка значения gauge"""
        metric = self.get_metric(name)
        if metric and hasattr(metric, "set"):
            if labels:
                metric.labels(**labels).set(value)
            else:
                metric.set(value)

    def inc_gauge(
        self, name: str, value: float = 1.0, labels: Optional[Dict[str, str]] = None
    ):
        """Увеличение значения gauge"""
        metric = self.get_metric(name)
        if metric and hasattr(metric, "inc"):
            if labels:
                metric.labels(**labels).inc(value)
            else:
                metric.inc(value)

    def dec_gauge(
        self, name: str, value: float = 1.0, labels: Optional[Dict[str, str]] = None
    ):
        """Уменьшение значения gauge"""
        metric = self.get_metric(name)
        if metric and hasattr(metric, "dec"):
            if labels:
                metric.labels(**labels).dec(value)
            else:
                metric.dec(value)

    def set_info(self, name: str, info: Dict[str, str]):
        """Установка информационной метрики"""
        metric = self.get_metric(name)
        if metric and hasattr(metric, "info"):
            metric.info(info)

    @contextmanager
    def time_histogram(self, name: str, labels: Optional[Dict[str, str]] = None):
        """Context manager для измерения времени выполнения"""
        start_time = time.time()
        try:
            yield
        finally:
            duration = time.time() - start_time
            self.observe_histogram(name, duration, labels)

    @contextmanager
    def track_active_gauge(self, name: str, labels: Optional[Dict[str, str]] = None):
        """Context manager для отслеживания активных операций"""
        self.inc_gauge(name, 1.0, labels)
        try:
            yield
        finally:
            self.dec_gauge(name, 1.0, labels)

    def export_metrics(self) -> str:
        """Экспорт метрик в формате Prometheus"""
        return generate_latest(self.registry).decode("utf-8")

    def get_metrics_summary(self) -> Dict[str, Any]:
        """Получение сводки по метрикам"""
        summary = {
            "total_metrics": len(self._metrics),
            "metrics_by_type": {},
            "metric_names": list(self._metrics.keys()),
        }

        # Группируем по типам
        for definition in self._metric_definitions.values():
            metric_type = definition.metric_type.value
            if metric_type not in summary["metrics_by_type"]:
                summary["metrics_by_type"][metric_type] = 0
            summary["metrics_by_type"][metric_type] += 1

        return summary


class ComponentMetricsTracker:
    """Трекер метрик для компонентов pipeline"""

    def __init__(
        self,
        metrics_collector: MetricsCollector,
        component_type: str,
        component_name: str,
        pipeline_id: str,
    ):
        self.metrics = metrics_collector
        self.component_type = component_type
        self.component_name = component_name
        self.pipeline_id = pipeline_id

        self._base_labels = {
            "component_type": component_type,
            "component_name": component_name,
            "pipeline_id": pipeline_id,
        }

    def track_execution_start(self):
        """Отметка начала выполнения компонента"""
        self.metrics.inc_gauge(
            "pipeline_active_executions", labels={"pipeline_id": self.pipeline_id}
        )

    def track_execution_end(self, status: str, duration: float):
        """Отметка завершения выполнения компонента"""
        # Уменьшаем счетчик активных выполнений
        self.metrics.dec_gauge(
            "pipeline_active_executions", labels={"pipeline_id": self.pipeline_id}
        )

        # Увеличиваем счетчик выполнений
        labels = {**self._base_labels, "status": status}
        self.metrics.increment_counter("pipeline_component_executions_total", labels)

        # Записываем продолжительность
        self.metrics.observe_histogram(
            "pipeline_component_duration_seconds", duration, self._base_labels
        )

    def track_data_processed(
        self, rows: Optional[int] = None, bytes_processed: Optional[int] = None
    ):
        """Отметка обработанных данных"""
        if rows is not None:
            self.metrics.increment_counter(
                "pipeline_data_processed_rows_total", self._base_labels, rows
            )

        if bytes_processed is not None:
            self.metrics.increment_counter(
                "pipeline_data_processed_bytes_total",
                self._base_labels,
                bytes_processed,
            )

    def track_memory_usage(self, memory_bytes: float):
        """Отметка использования памяти"""
        self.metrics.set_gauge(
            "pipeline_component_memory_usage_bytes", memory_bytes, self._base_labels
        )

    def track_error(self, error_type: str, stage_name: str):
        """Отметка ошибки"""
        self.metrics.increment_counter(
            "pipeline_errors_total",
            {
                "pipeline_id": self.pipeline_id,
                "stage_name": stage_name,
                "error_type": error_type,
            },
        )

    def track_retry(self, stage_name: str, attempt: int):
        """Отметка повторной попытки"""
        self.metrics.increment_counter(
            "pipeline_retries_total",
            {
                "pipeline_id": self.pipeline_id,
                "stage_name": stage_name,
                "attempt": str(attempt),
            },
        )


class PipelineMetricsTracker:
    """Трекер метрик для pipeline"""

    def __init__(
        self,
        metrics_collector: MetricsCollector,
        pipeline_id: str,
        environment: str = "development",
    ):
        self.metrics = metrics_collector
        self.pipeline_id = pipeline_id
        self.environment = environment

        self._base_labels = {"pipeline_id": pipeline_id, "environment": environment}

    def track_pipeline_start(self):
        """Отметка начала выполнения pipeline"""
        self.metrics.inc_gauge(
            "pipeline_active_executions", labels={"pipeline_id": self.pipeline_id}
        )

    def track_pipeline_end(self, status: str, duration: float):
        """Отметка завершения выполнения pipeline"""
        # Уменьшаем счетчик активных выполнений
        self.metrics.dec_gauge(
            "pipeline_active_executions", labels={"pipeline_id": self.pipeline_id}
        )

        # Увеличиваем счетчик выполнений
        labels = {**self._base_labels, "status": status}
        self.metrics.increment_counter("pipeline_executions_total", labels)

        # Записываем продолжительность
        self.metrics.observe_histogram(
            "pipeline_duration_seconds", duration, self._base_labels
        )

    def track_quality_check(self, check_name: str, status: str):
        """Отметка выполнения quality check"""
        self.metrics.increment_counter(
            "pipeline_quality_checks_total",
            {
                "pipeline_id": self.pipeline_id,
                "check_name": check_name,
                "status": status,
            },
        )


class SystemMetricsCollector:
    """Сборщик системных метрик"""

    def __init__(self, metrics_collector: MetricsCollector):
        self.metrics = metrics_collector
        self._logger = structlog.get_logger(component="system_metrics")

    def collect_system_metrics(self):
        """Сбор системных метрик"""
        try:
            import psutil

            # CPU usage
            cpu_percent = psutil.cpu_percent(interval=1)
            self.metrics.set_gauge("pipeline_system_cpu_usage_percent", cpu_percent)

            # Memory usage
            memory = psutil.virtual_memory()
            self.metrics.set_gauge("pipeline_system_memory_usage_bytes", memory.used)

            self._logger.debug(
                "System metrics collected",
                cpu_percent=cpu_percent,
                memory_used_gb=memory.used / (1024**3),
            )

        except ImportError:
            self._logger.warning("psutil not available, skipping system metrics")
        except Exception as e:
            self._logger.error("Failed to collect system metrics", error=str(e))


class MetricsServer:
    """HTTP сервер для экспорта метрик"""

    def __init__(
        self,
        metrics_collector: MetricsCollector,
        port: int = 8000,
        addr: str = "0.0.0.0",
    ):
        self.metrics = metrics_collector
        self.port = port
        self.addr = addr
        self._server = None
        self._logger = structlog.get_logger(component="metrics_server")

    def start(self):
        """Запуск HTTP сервера метрик"""
        try:
            self._server = start_http_server(
                self.port, addr=self.addr, registry=self.metrics.registry
            )

            self._logger.info(
                "Metrics server started",
                port=self.port,
                addr=self.addr,
                endpoint=f"http://{self.addr}:{self.port}/metrics",
            )

        except Exception as e:
            self._logger.error("Failed to start metrics server", error=str(e))
            raise

    def stop(self):
        """Остановка HTTP сервера метрик"""
        if self._server:
            self._server.shutdown()
            self._server = None
            self._logger.info("Metrics server stopped")


# Глобальный экземпляр для convenience
_default_metrics_collector: Optional[MetricsCollector] = None


def get_default_metrics_collector() -> MetricsCollector:
    """Получение глобального экземпляра MetricsCollector"""
    global _default_metrics_collector

    if _default_metrics_collector is None:
        _default_metrics_collector = MetricsCollector()

        # Устанавливаем информацию о framework
        try:
            import sys
            import platform

            _default_metrics_collector.set_info(
                "pipeline_framework_info",
                {
                    "version": "0.1.0",
                    "python_version": f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}",
                    "platform": platform.system(),
                },
            )
        except Exception:
            pass

    return _default_metrics_collector


def create_component_tracker(
    component_type: str, component_name: str, pipeline_id: str
) -> ComponentMetricsTracker:
    """Создание трекера метрик для компонента"""
    return ComponentMetricsTracker(
        get_default_metrics_collector(), component_type, component_name, pipeline_id
    )


def create_pipeline_tracker(
    pipeline_id: str, environment: str = "development"
) -> PipelineMetricsTracker:
    """Создание трекера метрик для pipeline"""
    return PipelineMetricsTracker(
        get_default_metrics_collector(), pipeline_id, environment
    )


def start_metrics_server(port: int = 8000, addr: str = "0.0.0.0") -> MetricsServer:
    """Запуск сервера метрик"""
    server = MetricsServer(get_default_metrics_collector(), port, addr)
    server.start()
    return server
