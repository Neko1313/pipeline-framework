"""
Command Line Interface для pipeline framework

Предоставляет команды для:
- Управления pipeline
- Валидации конфигураций
- Работы с компонентами
- Temporal интеграции
- Мониторинга
"""

import sys
import asyncio
from pathlib import Path
from typing import Optional, List, Dict, Any

import typer
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.text import Text
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.tree import Tree
from rich.syntax import Syntax
import yaml

from pipeline_core.config.yaml_loader import load_pipeline_config, validate_config_file
from pipeline_core.config.models import RuntimeConfig
from pipeline_core.pipeline.executor import PipelineBuilder
from pipeline_core.registry.component_registry import ComponentRegistry
from pipeline_core.temporal.client import TemporalClient, create_temporal_client
from pipeline_core.observability import (
    setup_logging,
    configure_for_development,
    start_metrics_server,
)

app = typer.Typer(
    name="pipeline",
    help="Pipeline Framework CLI - управление data workflows",
    add_completion=False,
)

# Создаем подкоманды
config_app = typer.Typer(name="config", help="Команды для работы с конфигурацией")
component_app = typer.Typer(name="component", help="Команды для работы с компонентами")
temporal_app = typer.Typer(name="temporal", help="Команды для работы с Temporal")
metrics_app = typer.Typer(name="metrics", help="Команды для работы с метриками")

app.add_typer(config_app)
app.add_typer(component_app)
app.add_typer(temporal_app)
app.add_typer(metrics_app)

# Console для rich output
console = Console()


# === Основные команды ===


@app.command()
def run(
    config_path: str = typer.Argument(..., help="Путь к конфигурации pipeline"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Режим сухого запуска"),
    environment: str = typer.Option(
        "development", "--env", help="Окружение выполнения"
    ),
    resume_from: Optional[str] = typer.Option(
        None, "--resume-from", help="Возобновить с этапа"
    ),
    skip_stages: Optional[List[str]] = typer.Option(
        None, "--skip", help="Пропустить этапы"
    ),
    use_temporal: bool = typer.Option(
        False, "--temporal", help="Использовать Temporal для выполнения"
    ),
    temporal_server: str = typer.Option(
        "localhost:7233", "--temporal-server", help="Адрес Temporal сервера"
    ),
    log_level: str = typer.Option("INFO", "--log-level", help="Уровень логирования"),
    output_format: str = typer.Option(
        "console", "--output", help="Формат вывода (console/json)"
    ),
):
    """Запуск pipeline"""

    # Настройка логирования
    if environment == "development":
        configure_for_development()
    else:
        setup_logging()

    console.print(f"🚀 Запуск pipeline: [bold]{config_path}[/bold]")

    try:
        # Создаем runtime конфигурацию
        runtime_config = RuntimeConfig(
            run_id=f"cli_{int(typer.get_current_context().resilient_parsing)}",
            dry_run=dry_run,
            resume_from_stage=resume_from,
            skip_stages=skip_stages or [],
            triggered_by="cli",
            environment_overrides={"ENVIRONMENT": environment},
        )

        if use_temporal:
            # Запуск через Temporal
            asyncio.run(
                _run_pipeline_temporal(config_path, runtime_config, temporal_server)
            )
        else:
            # Локальный запуск
            asyncio.run(_run_pipeline_local(config_path, runtime_config))

        console.print("✅ Pipeline выполнен успешно", style="green")

    except Exception as e:
        console.print(f"❌ Ошибка выполнения pipeline: {e}", style="red")
        sys.exit(1)


async def _run_pipeline_local(config_path: str, runtime_config: RuntimeConfig):
    """Локальный запуск pipeline"""

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console,
        transient=True,
    ) as progress:
        # Загрузка конфигурации
        task = progress.add_task("Загрузка конфигурации...", total=None)
        full_config = load_pipeline_config(config_path, runtime_config=runtime_config)
        progress.update(task, description="✅ Конфигурация загружена")

        # Создание и выполнение pipeline
        progress.update(task, description="Создание pipeline...")
        pipeline = PipelineBuilder.from_config(full_config.pipeline)

        progress.update(task, description="Выполнение pipeline...")
        result = await pipeline.execute(runtime_config)

        progress.remove_task(task)

    # Отображение результатов
    _display_pipeline_result(result)


async def _run_pipeline_temporal(
    config_path: str, runtime_config: RuntimeConfig, server_address: str
):
    """Запуск через Temporal"""

    from ..config.models import TemporalConfig

    temporal_config = TemporalConfig(enabled=True, server_address=server_address)

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console,
        transient=True,
    ) as progress:
        # Подключение к Temporal
        task = progress.add_task("Подключение к Temporal...", total=None)
        temporal_client = await create_temporal_client(temporal_config)

        # Загрузка конфигурации
        progress.update(task, description="Загрузка конфигурации...")
        full_config = load_pipeline_config(config_path, runtime_config=runtime_config)

        # Запуск workflow
        progress.update(task, description="Запуск Temporal workflow...")
        workflow_handle = await temporal_client.execute_pipeline(
            full_config.pipeline, runtime_config
        )

        progress.update(
            task, description=f"Ожидание завершения workflow {workflow_handle.id}..."
        )
        result = await temporal_client.wait_for_pipeline_completion(workflow_handle)

        progress.remove_task(task)

        await temporal_client.disconnect()

    console.print(f"✅ Temporal workflow завершен: [bold]{workflow_handle.id}[/bold]")


def _display_pipeline_result(result):
    """Отображение результатов выполнения pipeline"""

    # Основная информация
    status_color = "green" if result.is_success() else "red"
    status_icon = "✅" if result.is_success() else "❌"

    console.print(
        Panel(
            f"{status_icon} Pipeline: [bold]{result.pipeline_id}[/bold]\n"
            f"Run ID: {result.run_id}\n"
            f"Status: [{status_color}]{result.status.value.upper()}[/{status_color}]\n"
            f"Duration: {result.metrics.duration_seconds:.2f}s\n"
            f"Stages: {result.metrics.completed_stages}/{result.metrics.total_stages}",
            title="Pipeline Result",
            border_style=status_color,
        )
    )

    # Таблица результатов этапов
    if result.stage_results:
        table = Table(title="Stage Results")
        table.add_column("Stage", style="cyan")
        table.add_column("Status", style="magenta")
        table.add_column("Duration", style="green")
        table.add_column("Rows", style="blue")

        for stage_name, stage_result in result.stage_results.items():
            status_style = "green" if stage_result.is_success() else "red"
            duration = (
                f"{stage_result.metadata.duration_seconds:.2f}s"
                if stage_result.metadata.duration_seconds
                else "N/A"
            )
            rows = (
                str(stage_result.metadata.rows_processed)
                if stage_result.metadata.rows_processed
                else "N/A"
            )

            table.add_row(
                stage_name,
                f"[{status_style}]{stage_result.status.value}[/{status_style}]",
                duration,
                rows,
            )

        console.print(table)


# === Команды конфигурации ===


@config_app.command("validate")
def validate_config(
    config_path: str = typer.Argument(..., help="Путь к конфигурации pipeline"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Подробный вывод"),
):
    """Валидация конфигурации pipeline"""

    console.print(f"🔍 Валидация конфигурации: [bold]{config_path}[/bold]")

    try:
        errors = validate_config_file(config_path)

        if not errors:
            console.print("✅ Конфигурация валидна", style="green")

            if verbose:
                # Показываем детали конфигурации
                full_config = load_pipeline_config(config_path)
                _display_config_details(full_config.pipeline)
        else:
            console.print("❌ Найдены ошибки в конфигурации:", style="red")
            for error in errors:
                console.print(f"  • {error}", style="red")
            sys.exit(1)

    except Exception as e:
        console.print(f"❌ Ошибка при валидации: {e}", style="red")
        sys.exit(1)


@config_app.command("show")
def show_config(
    config_path: str = typer.Argument(..., help="Путь к конфигурации pipeline"),
    format: str = typer.Option("yaml", "--format", help="Формат вывода (yaml/json)"),
):
    """Отображение конфигурации pipeline"""

    try:
        full_config = load_pipeline_config(config_path)
        config_dict = full_config.pipeline.dict()

        if format == "yaml":
            syntax = Syntax(yaml.dump(config_dict, default_flow_style=False), "yaml")
            console.print(syntax)
        elif format == "json":
            import json

            syntax = Syntax(json.dumps(config_dict, indent=2), "json")
            console.print(syntax)
        else:
            console.print(
                "❌ Неподдерживаемый формат. Используйте 'yaml' или 'json'", style="red"
            )
            sys.exit(1)

    except Exception as e:
        console.print(f"❌ Ошибка загрузки конфигурации: {e}", style="red")
        sys.exit(1)


def _display_config_details(pipeline_config):
    """Отображение деталей конфигурации"""

    # Основная информация
    console.print(
        Panel(
            f"Name: [bold]{pipeline_config.name}[/bold]\n"
            f"Version: {pipeline_config.version}\n"
            f"Environment: {pipeline_config.metadata.environment.value}\n"
            f"Owner: {pipeline_config.metadata.owner or 'N/A'}\n"
            f"Description: {pipeline_config.metadata.description or 'N/A'}",
            title="Pipeline Information",
        )
    )

    # Дерево этапов
    if pipeline_config.stages:
        tree = Tree("🔄 Pipeline Stages")

        for stage in pipeline_config.stages:
            stage_node = tree.add(f"[cyan]{stage.name}[/cyan] ({stage.component})")

            if stage.depends_on:
                deps_node = stage_node.add("📋 Dependencies")
                for dep in stage.depends_on:
                    deps_node.add(f"[yellow]{dep}[/yellow]")

            if stage.retry_policy.maximum_attempts > 1:
                stage_node.add(
                    f"🔄 Retry: {stage.retry_policy.maximum_attempts} attempts"
                )

            if stage.timeout_seconds != 3600:
                stage_node.add(f"⏱️ Timeout: {stage.timeout_seconds}s")

        console.print(tree)


# === Команды компонентов ===


@component_app.command("list")
def list_components(
    component_type: Optional[str] = typer.Option(
        None, "--type", help="Фильтр по типу компонента"
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Подробный вывод"),
):
    """Список зарегистрированных компонентов"""

    registry = ComponentRegistry()
    components = registry.list_components(component_type)

    if not components:
        console.print("❌ Компоненты не найдены", style="yellow")
        return

    table = Table(title=f"Registered Components ({len(components)})")
    table.add_column("Type", style="cyan")
    table.add_column("Name", style="magenta")
    table.add_column("Version", style="green")
    table.add_column("Source", style="blue")

    if verbose:
        table.add_column("Description", style="white")
        table.add_column("Module", style="dim")

    for component in components:
        row = [
            component.component_type.value,
            component.name,
            component.version,
            component.source.value,
        ]

        if verbose:
            row.extend(
                [
                    component.description[:50] + "..."
                    if len(component.description) > 50
                    else component.description,
                    component.module_path,
                ]
            )

        table.add_row(*row)

    console.print(table)


@component_app.command("info")
def component_info(
    component_type: str = typer.Argument(..., help="Тип компонента"),
    component_name: str = typer.Argument(..., help="Имя компонента"),
):
    """Подробная информация о компоненте"""

    registry = ComponentRegistry()
    component_info = registry.get_component_info(component_type, component_name)

    if not component_info:
        console.print(
            f"❌ Компонент {component_type}/{component_name} не найден", style="red"
        )
        sys.exit(1)

    console.print(
        Panel(
            f"Type: [cyan]{component_info.component_type.value}[/cyan]\n"
            f"Name: [bold]{component_info.name}[/bold]\n"
            f"Version: [green]{component_info.version}[/green]\n"
            f"Source: [blue]{component_info.source.value}[/blue]\n"
            f"Module: [dim]{component_info.module_path}[/dim]\n\n"
            f"Description:\n{component_info.description}",
            title=f"Component: {component_type}/{component_name}",
        )
    )

    # Зависимости
    if component_info.dependencies:
        deps_text = Text("Dependencies:\n")
        for dep in component_info.dependencies:
            deps_text.append(f"  • {dep}\n", style="yellow")
        console.print(deps_text)


@component_app.command("validate")
def validate_components():
    """Валидация всех зарегистрированных компонентов"""

    registry = ComponentRegistry()

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console,
    ) as progress:
        task = progress.add_task("Валидация компонентов...", total=None)
        validation_results = registry.validate_all_components()

    # Отображение результатов
    valid_count = len(validation_results["valid"])
    invalid_count = len(validation_results["invalid"])

    console.print(f"✅ Валидных компонентов: {valid_count}", style="green")

    if invalid_count > 0:
        console.print(f"❌ Невалидных компонентов: {invalid_count}", style="red")
        for invalid in validation_results["invalid"]:
            console.print(f"  • {invalid}", style="red")
        sys.exit(1)


# === Команды Temporal ===


@temporal_app.command("worker")
def start_temporal_worker(
    server: str = typer.Option(
        "localhost:7233", "--server", help="Адрес Temporal сервера"
    ),
    namespace: str = typer.Option("default", "--namespace", help="Temporal namespace"),
    task_queue: str = typer.Option("pipeline-tasks", "--task-queue", help="Task queue"),
    max_activities: int = typer.Option(
        100, "--max-activities", help="Максимум concurrent activities"
    ),
    max_workflows: int = typer.Option(
        100, "--max-workflows", help="Максимум concurrent workflows"
    ),
):
    """Запуск Temporal Worker"""

    console.print(f"🔄 Запуск Temporal Worker...")
    console.print(f"Server: [bold]{server}[/bold]")
    console.print(f"Namespace: [bold]{namespace}[/bold]")
    console.print(f"Task Queue: [bold]{task_queue}[/bold]")

    from ..config.models import TemporalConfig

    temporal_config = TemporalConfig(
        enabled=True, server_address=server, namespace=namespace, task_queue=task_queue
    )

    async def run_worker():
        temporal_client = await create_temporal_client(temporal_config)
        worker = await temporal_client.start_worker(
            task_queue=task_queue,
            max_concurrent_activities=max_activities,
            max_concurrent_workflows=max_workflows,
        )

        console.print("✅ Temporal Worker запущен. Нажмите Ctrl+C для остановки.")

        try:
            # Держим worker работающим
            while True:
                await asyncio.sleep(1)
        except KeyboardInterrupt:
            console.print("🛑 Остановка Temporal Worker...")
            await temporal_client.stop_all_workers()
            await temporal_client.disconnect()

    try:
        asyncio.run(run_worker())
    except KeyboardInterrupt:
        console.print("✅ Temporal Worker остановлен")


@temporal_app.command("status")
def temporal_status(
    workflow_id: str = typer.Argument(..., help="ID Workflow"),
    server: str = typer.Option(
        "localhost:7233", "--server", help="Адрес Temporal сервера"
    ),
    namespace: str = typer.Option("default", "--namespace", help="Temporal namespace"),
):
    """Статус Temporal Workflow"""

    from ..config.models import TemporalConfig

    temporal_config = TemporalConfig(
        enabled=True, server_address=server, namespace=namespace
    )

    async def get_status():
        temporal_client = await create_temporal_client(temporal_config)
        status = await temporal_client.get_workflow_status(workflow_id)
        await temporal_client.disconnect()
        return status

    try:
        status = asyncio.run(get_status())

        console.print(
            Panel(
                f"Workflow ID: [bold]{status['workflow_id']}[/bold]\n"
                f"Run ID: {status['run_id']}\n"
                f"Status: [cyan]{status['status']}[/cyan]\n"
                f"Start Time: {status['start_time'] or 'N/A'}\n"
                f"Close Time: {status['close_time'] or 'N/A'}\n"
                f"Execution Time: {status['execution_time'] or 'N/A'}s\n"
                f"Task Queue: {status['task_queue']}",
                title="Temporal Workflow Status",
            )
        )

    except Exception as e:
        console.print(f"❌ Ошибка получения статуса: {e}", style="red")
        sys.exit(1)


# === Команды метрик ===


@metrics_app.command("server")
def start_metrics_server_cmd(
    port: int = typer.Option(8000, "--port", help="Порт для сервера метрик"),
    host: str = typer.Option("0.0.0.0", "--host", help="Хост для сервера метрик"),
):
    """Запуск сервера метрик"""

    console.print(f"📊 Запуск сервера метрик на [bold]{host}:{port}[/bold]")

    try:
        server = start_metrics_server(port, host)
        console.print(f"✅ Сервер метрик запущен: http://{host}:{port}/metrics")
        console.print("Нажмите Ctrl+C для остановки")

        # Держим сервер работающим
        try:
            while True:
                import time

                time.sleep(1)
        except KeyboardInterrupt:
            console.print("🛑 Остановка сервера метрик...")
            server.stop()
            console.print("✅ Сервер метрик остановлен")

    except Exception as e:
        console.print(f"❌ Ошибка запуска сервера метрик: {e}", style="red")
        sys.exit(1)


@metrics_app.command("export")
def export_metrics():
    """Экспорт текущих метрик"""

    from ..observability.metrics import get_default_metrics_collector

    collector = get_default_metrics_collector()
    metrics_data = collector.export_metrics()

    console.print("📊 Текущие метрики:")
    console.print(metrics_data)


# === Утилиты ===


@app.command()
def init(
    name: str = typer.Argument(..., help="Имя нового pipeline"),
    template: str = typer.Option(
        "basic", "--template", help="Шаблон pipeline (basic/sql/etl)"
    ),
    output_dir: str = typer.Option(
        ".", "--output", help="Директория для создания проекта"
    ),
):
    """Создание нового pipeline проекта"""

    console.print(f"🚀 Создание нового pipeline: [bold]{name}[/bold]")

    project_dir = Path(output_dir) / name
    project_dir.mkdir(exist_ok=True)

    # Создаем базовую структуру
    (project_dir / "config").mkdir(exist_ok=True)
    (project_dir / "components").mkdir(exist_ok=True)
    (project_dir / "scripts").mkdir(exist_ok=True)

    # Создаем базовую конфигурацию
    basic_config = {
        "pipeline": {
            "name": name,
            "version": "1.0.0",
            "metadata": {
                "description": f"Pipeline {name}",
                "owner": "data-team@company.com",
                "environment": "development",
            },
            "stages": [
                {
                    "name": "extract-data",
                    "component": "extractor/sql",
                    "config": {
                        "connection_string": "${DATABASE_URL}",
                        "query": "SELECT * FROM source_table",
                    },
                },
                {
                    "name": "transform-data",
                    "component": "transformer/polars",
                    "depends_on": ["extract-data"],
                    "config": {
                        "operations": [
                            {"type": "filter", "condition": "column IS NOT NULL"}
                        ]
                    },
                },
                {
                    "name": "load-data",
                    "component": "loader/sql",
                    "depends_on": ["transform-data"],
                    "config": {
                        "connection_string": "${WAREHOUSE_URL}",
                        "table": "target_table",
                        "write_mode": "append",
                    },
                },
            ],
        }
    }

    config_file = project_dir / "config" / "pipeline.yaml"
    with open(config_file, "w") as f:
        yaml.dump(basic_config, f, default_flow_style=False)

    # Создаем README
    readme_content = f"""# {name} Pipeline

## Описание
Pipeline для обработки данных {name}.

## Структура
- `config/pipeline.yaml` - конфигурация pipeline
- `components/` - кастомные компоненты
- `scripts/` - вспомогательные скрипты

## Запуск
```bash
pipeline run config/pipeline.yaml
```

## Валидация
```bash
pipeline config validate config/pipeline.yaml
```
"""

    readme_file = project_dir / "README.md"
    with open(readme_file, "w") as f:
        f.write(readme_content)

    console.print(f"✅ Проект создан в: [bold]{project_dir}[/bold]")
    console.print("📁 Структура проекта:")
    console.print(f"  📄 {config_file}")
    console.print(f"  📄 {readme_file}")
    console.print(f"  📁 {project_dir / 'components'}")
    console.print(f"  📁 {project_dir / 'scripts'}")


@app.command()
def version():
    """Версия pipeline framework"""
    console.print("Pipeline Framework v0.1.0")


def main():
    """Entry point для CLI"""
    app()


if __name__ == "__main__":
    main()
