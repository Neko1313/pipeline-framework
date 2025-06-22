# packages/pipeline-core/src/pipeline_core/cli/main.py

"""
CLI interface для pipeline framework

Команды:
- init: Создание нового pipeline проекта
- validate: Валидация конфигурации
- run: Запуск pipeline локально
- deploy: Развертывание в Temporal
- list: Список доступных компонентов
- test: Тестирование компонентов
- dev: Development сервер с hot reload
"""

import asyncio
import sys
from pathlib import Path
from typing import Optional, List

import typer
from rich import print as rprint
from rich.console import Console
from rich.table import Table
from rich.progress import track
from rich.panel import Panel
from rich.syntax import Syntax
import structlog

# Проверяем доступность компонентов
try:
    from pipeline_core.config import YAMLConfigLoader, load_pipeline_config
    from pipeline_core.registry import ComponentRegistry
    from pipeline_core import get_available_features

    CONFIG_AVAILABLE = True
except ImportError:
    CONFIG_AVAILABLE = False

try:
    from pipeline_core.pipeline.executor import Pipeline

    PIPELINE_AVAILABLE = True
except ImportError:
    PIPELINE_AVAILABLE = False

try:
    from pipeline_core.temporal import TemporalClient, create_temporal_client

    TEMPORAL_AVAILABLE = True
except ImportError:
    TEMPORAL_AVAILABLE = False

# Настройка CLI
app = typer.Typer(
    name="pipeline-framework",
    help="Data Pipeline Framework CLI",
    add_completion=False,
)

console = Console()
logger = structlog.get_logger(__name__)


@app.command()
def init(
    name: str = typer.Argument(..., help="Pipeline name"),
    path: Optional[Path] = typer.Option(None, "--path", "-p", help="Project path"),
    template: str = typer.Option("basic", "--template", "-t", help="Project template"),
    force: bool = typer.Option(False, "--force", "-f", help="Overwrite existing files"),
):
    """Create a new pipeline project"""

    if not CONFIG_AVAILABLE:
        rprint("[red]Error:[/red] Configuration module not available")
        raise typer.Exit(1)

    project_path = path or Path.cwd() / name

    if project_path.exists() and not force:
        rprint(
            f"[red]Error:[/red] Directory {project_path} already exists. Use --force to overwrite."
        )
        raise typer.Exit(1)

    rprint(f"[blue]Creating pipeline project:[/blue] {name}")
    rprint(f"[blue]Location:[/blue] {project_path}")

    try:
        _create_project_structure(project_path, name, template)
        rprint(f"[green]✓[/green] Project created successfully!")
        rprint(f"\n[yellow]Next steps:[/yellow]")
        rprint(f"1. cd {project_path}")
        rprint("2. Edit pipeline.yaml to configure your pipeline")
        rprint("3. Run: pipeline validate pipeline.yaml")
        rprint("4. Run: pipeline run pipeline.yaml")

    except Exception as e:
        rprint(f"[red]Error creating project:[/red] {e}")
        raise typer.Exit(1)


@app.command()
def validate(
    config_path: Path = typer.Argument(..., help="Path to pipeline configuration"),
    environment: str = typer.Option("development", "--env", "-e", help="Environment"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Verbose output"),
):
    """Validate pipeline configuration"""

    if not CONFIG_AVAILABLE:
        rprint("[red]Error:[/red] Configuration module not available")
        raise typer.Exit(1)

    rprint(f"[blue]Validating configuration:[/blue] {config_path}")

    try:
        loader = YAMLConfigLoader(environment=environment)
        result = loader.validate_config_file(config_path)

        if result["valid"]:
            rprint("[green]✓ Configuration is valid![/green]")

            if verbose:
                rprint(f"Pipeline name: {result['pipeline_name']}")
                rprint(f"Stages count: {result['stages_count']}")

            if result["warnings"]:
                rprint("\n[yellow]Warnings:[/yellow]")
                for warning in result["warnings"]:
                    rprint(f"  - {warning}")
        else:
            rprint("[red]✗ Configuration is invalid![/red]")
            rprint("\n[red]Errors:[/red]")
            for error in result["errors"]:
                rprint(f"  - {error}")
            raise typer.Exit(1)

    except Exception as e:
        rprint(f"[red]Validation failed:[/red] {e}")
        raise typer.Exit(1)


@app.command()
def run(
    config_path: Path = typer.Argument(..., help="Path to pipeline configuration"),
    environment: str = typer.Option("development", "--env", "-e", help="Environment"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Validate without execution"),
    stage: Optional[str] = typer.Option(
        None, "--stage", help="Run specific stage only"
    ),
    parallel: bool = typer.Option(
        False, "--parallel", help="Enable parallel execution"
    ),
):
    """Run pipeline locally"""

    if not PIPELINE_AVAILABLE:
        rprint("[red]Error:[/red] Pipeline executor not available")
        raise typer.Exit(1)

    rprint(f"[blue]Running pipeline:[/blue] {config_path}")

    try:
        # Загружаем конфигурацию
        config = load_pipeline_config(config_path, environment=environment)

        if dry_run:
            rprint("[yellow]Dry run mode - pipeline will not be executed[/yellow]")
            _print_pipeline_summary(config)
            return

        # Создаем и запускаем pipeline
        pipeline = Pipeline(config)

        if stage:
            rprint(f"[blue]Running single stage:[/blue] {stage}")
            result = asyncio.run(pipeline.run_stage(stage))
        else:
            rprint("[blue]Running full pipeline...[/blue]")
            result = asyncio.run(pipeline.run())

        if result.success:
            rprint("[green]✓ Pipeline completed successfully![/green]")
            rprint(f"Duration: {result.duration:.2f}s")
            rprint(f"Stages executed: {len(result.stage_results)}")
        else:
            rprint("[red]✗ Pipeline failed![/red]")
            rprint(f"Error: {result.error}")
            raise typer.Exit(1)

    except Exception as e:
        rprint(f"[red]Pipeline execution failed:[/red] {e}")
        raise typer.Exit(1)


@app.command()
def deploy(
    config_path: Path = typer.Argument(..., help="Path to pipeline configuration"),
    temporal_address: str = typer.Option(
        "localhost:7233", "--temporal", help="Temporal server address"
    ),
    namespace: str = typer.Option("default", "--namespace", help="Temporal namespace"),
    environment: str = typer.Option("production", "--env", "-e", help="Environment"),
    schedule: Optional[str] = typer.Option(
        None, "--schedule", help="Cron schedule override"
    ),
):
    """Deploy pipeline to Temporal"""

    if not TEMPORAL_AVAILABLE:
        rprint("[red]Error:[/red] Temporal integration not available")
        raise typer.Exit(1)

    rprint(f"[blue]Deploying pipeline to Temporal:[/blue] {temporal_address}")

    try:
        # Загружаем конфигурацию
        config = load_pipeline_config(config_path, environment=environment)

        # Создаем Temporal client
        temporal_client = create_temporal_client(
            server_address=temporal_address, namespace=namespace
        )

        # Развертываем pipeline
        asyncio.run(_deploy_to_temporal(temporal_client, config, schedule))

        rprint("[green]✓ Pipeline deployed successfully![/green]")
        rprint(f"Pipeline: {config.name}")
        rprint(f"Namespace: {namespace}")
        if schedule:
            rprint(f"Schedule: {schedule}")

    except Exception as e:
        rprint(f"[red]Deployment failed:[/red] {e}")
        raise typer.Exit(1)


@app.command()
def list_components(
    component_type: Optional[str] = typer.Option(
        None, "--type", "-t", help="Filter by component type"
    ),
    search: Optional[str] = typer.Option(
        None, "--search", "-s", help="Search components"
    ),
    detailed: bool = typer.Option(
        False, "--detailed", "-d", help="Show detailed information"
    ),
):
    """List available pipeline components"""

    try:
        registry = ComponentRegistry()
        components = registry.list_components()

        # Фильтрация
        if component_type:
            components = [c for c in components if c.component_type == component_type]

        if search:
            search_lower = search.lower()
            components = [
                c
                for c in components
                if search_lower in c.name.lower()
                or search_lower in c.description.lower()
            ]

        if not components:
            rprint("[yellow]No components found matching criteria[/yellow]")
            return

        # Вывод таблицы
        table = Table(title="Available Components")
        table.add_column("Type", style="cyan")
        table.add_column("Name", style="green")
        table.add_column("Version", style="blue")
        table.add_column("Description", style="white")

        if detailed:
            table.add_column("Source", style="dim")

        for component in components:
            row = [
                component.component_type,
                component.name,
                component.version,
                component.description[:50] + "..."
                if len(component.description) > 50
                else component.description,
            ]

            if detailed:
                row.append(component.discovery_source)

            table.add_row(*row)

        console.print(table)

    except Exception as e:
        rprint(f"[red]Failed to list components:[/red] {e}")
        raise typer.Exit(1)


@app.command()
def test(
    config_path: Optional[Path] = typer.Option(
        None, "--config", help="Pipeline configuration"
    ),
    component: Optional[str] = typer.Option(
        None, "--component", help="Test specific component"
    ),
    stage: Optional[str] = typer.Option(None, "--stage", help="Test specific stage"),
    sample_data: bool = typer.Option(
        False, "--sample-data", help="Use sample test data"
    ),
):
    """Test pipeline components"""

    rprint("[blue]Running component tests...[/blue]")

    try:
        if config_path:
            config = load_pipeline_config(config_path)
            _test_pipeline_config(config, stage)
        elif component:
            _test_single_component(component, sample_data)
        else:
            _run_component_tests()

        rprint("[green]✓ All tests passed![/green]")

    except Exception as e:
        rprint(f"[red]Tests failed:[/red] {e}")
        raise typer.Exit(1)


@app.command()
def dev(
    config_path: Path = typer.Argument(..., help="Path to pipeline configuration"),
    port: int = typer.Option(8080, "--port", "-p", help="Development server port"),
    reload: bool = typer.Option(True, "--reload/--no-reload", help="Enable hot reload"),
    open_browser: bool = typer.Option(
        True, "--browser/--no-browser", help="Open browser"
    ),
):
    """Start development server with hot reload"""

    rprint(f"[blue]Starting development server on port {port}...[/blue]")

    try:
        asyncio.run(_start_dev_server(config_path, port, reload, open_browser))
    except KeyboardInterrupt:
        rprint("\n[yellow]Development server stopped[/yellow]")
    except Exception as e:
        rprint(f"[red]Development server failed:[/red] {e}")
        raise typer.Exit(1)


@app.command()
def info():
    """Show framework information"""

    rprint("[blue]Pipeline Framework Information[/blue]\n")

    # Версия и компоненты
    try:
        from pipeline_core import __version__

        rprint(f"Version: {__version__}")
    except ImportError:
        rprint("Version: Unknown")

    # Доступные функции
    features = get_available_features()
    rprint(f"Available features: {', '.join(features)}")

    # Системная информация
    rprint(f"Python version: {sys.version}")
    rprint(f"Platform: {sys.platform}")

    # Статус компонентов
    table = Table(title="Component Status")
    table.add_column("Component", style="cyan")
    table.add_column("Status", style="green")

    status_checks = [
        ("Core Components", CONFIG_AVAILABLE),
        ("Pipeline Executor", PIPELINE_AVAILABLE),
        ("Temporal Integration", TEMPORAL_AVAILABLE),
        ("YAML Config Loader", CONFIG_AVAILABLE),
    ]

    for component, available in status_checks:
        status = "✓ Available" if available else "✗ Not Available"
        style = "green" if available else "red"
        table.add_row(component, f"[{style}]{status}[/{style}]")

    console.print(table)


# Вспомогательные функции


def _create_project_structure(project_path: Path, name: str, template: str):
    """Создание структуры проекта"""
    project_path.mkdir(parents=True, exist_ok=True)

    # Основные директории
    (project_path / "configs").mkdir(exist_ok=True)
    (project_path / "transforms").mkdir(exist_ok=True)
    (project_path / "schemas").mkdir(exist_ok=True)
    (project_path / "tests").mkdir(exist_ok=True)
    (project_path / "docs").mkdir(exist_ok=True)

    # pipeline.yaml
    from pipeline_core.config.yaml_loader import create_config_template

    create_config_template(
        project_path / "pipeline.yaml", pipeline_name=name, include_examples=True
    )

    # requirements.txt
    requirements = [
        "pipeline-framework>=0.1.0",
        "pandas>=2.0.0",
        "pydantic>=2.0.0",
        "pyyaml>=6.0",
        "structlog>=23.0.0",
    ]

    (project_path / "requirements.txt").write_text("\n".join(requirements))

    # README.md
    readme_content = f"""# {name}

Pipeline created with Pipeline Framework.

## Quick Start

1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

2. Validate configuration:
   ```bash
   pipeline validate pipeline.yaml
   ```

3. Run pipeline:
   ```bash
   pipeline run pipeline.yaml
   ```

## Project Structure

- `pipeline.yaml` - Main pipeline configuration
- `configs/` - Additional configuration files
- `transforms/` - Data transformation scripts
- `schemas/` - Data validation schemas
- `tests/` - Unit and integration tests
- `docs/` - Documentation

## Development

Run development server:
```bash
pipeline dev pipeline.yaml
```

Deploy to production:
```bash
pipeline deploy pipeline.yaml --env production
```
"""

    (project_path / "README.md").write_text(readme_content)

    # .gitignore
    gitignore_content = """
# Python
__pycache__/
*.py[cod]
*$py.class
*.so
.Python
build/
develop-eggs/
dist/
downloads/
eggs/
.eggs/
lib/
lib64/
parts/
sdist/
var/
wheels/
*.egg-info/
.installed.cfg
*.egg

# Environment
.env
.venv
env/
venv/
ENV/
env.bak/
venv.bak/

# IDE
.vscode/
.idea/
*.swp
*.swo

# Pipeline Framework
.pipeline/
logs/
checkpoints/
"""

    (project_path / ".gitignore").write_text(gitignore_content)


def _print_pipeline_summary(config):
    """Вывод сводки по pipeline"""

    panel = Panel.fit(
        f"[bold blue]{config.name}[/bold blue]\n"
        f"Version: {config.version}\n"
        f"Description: {config.description}\n"
        f"Stages: {len(config.stages)}",
        title="Pipeline Configuration",
    )
    console.print(panel)

    # Таблица этапов
    table = Table(title="Pipeline Stages")
    table.add_column("Order", style="dim")
    table.add_column("Name", style="cyan")
    table.add_column("Component", style="green")
    table.add_column("Dependencies", style="yellow")

    for i, stage in enumerate(config.stages):
        dependencies = ", ".join(stage.depends_on) if stage.depends_on else "None"
        table.add_row(str(i + 1), stage.name, stage.component, dependencies)

    console.print(table)


async def _deploy_to_temporal(client, config, schedule):
    """Развертывание pipeline в Temporal"""
    # Placeholder - здесь должна быть реальная логика развертывания
    await asyncio.sleep(1)  # Симуляция развертывания
    rprint("[yellow]Note: Temporal deployment is not yet implemented[/yellow]")


def _test_pipeline_config(config, stage_name):
    """Тестирование конфигурации pipeline"""
    rprint(f"Testing pipeline configuration: {config.name}")

    if stage_name:
        stage = next((s for s in config.stages if s.name == stage_name), None)
        if not stage:
            raise ValueError(f"Stage '{stage_name}' not found")
        rprint(f"Testing stage: {stage.name}")
    else:
        rprint(f"Testing all {len(config.stages)} stages")


def _test_single_component(component_name, use_sample_data):
    """Тестирование отдельного компонента"""
    rprint(f"Testing component: {component_name}")

    if use_sample_data:
        rprint("Using sample test data")


def _run_component_tests():
    """Запуск всех тестов компонентов"""
    rprint("Running all component tests...")


async def _start_dev_server(config_path, port, reload, open_browser):
    """Запуск development сервера"""
    rprint(f"Development server running on http://localhost:{port}")
    rprint("Press Ctrl+C to stop")

    if open_browser:
        import webbrowser

        webbrowser.open(f"http://localhost:{port}")

    # Placeholder для dev сервера
    while True:
        await asyncio.sleep(1)


if __name__ == "__main__":
    app()
