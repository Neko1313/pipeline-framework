"""
CLI интерфейс для SQL Extractor

Предоставляет команды для тестирования, запуска и отладки SQL extraction компонентов.
"""

import asyncio
import json
from pathlib import Path
import time
from typing import Any

from rich.console import Console
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.syntax import Syntax
from rich.table import Table
import typer

from pipeline_core.components.base import ExecutionContext

from extractor_sql.components import (
    ConnectionPoolConfig,
    QueryConfig,
    RetryConfig,
    SQLExtractor,
    SQLExtractorConfig,
)
from extractor_sql.exceptions import SQLExtractorError
from extractor_sql.utils import (
    estimate_query_cost,
    format_bytes,
    format_duration,
    mask_connection_string,
    validate_connection_string,
)

app = typer.Typer(help="SQL Extractor CLI для работы с базами данных")
console = Console()


# ================================
# Helper Functions
# ================================


def load_config_from_file(config_path: Path) -> dict[str, Any]:
    """Загрузка конфигурации из файла"""
    if not config_path.exists():
        console.print(f"[red]Config file not found: {config_path}[/red]")
        raise typer.Exit(1)

    try:
        with open(config_path) as f:
            if config_path.suffix.lower() == ".json":
                return json.load(f)
            elif config_path.suffix.lower() in [".yaml", ".yml"]:
                import yaml

                return yaml.safe_load(f)
            else:
                console.print(
                    f"[red]Unsupported config format: {config_path.suffix}[/red]"
                )
                raise typer.Exit(1)
    except Exception as e:
        console.print(f"[red]Error loading config: {e}[/red]")
        raise typer.Exit(1)


def create_extractor_from_cli_args(
    connection_string: str,
    query: str,
    output_format: str = "pandas",
    timeout: float = 300.0,
    fetch_size: int = 10000,
    pool_size: int = 5,
    max_attempts: int = 3,
    **kwargs,
) -> SQLExtractor:
    """Создание extractor'а из CLI аргументов"""

    query_config = QueryConfig(
        query=query,
        timeout=timeout,
        fetch_size=fetch_size,
        parameters=kwargs.get("parameters", {}),
    )

    pool_config = ConnectionPoolConfig(
        pool_size=pool_size,
    )

    retry_config = RetryConfig(
        max_attempts=max_attempts,
    )

    config = SQLExtractorConfig(
        connection_string=connection_string,
        query_config=query_config,
        output_format=output_format,
        pool_config=pool_config,
        retry_config=retry_config,
    )

    return SQLExtractor(config)


def display_extraction_result(result, show_data: bool = True, max_rows: int = 10):
    """Отображение результатов extraction"""

    if not result.success:
        console.print("[red]Extraction failed![/red]")
        console.print(f"Error: {result.error}")
        return

    # Метаданные
    metadata = result.metadata
    table = Table(title="Extraction Metadata")
    table.add_column("Metric", style="cyan")
    table.add_column("Value", style="magenta")

    table.add_row("Rows Processed", str(metadata.rows_processed or 0))
    table.add_row("Duration", format_duration(metadata.duration_seconds or 0))

    if metadata.bytes_processed:
        table.add_row("Bytes Processed", format_bytes(metadata.bytes_processed))

    if metadata.custom_metrics:
        for key, value in metadata.custom_metrics.items():
            table.add_row(key.replace("_", " ").title(), str(value))

    console.print(table)

    # Данные
    if show_data and result.data is not None:
        console.print("\n[bold]Data Preview:[/bold]")

        try:
            if hasattr(result.data, "head"):  # pandas DataFrame
                df = result.data
                console.print(f"DataFrame shape: {df.shape}")
                console.print(df.head(max_rows).to_string())
            elif hasattr(result.data, "to_pandas"):  # polars DataFrame
                df = result.data.to_pandas()
                console.print(f"DataFrame shape: {df.shape}")
                console.print(df.head(max_rows).to_string())
            elif isinstance(result.data, list):
                console.print(f"List with {len(result.data)} items")
                for i, item in enumerate(result.data[:max_rows]):
                    console.print(f"[{i}]: {item}")
            else:
                console.print(f"Data type: {type(result.data)}")
                console.print(str(result.data)[:1000])

        except Exception as e:
            console.print(f"[yellow]Warning: Could not display data - {e}[/yellow]")


# ================================
# CLI Commands
# ================================


@app.command()
def test_connection(
    connection_string: str = typer.Argument(..., help="Database connection string"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Verbose output"),
):
    """Тестирование подключения к базе данных"""

    console.print(
        f"🔗 Testing connection to: [bold]{mask_connection_string(connection_string)}[/bold]"
    )

    # Валидация connection string
    is_valid, error_msg = validate_connection_string(connection_string)
    if not is_valid:
        console.print(f"[red]Invalid connection string: {error_msg}[/red]")
        raise typer.Exit(1)

    async def _test():
        try:
            config = SQLExtractorConfig(
                connection_string=connection_string,
                query_config=QueryConfig(query="SELECT 1"),
            )

            extractor = SQLExtractor(config)

            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                console=console,
            ) as progress:
                task = progress.add_task("Connecting...", total=None)

                await extractor.initialize()

                progress.update(task, description="Testing query...")
                context = ExecutionContext(
                    pipeline_id="test",
                    stage_name="connection_test",
                )

                result = await extractor.execute(context)

                await extractor.cleanup()

            if result.success:
                console.print("[green]✓ Connection successful![/green]")
                if verbose:
                    console.print(f"Query result: {result.data}")
            else:
                console.print(f"[red]✗ Connection failed: {result.error}[/red]")
                raise typer.Exit(1)

        except SQLExtractorError as e:
            console.print(f"[red]✗ Connection failed: {e}[/red]")
            if verbose:
                console.print(f"Error details: {e.to_dict()}")
            raise typer.Exit(1)
        except Exception as e:
            console.print(f"[red]✗ Unexpected error: {e}[/red]")
            if verbose:
                import traceback

                console.print(traceback.format_exc())
            raise typer.Exit(1)

    asyncio.run(_test())


@app.command()
def extract(
    connection_string: str = typer.Argument(..., help="Database connection string"),
    query: str = typer.Argument(..., help="SQL query to execute"),
    output_format: str = typer.Option(
        "pandas", help="Output format (pandas, polars, dict, raw)"
    ),
    output_file: Path | None = typer.Option(
        None, "--output", "-o", help="Output file path"
    ),
    timeout: float = typer.Option(300.0, help="Query timeout in seconds"),
    fetch_size: int = typer.Option(10000, help="Fetch size for query execution"),
    pool_size: int = typer.Option(5, help="Connection pool size"),
    max_attempts: int = typer.Option(3, help="Maximum retry attempts"),
    show_data: bool = typer.Option(True, help="Show data preview"),
    max_rows: int = typer.Option(10, help="Maximum rows to show in preview"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Verbose output"),
):
    """Извлечение данных из базы данных"""

    console.print(
        f"🗃️  Extracting data from: [bold]{mask_connection_string(connection_string)}[/bold]"
    )

    if verbose:
        console.print(f"Query: {query}")
        console.print(f"Output format: {output_format}")
        console.print(f"Timeout: {timeout}s")

    async def _extract():
        try:
            extractor = create_extractor_from_cli_args(
                connection_string=connection_string,
                query=query,
                output_format=output_format,
                timeout=timeout,
                fetch_size=fetch_size,
                pool_size=pool_size,
                max_attempts=max_attempts,
            )

            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                console=console,
            ) as progress:
                task = progress.add_task("Initializing...", total=None)

                await extractor.initialize()

                progress.update(task, description="Executing query...")

                context = ExecutionContext(
                    pipeline_id="cli_extract",
                    stage_name="data_extraction",
                )

                start_time = time.time()
                result = await extractor.execute(context)
                end_time = time.time()

                await extractor.cleanup()

            # Отображаем результаты
            display_extraction_result(result, show_data, max_rows)

            # Сохраняем в файл если указан
            if output_file and result.success and result.data is not None:
                console.print(f"\n💾 Saving to: [bold]{output_file}[/bold]")

                output_file.parent.mkdir(parents=True, exist_ok=True)

                if output_file.suffix.lower() == ".csv":
                    if hasattr(result.data, "to_csv"):
                        result.data.to_csv(output_file, index=False)
                    else:
                        # Convert to pandas if needed
                        import pandas as pd

                        df = pd.DataFrame(result.data)
                        df.to_csv(output_file, index=False)
                elif output_file.suffix.lower() == ".json":
                    if hasattr(result.data, "to_json"):
                        result.data.to_json(output_file, orient="records")
                    else:
                        with open(output_file, "w") as f:
                            json.dump(result.data, f, indent=2, default=str)
                elif output_file.suffix.lower() in [".parquet", ".pq"]:
                    if hasattr(result.data, "to_parquet"):
                        result.data.to_parquet(output_file)
                    else:
                        import pandas as pd

                        df = pd.DataFrame(result.data)
                        df.to_parquet(output_file)
                else:
                    console.print(
                        f"[yellow]Warning: Unsupported output format {output_file.suffix}[/yellow]"
                    )

                console.print("[green]✓ Data saved successfully![/green]")

        except SQLExtractorError as e:
            console.print(f"[red]✗ Extraction failed: {e}[/red]")
            if verbose:
                console.print(f"Error details: {e.to_dict()}")
            raise typer.Exit(1)
        except Exception as e:
            console.print(f"[red]✗ Unexpected error: {e}[/red]")
            if verbose:
                import traceback

                console.print(traceback.format_exc())
            raise typer.Exit(1)

    asyncio.run(_extract())


@app.command()
def analyze_query(
    query: str = typer.Argument(..., help="SQL query to analyze"),
    show_syntax: bool = typer.Option(True, help="Show syntax-highlighted query"),
):
    """Анализ SQL запроса"""

    console.print("🔍 Analyzing SQL query...")

    # Отображаем запрос с подсветкой синтаксиса
    if show_syntax:
        syntax = Syntax(query, "sql", theme="monokai", line_numbers=True)
        console.print(Panel(syntax, title="SQL Query"))

    # Анализируем сложность
    cost_analysis = estimate_query_cost(query)

    table = Table(title="Query Analysis")
    table.add_column("Metric", style="cyan")
    table.add_column("Value", style="magenta")

    table.add_row("Complexity", cost_analysis["complexity"].upper())
    table.add_row("Complexity Score", str(cost_analysis["complexity_score"]))
    table.add_row("JOIN Count", str(cost_analysis["join_count"]))
    table.add_row("Subquery Count", str(cost_analysis["subquery_count"]))
    table.add_row("Aggregate Count", str(cost_analysis["aggregate_count"]))
    table.add_row("Estimated Time", cost_analysis["estimated_time_category"].upper())

    console.print(table)

    # Рекомендации
    recommendations = []

    if cost_analysis["complexity_score"] > 10:
        recommendations.append("🚨 Very complex query - consider optimization")
    elif cost_analysis["complexity_score"] > 5:
        recommendations.append("⚠️  Complex query - monitor performance")

    if cost_analysis["join_count"] > 3:
        recommendations.append("🔗 Multiple JOINs detected - ensure proper indexing")

    if cost_analysis["subquery_count"] > 2:
        recommendations.append("🔄 Multiple subqueries - consider using CTEs")

    if recommendations:
        console.print("\n[bold]Recommendations:[/bold]")
        for rec in recommendations:
            console.print(f"  {rec}")


@app.command()
def run_from_config(
    config_path: Path = typer.Argument(..., help="Path to configuration file"),
    show_data: bool = typer.Option(True, help="Show data preview"),
    max_rows: int = typer.Option(10, help="Maximum rows to show in preview"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Verbose output"),
):
    """Запуск extraction из конфигурационного файла"""

    console.print(f"📋 Loading configuration from: [bold]{config_path}[/bold]")

    config_data = load_config_from_file(config_path)

    async def _run():
        try:
            # Создаем конфигурацию из файла
            config = SQLExtractorConfig(**config_data)
            extractor = SQLExtractor(config)

            if verbose:
                console.print(
                    f"Connection: {mask_connection_string(config.connection_string)}"
                )
                console.print(f"Query: {config.query_config.query[:100]}...")

            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                console=console,
            ) as progress:
                task = progress.add_task("Initializing...", total=None)

                await extractor.initialize()

                progress.update(task, description="Executing extraction...")

                context = ExecutionContext(
                    pipeline_id="config_extract",
                    stage_name="data_extraction",
                )

                result = await extractor.execute(context)

                await extractor.cleanup()

            # Отображаем результаты
            display_extraction_result(result, show_data, max_rows)

        except SQLExtractorError as e:
            console.print(f"[red]✗ Extraction failed: {e}[/red]")
            if verbose:
                console.print(f"Error details: {e.to_dict()}")
            raise typer.Exit(1)
        except Exception as e:
            console.print(f"[red]✗ Unexpected error: {e}[/red]")
            if verbose:
                import traceback

                console.print(traceback.format_exc())
            raise typer.Exit(1)

    asyncio.run(_run())


@app.command()
def generate_config(
    output_path: Path = typer.Argument(..., help="Output path for configuration file"),
    connection_string: str = typer.Option("", help="Database connection string"),
    query: str = typer.Option("SELECT 1", help="SQL query"),
    format: str = typer.Option("yaml", help="Config format (yaml or json)"),
):
    """Генерация шаблона конфигурационного файла"""

    console.print(f"📝 Generating configuration template: [bold]{output_path}[/bold]")

    # Создаем конфигурацию по умолчанию
    config = {
        "connection_string": connection_string
        or "postgresql+asyncpg://user:password@localhost:5432/database",
        "dialect": "postgresql",
        "query_config": {
            "query": query,
            "parameters": {},
            "timeout": 300.0,
            "fetch_size": 10000,
            "stream_results": False,
        },
        "output_format": "pandas",
        "pool_config": {
            "pool_size": 5,
            "max_overflow": 10,
            "pool_timeout": 30.0,
            "pool_recycle": 3600,
            "pool_pre_ping": True,
        },
        "retry_config": {
            "max_attempts": 3,
            "initial_wait": 1.0,
            "max_wait": 60.0,
            "multiplier": 2.0,
            "jitter": True,
        },
        "engine_options": {},
    }

    # Создаем директорию если нужно
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Сохраняем файл
    with open(output_path, "w") as f:
        if format.lower() == "json":
            json.dump(config, f, indent=2)
        else:  # yaml
            import yaml

            yaml.dump(config, f, default_flow_style=False, sort_keys=False)

    console.print("[green]✓ Configuration template generated![/green]")
    console.print(
        f"Edit {output_path} and run: [bold]extractor-sql run-from-config {output_path}[/bold]"
    )


@app.command()
def version():
    """Показать версию SQL Extractor"""
    from . import __version__

    console.print(f"SQL Extractor version: [bold]{__version__}[/bold]")


if __name__ == "__main__":
    app()


def main():
    """Entry point для console script"""
    app()
