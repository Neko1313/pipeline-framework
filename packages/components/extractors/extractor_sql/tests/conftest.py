# packages/components/extractors/extractor_sql/tests/conftest.py

"""
Общие фикстуры и настройки для тестов SQL Extractor
"""

import asyncio
import os
from pathlib import Path
import sqlite3
import tempfile
from unittest.mock import AsyncMock, Mock

import pandas as pd
import pytest
from testcontainers.mysql import MySqlContainer
from testcontainers.postgres import PostgresContainer

from extractor_sql.components import (
    ConnectionPoolConfig,
    QueryConfig,
    RetryConfig,
    SQLExtractorConfig,
)
from pipeline_core.components.base import ExecutionContext

# ================================
# Event Loop Configuration
# ================================


@pytest.fixture(scope="session")
def event_loop():
    """Создание event loop для всей сессии тестов"""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


# ================================
# Database Containers (для integration тестов)
# ================================


@pytest.fixture(scope="session")
def postgresql_container():
    """PostgreSQL контейнер для интеграционных тестов"""
    pytest.importorskip("testcontainers")

    with PostgresContainer("postgres:15") as postgres:
        # Ждем готовности контейнера
        postgres.get_connection_url()
        yield postgres


@pytest.fixture(scope="session")
def mysql_container():
    """MySQL контейнер для интеграционных тестов"""
    pytest.importorskip("testcontainers")

    with MySqlContainer("mysql:8.0") as mysql:
        # Ждем готовности контейнера
        mysql.get_connection_url()
        yield mysql


# ================================
# SQLite Database Fixtures
# ================================


@pytest.fixture(scope="session")
def sqlite_test_db_path():
    """Путь к тестовой SQLite базе данных"""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp_file:
        db_path = tmp_file.name

    yield db_path

    # Cleanup
    try:
        os.unlink(db_path)
    except FileNotFoundError:
        pass


@pytest.fixture(scope="session")
def sqlite_test_data():
    """Тестовые данные для заполнения базы"""
    return {
        "users": [
            {
                "id": 1,
                "name": "Alice Smith",
                "age": 25,
                "email": "alice@example.com",
                "is_active": True,
                "department": "Engineering",
            },
            {
                "id": 2,
                "name": "Bob Johnson",
                "age": 30,
                "email": "bob@example.com",
                "is_active": True,
                "department": "Marketing",
            },
            {
                "id": 3,
                "name": "Charlie Brown",
                "age": 35,
                "email": "charlie@example.com",
                "is_active": False,
                "department": "Sales",
            },
            {
                "id": 4,
                "name": "Diana Prince",
                "age": 28,
                "email": "diana@example.com",
                "is_active": True,
                "department": "Engineering",
            },
            {
                "id": 5,
                "name": "Eve Davis",
                "age": 22,
                "email": "eve@example.com",
                "is_active": True,
                "department": "HR",
            },
            {
                "id": 6,
                "name": "Frank Miller",
                "age": 45,
                "email": "frank@example.com",
                "is_active": True,
                "department": "Finance",
            },
            {
                "id": 7,
                "name": "Grace Lee",
                "age": 33,
                "email": "grace@example.com",
                "is_active": False,
                "department": "Marketing",
            },
            {
                "id": 8,
                "name": "Henry Wilson",
                "age": 29,
                "email": "henry@example.com",
                "is_active": True,
                "department": "Engineering",
            },
        ],
        "orders": [
            {
                "id": 1,
                "user_id": 1,
                "product": "Laptop",
                "amount": 1200.50,
                "order_date": "2024-01-15",
            },
            {
                "id": 2,
                "user_id": 2,
                "product": "Phone",
                "amount": 800.00,
                "order_date": "2024-01-16",
            },
            {
                "id": 3,
                "user_id": 1,
                "product": "Tablet",
                "amount": 400.25,
                "order_date": "2024-01-17",
            },
            {
                "id": 4,
                "user_id": 4,
                "product": "Monitor",
                "amount": 300.00,
                "order_date": "2024-01-18",
            },
            {
                "id": 5,
                "user_id": 5,
                "product": "Keyboard",
                "amount": 120.00,
                "order_date": "2024-01-19",
            },
            {
                "id": 6,
                "user_id": 6,
                "product": "Mouse",
                "amount": 50.00,
                "order_date": "2024-01-20",
            },
        ],
        "products": [
            {
                "id": 1,
                "name": "Laptop",
                "category": "Electronics",
                "price": 1200.50,
                "in_stock": True,
            },
            {
                "id": 2,
                "name": "Phone",
                "category": "Electronics",
                "price": 800.00,
                "in_stock": True,
            },
            {
                "id": 3,
                "name": "Tablet",
                "category": "Electronics",
                "price": 400.25,
                "in_stock": False,
            },
            {
                "id": 4,
                "name": "Monitor",
                "category": "Electronics",
                "price": 300.00,
                "in_stock": True,
            },
            {
                "id": 5,
                "name": "Keyboard",
                "category": "Accessories",
                "price": 120.00,
                "in_stock": True,
            },
            {
                "id": 6,
                "name": "Mouse",
                "category": "Accessories",
                "price": 50.00,
                "in_stock": True,
            },
        ],
    }


@pytest.fixture(scope="session")
def populated_sqlite_db(sqlite_test_db_path, sqlite_test_data):
    """SQLite база данных с тестовыми данными"""
    conn = sqlite3.connect(sqlite_test_db_path)
    cursor = conn.cursor()

    # Создаем таблицы
    cursor.execute("""
        CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            age INTEGER,
            email TEXT UNIQUE,
            is_active BOOLEAN DEFAULT 1,
            department TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    cursor.execute("""
        CREATE TABLE orders (
            id INTEGER PRIMARY KEY,
            user_id INTEGER,
            product TEXT NOT NULL,
            amount DECIMAL(10, 2),
            order_date DATE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users (id)
        )
    """)

    cursor.execute("""
        CREATE TABLE products (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            category TEXT,
            price DECIMAL(10, 2),
            in_stock BOOLEAN DEFAULT 1,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Заполняем данными
    for user in sqlite_test_data["users"]:
        cursor.execute(
            """
            INSERT INTO users (id, name, age, email, is_active, department)
            VALUES (?, ?, ?, ?, ?, ?)
        """,
            (
                user["id"],
                user["name"],
                user["age"],
                user["email"],
                user["is_active"],
                user["department"],
            ),
        )

    for order in sqlite_test_data["orders"]:
        cursor.execute(
            """
            INSERT INTO orders (id, user_id, product, amount, order_date)
            VALUES (?, ?, ?, ?, ?)
        """,
            (
                order["id"],
                order["user_id"],
                order["product"],
                order["amount"],
                order["order_date"],
            ),
        )

    for product in sqlite_test_data["products"]:
        cursor.execute(
            """
            INSERT INTO products (id, name, category, price, in_stock)
            VALUES (?, ?, ?, ?, ?)
        """,
            (
                product["id"],
                product["name"],
                product["category"],
                product["price"],
                product["in_stock"],
            ),
        )

    # Создаем индексы для лучшей производительности
    cursor.execute("CREATE INDEX idx_users_department ON users(department)")
    cursor.execute("CREATE INDEX idx_users_active ON users(is_active)")
    cursor.execute("CREATE INDEX idx_orders_user_id ON orders(user_id)")
    cursor.execute("CREATE INDEX idx_orders_date ON orders(order_date)")
    cursor.execute("CREATE INDEX idx_products_category ON products(category)")

    conn.commit()
    conn.close()

    yield f"sqlite+aiosqlite:///{sqlite_test_db_path}"


# ================================
# Configuration Fixtures
# ================================


@pytest.fixture
def basic_query_config():
    """Базовая конфигурация запроса"""
    return QueryConfig(
        query="SELECT * FROM users",
        timeout=30.0,
        fetch_size=1000,
        stream_results=False,
    )


@pytest.fixture
def advanced_query_config():
    """Продвинутая конфигурация запроса с параметрами"""
    return QueryConfig(
        query="""
        SELECT u.name, u.age, u.department, COUNT(o.id) as order_count
        FROM users u
        LEFT JOIN orders o ON u.id = o.user_id
        WHERE u.is_active = :is_active
        GROUP BY u.id, u.name, u.age, u.department
        ORDER BY order_count DESC
        """,
        parameters={"is_active": True},
        timeout=60.0,
        fetch_size=500,
    )


@pytest.fixture
def pool_config():
    """Конфигурация connection pool"""
    return ConnectionPoolConfig(
        pool_size=3,
        max_overflow=5,
        pool_timeout=10.0,
        pool_recycle=1800,
        pool_pre_ping=True,
    )


@pytest.fixture
def retry_config():
    """Конфигурация retry политики"""
    return RetryConfig(
        max_attempts=3,
        initial_wait=0.1,  # Быстрее для тестов
        max_wait=1.0,
        multiplier=2.0,
        jitter=False,  # Отключаем для предсказуемости в тестах
    )


@pytest.fixture
def sqlite_extractor_config(
    populated_sqlite_db, basic_query_config, pool_config, retry_config
):
    """Полная конфигурация SQLite extractor'а"""
    return SQLExtractorConfig(
        connection_string=populated_sqlite_db,
        query_config=basic_query_config,
        output_format="pandas",
        pool_config=pool_config,
        retry_config=retry_config,
    )


@pytest.fixture
def postgresql_extractor_config(basic_query_config, pool_config, retry_config):
    """Конфигурация PostgreSQL extractor'а"""
    return SQLExtractorConfig(
        connection_string="postgresql+asyncpg://user:password@localhost:5432/testdb",
        dialect="postgresql",
        query_config=basic_query_config,
        output_format="pandas",
        pool_config=pool_config,
        retry_config=retry_config,
    )


# ================================
# Execution Context Fixtures
# ================================


@pytest.fixture
def execution_context():
    """Базовый контекст выполнения"""
    return ExecutionContext(
        pipeline_id="test_pipeline_001",
        stage_name="sql_extraction",
        run_id="test_run_001",
        previous_results=[],
    )


@pytest.fixture
def execution_context_with_history():
    """Контекст выполнения с историей предыдущих результатов"""
    from pipeline_core.components.base import ExecutionMetadata, ExecutionResult

    previous_result = ExecutionResult(
        data={"previous_stage_output": "some_data"},
        metadata=ExecutionMetadata(rows_processed=100),
        success=True,
    )

    return ExecutionContext(
        pipeline_id="test_pipeline_002",
        stage_name="sql_extraction_with_history",
        run_id="test_run_002",
        previous_results=[previous_result],
    )


# ================================
# Mock Fixtures
# ================================


@pytest.fixture
def mock_engine():
    """Mock SQLAlchemy engine"""
    engine = AsyncMock()
    engine.begin = AsyncMock()
    engine.dispose = AsyncMock()
    return engine


@pytest.fixture
def mock_connection():
    """Mock SQLAlchemy connection"""
    conn = AsyncMock()

    # Мокаем результат запроса
    mock_result = AsyncMock()
    mock_result.fetchall = AsyncMock(
        return_value=[
            ("Alice", 25, "alice@example.com"),
            ("Bob", 30, "bob@example.com"),
        ]
    )
    mock_result.keys = Mock(return_value=["name", "age", "email"])

    conn.execute = AsyncMock(return_value=mock_result)

    return conn


@pytest.fixture
def mock_session():
    """Mock SQLAlchemy session"""
    session = AsyncMock()
    session.execute = AsyncMock()
    session.commit = AsyncMock()
    session.rollback = AsyncMock()
    session.close = AsyncMock()
    return session


# ================================
# Data Fixtures
# ================================


@pytest.fixture
def sample_dataframe():
    """Sample pandas DataFrame для тестов"""
    data = {
        "id": [1, 2, 3, 4, 5],
        "name": ["Alice", "Bob", "Charlie", "Diana", "Eve"],
        "age": [25, 30, 35, 28, 22],
        "salary": [50000.0, 60000.0, 70000.0, 55000.0, 45000.0],
        "is_active": [True, True, False, True, True],
        "department": ["Engineering", "Marketing", "Sales", "Engineering", "HR"],
    }
    return pd.DataFrame(data)


@pytest.fixture
def large_dataframe():
    """Большой DataFrame для тестов производительности"""
    import numpy as np

    size = 10000
    data = {
        "id": range(1, size + 1),
        "value": np.random.random(size),
        "category": np.random.choice(["A", "B", "C", "D"], size),
        "is_flag": np.random.choice([True, False], size),
        "count": np.random.randint(1, 100, size),
    }
    return pd.DataFrame(data)


# ================================
# Temporary File Fixtures
# ================================


@pytest.fixture
def temp_dir():
    """Временная директория для тестов"""
    with tempfile.TemporaryDirectory() as tmp_dir:
        yield Path(tmp_dir)


@pytest.fixture
def temp_config_file(temp_dir):
    """Временный файл конфигурации"""
    config_data = {
        "connection_string": "sqlite+aiosqlite:///:memory:",
        "query_config": {
            "query": "SELECT 1 as test_col",
            "timeout": 30.0,
            "fetch_size": 1000,
        },
        "output_format": "pandas",
    }

    config_file = temp_dir / "test_config.json"

    import json

    with open(config_file, "w") as f:
        json.dump(config_data, f, indent=2)

    yield config_file


# ================================
# Environment Fixtures
# ================================


@pytest.fixture
def clean_environment():
    """Очищает переменные окружения для изолированных тестов"""
    import os

    # Сохраняем текущие переменные
    original_env = os.environ.copy()

    # Очищаем SQL-related переменные
    sql_env_vars = [
        "DATABASE_URL",
        "DB_HOST",
        "DB_PORT",
        "DB_USER",
        "DB_PASSWORD",
        "DB_NAME",
    ]

    for var in sql_env_vars:
        if var in os.environ:
            del os.environ[var]

    yield

    # Восстанавливаем исходные переменные
    os.environ.clear()
    os.environ.update(original_env)


@pytest.fixture
def mock_environment(clean_environment):
    """Настраивает тестовое окружение с mock переменными"""
    import os

    test_env = {
        "DATABASE_URL": "postgresql://test:test@localhost:5432/testdb",
        "DB_HOST": "localhost",
        "DB_PORT": "5432",
        "DB_USER": "test_user",
        "DB_PASSWORD": "test_password",
        "DB_NAME": "test_database",
    }

    os.environ.update(test_env)
    yield test_env


# ================================
# Marker Configuration
# ================================


def pytest_configure(config):
    """Конфигурация pytest markers"""
    config.addinivalue_line("markers", "unit: Unit tests")
    config.addinivalue_line("markers", "integration: Integration tests")
    config.addinivalue_line("markers", "slow: Slow tests that may take a while")
    config.addinivalue_line("markers", "database: Tests that require database")
    config.addinivalue_line("markers", "postgresql: Tests that require PostgreSQL")
    config.addinivalue_line("markers", "mysql: Tests that require MySQL")
    config.addinivalue_line("markers", "sqlite: Tests that require SQLite")


def pytest_runtest_setup(item):
    """Setup для каждого теста"""
    # Пропускаем integration тесты если не установлены testcontainers
    if "integration" in item.keywords:
        pytest.importorskip("testcontainers")

    # Пропускаем PostgreSQL тесты если не установлен asyncpg
    if "postgresql" in item.keywords:
        pytest.importorskip("asyncpg")

    # Пропускаем MySQL тесты если не установлен aiomysql
    if "mysql" in item.keywords:
        pytest.importorskip("aiomysql")


# ================================
# Cleanup Fixtures
# ================================


@pytest.fixture(autouse=True)
def cleanup_after_test():
    """Автоматическая очистка после каждого теста"""
    yield

    # Закрываем все asyncio tasks
    import asyncio

    try:
        loop = asyncio.get_running_loop()
        pending = asyncio.all_tasks(loop)
        for task in pending:
            if not task.done():
                task.cancel()
    except RuntimeError:
        pass  # No running loop


# ================================
# Custom Pytest Fixtures для специфичных тестов
# ================================


@pytest.fixture
def mock_prometheus_metrics():
    """Mock Prometheus метрики для тестирования"""
    from unittest.mock import patch

    with (
        patch("extractor_sql.components.SQL_QUERIES_TOTAL") as mock_queries,
        patch("extractor_sql.components.SQL_QUERY_DURATION") as mock_duration,
        patch("extractor_sql.components.SQL_ROWS_EXTRACTED") as mock_rows,
        patch("extractor_sql.components.SQL_ACTIVE_CONNECTIONS") as mock_connections,
    ):
        yield {
            "queries_total": mock_queries,
            "query_duration": mock_duration,
            "rows_extracted": mock_rows,
            "active_connections": mock_connections,
        }


@pytest.fixture
def error_injection():
    """Утилита для инъекции ошибок в тестах"""

    class ErrorInjector:
        def __init__(self):
            self.call_count = 0
            self.error_on_call = None
            self.error_to_raise = None

        def set_error(self, call_number: int, error: Exception):
            self.error_on_call = call_number
            self.error_to_raise = error

        def check_and_raise(self):
            self.call_count += 1
            if self.call_count == self.error_on_call:
                raise self.error_to_raise

    return ErrorInjector()
