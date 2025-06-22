# packages/components/transformers/transformer-pandas/src/transformer_pandas/components.py

"""
Pandas Transformer для pipeline framework

Компоненты:
- PandasTransformer: Базовый transformer с pandas операциями
- ScriptTransformer: Transformer с выполнением Python скриптов
- SQLTransformer: Transformer с SQL-подобным синтаксисом для pandas
"""

import ast
import asyncio
import importlib.util
import inspect
import sys
import tempfile
import warnings
from pathlib import Path
from typing import Any, Dict, List, Optional, Union, Callable

import pandas as pd
import structlog
from pydantic import BaseModel, Field, field_validator

from pipeline_core.components.base import (
    BaseTransformer,
    ExecutionContext,
    ComponentSettings,
)

logger = structlog.get_logger(__name__)


class PandasTransformerConfig(ComponentSettings):
    """Конфигурация Pandas Transformer"""

    # Операции трансформации
    operations: List[Dict[str, Any]] = Field(
        default_factory=list,
        description="Список операций для применения к данным"
    )
    
    # Скрипт для выполнения
    script_path: Optional[str] = Field(
        default=None,
        description="Путь к Python скрипту трансформации"
    )
    
    script_content: Optional[str] = Field(
        default=None,
        description="Содержимое Python скрипта"
    )
    
    # Функция для выполнения
    function_name: str = Field(
        default="transform",
        description="Имя функции в скрипте для выполнения"
    )
    
    # Настройки производительности
    chunk_size: Optional[int] = Field(
        default=None,
        ge=1000,
        description="Размер чанка для обработки больших DataFrame"
    )
    
    memory_limit_mb: Optional[int] = Field(
        default=None,
        ge=100,
        description="Лимит памяти в МБ"
    )
    
    # Валидация
    validate_schema: bool = Field(
        default=False,
        description="Валидировать схему после трансформации"
    )
    
    expected_columns: Optional[List[str]] = Field(
        default=None,
        description="Ожидаемые колонки после трансформации"
    )
    
    # Настройки безопасности
    allow_imports: List[str] = Field(
        default_factory=lambda: ["pandas", "numpy", "datetime", "math"],
        description="Разрешенные модули для импорта в скриптах"
    )
    
    restrict_builtins: bool = Field(
        default=True,
        description="Ограничить доступ к встроенным функциям"
    )

    @field_validator("operations")
    @classmethod
    def validate_operations(cls, v: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Валидация операций"""
        allowed_operations = {
            "drop_columns", "rename_columns", "filter_rows", "add_column",
            "group_by", "sort_by", "merge", "pivot", "melt", "apply_function"
        }
        
        for operation in v:
            if "type" not in operation:
                raise ValueError("Each operation must have a 'type' field")
            
            if operation["type"] not in allowed_operations:
                raise ValueError(f"Unsupported operation type: {operation['type']}")
        
        return v

    @field_validator("script_content")
    @classmethod 
    def validate_script_content(cls, v: Optional[str]) -> Optional[str]:
        """Валидация содержимого скрипта"""
        if v is None:
            return v
        
        # Проверяем синтаксис Python
        try:
            ast.parse(v)
        except SyntaxError as e:
            raise ValueError(f"Invalid Python syntax in script: {e}")
        
        return v


class PandasTransformer(BaseTransformer[pd.DataFrame, PandasTransformerConfig]):
    """
    Универсальный Pandas Transformer
    
    Поддерживает:
    - Декларативные операции через конфигурацию
    - Выполнение Python скриптов
    - Chunked обработка для больших данных
    - Валидацию результатов
    """

    def __init__(self, config: PandasTransformerConfig, name: str = "pandas-transformer"):
        super().__init__(config)
        self._name = name
        self._compiled_script = None
        self._transform_function = None

    @property
    def name(self) -> str:
        return self._name

    async def initialize(self) -> None:
        """Инициализация transformer'а"""
        await super().initialize()
        
        # Компилируем скрипт если есть
        if self.config.script_content or self.config.script_path:
            await self._compile_script()
        
        self.logger.info("Pandas transformer initialized")

    async def _compile_script(self) -> None:
        """Компиляция Python скрипта"""
        try:
            script_content = self.config.script_content
            
            # Загружаем скрипт из файла если нужно
            if not script_content and self.config.script_path:
                script_path = Path(self.config.script_path)
                if not script_path.exists():
                    raise FileNotFoundError(f"Script file not found: {script_path}")
                
                script_content = script_path.read_text(encoding='utf-8')
            
            if not script_content:
                return
            
            # Валидируем безопасность скрипта
            self._validate_script_security(script_content)
            
            # Компилируем скрипт
            self._compiled_script = compile(script_content, '<transform_script>', 'exec')
            
            # Загружаем функцию трансформации
            namespace = self._create_safe_namespace()
            exec(self._compiled_script, namespace)
            
            if self.config.function_name not in namespace:
                raise ValueError(f"Function '{self.config.function_name}' not found in script")
            
            self._transform_function = namespace[self.config.function_name]
            
            # Проверяем сигнатуру функции
            self._validate_function_signature(self._transform_function)
            
            self.logger.info("Transform script compiled successfully")
            
        except Exception as e:
            self.logger.error("Failed to compile transform script", error=str(e))
            raise

    def _validate_script_security(self, script_content: str) -> None:
        """Валидация безопасности скрипта"""
        # Парсим AST для проверки опасных операций
        tree = ast.parse(script_content)
        
        dangerous_names = {
            'exec', 'eval', 'open', '__import__', 'compile',
            'input', 'raw_input', 'file', 'execfile'
        }
        
        restricted_modules = {
            'os', 'sys', 'subprocess', 'shutil', 'glob',
            'socket', 'urllib', 'requests', 'pickle'
        }
        
        class SecurityValidator(ast.NodeVisitor):
            def visit_Name(self, node):
                if node.id in dangerous_names:
                    raise ValueError(f"Dangerous function '{node.id}' not allowed in script")
                self.generic_visit(node)
            
            def visit_Import(self, node):
                for alias in node.names:
                    if alias.name not in self.allowed_imports:
                        if alias.name in restricted_modules:
                            raise ValueError(f"Restricted module '{alias.name}' not allowed")
                        warnings.warn(f"Unknown module '{alias.name}' imported")
                self.generic_visit(node)
            
            def visit_ImportFrom(self, node):
                if node.module and node.module not in self.allowed_imports:
                    if node.module in restricted_modules:
                        raise ValueError(f"Restricted module '{node.module}' not allowed")
                    warnings.warn(f"Unknown module '{node.module}' imported")
                self.generic_visit(node)
        
        validator = SecurityValidator()
        validator.allowed_imports = self.config.allow_imports
        validator.visit(tree)

    def _create_safe_namespace(self) -> Dict[str, Any]:
        """Создание безопасного namespace для выполнения скрипта"""
        # Базовые безопасные модули
        safe_namespace = {
            'pd': pd,
            'pandas': pd,
        }
        
        # Добавляем разрешенные модули
        for module_name in self.config.allow_imports:
            try:
                module = __import__(module_name)
                safe_namespace[module_name] = module
                
                # Для некоторых модулей добавляем сокращения
                if module_name == 'numpy':
                    safe_namespace['np'] = module
                elif module_name == 'datetime':
                    safe_namespace['datetime'] = module
                    
            except ImportError:
                self.logger.warning(f"Allowed module '{module_name}' not available")
        
        # Ограничиваем builtins если нужно
        if self.config.restrict_builtins:
            safe_builtins = {
                'len', 'str', 'int', 'float', 'bool', 'list', 'dict', 'tuple',
                'set', 'min', 'max', 'sum', 'abs', 'round', 'sorted', 'enumerate',
                'zip', 'range', 'isinstance', 'hasattr', 'getattr', 'setattr',
                'type', 'print'
            }
            
            safe_namespace['__builtins__'] = {
                name: getattr(__builtins__, name)
                for name in safe_builtins
                if hasattr(__builtins__, name)
            }
        
        return safe_namespace

    def _validate_function_signature(self, func: Callable) -> None:
        """Валидация сигнатуры функции трансформации"""
        sig = inspect.signature(func)
        params = list(sig.parameters.keys())
        
        # Функция должна принимать хотя бы один DataFrame
        if len(params) < 1:
            raise ValueError("Transform function must accept at least one parameter (DataFrame)")
        
        # Проверяем что функция возвращает что-то
        if sig.return_annotation == inspect.Signature.empty:
            self.logger.warning("Transform function has no return annotation")

    async def _execute_impl(self, context: ExecutionContext) -> pd.DataFrame:
        """Основная логика трансформации"""
        # Получаем данные из предыдущих этапов
        input_data = self._get_input_data(context)
        
        if input_data is None or (isinstance(input_data, pd.DataFrame) and input_data.empty):
            self.logger.warning("No input data for transformation")
            return pd.DataFrame()
        
        # Конвертируем в pandas DataFrame если нужно
        if not isinstance(input_data, pd.DataFrame):
            input_data = self._convert_to_dataframe(input_data)
        
        self.logger.info(
            "Starting transformation",
            input_rows=len(input_data),
            input_columns=len(input_data.columns)
        )
        
        # Выполняем трансформацию
        if self._transform_function:
            result = await self._execute_script_transformation(input_data, context)
        elif self.config.operations:
            result = await self._execute_declarative_operations(input_data)
        else:
            # Просто возвращаем данные без изменений
            result = input_data.copy()
        
        # Валидируем результат
        if self.config.validate_schema:
            self._validate_result_schema(result)
        
        self.logger.info(
            "Transformation completed",
            output_rows=len(result),
            output_columns=len(result.columns)
        )
        
        return result

    def _get_input_data(self, context: ExecutionContext) -> Any:
        """Получение входных данных"""
        if not context.previous_results:
            raise ValueError("No previous results available for transformation")
        
        # Берем данные из последнего успешного этапа
        for result in reversed(context.previous_results):
            if hasattr(result, 'success') and result.success and result.data is not None:
                return result.data
        
        raise ValueError("No valid input data found in previous results")

    def _convert_to_dataframe(self, data: Any) -> pd.DataFrame:
        """Конвертация данных в pandas DataFrame"""
        if isinstance(data, pd.DataFrame):
            return data
        elif isinstance(data, dict):
            return pd.DataFrame([data])
        elif isinstance(data, list):
            if data and isinstance(data[0], dict):
                return pd.DataFrame(data)
            else:
                return pd.DataFrame({'data': data})
        else:
            return pd.DataFrame({'data': [data]})

    async def _execute_script_transformation(
        self, 
        input_data: pd.DataFrame, 
        context: ExecutionContext
    ) -> pd.DataFrame:
        """Выполнение трансформации через Python скрипт"""
        try:
            # Подготавливаем аргументы для функции
            sig = inspect.signature(self._transform_function)
            params = list(sig.parameters.keys())
            
            # Определяем какие аргументы передать
            args = [input_data]
            
            # Если функция принимает больше аргументов
            if len(params) > 1:
                # Можем передать данные из других этапов
                for i, result in enumerate(context.previous_results):
                    if i + 1 < len(params) and hasattr(result, 'data') and result.data is not None:
                        if isinstance(result.data, pd.DataFrame):
                            args.append(result.data)
                        else:
                            args.append(self._convert_to_dataframe(result.data))
            
            # Выполняем функцию
            if asyncio.iscoroutinefunction(self._transform_function):
                result = await self._transform_function(*args[:len(params)])
            else:
                result = self._transform_function(*args[:len(params)])
            
            # Проверяем результат
            if not isinstance(result, pd.DataFrame):
                raise ValueError(f"Transform function must return pandas DataFrame, got {type(result)}")
            
            return result
            
        except Exception as e:
            self.logger.error("Script transformation failed", error=str(e))
            raise

    async def _execute_declarative_operations(self, data: pd.DataFrame) -> pd.DataFrame:
        """Выполнение декларативных операций"""
        result = data.copy()
        
        for operation in self.config.operations:
            operation_type = operation["type"]
            
            try:
                if operation_type == "drop_columns":
                    columns = operation["columns"]
                    result = result.drop(columns=columns, errors='ignore')
                
                elif operation_type == "rename_columns":
                    mapping = operation["mapping"]
                    result = result.rename(columns=mapping)
                
                elif operation_type == "filter_rows":
                    condition = operation["condition"]
                    result = result.query(condition)
                
                elif operation_type == "add_column":
                    column_name = operation["name"]
                    if "value" in operation:
                        result[column_name] = operation["value"]
                    elif "expression" in operation:
                        result[column_name] = result.eval(operation["expression"])
                
                elif operation_type == "group_by":
                    group_cols = operation["columns"]
                    agg_dict = operation.get("aggregations", {})
                    result = result.groupby(group_cols).agg(agg_dict).reset_index()
                
                elif operation_type == "sort_by":
                    sort_cols = operation["columns"]
                    ascending = operation.get("ascending", True)
                    result = result.sort_values(sort_cols, ascending=ascending)
                
                elif operation_type == "pivot":
                    index = operation["index"]
                    columns = operation["columns"]
                    values = operation["values"]
                    result = result.pivot_table(
                        index=index, 
                        columns=columns, 
                        values=values,
                        aggfunc=operation.get("aggfunc", "mean")
                    ).reset_index()
                
                elif operation_type == "melt":
                    id_vars = operation.get("id_vars")
                    value_vars = operation.get("value_vars")
                    result = pd.melt(
                        result,
                        id_vars=id_vars,
                        value_vars=value_vars,
                        var_name=operation.get("var_name", "variable"),
                        value_name=operation.get("value_name", "value")
                    )
                
                self.logger.debug(f"Applied operation: {operation_type}")
                
            except Exception as e:
                self.logger.error(f"Operation {operation_type} failed", error=str(e))
                raise
        
        return result

    def _validate_result_schema(self, result: pd.DataFrame) -> None:
        """Валидация схемы результата"""
        if self.config.expected_columns:
            missing_columns = set(self.config.expected_columns) - set(result.columns)
            if missing_columns:
                raise ValueError(f"Missing expected columns: {missing_columns}")
        
        # Дополнительные проверки можно добавить здесь
        if result.empty:
            self.logger.warning("Transform result is empty")


# Специализированные трансформеры

class SQLTransformer(PandasTransformer):
    """Transformer с SQL-подобным синтаксисом"""

    def __init__(self, config: PandasTransformerConfig, name: str = "sql-transformer"):
        super().__init__(config, name=name)

    async def _execute_impl(self, context: ExecutionContext) -> pd.DataFrame:
        """Выполнение SQL-подобных операций"""
        # Здесь можно добавить поддержку SQL синтаксиса через pandasql или duckdb
        return await super()._execute_impl(context)


class ScriptTransformer(PandasTransformer):
    """Transformer для выполнения произвольных Python скриптов"""

    def __init__(self, config: PandasTransformerConfig, name: str = "script-transformer"):
        super().__init__(config, name=name)


# Утилитарные функции

def create_pandas_transformer(
    operations: Optional[List[Dict[str, Any]]] = None,
    script_content: Optional[str] = None,
    script_path: Optional[str] = None,
    **kwargs
) -> PandasTransformer:
    """Фабричная функция для создания Pandas transformer'а"""
    
    config = PandasTransformerConfig(
        operations=operations or [],
        script_content=script_content,
        script_path=script_path,
        **kwargs
    )
    
    return PandasTransformer(config)


def validate_transform_script(script_content: str, function_name: str = "transform") -> Dict[str, Any]:
    """
    Валидация скрипта трансформации
    
    Returns:
        Результат валидации
    """
    issues = []
    warnings_list = []
    
    try:
        # Проверяем синтаксис
        ast.parse(script_content)
        
        # Компилируем и проверяем функцию
        namespace = {'pd': pd, 'pandas': pd}
        exec(compile(script_content, '<test>', 'exec'), namespace)
        
        if function_name not in namespace:
            issues.append(f"Function '{function_name}' not found in script")
        else:
            func = namespace[function_name]
            if not callable(func):
                issues.append(f"'{function_name}' is not a function")
            else:
                # Проверяем сигнатуру
                sig = inspect.signature(func)
                if len(sig.parameters) < 1:
                    issues.append("Transform function must accept at least one parameter")
        
    except SyntaxError as e:
        issues.append(f"Syntax error: {e}")
    except Exception as e:
        issues.append(f"Script validation error: {e}")
    
    return {
        "valid": len(issues) == 0,
        "issues": issues,
        "warnings": warnings_list,
    }


# Примеры операций для конфигурации

EXAMPLE_OPERATIONS = {
    "drop_duplicates": {
        "type": "apply_function",
        "function": "drop_duplicates",
        "args": [],
        "kwargs": {"keep": "first"}
    },
    
    "fill_missing_values": {
        "type": "apply_function", 
        "function": "fillna",
        "args": [0],
        "kwargs": {}
    },
    
    "calculate_age": {
        "type": "add_column",
        "name": "age",
        "expression": "(pd.Timestamp.now() - birth_date).dt.days / 365.25"
    },
    
    "customer_segmentation": {
        "type": "add_column",
        "name": "segment",
        "expression": """
            np.where(total_spent > 1000, 'high_value',
            np.where(total_spent > 500, 'medium_value', 'low_value'))
        """
    }
}

# Пример скрипта трансформации
EXAMPLE_TRANSFORM_SCRIPT = '''
import pandas as pd
import numpy as np
from datetime import datetime

def transform(df):
    """
    Пример функции трансформации данных клиентов
    
    Args:
        df: pandas DataFrame с данными клиентов
        
    Returns:
        Трансформированный DataFrame
    """
    # Копируем данные
    result = df.copy()
    
    # Очищаем данные
    result = result.drop_duplicates()
    result = result.dropna(subset=['email'])
    
    # Добавляем новые колонки
    result['full_name'] = result['first_name'] + ' ' + result['last_name']
    result['email_domain'] = result['email'].str.split('@').str[1]
    
    # Категоризация клиентов
    result['customer_tier'] = pd.cut(
        result['total_spent'], 
        bins=[0, 100, 500, 1000, float('inf')],
        labels=['bronze', 'silver', 'gold', 'platinum']
    )
    
    # Расчет метрик
    result['days_since_registration'] = (
        pd.Timestamp.now() - pd.to_datetime(result['registration_date'])
    ).dt.days
    
    # Сортировка
    result = result.sort_values(['total_spent'], ascending=False)
    
    return result
'''
