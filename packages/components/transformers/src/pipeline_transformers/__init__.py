"""
Pipeline Transformers - Компоненты для трансформации данных
"""

__version__ = "0.1.0"
__author__ = "Pipeline Framework Team"

# Импорты базовых классов
from .base import BaseTransformer, DataTransformer, TransformerConfig

# Импорты конкретных трансформеров
from .data.data_transformer import DataTransformerComponent, DataTransformerConfig

__all__ = [
    # Версия
    "__version__",
    "__author__",
    # Базовые классы
    "BaseTransformer",
    "DataTransformer",
    "TransformerConfig",
    # Data transformers
    "DataTransformerComponent",
    "DataTransformerConfig",
]
