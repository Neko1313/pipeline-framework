"""
Pipeline Extractors - Компоненты для извлечения данных
"""

__version__ = "0.1.0"
__author__ = "Pipeline Framework Team"

# Импорты базовых классов
from .base import BaseExtractor, StreamingExtractor, ExtractorConfig

# Импорты конкретных экстракторов
from .file.csv_extractor import CSVExtractor, CSVExtractorConfig
from .api.http_extractor import HTTPExtractor, HTTPExtractorConfig

__all__ = [
    # Версия
    "__version__",
    "__author__",

    # Базовые классы
    "BaseExtractor",
    "StreamingExtractor",
    "ExtractorConfig",

    # File extractors
    "CSVExtractor",
    "CSVExtractorConfig",

    # API extractors
    "HTTPExtractor",
    "HTTPExtractorConfig",
]
