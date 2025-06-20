"""
Configuration package для pipeline framework
"""

# Безопасные импорты
try:
    from pipeline_core.config.models import (
        PipelineConfig,
        StageConfig,
        RetryPolicy,
        PipelineMetadata,
        TemporalConfig,
        RuntimeConfig,
        FullPipelineConfig,
        QualityCheck,
        NotificationConfig,
        Environment,
        RetryPolicyType,
        WriteMode,
        DataFormat,
        LogLevel,
    )
    MODELS_AVAILABLE = True
except ImportError as e:
    import warnings
    warnings.warn(f"Config models import failed: {e}")
    MODELS_AVAILABLE = False
    PipelineConfig = None
    StageConfig = None
    RetryPolicy = None
    PipelineMetadata = None
    TemporalConfig = None
    RuntimeConfig = None
    FullPipelineConfig = None
    QualityCheck = None
    NotificationConfig = None
    Environment = None
    RetryPolicyType = None
    WriteMode = None
    DataFormat = None
    LogLevel = None

# Settings
try:
    from pipeline_core.config.settings import ComponentSettings
    SETTINGS_AVAILABLE = True
except ImportError as e:
    import warnings
    warnings.warn(f"Settings import failed: {e}")
    ComponentSettings = None
    SETTINGS_AVAILABLE = False

# YAML Loader
try:
    from pipeline_core.config.yaml_loader import YAMLConfigLoader
    YAML_LOADER_AVAILABLE = True
except ImportError as e:
    import warnings
    warnings.warn(f"YAML loader import failed: {e}")
    YAMLConfigLoader = None
    YAML_LOADER_AVAILABLE = False

# Формируем __all__ только из доступных модулей
__all__ = []

if MODELS_AVAILABLE:
    __all__.extend([
        "PipelineConfig",
        "StageConfig",
        "RetryPolicy",
        "PipelineMetadata",
        "TemporalConfig",
        "RuntimeConfig",
        "FullPipelineConfig",
        "QualityCheck",
        "NotificationConfig",
        "Environment",
        "RetryPolicyType",
        "WriteMode",
        "DataFormat",
        "LogLevel",
    ])

if SETTINGS_AVAILABLE:
    __all__.append("ComponentSettings")

if YAML_LOADER_AVAILABLE:
    __all__.append("YAMLConfigLoader")