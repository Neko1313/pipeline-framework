"""
Базовые тесты для проверки работоспособности
"""

import pytest


def test_imports():
    """Тест базовых импортов"""
    try:
        from pipeline_core import (
            BaseComponent,
            ComponentConfig,
            ExecutionContext,
            ExecutionResult,
            ExecutionStatus,
            PipelineConfig,
            PipelineYAMLParser,
            get_registry,
        )

        # Проверяем что все импорты прошли успешно
        assert BaseComponent is not None
        assert ComponentConfig is not None
        assert ExecutionContext is not None
        assert ExecutionResult is not None
        assert ExecutionStatus is not None
        assert PipelineConfig is not None
        assert PipelineYAMLParser is not None
        assert get_registry is not None

    except ImportError as e:
        pytest.fail(f"Ошибка импорта: {e}")


def test_execution_context():
    """Тест базовой функциональности ExecutionContext"""
    from pipeline_core import ExecutionContext, ExecutionResult, ExecutionStatus

    context = ExecutionContext()

    # Тест глобальных переменных
    context.set_global_variable("test_var", "test_value")
    assert context.get_global_variable("test_var") == "test_value"
    assert context.get_global_variable("missing", "default") == "default"

    # Тест результатов стадий
    result = ExecutionResult(
        status=ExecutionStatus.SUCCESS, data="test_data", processed_records=1
    )

    context.set_stage_result("test_stage", result)
    retrieved_result = context.get_stage_result("test_stage")

    assert retrieved_result is not None
    assert retrieved_result.status == ExecutionStatus.SUCCESS
    assert retrieved_result.data == "test_data"
    assert context.get_stage_data("test_stage") == "test_data"
    assert context.has_stage_succeeded("test_stage") is True


def test_pipeline_metadata():
    """Тест создания метаданных pipeline"""
    from pipeline_core.models.pipeline import PipelineMetadata

    metadata = PipelineMetadata(
        name="test-pipeline", description="Test pipeline", version="1.0.0"
    )

    assert metadata.name == "test-pipeline"
    assert metadata.description == "Test pipeline"
    assert metadata.version == "1.0.0"


def test_stage_config():
    """Тест создания конфигурации стадии"""
    from pipeline_core.models.pipeline import StageConfig

    stage = StageConfig(
        type="test-component", config={"param": "value"}, depends_on=["other_stage"]
    )

    assert stage.type == "test-component"
    assert stage.config["param"] == "value"
    assert stage.depends_on == ["other_stage"]


def test_component_registry():
    """Тест базовой функциональности реестра"""
    from pipeline_core import ComponentRegistry

    registry = ComponentRegistry()

    # Проверяем что реестр создается
    assert registry is not None

    # Проверяем health check
    health = registry.health_check()
    assert isinstance(health, dict)
    assert "total_components" in health
    assert "total_plugins" in health


if __name__ == "__main__":
    pytest.main([__file__])
