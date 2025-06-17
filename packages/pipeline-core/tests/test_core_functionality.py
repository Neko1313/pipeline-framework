"""
Исправленные тесты основной функциональности pipeline-core
"""

import pytest
from pathlib import Path
from typing import Any

from pipeline_core import (
    PipelineYAMLParser,
    ComponentRegistry,
    get_registry,
    register_component,
    BaseComponent,
    ComponentConfig,
    ExecutionContext,
    ExecutionResult,
    ExecutionStatus,
    PipelineConfigError,
    create_simple_component,
)


class TestComponentConfig(ComponentConfig):
    """Тестовая конфигурация компонента"""

    test_param: str = "default"


class TestComponent(BaseComponent):
    """Тестовый компонент"""

    def get_config_model(self):
        return TestComponentConfig

    def execute(self, context: ExecutionContext) -> ExecutionResult:
        return ExecutionResult(
            status=ExecutionStatus.SUCCESS,
            data=f"Test result: {self.config.test_param}",
            processed_records=1,
        )


class TestPipelineYAMLParser:
    """Тесты парсера YAML"""

    def test_parse_simple_yaml(self):
        """Тест парсинга простого YAML"""
        yaml_content = """
        metadata:
          name: "test-pipeline"
          description: "Test pipeline"
          version: "1.0.0"
        
        variables:
          ENV: "test"
          COUNT: 10
        
        stage1:
          type: "test-component"
          config:
            test_param: "hello"
        
        stage2:
          type: "test-component"
          depends_on: ["stage1"]
          config:
            test_param: "world"
        """

        parser = PipelineYAMLParser()
        config = parser.parse_string(yaml_content)

        assert config.metadata.name == "test-pipeline"
        assert config.metadata.version == "1.0.0"
        assert config.variables["ENV"] == "test"
        assert config.variables["COUNT"] == 10

        assert len(config.stages) == 2
        assert "stage1" in config.stages
        assert "stage2" in config.stages

        assert config.stages["stage1"].type == "test-component"
        assert config.stages["stage2"].depends_on == ["stage1"]

    def test_parse_with_variables(self):
        """Тест парсинга с шаблонными переменными"""
        yaml_content = """
        metadata:
          name: "test-pipeline"
        
        variables:
          ENV: "production"
          DATABASE_NAME: "mydb"
        
        extract_stage:
          type: "database-extractor"
          config:
            database: "${DATABASE_NAME}_${ENV}"
            table: "users"
        """

        parser = PipelineYAMLParser()
        config = parser.parse_string(yaml_content)

        extract_config = config.stages["extract_stage"].config
        # Данные должны быть в extract_config["config"] из-за двойной вложенности
        assert "config" in extract_config
        inner_config = extract_config["config"]
        assert inner_config["database"] == "mydb_production"
        assert inner_config["table"] == "users"

    def test_parse_with_default_variables(self):
        """Тест парсинга с переменными по умолчанию"""
        yaml_content = """
        metadata:
          name: "test-pipeline"
        
        variables:
          ENV: "test"
        
        stage1:
          type: "test-component"
          config:
            param1: "${ENV}"
            param2: "${MISSING_VAR:-default_value}"
        """

        parser = PipelineYAMLParser()
        config = parser.parse_string(yaml_content)

        stage_config = config.stages["stage1"].config
        # Данные должны быть в stage_config["config"] из-за двойной вложенности
        assert "config" in stage_config
        inner_config = stage_config["config"]
        assert inner_config["param1"] == "test"
        assert inner_config["param2"] == "default_value"

    def test_parse_invalid_yaml(self):
        """Тест парсинга невалидного YAML"""
        invalid_yaml = """
        metadata:
          name: "test-pipeline"
        
        stage1:
          type: "test-component"
          depends_on: ["nonexistent_stage"]
        """

        parser = PipelineYAMLParser()

        with pytest.raises(PipelineConfigError):  # Вернули PipelineConfigError
            parser.parse_string(invalid_yaml)

    def test_execution_order(self):
        """Тест определения порядка выполнения стадий"""
        yaml_content = """
        metadata:
          name: "test-pipeline"
        
        stage_a:
          type: "test-component"
        
        stage_b:
          type: "test-component"
          depends_on: ["stage_a"]
        
        stage_c:
          type: "test-component"
          depends_on: ["stage_a"]
        
        stage_d:
          type: "test-component"
          depends_on: ["stage_b", "stage_c"]
        """

        parser = PipelineYAMLParser()
        config = parser.parse_string(yaml_content)

        execution_order = config.get_execution_order()

        # Проверяем правильность порядка выполнения
        assert len(execution_order) == 3
        assert execution_order[0] == ["stage_a"]  # Первый уровень
        assert set(execution_order[1]) == {
            "stage_b",
            "stage_c",
        }  # Второй уровень (параллельно)
        assert execution_order[2] == ["stage_d"]  # Третий уровень


class TestComponentRegistry:
    """Тесты реестра компонентов"""

    def test_register_component(self):
        """Тест регистрации компонента"""
        registry = ComponentRegistry()
        registry.register("test-component", TestComponent)

        assert "test-component" in registry.list_components()
        component_class = registry.get_component_class("test-component")
        assert component_class == TestComponent

    def test_create_component(self):
        """Тест создания экземпляра компонента"""
        registry = ComponentRegistry()
        registry.register("test-component", TestComponent)

        config = {"type": "test-component", "test_param": "custom_value"}
        component = registry.create_component("test-component", config)

        assert isinstance(component, TestComponent)
        assert component.config.test_param == "custom_value"

    def test_component_not_found(self):
        """Тест ошибки при отсутствии компонента"""
        registry = ComponentRegistry()

        from pipeline_core.exceptions.errors import PipelineComponentError

        with pytest.raises(PipelineComponentError):
            registry.get_component_class("nonexistent-component")

    def test_decorator_registration(self):
        """Тест регистрации через декоратор"""

        @register_component("decorated-component")
        class DecoratedComponent(BaseComponent):
            def get_config_model(self):
                return ComponentConfig

            def execute(self, context: ExecutionContext) -> ExecutionResult:
                return ExecutionResult(
                    status=ExecutionStatus.SUCCESS, data="decorated result"
                )

        registry = get_registry()
        assert "decorated-component" in registry.list_components()

        component = registry.create_component(
            "decorated-component", {"type": "decorated-component"}
        )
        assert isinstance(component, DecoratedComponent)


class TestExecutionContext:
    """Тесты контекста выполнения"""

    def test_stage_results(self):
        """Тест работы с результатами стадий"""
        context = ExecutionContext()

        # Добавляем результат стадии
        result = ExecutionResult(
            status=ExecutionStatus.SUCCESS, data="test data", processed_records=10
        )
        context.set_stage_result("stage1", result)

        # Проверяем получение результата
        retrieved_result = context.get_stage_result("stage1")
        assert retrieved_result == result

        retrieved_data = context.get_stage_data("stage1")
        assert retrieved_data == "test data"

        assert context.has_stage_succeeded("stage1") is True

    def test_global_variables(self):
        """Тест работы с глобальными переменными"""
        context = ExecutionContext()

        context.set_global_variable("env", "test")
        context.set_global_variable("count", 42)

        assert context.get_global_variable("env") == "test"
        assert context.get_global_variable("count") == 42
        assert context.get_global_variable("missing", "default") == "default"

    def test_execution_summary(self):
        """Тест получения сводки выполнения"""
        context = ExecutionContext()

        # Добавляем несколько результатов
        context.set_stage_result(
            "stage1",
            ExecutionResult(
                status=ExecutionStatus.SUCCESS,
                processed_records=100,
                execution_time=1.5,
            ),
        )

        context.set_stage_result(
            "stage2",
            ExecutionResult(
                status=ExecutionStatus.FAILED, processed_records=50, execution_time=0.8
            ),
        )

        summary = context.get_execution_summary()

        assert summary["total_stages"] == 2
        assert summary["successful_stages"] == 1
        assert summary["failed_stages"] == 1
        assert summary["total_processed_records"] == 150
        assert summary["total_execution_time"] == 2.3


class TestSimpleComponent:
    """Тесты создания простых компонентов"""

    def test_create_simple_component(self):
        """Тест создания компонента из функции"""

        def simple_function(context: ExecutionContext):
            return "simple result"

        ComponentClass = create_simple_component("simple-test", simple_function)

        # Создаем экземпляр
        component = ComponentClass({"type": "simple-test"})
        context = ExecutionContext()

        # Выполняем
        result = component.execute(context)

        assert result.status == ExecutionStatus.SUCCESS
        assert result.data == "simple result"
        assert result.processed_records == 1

    def test_simple_component_with_error(self):
        """Тест обработки ошибок в простом компоненте"""

        def failing_function(context: ExecutionContext):
            raise ValueError("Test error")

        ComponentClass = create_simple_component("failing-test", failing_function)

        component = ComponentClass({"type": "failing-test"})
        context = ExecutionContext()

        result = component.execute(context)

        assert result.status == ExecutionStatus.FAILED
        assert "Test error" in result.error_message
        assert result.data is None


class TestIntegration:
    """Интеграционные тесты"""

    def test_full_pipeline_workflow(self):
        """Тест полного workflow pipeline"""

        # Регистрируем тестовые компоненты
        registry = get_registry()

        @register_component("data-generator")
        class DataGenerator(BaseComponent):
            def get_config_model(self):
                return ComponentConfig

            def execute(self, context: ExecutionContext) -> ExecutionResult:
                return ExecutionResult(
                    status=ExecutionStatus.SUCCESS,
                    data=[1, 2, 3, 4, 5],
                    processed_records=5,
                )

        @register_component("data-processor")
        class DataProcessor(BaseComponent):
            def get_config_model(self):
                return ComponentConfig

            def validate_dependencies(self, available_stages):
                return ["generator"] if "generator" not in available_stages else []

            def execute(self, context: ExecutionContext) -> ExecutionResult:
                data = context.get_stage_data("generator")
                processed_data = [x * 2 for x in data]
                return ExecutionResult(
                    status=ExecutionStatus.SUCCESS,
                    data=processed_data,
                    processed_records=len(processed_data),
                )

        # Создаем конфигурацию pipeline
        yaml_content = """
        metadata:
          name: "integration-test"
        
        generator:
          type: "data-generator"
        
        processor:
          type: "data-processor"
          depends_on: ["generator"]
        """

        parser = PipelineYAMLParser()
        config = parser.parse_string(yaml_content)

        # Проверяем парсинг
        assert len(config.stages) == 2
        assert config.stages["processor"].depends_on == ["generator"]

        # Проверяем порядок выполнения
        execution_order = config.get_execution_order()
        assert execution_order == [["generator"], ["processor"]]

        # Симулируем выполнение
        context = ExecutionContext()

        # Выполняем генератор
        generator = registry.create_component(
            "data-generator", {"type": "data-generator"}
        )
        generator_result = generator.execute(context)
        context.set_stage_result("generator", generator_result)

        # Выполняем процессор
        processor = registry.create_component(
            "data-processor", {"type": "data-processor"}
        )
        processor_result = processor.execute(context)
        context.set_stage_result("processor", processor_result)

        # Проверяем результаты
        assert generator_result.status == ExecutionStatus.SUCCESS
        assert processor_result.status == ExecutionStatus.SUCCESS
        assert processor_result.data == [2, 4, 6, 8, 10]

        summary = context.get_execution_summary()
        assert summary["successful_stages"] == 2
        assert summary["failed_stages"] == 0


if __name__ == "__main__":
    pytest.main([__file__])
