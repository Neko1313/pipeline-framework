#!/bin/bash
# Скрипт быстрого старта для pipeline-core

set -e

# Цвета для вывода
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}🚀 Быстрый старт pipeline-core${NC}"
echo

# Проверяем наличие uv
if ! command -v uv &> /dev/null; then
    echo -e "${RED}❌ uv не найден. Устанавливаем...${NC}"
    curl -LsSf https://astral.sh/uv/install.sh | sh
    export PATH="$HOME/.cargo/bin:$PATH"
    echo -e "${GREEN}✅ uv установлен${NC}"
else
    echo -e "${GREEN}✅ uv найден: $(uv --version)${NC}"
fi

# Проверяем Python версию
echo -e "${BLUE}🐍 Проверяем Python...${NC}"
if ! uv python list | grep -q "3.11\|3.12"; then
    echo -e "${YELLOW}⚠️  Устанавливаем Python 3.11...${NC}"
    uv python install 3.11
fi

echo -e "${GREEN}✅ Python готов${NC}"

# Устанавливаем зависимости
echo -e "${BLUE}📦 Устанавливаем зависимости...${NC}"
uv sync --dev

# Настраиваем pre-commit
echo -e "${BLUE}🔧 Настраиваем pre-commit хуки...${NC}"
uv run pre-commit install

# Запускаем тесты
echo -e "${BLUE}🧪 Запускаем тесты...${NC}"
uv run pytest tests/ -v

# Проверяем код
echo -e "${BLUE}🔍 Проверяем качество кода...${NC}"
uv run ruff check src tests
uv run ruff format --check src tests
uv run pyright

echo
echo -e "${GREEN}🎉 Готово! pipeline-core настроен и готов к использованию${NC}"
echo
echo -e "${BLUE}Полезные команды:${NC}"
echo -e "  ${YELLOW}make help${NC}           - показать все доступные команды"
echo -e "  ${YELLOW}make test${NC}           - запустить тесты"
echo -e "  ${YELLOW}make test-watch${NC}     - тесты в режиме watch"
echo -e "  ${YELLOW}make lint${NC}           - проверить код"
echo -e "  ${YELLOW}make format${NC}         - отформатировать код"
echo -e "  ${YELLOW}make type-check${NC}     - проверить типы"
echo -e "  ${YELLOW}make check-all${NC}      - запустить все проверки"
echo
echo -e "${BLUE}Для создания первого компонента:${NC}"
echo -e "  1. Создайте файл в ${YELLOW}src/pipeline_core/components/${NC}"
echo -e "  2. Наследуйтесь от ${YELLOW}BaseComponent${NC}"
echo -e "  3. Зарегистрируйте с помощью ${YELLOW}@register_component${NC}"
echo -e "  4. Запустите ${YELLOW}make test${NC} для проверки"
echo

# Создаем пример компонента если его нет
EXAMPLE_FILE="examples/my_first_component.py"
if [ ! -f "$EXAMPLE_FILE" ]; then
    echo -e "${BLUE}📝 Создаем пример компонента...${NC}"
    mkdir -p examples

    cat > "$EXAMPLE_FILE" << 'EOF'
"""
Мой первый компонент для pipeline-core
"""
from typing import Type
from pydantic import Field

from pipeline_core import (
    BaseComponent,
    ComponentConfig,
    ExecutionContext,
    ExecutionResult,
    ExecutionStatus,
    register_component
)


class MyFirstConfig(ComponentConfig):
    """Конфигурация моего первого компонента"""
    type: str = Field(default="my-first-component", const=True)
    greeting: str = Field(default="Hello", description="Приветствие")
    name: str = Field(..., description="Имя для приветствия")


@register_component("my-first-component")
class MyFirstComponent(BaseComponent):
    """Мой первый компонент - просто приветствует пользователя"""

    def get_config_model(self) -> Type[ComponentConfig]:
        return MyFirstConfig

    def execute(self, context: ExecutionContext) -> ExecutionResult:
        config: MyFirstConfig = self.config

        # Создаем приветствие
        message = f"{config.greeting}, {config.name}!"

        self.logger.info(f"Создано приветствие: {message}")

        return ExecutionResult(
            status=ExecutionStatus.SUCCESS,
            data=message,
            processed_records=1,
            metadata={"greeting": config.greeting, "name": config.name}
        )


# Пример использования
if __name__ == "__main__":
    from pipeline_core import ExecutionContext

    # Создаем компонент
    component = MyFirstComponent({
        "type": "my-first-component",
        "greeting": "Привет",
        "name": "Мир"
    })

    # Выполняем
    context = ExecutionContext()
    result = component.execute(context)

    print(f"Результат: {result.data}")
    print(f"Статус: {result.status}")
    print(f"Метаданные: {result.metadata}")
EOF

    echo -e "${GREEN}✅ Создан пример: ${EXAMPLE_FILE}${NC}"
    echo -e "${BLUE}Запустите: ${YELLOW}uv run python examples/my_first_component.py${NC}"
fi

echo -e "${GREEN}🎊 Добро пожаловать в pipeline-core!${NC}"