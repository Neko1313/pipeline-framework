"""
HTTP API Extractor - извлечение данных из HTTP API
"""
from typing import Any, Dict, List, Optional, AsyncGenerator, Union
import httpx
import asyncio
import json
from urllib.parse import urljoin, urlparse

from pipeline_core import ExecutionContext
from ..base import BaseExtractor, StreamingExtractor, ExtractorConfig
from pydantic import Field, field_validator


class HTTPExtractorConfig(ExtractorConfig):
    """Конфигурация HTTP экстрактора"""
    type: str = Field(default="http-extractor", const=True)

    # URL и аутентификация
    base_url: str = Field(..., description="Базовый URL API")
    endpoint: str = Field(default="", description="Конечная точка API")
    method: str = Field(default="GET", description="HTTP метод")

    # Параметры запроса
    headers: Dict[str, str] = Field(default_factory=dict, description="HTTP заголовки")
    params: Dict[str, Any] = Field(default_factory=dict, description="Query параметры")
    data: Optional[Dict[str, Any]] = Field(None, description="Данные для POST/PUT")

    # Аутентификация
    auth_type: Optional[str] = Field(None, description="Тип аутентификации: bearer, basic, api_key")
    auth_token: Optional[str] = Field(None, description="Токен для аутентификации")
    auth_username: Optional[str] = Field(None, description="Имя пользователя для basic auth")
    auth_password: Optional[str] = Field(None, description="Пароль для basic auth")
    api_key_header: str = Field(default="X-API-Key", description="Заголовок для API ключа")

    # Параметры запроса
    timeout: int = Field(default=30, description="Таймаут запроса в секундах")
    follow_redirects: bool = Field(default=True, description="Следовать редиректам")
    verify_ssl: bool = Field(default=True, description="Проверять SSL сертификаты")

    # Пагинация
    pagination_enabled: bool = Field(default=False, description="Включить поддержку пагинации")
    pagination_type: str = Field(default="offset", description="Тип пагинации: offset, cursor, page")
    pagination_param: str = Field(default="offset", description="Параметр для пагинации")
    pagination_size_param: str = Field(default="limit", description="Параметр размера страницы")
    pagination_size: int = Field(default=100, description="Размер страницы")

    # Обработка ответа
    response_data_path: Optional[str] = Field(None, description="JSONPath к данным в ответе")
    response_format: str = Field(default="json", description="Формат ответа: json, xml, csv, text")

    @field_validator('base_url')
    @classmethod
    def validate_base_url(cls, v: str) -> str:
        if not v:
            raise ValueError("base_url не может быть пустым")

        parsed = urlparse(v)
        if not parsed.scheme or not parsed.netloc:
            raise ValueError("base_url должен быть валидным URL")

        return v.rstrip('/')

    @field_validator('method')
    @classmethod
    def validate_method(cls, v: str) -> str:
        allowed_methods = ["GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"]
        if v.upper() not in allowed_methods:
            raise ValueError(f"method должен быть одним из: {allowed_methods}")
        return v.upper()


class HTTPExtractor(StreamingExtractor):
    """
    Экстрактор для HTTP API

    Поддерживает:
    - Различные методы HTTP
    - Аутентификацию (Bearer, Basic, API Key)
    - Пагинацию (offset, cursor, page)
    - Различные форматы ответов
    - Retry механизмы
    """

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self._client: Optional[httpx.AsyncClient] = None

    def get_config_model(self):
        return HTTPExtractorConfig

    def extract_data(self, context: ExecutionContext) -> Any:
        """Синхронное извлечение данных через HTTP"""
        return asyncio.run(self.extract_data_async(context))

    async def extract_data_async(self, context: ExecutionContext) -> Any:
        """Асинхронное извлечение данных через HTTP"""
        config: HTTPExtractorConfig = self.config

        if config.pagination_enabled:
            # Пагинированное извлечение
            all_data = []
            async for batch in self.extract_batches(context):
                all_data.extend(batch)
            return all_data
        else:
            # Одиночный запрос
            async with self._get_client() as client:
                response_data = await self._make_request(client, config)
                return self._process_response(response_data, config)

    async def extract_batches(self, context: ExecutionContext) -> AsyncGenerator[List[Dict], None]:
        """Генератор batch'ей для пагинированных запросов"""
        config: HTTPExtractorConfig = self.config

        async with self._get_client() as client:
            if config.pagination_type == "offset":
                yield from self._paginate_offset(client, config)
            elif config.pagination_type == "cursor":
                yield from self._paginate_cursor(client, config)
            elif config.pagination_type == "page":
                yield from self._paginate_page(client, config)
            else:
                raise ValueError(f"Неподдерживаемый тип пагинации: {config.pagination_type}")

    async def _paginate_offset(self, client: httpx.AsyncClient, config: HTTPExtractorConfig) -> AsyncGenerator[List[Dict], None]:
        """Пагинация через offset/limit"""
        offset = 0

        while True:
            # Подготавливаем параметры с offset
            params = dict(config.params)
            params[config.pagination_param] = offset
            params[config.pagination_size_param] = config.pagination_size

            # Выполняем запрос
            response_data = await self._make_request(client, config, params)
            batch_data = self._process_response(response_data, config)

            if not batch_data:
                break

            yield batch_data

            # Проверяем нужно ли продолжить
            if len(batch_data) < config.pagination_size:
                break

            offset += config.pagination_size

            # Проверяем лимиты
            if config.max_records and offset >= config.max_records:
                break

    async def _paginate_cursor(self, client: httpx.AsyncClient, config: HTTPExtractorConfig) -> AsyncGenerator[List[Dict], None]:
        """Пагинация через cursor"""
        cursor = None

        while True:
            # Подготавливаем параметры с cursor
            params = dict(config.params)
            if cursor:
                params[config.pagination_param] = cursor
            params[config.pagination_size_param] = config.pagination_size

            # Выполняем запрос
            response_data = await self._make_request(client, config, params)
            batch_data = self._process_response(response_data, config)

            if not batch_data:
                break

            yield batch_data

            # Извлекаем следующий cursor из ответа
            cursor = self._extract_cursor(response_data)
            if not cursor:
                break

    async def _paginate_page(self, client: httpx.AsyncClient, config: HTTPExtractorConfig) -> AsyncGenerator[List[Dict], None]:
        """Пагинация через номер страницы"""
        page = 1

        while True:
            # Подготавливаем параметры со страницей
            params = dict(config.params)
            params[config.pagination_param] = page
            params[config.pagination_size_param] = config.pagination_size

            # Выполняем запрос
            response_data = await self._make_request(client, config, params)
            batch_data = self._process_response(response_data, config)

            if not batch_data:
                break

            yield batch_data

            # Проверяем нужно ли продолжить
            if len(batch_data) < config.pagination_size:
                break

            page += 1

    async def _make_request(self, client: httpx.AsyncClient, config: HTTPExtractorConfig,
                            custom_params: Optional[Dict] = None) -> Any:
        """Выполнение HTTP запроса"""
        # Строим URL
        url = urljoin(config.base_url + '/', config.endpoint.lstrip('/'))

        # Подготавливаем параметры
        params = custom_params or config.params
        headers = self._prepare_headers(config)

        # Подготавливаем данные для POST/PUT
        request_data = None
        json_data = None
        if config.data and config.method in ["POST", "PUT", "PATCH"]:
            if isinstance(config.data, dict):
                json_data = config.data
            else:
                request_data = config.data

        self.logger.debug(f"Выполнение {config.method} запроса к {url}")

        # Выполняем запрос с retry
        response = await self.with_retry_async(
            client.request,
            method=config.method,
            url=url,
            params=params,
            headers=headers,
            json=json_data,
            data=request_data,
            timeout=config.timeout,
            follow_redirects=config.follow_redirects
        )

        # Проверяем статус ответа
        response.raise_for_status()

        # Возвращаем данные в зависимости от формата
        if config.response_format == "json":
            return response.json()
        elif config.response_format == "text":
            return response.text
        elif config.response_format == "csv":
            return response.text
        else:
            return response.content

    def _prepare_headers(self, config: HTTPExtractorConfig) -> Dict[str, str]:
        """Подготовка заголовков с аутентификацией"""
        headers = dict(config.headers)

        # Добавляем аутентификацию
        if config.auth_type == "bearer" and config.auth_token:
            headers["Authorization"] = f"Bearer {config.auth_token}"
        elif config.auth_type == "api_key" and config.auth_token:
            headers[config.api_key_header] = config.auth_token

        # Устанавливаем Content-Type если не задан
        if config.method in ["POST", "PUT", "PATCH"] and "Content-Type" not in headers:
            headers["Content-Type"] = "application/json"

        return headers

    def _process_response(self, response_data: Any, config: HTTPExtractorConfig) -> List[Dict]:
        """Обработка ответа API"""
        if config.response_format == "json":
            # Извлекаем данные по JSONPath если указан
            if config.response_data_path:
                data = self._extract_json_path(response_data, config.response_data_path)
            else:
                data = response_data

            # Преобразуем в список словарей
            if isinstance(data, list):
                return data
            elif isinstance(data, dict):
                return [data]
            else:
                return [{"data": data}]

        elif config.response_format == "csv":
            # Парсим CSV
            import csv
            import io

            csv_data = []
            reader = csv.DictReader(io.StringIO(response_data))
            for row in reader:
                csv_data.append(row)
            return csv_data

        else:
            # Для других форматов возвращаем как есть
            return [{"data": response_data}]

    def _extract_json_path(self, data: Any, path: str) -> Any:
        """Простое извлечение данных по JSONPath"""
        # Простая реализация для основных случаев
        parts = path.split('.')
        current = data

        for part in parts:
            if isinstance(current, dict):
                current = current.get(part)
            elif isinstance(current, list) and part.isdigit():
                index = int(part)
                current = current[index] if index < len(current) else None
            else:
                return None

        return current

    def _extract_cursor(self, response_data: Any) -> Optional[str]:
        """Извлечение cursor для следующей страницы"""
        # Пытаемся найти cursor в стандартных местах
        if isinstance(response_data, dict):
            # Проверяем стандартные поля
            for cursor_field in ["next_cursor", "cursor", "next", "continuation_token"]:
                if cursor_field in response_data:
                    return response_data[cursor_field]

            # Проверяем в метаданных
            if "meta" in response_data and isinstance(response_data["meta"], dict):
                for cursor_field in ["next_cursor", "cursor", "next"]:
                    if cursor_field in response_data["meta"]:
                        return response_data["meta"][cursor_field]

        return None

    async def with_retry_async(self, func, *args, **kwargs):
        """Асинхронная версия retry логики"""
        config: HTTPExtractorConfig = self.config
        last_exception = None

        for attempt in range(config.retry_attempts + 1):
            try:
                return await func(*args, **kwargs)
            except Exception as e:
                last_exception = e
                if attempt == config.retry_attempts:
                    break

                delay = config.retry_delay * (config.retry_exponential_base ** attempt)
                self.logger.warning(
                    f"HTTP запрос неудачен, попытка {attempt + 1}: {e}. "
                    f"Повтор через {delay:.2f} сек"
                )
                await asyncio.sleep(delay)

        raise last_exception

    def _get_client(self) -> httpx.AsyncClient:
        """Получение HTTP клиента"""
        config: HTTPExtractorConfig = self.config

        # Подготавливаем auth для httpx
        auth = None
        if config.auth_type == "basic" and config.auth_username and config.auth_password:
            auth = httpx.BasicAuth(config.auth_username, config.auth_password)

        return httpx.AsyncClient(
            verify=config.verify_ssl,
            auth=auth,
            timeout=config.timeout
        )

    def setup(self) -> None:
        """Инициализация"""
        config: HTTPExtractorConfig = self.config
        self.logger.info(f"Инициализация HTTP экстрактора для: {config.base_url}")

    def teardown(self) -> None:
        """Очистка ресурсов"""
        if self._client:
            asyncio.run(self._client.aclose())
        self.logger.debug("Очистка ресурсов HTTP экстрактора")
