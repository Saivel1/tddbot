from config import settings
import pytest, pytest_asyncio
import json
from datetime import datetime, timedelta
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from litestar.testing import AsyncTestClient
from unittest.mock import AsyncMock, MagicMock, patch
from db.models import Base
import httpx

# Устанавливаем токен перед импортом
settings.BOT_TOKEN = "123456789:ABCdefGHIjklMNOpqrsTUVwxyz1234567890"

from db.models import User, UserLinks
from repositories.base import BaseRepository


@pytest_asyncio.fixture
async def redis_client():
    """Один Redis клиент на всю сессию"""
    client = Redis(
        host='localhost',
        port=6379,
        decode_responses=True
    )
    yield client
    await client.flushall()
    await client.aclose()


@pytest_asyncio.fixture(autouse=True)
async def clear_redis(redis_client):
    """Очищаем Redis перед каждым тестом"""
    await redis_client.flushdb()
    yield


@pytest_asyncio.fixture(scope="session")
async def test_engine():
    """Движок для тестовой БД"""
    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        echo=False
    )
    
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    
    yield engine
    await engine.dispose()


@pytest_asyncio.fixture
async def test_session_maker(test_engine):
    """Session maker для тестов"""
    return async_sessionmaker(
        test_engine,
        class_=AsyncSession,
        expire_on_commit=False
    )


@pytest_asyncio.fixture
async def test_session(test_session_maker):
    """Одна сессия для теста"""
    async with test_session_maker() as session:
        yield session


@pytest.mark.asyncio
async def test_bot_webhook_start_command_new_user(
    test_client: httpx.AsyncClient,
    test_session: AsyncSession,
    inject_test_deps
):
    """Тест: /start для нового пользователя"""
    user_id = 12345
    
    webhook_data = {
        "update_id": 123456789,
        "message": {
            "message_id": 1,
            "from": {
                "id": user_id,
                "is_bot": False,
                "first_name": "Test",
                "username": "testuser",
                "language_code": "ru"
            },
            "chat": {
                "id": user_id,
                "type": "private"
            },
            "date": int(datetime.now().timestamp()),
            "text": "/start"
        }
    }
    
    # ✅ Мокаем session.request (aiohttp) вместо TelegramMethod
    with patch('aiogram.client.session.aiohttp.AiohttpSession.make_request', new_callable=AsyncMock) as mock_request:
        # Возвращаем успешный ответ от "Telegram API"
        mock_request.return_value = {
            "ok": True,
            "result": {
                "message_id": 2,
                "chat": {"id": user_id, "type": "private"},
                "text": "Привет!"
            }
        }
        
        response = await test_client.post("/bot-webhook", json=webhook_data)
        
        assert response.status_code == 200
        assert response.json() == {"ok": True}
        
        # Проверяем что бот вызвал API
        assert mock_request.called
    
    # Проверяем БД
    repo = BaseRepository(session=test_session, model=User)
    user = await repo.get_one(user_id=user_id)
    
    assert user is not None
    assert user.user_id == user_id
    assert user.username == "testuser"


@pytest.mark.asyncio
async def test_bot_webhook_start_trial_used(
    test_client: httpx.AsyncClient,
    test_session: AsyncSession,
    create_user,
    inject_test_deps
):
    """Тест: /start для пользователя с триалом"""
    user_id = 54321
    
    await create_user(
        user_id=user_id,
        username="trial_user",
        trial_used=True,
        subscription_end=datetime.now() - timedelta(days=1)
    )
    
    webhook_data = {
        "update_id": 123456790,
        "message": {
            "message_id": 2,
            "from": {
                "id": user_id,
                "is_bot": False,
                "first_name": "Trial",
                "username": "trial_user",
                "language_code": "ru"
            },
            "chat": {
                "id": user_id,
                "type": "private"
            },
            "date": int(datetime.now().timestamp()),
            "text": "/start"
        }
    }
    
    with patch('aiogram.client.session.aiohttp.AiohttpSession.make_request', new_callable=AsyncMock) as mock_request:
        mock_request.return_value = {
            "ok": True,
            "result": {"message_id": 3}
        }
        
        response = await test_client.post("/bot-webhook", json=webhook_data)
        
        assert response.status_code == 200
    
    repo = BaseRepository(session=test_session, model=User)
    user = await repo.get_one(user_id=user_id)
    assert user.trial_used is True #type: ignore


@pytest.mark.asyncio
async def test_bot_webhook_pay_menu_callback(
    test_client: httpx.AsyncClient,
    inject_test_deps
):
    """Тест: callback pay_menu"""
    user_id = 11111
    
    webhook_data = {
        "update_id": 123456791,
        "callback_query": {
            "id": "callback_123",
            "from": {
                "id": user_id,
                "is_bot": False,
                "first_name": "Pay",
                "username": "payuser"
            },
            "message": {
                "message_id": 10,
                "from": {
                    "id": 987654321,
                    "is_bot": True,
                    "first_name": "TestBot"
                },
                "chat": {
                    "id": user_id,
                    "type": "private"
                },
                "date": int(datetime.now().timestamp()),
                "text": "Главное меню"
            },
            "chat_instance": "123456789",
            "data": "pay_menu"
        }
    }
    
    with patch('aiogram.client.session.aiohttp.AiohttpSession.make_request', new_callable=AsyncMock) as mock_request:
        mock_request.return_value = {"ok": True, "result": True}
        
        response = await test_client.post("/bot-webhook", json=webhook_data)
        
        assert response.status_code == 200
        assert mock_request.called


@pytest.mark.asyncio
async def test_bot_webhook_malformed_update(test_client: httpx.AsyncClient):
    """Тест: невалидный update"""
    
    response = await test_client.post("/bot-webhook", json={})
    assert response.status_code == 422