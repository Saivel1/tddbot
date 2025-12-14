TEST_BOT_TOKEN = "123456789:ABCdefGHIjklMNOpqrsTUVwxyz1234567890"
from config import settings

settings.BOT_TOKEN = TEST_BOT_TOKEN

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy.pool import StaticPool
from redis.asyncio import Redis
from db.models import Base, User, UserLinks
from repositories.base import BaseRepository
from litestar.testing import AsyncTestClient
from app.main import app
from unittest.mock import patch
import httpx


@pytest_asyncio.fixture
async def test_client():
    """httpx AsyncClient для прямых запросов к app"""
    from app.main import app
    
    # Запускаем Litestar app в тестовом режиме
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), # type: ignore
        base_url="http://testserver"
    ) as client:
        yield client


@pytest.fixture
def mock_bot_token(monkeypatch):
    """Подменяем токен бота на тестовый для безопасности"""
    test_token = "123456789:ABCdefGHIjklMNOpqrsTUVwxyz1234567890"
    monkeypatch.setenv("BOT_TOKEN", test_token)
    return test_token


@pytest_asyncio.fixture
async def redis_client():
    """Redis клиент для тестов"""
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


@pytest_asyncio.fixture(scope="function")
async def test_engine():
    """Движок для тестовой БД - новый для каждого теста"""
    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        poolclass=StaticPool,
        echo=False
    )
    
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    
    yield engine
    
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        
    await engine.dispose()


@pytest_asyncio.fixture(scope="function")
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


@pytest_asyncio.fixture
async def create_user(test_session_maker):
    """Fixture для создания тестовых пользователей"""
    async def _create_user(**kwargs):
        async with test_session_maker() as session:
            user = User(**kwargs)
            session.add(user)
            await session.commit()
            await session.refresh(user)
            return user
    return _create_user


@pytest_asyncio.fixture
async def create_user_in_links(test_session_maker):
    """Fixture для создания UserLinks"""
    async def _create_links(**kwargs):
        async with test_session_maker() as session:
            links = UserLinks(**kwargs)
            session.add(links)
            await session.commit()
            await session.refresh(links)
            return links
    return _create_links

@pytest.fixture
def inject_test_deps(test_session, redis_client):
    """Фикстура для инжекта тестовых зависимостей"""
    from bot_in import dp
    original_feed_update = dp.feed_update
    
    async def mock_feed_update(bot, update, **kwargs):
        return await original_feed_update(
            bot=bot,
            update=update,
            session=test_session,
            redis_cache=redis_client,
            redis_cahce=redis_client
        )
    
    with patch.object(dp, 'feed_update', side_effect=mock_feed_update):
        yield