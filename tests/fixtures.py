import pytest
from unittest.mock import AsyncMock, Mock
from aiogram import types
from typing import Any
from repositories.base import BaseRepository
from db.models import User, UserLinks


@pytest.fixture
def message():
    """Универсальная фикстура для Message"""
    
    def _create_message(
        text: str = "/start",
        user_id: int = 123,
        username: str = "iv_test",
        first_name: str = "Иван",
        chat_id: int | None = None,
        **kwargs: Any
    ) -> Mock:
        msg = Mock(spec=types.Message)
        
        # Основные атрибуты
        msg.message_id = kwargs.get("message_id", 1)
        msg.text = text
        
        # User
        msg.from_user = Mock(spec=types.User)
        msg.from_user.id = user_id
        msg.from_user.username = username
        msg.from_user.first_name = first_name
        msg.from_user.is_bot = False
        
        # Chat
        msg.chat = Mock(spec=types.Chat)
        msg.chat.id = chat_id or user_id
        msg.chat.type = kwargs.get("chat_type", "private")
        
        # Методы
        msg.answer = AsyncMock()
        msg.reply = AsyncMock()
        msg.edit_text = AsyncMock()
        
        return msg
    
    return _create_message


@pytest.fixture
def callback_query():
    """Универсальная фикстура для CallbackQuery"""
    
    def _create_callback(
        data: str = "pay_menu",
        user_id: int = 123,
        username: str = "iv_test",
        first_name: str = "Иван",
        chat_id: int | None = None,
        message_text: str | None = None,
        **kwargs: Any
    ) -> Mock:
        # Message
        message = Mock(spec=types.Message)
        message.message_id = kwargs.get("message_id", 1)
        message.text = message_text
        message.chat = Mock(spec=types.Chat)
        message.chat.id = chat_id or user_id
        message.chat.type = kwargs.get("chat_type", "private")
        message.edit_text = AsyncMock()
        message.delete = AsyncMock()
        message.answer = AsyncMock()
        
        # User
        user = Mock(spec=types.User)
        user.id = user_id
        user.username = username
        user.first_name = first_name
        user.is_bot = False
        
        # CallbackQuery
        callback = Mock(spec=types.CallbackQuery)
        callback.id = kwargs.get("callback_id", f"callback_{user_id}")
        callback.data = data
        callback.from_user = user
        callback.message = message
        callback.answer = AsyncMock()
        
        return callback
    
    return _create_callback


@pytest.fixture
def create_user(test_session):
    """С использованием репозитория"""

    async def _create(**kwargs):
        repo = BaseRepository(test_session, User)
        return await repo.create(**kwargs)
    
    return _create


@pytest.fixture
def create_user_in_links(test_session):
    """С использованием репозитория"""

    async def _create(**kwargs):
        repo = BaseRepository(test_session, UserLinks)
        return await repo.create(**kwargs)
    
    return _create