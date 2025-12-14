from aiogram.types import Message, CallbackQuery
from keyboards.markup import MainKeyboard
from sqlalchemy.ext.asyncio import AsyncSession
from repositories.base import BaseRepository
from db.models import User
from misc.utils import is_cached
from redis.asyncio import Redis
from bot_in import dp
from aiogram import F
from aiogram.filters import Command

@dp.message(Command("start"))
async def start_command(message: Message, session: AsyncSession, redis_cache: Redis):
    user_id = message.from_user.id #type: ignore
    username = message.from_user.username #type: ignore
    user = await is_cached(redis_cache=redis_cache, user_id=user_id, session=session)

    if user is None:
        repo = BaseRepository(session=session, model=User)
        user = await repo.create(
            user_id=user_id,
            username=username
        )
    
    if user.trial_used:
        await message.answer(
            text="Привет!",
            reply_markup=MainKeyboard.main_keyboard())
        return
    
    await message.answer(
        text="Привет!",
        reply_markup=MainKeyboard.main_keyboard_with_trial()
    )


@dp.callback_query(F.data == "start_menu")
async def start_callback(callback: CallbackQuery, session: AsyncSession, redis_cache: Redis):
    user_id = callback.from_user.id #type: ignore
    username = callback.from_user.username #type: ignore
    user = await is_cached(redis_cache=redis_cache, user_id=user_id, session=session)

    if user is None:
        repo = BaseRepository(session=session, model=User)
        user = await repo.create(
            user_id=user_id,
            username=username
        )
    
    if user.trial_used:
        await callback.message.edit_text( #type: ignore
            text="Привет!",
            reply_markup=MainKeyboard.main_keyboard())
        return
    
    await callback.message.edit_text( #type: ignore
        text="Привет!",
        reply_markup=MainKeyboard.main_keyboard_with_trial()
    )