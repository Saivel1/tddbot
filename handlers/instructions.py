from aiogram.types import CallbackQuery
from sqlalchemy.ext.asyncio import AsyncSession
from redis.asyncio import Redis

from keyboards.markup import Instruction
from repositories.base import BaseRepository
from misc.utils import is_cached

from db.models import UserLinks
from aiogram import F
from bot_in import dp

from handlers.deps import get_uuid_cache


@dp.callback_query(F.data == 'instruction')
async def menu(callback: CallbackQuery, session: AsyncSession, redis_cache: Redis):
    user_id = callback.from_user.id
    user = await is_cached(
        redis_cache=redis_cache,
        session=session,
        user_id=user_id
    )

    if user is None:
        await callback.message.edit_text( #type: ignore
            text="Нажмите /start для продолжения работы"
        )
        return
    
    if user.subscription_end is None:
        await callback.answer( #type: ignore 
            text="У вас нет подписки"
        )
        return
    
    uuid = await get_uuid_cache(
        redis_cache=redis_cache,
        user_id=user_id
    )

    await callback.message.edit_text( #type: ignore 
        text="🪞 Нажмите на кнопку ниже для просмотра инструкции:",
        reply_markup=Instruction.web_app_keyboard(uuid) # uuid
    )