# Кеширование для тех, кто воспользовался пробным периодом
from aiogram.types import CallbackQuery
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession
import json

from misc.utils import worker_exsists, is_cached

from bot_in import dp
from aiogram import F

from keyboards.deps import BackButton


# Уведомление о начале работы пробного периода (о том, что он активирован) должно быть answer
@dp.callback_query(F.data == 'trial')
async def trial_handler(
    callback: CallbackQuery,
    redis_cache: Redis,
    session: AsyncSession
):
    user_id = callback.from_user.id
    username = callback.from_user.username
    cache = await is_cached(
        redis_cache=redis_cache, 
        user_id=user_id, 
        session=session
    )

    if cache:
        if cache.trial_used:
            await callback.message.edit_text( #type: ignore
                text="Пробный период уже активирован.",
                reply_markup=BackButton.back_start()
            )
            return "Уже активируем"

    data = {
        "user_id": user_id,
        "username": username
    }

    if await worker_exsists(redis_cli=redis_cache, worker="TRIAL_ACTIVATION", data=data): #sorted при преобразование по этому тип данных словарь
        await callback.message.edit_text( #type:ignore
            text="Пробный период в процессе активации. Ожидайте.",
            reply_markup=BackButton.back_start()
        )
        return "Ожидайте"
    
    else:
        await redis_cache.lpush(
            'TRIAL_ACTIVATION',
            json.dumps(
                data,
                sort_keys=True
            )
        ) #type: ignore

        await callback.message.edit_text( #type:ignore
            text="Активируем пробный период!",
            reply_markup=BackButton.back_start()
        )
        return "Создали задачу"