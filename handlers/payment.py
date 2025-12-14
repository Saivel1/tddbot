from aiogram.types import CallbackQuery, InlineKeyboardButton
from sqlalchemy.ext.asyncio import AsyncSession
from keyboards.builder import PayMenu
from keyboards.deps import BackButton
from keyboards.markup import PayMenyMarkup
from misc.utils import cache_popular_pay_time, is_cached_payment
from redis.asyncio import Redis
from core.yoomoney.payment import YooPay
import json

from bot_in import dp
from aiogram import F


async def keyboard_build(order_url: str):
    to_pay = [InlineKeyboardButton(
        text="💳 Перейти к оплате", 
        url=order_url
    )]
    
    keyboard = BackButton.back_pay_choose()
    keyboard.inline_keyboard.insert(0, to_pay)
    return keyboard


@dp.callback_query(F.data == "pay_menu")
async def choose_sum(
    callback: CallbackQuery, 
    redis_cache: Redis
):
    user_id = callback.from_user.id
    await cache_popular_pay_time(user_id=user_id, redis_cache=redis_cache)

    await callback.message.edit_text( # type:ignore
        text="Тест",
        reply_markup=PayMenu.main_keyboard()
    )

from misc.bot_setup import prices

price_list = [v for k, v in prices]

# Обновление кэша на новый
@dp.callback_query(F.data.in_(price_list))
async def payment_process(
    callback: CallbackQuery, 
    redis_cache: Redis
):
    user_id = callback.from_user.id
    amount = int(callback.data.replace("pay_", "")) # type:ignore
    cache_pay = await is_cached_payment(redis_cache=redis_cache, user_id=user_id, amount=amount)
    
    if not cache_pay:
        yoo = YooPay()
        try:
            data = await yoo.create_payment(
                amount=amount,
                email='saivel.mezencev1@gmail.com', # email из локального сервиса по выдаче имэил
                plan="anything"
            )
            if data is None:
                await callback.message.edit_text( #type:ignore
                    text="Возникла ошибка"
                )
                return

            order_url = data[0]
            pay_reg = f"PAY:{user_id}:{amount}"

            data_for_load = {
                    "payment_url": data[0],
                    "payment_id": data[1]
                }
            await redis_cache.set(pay_reg, json.dumps(data_for_load), ex=600)

        except Exception as e:
            await callback.message.edit_text( #type:ignore
                text="Возникла ошибка"
            )
            return
    else:
        order_url = cache_pay.payment_url
    
    keyboard = await keyboard_build(order_url=order_url)

    await callback.message.edit_text( # type:ignore
        text=f"Сумма для оплаты {amount}",
        reply_markup=keyboard
    )