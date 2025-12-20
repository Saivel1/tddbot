from aiogram.types import CallbackQuery, InlineKeyboardButton
from sqlalchemy.ext.asyncio import AsyncSession
from keyboards.builder import PayMenu
from keyboards.deps import BackButton
from keyboards.markup import PayMenyMarkup
from misc.utils import cache_popular_pay_time, is_cached_payment
from redis.asyncio import Redis
from core.yoomoney.payment import YooPay
from core.mails.client import create_user_mailbox
import json

from bot_in import dp
from aiogram import F

from logger_setup import logger

async def create_order(amount: int, user_id):
    mail = await create_user_mailbox(user_id)
    logger.debug(mail)
    if not isinstance(mail, str):
        mail = 'saivel.mezencev1@gmail.com'
    yoo = YooPay()
    res = await yoo.create_payment(amount=amount, 
                                        plan=f"Подписка на {str((amount/50))} мес. {user_id}", 
                                        email=mail
    )
    logger.debug(res)
    return res

PAY_MENU_TEXT = """
💳 <b>Оформление подписки</b>

🪞 <b>IV VPN</b> — ваш безопасный доступ к свободному интернету.

<b>Что входит в подписку:</b>
✓ Безлимитный трафик
✓ Высокая скорость
✓ Все серверы доступны
✓ Поддержка 24/7
✓ Полная конфиденциальность

Выберите тариф:
"""


ERROR_TEXT = """
🚧 <b>Упс! Что-то пошло не так</b>

Мы уже работаем над решением проблемы.
Попробуйте обновить через пару минут 🔄

Нужна помощь? → /help
"""


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
        text=PAY_MENU_TEXT,
        reply_markup=PayMenu.main_keyboard(),
        parse_mode="HTML"
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
    
    logger.debug(cache_pay)
    if not cache_pay:
        try:
            data = await create_order(
                amount=amount,
                user_id=user_id
            )

            if data is None:
                raise ValueError

            order_url = data[0]
            pay_reg = f"PAY:{user_id}:{amount}"

            data_for_load = {
                    "payment_url": data[0],
                    "payment_id": data[1]
                }
            
            data_for_webhook = {
                    "user_id": user_id,
                    "amount": amount
            }
            
            web_wrk_label = f"YOO:{data[1]}"
            await redis_cache.set(pay_reg, json.dumps(data_for_load), ex=600)
            await redis_cache.set(web_wrk_label, json.dumps(data_for_webhook), ex=700)


        except:
            await callback.message.edit_text( #type:ignore
                    text=ERROR_TEXT,
                    parse_mode="HTML"
                )
            return
    else:
        order_url = cache_pay.payment_url
    
    keyboard = await keyboard_build(order_url=order_url)

    reply_text = f"""
Ссылка для оплаты:

{order_url}
"""

    await callback.message.edit_text( # type:ignore
        text=reply_text,
        reply_markup=keyboard,
        parse_mode="HTML"
    )