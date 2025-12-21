from bot_in import dp
from aiogram.types import CallbackQuery
from logger_setup import logger


@dp.callback_query()
async def others_to_start(callback: CallbackQuery):
    logger.info(f'Пользователь {callback.from_user.id} нажал что-то что не входит в основу')
    await callback.answer()
    await callback.message.answer( #type: ignore
        text="Нажмите /start"
    )