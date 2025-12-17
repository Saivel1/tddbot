from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.types import InlineKeyboardButton
from misc.bot_setup import *
from .deps import back

class PayMenu:

    @staticmethod
    def main_keyboard():
        builder = InlineKeyboardBuilder()

        for text, callback_data in prices:
            builder.add(InlineKeyboardButton(text=text, callback_data=callback_data))

        builder.add(back)
        builder.adjust(1)
        return builder.as_markup()
    

class SubMenu:

    @staticmethod
    def links_keyboard(links: list):
        builder = InlineKeyboardBuilder()

        cnt = 0
        for text in links:
            builder.add(InlineKeyboardButton(text=text, callback_data=f'sub_{cnt}'))
            cnt += 1

        builder.add(back)
        builder.adjust(1)
        return builder.as_markup()