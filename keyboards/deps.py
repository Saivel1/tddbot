from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

back = InlineKeyboardButton(text="⬅️ Назад", callback_data="start_menu")


class BackButton:

    @staticmethod
    def back_subs():
        return InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="⬅️ Назад", callback_data="subs")]
                ]
        )


    @staticmethod
    def back_start():
        return InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="⬅️ Назад", callback_data="start_menu")]
                ]
        )
    
    @staticmethod
    def back_pay_choose():
        return InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="⬅️ Назад", callback_data="pay_menu")]
                ]
        )