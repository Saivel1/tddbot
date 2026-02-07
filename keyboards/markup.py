from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, WebAppInfo
from config import settings

class MainKeyboard:
    
    @staticmethod
    def main_keyboard():
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="💳 Оплатить", callback_data="pay_menu")],
            [InlineKeyboardButton(text="🔗 Подписка и ссылки", callback_data="subs")],
            [InlineKeyboardButton(text="📱 Инструкция", callback_data="instruction")]
        ])
    
    @staticmethod
    def main_keyboard_with_trial():
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🎁 Пробный период", callback_data="trial")],
            [InlineKeyboardButton(text="💳 Оплатить", callback_data="pay_menu")],
            [InlineKeyboardButton(text="🔗 Подписка и ссылки", callback_data="subs")],
            [InlineKeyboardButton(text="📱 Инструкция", callback_data="instruction")]
        ])
    
    @staticmethod
    def main_keyboard_without_pay():
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔗 Подписка и ссылки", callback_data="subs")],
            [InlineKeyboardButton(text="📱 Инструкция", callback_data="instruction")]
        ])
    
    @staticmethod
    def main_keyboard_without_pay_back():
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔗 Подписка и ссылки", callback_data="subs")],
            [InlineKeyboardButton(text="📱 Инструкция", callback_data="instruction")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="start_menu")]
        ])


class Instruction:

    @staticmethod
    def web_app_keyboard(uuid):
        return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text="📱 Инструкция по установке",
            web_app=WebAppInfo(url=f"{settings.IN_GUIDE_LINK}{uuid}")
        )],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="start_menu")]
    ])


class Admin:

    @staticmethod
    def main_keyboard():
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Health check", callback_data="health")],
            [InlineKeyboardButton(text="Users count", callback_data="users_cnt")]
        ])
    

    @staticmethod
    def back():
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Назад", callback_data="admin_menu")]
        ])
    

class PayMenyMarkup:

    @staticmethod
    def pay_action(url: str):
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Ссылка на оплату", url=url)]
        ])