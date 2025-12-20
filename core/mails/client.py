import aiohttp
from config import settings as s
from logger_setup import logger
import secrets
import string

def generate_random_password(length: int = 16) -> str:
    """
    Генерирует случайный безопасный пароль
    
    Args:
        length: длина пароля (по умолчанию 16)
    
    Returns:
        Строка с паролем
    """
    # Все возможные символы
    alphabet = string.ascii_letters + string.digits + string.punctuation
    
    # Генерируем пароль
    password = ''.join(secrets.choice(alphabet) for _ in range(length))
    
    return password


class Anymessage():
    def __init__(self):
        self.email = None

    async def get_balance(self):
        async with aiohttp.ClientSession() as session:
            url = f'https://api.anymessage.shop/user/balance?token={s.ANY_TOKEN}'
            response = await session.get(url)
            data = await response.json()
            return data

    async def order_email(self):
        async with aiohttp.ClientSession() as session:
            try:
                url = f'https://api.anymessage.shop/email/order?token={s.ANY_TOKEN}&site={s.ANY_SITE}&domain={s.ANY_DOMAIN}'
                response = await session.get(url)
                data = await response.json()
                self.email = data['email']
                return self.email
            except Exception as e:
                balance = await self.get_balance()
                logger.warning(f'Баланс: {balance}')
                logger.warning(f'Ошибка в покупке email, функции order_email: {e}')


async def create_user_mailbox(user_id: int):
    """Создать почтовый ящик для пользователя"""
    email = f"user{user_id}@ivvpn.world"
    
    try:
        # Проверяем что не существует
        async with aiohttp.ClientSession() as session:
            async with session.get(f"http://localhost:8001/api/mailbox/check/{email}") as response:
                data = await response.json()
                
                if data["exists"]:
                    print(f"Ящик {email} уже существует")
                    return email
            
            # Создаём новый
            password = generate_random_password()
            
            async with session.post(
                "http://localhost:8001/api/mailbox/create",
                json={"email": email, "password": password}
            ) as response:
                if response.status == 200:
                    logger.info(f'Содан email: {email} password: {password}')
                    return email
                else:
                    return None
    except Exception as e:
        logger.error(e)
        return None