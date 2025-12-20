import subprocess
import aiohttp
from config import settings as s
from logger_setup import logger
import secrets
import string
import re


def run_docker_command(command: list) -> tuple[bool, str]:
    """Выполнить команду в контейнере mailserver"""
    try:
        result = subprocess.run(
            ["docker", "exec", "mailserver"] + command,
            capture_output=True,
            text=True,
            check=True,
            timeout=10
        )
        return True, result.stdout
    except subprocess.CalledProcessError as e:
        logger.error(f"Command failed: {e.stderr}")
        return False, e.stderr
    except subprocess.TimeoutExpired:
        logger.error("Command timeout")
        return False, "Timeout"
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return False, str(e)


async def list_mailboxes():
    """Список всех ящиков"""
    success, output = run_docker_command(["setup", "email", "list"])

    if success:
        mailboxes = []

        # Регулярка для извлечения email
        email_pattern = r'[\w\.-]+@[\w\.-]+\.\w+'

        for line in output.strip().split("\n"):
            match = re.search(email_pattern, line)
            if match:
                mailboxes.append(match.group(0))

        return {
            "count": len(mailboxes),
            "mailboxes": mailboxes
        }
    else:
        return None


async def check_mailbox_exists(email: str):
    """Проверить существование ящика"""
    try:
        response = await list_mailboxes()
        if response is None:
            return None

        exists = email in response["mailboxes"]
        
        return {
            "exists": exists,
            "email": email
        }
    except Exception as e:
        logger.error(f"Error checking mailbox: {e}")
        return None
    


async def create_mailbox(mailbox: str, pwd: str):
    """Создать почтовый ящик"""
    logger.info(f"Creating mailbox: {mailbox}")
    
    success, output = run_docker_command([
        "setup", "email", "add", mailbox, pwd
    ])
    
    if success:
        return {
            "status": "created",
            "email": mailbox
        }
    else:
        return None
    

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
    email = f"user{user_id}@docs-sharing.world"
    
    try:
        data = await check_mailbox_exists(email=email)

        if data is None:
            return None

        if data["exists"]:
            logger.info(f"Ящик {email} уже существует")
            return email

        password = generate_random_password()

        email = await create_mailbox(
            mailbox=email,
            pwd=password
        )

        logger.info(f'Содан email: {email} password: {password}')
        return email
    except Exception as e:
        logger.error(e)
        return None