from granian import Granian
from granian.constants import Loops, Interfaces
import asyncio
from aiogram import Bot
from config import settings

async def setup_webhook():
    """Установить webhook один раз"""
    bot = Bot(token=settings.BOT_TOKEN)
    
    webhook_url = f"{settings.WEBHOOK_URL}"
    
    try:
        # Удалить старый webhook
        await bot.delete_webhook(drop_pending_updates=False)
        print("✅ Old webhook deleted")
        
        await asyncio.sleep(2)  # Подождать перед новым
        
        # Установить новый
        await bot.set_webhook(url=webhook_url, drop_pending_updates=False)
        print(f"✅ Webhook set: {webhook_url}")
        
        # Проверить
        info = await bot.get_webhook_info()
        print(f"📊 Webhook info: {info.url}")
        
    finally:
        await bot.session.close()


if __name__ == "__main__":
        asyncio.run(setup_webhook())
        Granian(
            target="app.main:app",
            address="0.0.0.0",
            port=8000,
            workers=1,
            loop=Loops.asyncio,
            log_enabled=True,
            interface=Interfaces.ASGI,
        ).serve()

