import asyncio
from aiogram import Bot
from config import settings

async def setup_webhook():
    """Установить webhook один раз"""
    bot = Bot(token=settings.BOT_TOKEN)
    
    webhook_url = f"https://{settings.WEBHOOK_URL}/bot-webhook"
    
    try:
        # Удалить старый webhook
        await bot.delete_webhook(drop_pending_updates=True)
        print("✅ Old webhook deleted")
        
        await asyncio.sleep(2)  # Подождать перед новым
        
        # Установить новый
        await bot.set_webhook(url=webhook_url, drop_pending_updates=True)
        print(f"✅ Webhook set: {webhook_url}")
        
        # Проверить
        info = await bot.get_webhook_info()
        print(f"📊 Webhook info: {info.url}")
        
    finally:
        await bot.session.close()

if __name__ == "__main__":
    asyncio.run(setup_webhook())