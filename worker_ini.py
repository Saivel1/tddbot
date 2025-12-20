import asyncio
import signal
from db.database import async_session_maker, engine
from db.models import Base
from config import settings as s
from bot_in import bot

# Utils / workers
from misc.utils import (
    db_worker,
    marzban_worker,
    trial_activation_worker,
    nightly_cache_refresh_worker,
    pub_listner,
    payment_wrk,
)

# Глобальные переменные для graceful shutdown
worker_tasks = []
redis_client = None


async def setup_database():
    """Создание таблиц БД если их нет"""
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    print("✅ Database tables created")


async def setup_webhook():
    """Настройка webhook для бота (выполняется один раз)"""
    try:
        # Удаляем старый webhook
        await bot.delete_webhook(drop_pending_updates=True)
        print("🗑️ Старый webhook удалён")
        
        # Устанавливаем новый
        webhook_url = f"{s.WEBHOOK_URL}"
        webhook_info = await bot.get_webhook_info()
        
        # Проверяем, нужно ли обновлять
        if webhook_info.url != webhook_url:
            await bot.set_webhook(
                url=webhook_url,
                drop_pending_updates=True,
                allowed_updates=["message", "callback_query"]  # Укажи нужные типы
            )
            print(f"✅ Webhook установлен: {webhook_url}")
        else:
            print(f"ℹ️ Webhook уже установлен: {webhook_url}")
        
        # Проверяем статус
        info = await bot.get_webhook_info()
        if info.last_error_date:
            print(f"⚠️ Последняя ошибка webhook: {info.last_error_message}")
        else:
            print(f"✅ Webhook работает корректно")
            
    except Exception as e:
        print(f"❌ Ошибка при настройке webhook: {e}")
        raise


async def start_workers(redis):
    """Запуск всех воркеров"""
    global worker_tasks
    
    worker_tasks = [
        asyncio.create_task(
            db_worker(redis_cli=redis, session=async_session_maker), 
            name="db_worker"
        ),
        asyncio.create_task(
            trial_activation_worker(redis_cli=redis, session=async_session_maker), 
            name="trial_worker"
        ),
        asyncio.create_task(
            marzban_worker(redis_cli=redis), 
            name="marzban_worker"
        ),
        asyncio.create_task(
            pub_listner(redis_cli=redis), 
            name="pub_listner"
        ),
        asyncio.create_task(
            payment_wrk(redis_cli=redis), 
            name="payment_wrk"
        ),
        asyncio.create_task(
            nightly_cache_refresh_worker(
                redis_cache=redis, 
                session_maker=async_session_maker
            ), 
            name="cache_worker"
        ),
    ]
    
    print(f"✅ Workers started: {len(worker_tasks)}")
    return worker_tasks


async def shutdown(signal_name=None):
    """Graceful shutdown всех воркеров"""
    global worker_tasks, redis_client
    
    if signal_name:
        print(f"\n🛑 Получен сигнал {signal_name}, начинаю остановку...")
    else:
        print("\n🛑 Начинаю остановку воркеров...")
    
    # Останавливаем воркеры
    if worker_tasks:
        print("⏳ Останавливаю воркеры...")
        for task in worker_tasks:
            task.cancel()
        
        # Ждём завершения с обработкой исключений
        results = await asyncio.gather(*worker_tasks, return_exceptions=True)
        
        # Проверяем на ошибки кроме CancelledError
        for i, result in enumerate(results):
            if isinstance(result, Exception) and not isinstance(result, asyncio.CancelledError):
                print(f"⚠️ Воркер {worker_tasks[i].get_name()} завершился с ошибкой: {result}")
        
        print("✅ Воркеры остановлены")
    
    # Закрываем Redis
    if redis_client:
        from app.redis_client import close_redis
        await redis_client.flushall()
        await close_redis()
        print("✅ Redis отключен")
    
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
    print("✅ Database tables dropped")
    
    # Закрываем движок БД
    await engine.dispose()
    print("✅ Database engine закрыт")
    
    # Закрываем сессию бота
    try:
        await bot.session.close()
        print("✅ Bot session закрыта")
    except Exception as e:
        print(f"⚠️ Ошибка при закрытии bot session: {e}")


def handle_signals(loop):
    """Обработка системных сигналов для graceful shutdown"""
    def signal_handler(sig):
        print(f"\n⚠️ Получен сигнал {signal.Signals(sig).name}")
        asyncio.create_task(shutdown(signal.Signals(sig).name))
    
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, lambda s=sig: signal_handler(s))


async def main():
    """Основная функция запуска воркеров"""
    global redis_client
    
    try:
        # Инициализация Redis
        from app.redis_client import init_redis
        redis_client = await init_redis()
        await redis_client.ping() #type: ignore
        print("✅ Redis подключен")
        
        # Инициализация БД
        await setup_database()
        
        # Настройка webhook (один раз при старте)
        await setup_webhook()
        
        # Запуск воркеров
        tasks = await start_workers(redis_client)
        
        print("🚀 Все воркеры запущены, ожидаю завершения...")
        print("📊 Активные воркеры:")
        for task in tasks:
            print(f"   - {task.get_name()}")
        
        # Ждём завершения (воркеры бесконечные, завершатся только по сигналу)
        await asyncio.gather(*tasks, return_exceptions=True)
        
    except Exception as e:
        print(f"❌ Критическая ошибка при запуске: {e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        await shutdown()


if __name__ == "__main__":
    try:
        # Создаём event loop
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        # Регистрируем обработчики сигналов
        handle_signals(loop)
        
        # Запускаем воркеры
        loop.run_until_complete(main())
        
    except KeyboardInterrupt:
        print("\n⚠️ Прервано пользователем (Ctrl+C)")
    except Exception as e:
        print(f"\n❌ Фатальная ошибка: {e}")
        import traceback
        traceback.print_exc()
    finally:
        # Очищаем event loop
        try:
            # Отменяем все оставшиеся задачи
            pending = asyncio.all_tasks(loop)
            for task in pending:
                task.cancel()
            
            # Ждём их завершения
            loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
        except Exception:
            pass
        finally:
            loop.close()
            print("👋 Воркеры полностью остановлены")