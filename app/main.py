from litestar import Litestar, get, post, Request
from litestar.exceptions import HTTPException
from litestar.di import Provide
from aiogram import Bot, Dispatcher
from aiogram.types import Update
from config import settings as s
from logger_setup import logger
import json
from redis.asyncio import Redis
from bot_in import bot, dp
from sqlalchemy.ext.asyncio import AsyncSession
from pydantic import ValidationError
from midllewares.db import DatabaseMiddleware
from db.database import async_session_maker, engine
from db.models import Base
from contextlib import asynccontextmanager
import asyncio
from misc.utils import (db_worker, marzban_worker, trial_activation_worker, nightly_cache_refresh_worker) 



import handlers.start
import handlers.instructions
import handlers.payment
import handlers.trial

@asynccontextmanager
async def lifespan(app: Litestar):
    """Lifecycle"""
    
    # ✅ Инициализируем Redis
    from app.redis_client import init_redis, close_redis
    
    redis = await init_redis()
    await redis.ping()  #type: ignore
    print("✅ Redis connected")
    
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    print("✅ Database tables created")

    # Webhook setup
    await bot.delete_webhook()
    webhook_url = f"{s.WEBHOOK_URL}"
    await bot.set_webhook(url=webhook_url, drop_pending_updates=True)
    print(f"✅ Webhook установлен: {webhook_url}")

    async with async_session_maker() as session:
        asyncio.create_task(db_worker(redis_cli=redis, session=session))
        asyncio.create_task(trial_activation_worker(redis_cli=redis, session=session))
        asyncio.create_task(nightly_cache_refresh_worker(redis_cache=redis, session_maker=session))

    asyncio.create_task(marzban_worker(redis_cli=redis))
    
    
    yield
    
    # Cleanup
    await close_redis()
    print("✅ Redis disconnected")

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
    print("✅ Database tables dropped")
    
    await bot.delete_webhook()
    await bot.session.close()


async def provide_redis() -> Redis: #type: ignore
    """Production Redis provider"""
    redis = Redis(host='localhost', port=6379, decode_responses=True)
    try:
        yield redis #type: ignore
    finally:
        await redis.aclose()

dp.message.middleware(DatabaseMiddleware(session_maker=async_session_maker))
dp.callback_query.middleware(DatabaseMiddleware(session_maker=async_session_maker))


@get("/")
async def root() -> dict:
    return {"status": "running"}


@post("/bot-webhook", status_code=200)
async def bot_webhook(
    request: Request
) -> dict:
    try:
        data = await request.json()
        update = Update(**data)

        await dp.feed_update(
            bot=bot, 
            update=update
        )
    except ValidationError as e:
        # ✅ Обрабатываем невалидный update от Telegram
        logger.warning(f"Invalid update received: {e}")
        raise HTTPException(status_code=422, detail="Invalid update format")
    except Exception as e:
        logger.error(f"Webhook error: {e}", exc_info=True)
        # Telegram ожидает 200 даже при внутренних ошибках
        return {"ok": True}
    return {"ok": True}


@post("/marzban", status_code=200)
async def webhook_marz(
    request: Request,
    redis_cli: Redis
) -> dict:
    data = await request.json()
    if not data or not isinstance(data, list):
        logger.error(f"Invalid data format: {data}")
        raise HTTPException(status_code=422, detail="Invalid data format")
    
    if len(data) == 0:
        logger.error("Empty data array")
        raise HTTPException(status_code=422, detail="Empty data array")
    
    logger.debug(data)
    data_str = json.dumps(data, ensure_ascii=False)

    first_item = data[0]

    username  = first_item.get('username')
    action    = first_item.get('action')
    cache_key = f"marzban:{username}:{action}"

    if username is None or action is None:
        raise HTTPException(status_code=422, detail="Missing arguments")

    if action == "reached_days_left":
        ttl = 3600  
    elif action == "user_expired":
        ttl = 300   
    else:
        ttl = 60    

    logger.debug(f'Пришли данные до Редиса {username} | {action} | {cache_key}')
    exist = await redis_cli.exists(cache_key) #type: ignore
    logger.debug(exist)

    if exist: #type: ignore
        logger.info(f'Дублирование операции для {username}')
        return {'msg': 'operation for user been'}

    logger.debug(action)
    await redis_cli.set(cache_key, "1", ex=ttl) #type: ignore
    logger.debug('Добавлен в Redis')


    logger.debug(f'Пришёл запрос от Marzban {data_str[:20]}')

    wrk_data: dict = { 
        "user_id": username,
        "expire": data[0]['expire']
    }

    if action == 'user_created':
        wrk_data['id'] = data[0]["proxies"]["vless"]['id']
        await redis_cli.lpush( #type: ignore
            "MARZBAN",
            json.dumps(wrk_data, sort_keys=True, default=str)
        )

    elif action == 'user_updated':
        await redis_cli.lpush( #type: ignore
            "MARZBAN",
            json.dumps(wrk_data, sort_keys=True, default=str)
        )    

    # elif action == 'user_expired':
    #     print('Отправить сообщение юзеру')
        
    # elif action == 'reached_days_left':
    #     print('Отправить сообщение юзеру День остался')

    return {"ok": True}


app = Litestar(
    route_handlers=[
        webhook_marz,
        bot_webhook,
        root
    ],
    debug=True,
    dependencies={
        "redis_cli": Provide(provide_redis),
    },
    lifespan=[lifespan]
)