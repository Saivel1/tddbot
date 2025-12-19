# Framework / web
from litestar import Litestar, Response, get, post, Request
from litestar.exceptions import HTTPException
from litestar.di import Provide
from litestar.params import Dependency

# Bot / Telegram
from aiogram import Bot, Dispatcher
from aiogram.types import Update
from bot_in import bot, dp
from misc.bot_setup import SUB_EXPIRED_TEXT, SUB_WILL_EXPIRE

# Config & logging
from config import settings as s
from logger_setup import logger

# Database / ORM
from sqlalchemy.ext.asyncio import AsyncSession
from db.database import async_session_maker, engine
from db.models import Base
from midllewares.db import DatabaseMiddleware

# Redis / cache
from redis.asyncio import Redis

# Validation / schemas
from pydantic import ValidationError

# Utils / workers
from misc.utils import (
    db_worker,
    marzban_worker,
    trial_activation_worker,
    nightly_cache_refresh_worker,
    pub_listner,
    is_cached_payment,
    worker_exsists,
    payment_wrk
)

# Stdlib
import json
import asyncio
from contextlib import asynccontextmanager
from typing import Annotated, AsyncGenerator, Optional




import handlers.start
import handlers.instructions
import handlers.payment
import handlers.trial
import handlers.sub_n_links

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

    # ✅ Создаём долгоживущую сессию для воркеров
    worker_session = async_session_maker()
    
    # ✅ СОХРАНЯЕМ ссылки на задачи
    worker_tasks = [
        asyncio.create_task(db_worker(redis_cli=redis, session=worker_session), name="db_worker"), # type: ignore
        asyncio.create_task(trial_activation_worker(redis_cli=redis, session=worker_session), name="trial_worker"),
        asyncio.create_task(nightly_cache_refresh_worker(redis_cache=redis, session_maker=async_session_maker), name="cache_worker"),
        asyncio.create_task(marzban_worker(redis_cli=redis), name="marzban_worker"),
        asyncio.create_task(pub_listner(redis_cli=redis), name="pub_listner"),
        asyncio.create_task(payment_wrk(redis_cli=redis), name="payment_wrk"),
    ]
    print(f"✅ Workers started: {len(worker_tasks)}")
    
    
    yield

    await redis.flushall()

    await close_redis()
    print("✅ Redis disconnected")
    
    for wrk in worker_tasks:
        wrk.cancel()
        print("✅ Worker stopped")

    
    # Закрываем сессию воркеров
    await worker_session.close()
    print("✅ Worker session closed")

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
    print("✅ Database tables dropped")
    
    await bot.delete_webhook()
    await bot.session.close()


async def provide_redis() -> Redis: #type: ignore
    """Production Redis provider"""
    redis = Redis(host='localhost', port=6379, password=s.REDIS_PASS, decode_responses=True)
    try:
        yield redis #type: ignore
    finally:
        await redis.aclose()

async def provide_db() -> AsyncGenerator[AsyncSession, None]:
    """Dependency для DB session"""
    async with async_session_maker() as session:
        try:
            yield session
        finally:
            await session.close()


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
    """
    Это функция просто роутинг и синхронизация между панелями
    она принимает разные action'ы от одной панели и роутит
    их в другую
    """


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
    from_panel = first_item.get('subscription_url')

    if "dns1" in from_panel:
        wrk_data['panel'] = s.DNS2_URL
    elif "dns2" in from_panel:
        wrk_data['panel'] = s.DNS1_URL


    if action == 'user_created':
        wrk_data['id'] = data[0]["proxies"]["vless"]['id']
        wrk_data['type'] = 'create'
        await redis_cli.lpush( #type: ignore
            "MARZBAN",
            json.dumps(wrk_data, sort_keys=True, default=str)
        )

    elif action == 'user_updated':
        wrk_data['type'] = 'modify'
        await redis_cli.lpush( #type: ignore
            "MARZBAN",
            json.dumps(wrk_data, sort_keys=True, default=str)
        )    

    elif action == 'user_expired':
        await bot.send_message(
            chat_id=int(username),
            text=SUB_EXPIRED_TEXT
        )
        
    elif action == 'reached_days_left':
        await bot.send_message(
            chat_id=int(username),
            text=SUB_WILL_EXPIRE
        )

    return {"ok": True}


@post("/pay", status_code=200) #yooKassWebhook
async def yoo_webhook(
    request: Request,
    redis_cli: Redis,
    session: AsyncSession
) -> dict | Response[str]:
    data = await request.json()
    event: str = data.get('event')
    order_id = data.get('object', {}).get("id")

    if not order_id:
        logger.warning(f'Missing order_id. Response: {data}')
        return {"status": "error", 
                "message": "missing order_id"
                }
    
    logger.info(f"Webhook received: order={order_id}, event={event}")

    status = event.split(".")[1]

    from repositories.base import BaseRepository
    from db.models import PaymentData
    
    repo = BaseRepository(session=session, model=PaymentData)
    existing_payment = await repo.get_one(payment_id=order_id)
    
    if existing_payment:
        logger.warning(f"⏭️  Duplicate webhook (payment exists in DB): order={order_id}")
        return Response(
            content=json.dumps({"status": "duplicate"}),
            status_code=200,
            media_type="application/json"
        )

    if status == 'succeeded':
        web_wrk_label = f"YOO:{order_id}"
        cache: Optional[str] = await redis_cli.get(web_wrk_label)
        
        if not cache:
            logger.error(f"Кеш умер для платежа")
            return {"status": "ok"}

        # data_cache != 
        # data_for_webhook = {
        #             "user_id": user_id,
        #             "amount": amount,
        #     }

        data_cache = json.loads(cache)
        wrk_label = 'YOO:PROCEED'
        data_cache['order_id'] = order_id

        if not await worker_exsists(
            redis_cli=redis_cli,
            worker=wrk_label,
            data=data_cache
        ):
            await redis_cli.lpush(
                wrk_label,
                json.dumps(data_cache, sort_keys=True, default=str)
            ) # type: ignore

            return {"status": "ok"}

    return {"status": "ok"}

@post("/pay-test", status_code=200) #yooKassWebhook
async def yoo_webhoo_test(
    request: Request,
    redis_cli: Annotated[Redis, Dependency(skip_validation=True)],  # ✅
    session: Annotated[AsyncSession, Dependency(skip_validation=True)] 
) -> dict | Response[str]:
    data = await request.json()
    event: str = data.get('event')
    order_id = data.get('object', {}).get("id")

    if not order_id:
        logger.warning(f'Missing order_id. Response: {data}')
        return {"status": "error", 
                "message": "missing order_id"
                }
    
    logger.info(f"Webhook received: order={order_id}, event={event}")

    status = event.split(".")[1]

    from repositories.base import BaseRepository
    from db.models import PaymentData
    
    repo = BaseRepository(session=session, model=PaymentData)
    existing_payment = await repo.get_one(payment_id=order_id)
    
    if existing_payment:
        logger.warning(f"⏭️  Duplicate webhook (payment exists in DB): order={order_id}")
        return Response(
            content=json.dumps({"status": "duplicate"}),
            status_code=200,
            media_type="application/json"
        )

    if status == 'succeeded':
        web_wrk_label = f"YOO:{order_id}"
        cache: Optional[str] = await redis_cli.get(web_wrk_label)
        
        if not cache:
            logger.error(f"Кеш умер для платежа")
            return {"status": "ok"}

        # data_cache != 
        # data_for_webhook = {
        #             "user_id": user_id,
        #             "amount": amount,
        #     }

        data_cache = json.loads(cache)
        wrk_label = 'YOO:PROCEED'
        data_cache['order_id'] = order_id

        if not await worker_exsists(
            redis_cli=redis_cli,
            worker=wrk_label,
            data=data_cache
        ):
            await redis_cli.lpush(
                wrk_label,
                json.dumps(data_cache, sort_keys=True, default=str)
            ) # type: ignore

            return {"status": "ok"}

    return {"status": "ok"}


app = Litestar(
    route_handlers=[
        webhook_marz,
        bot_webhook,
        root,
        yoo_webhook,
        yoo_webhoo_test
    ],
    debug=True,
    dependencies={
        "redis_cli": Provide(provide_redis),
        "session": Provide(provide_db)
    },
    lifespan=[lifespan]
)