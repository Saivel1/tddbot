# Database / ORM
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, text
from repositories.base import BaseRepository
from db.models import User, UserLinks, PaymentData
from db.database import async_session_maker

# Redis
from redis.asyncio import Redis

# Schemas
from schemas.schem import (
    UserModel,
    PayDataModel,
    CreateUserMarzbanModel,
)

# External services / clients
from core.yoomoney.payment import YooPay
from core.marzban.Client import MarzbanClient
import aiohttp

# Config
from config import settings
from config import settings as s

# Logging
from logger_setup import logger

# Typing
from typing import Any, Type, Dict

# Date & time
from datetime import datetime, timedelta

# Stdlib
import json
import asyncio
import uuid

# Other
from bot_in import bot
from misc.decorators import queue_worker, SkipTask


#TODO: Обработчик для платежей

PRICE_PER_MONTH: int = 50


MODEL_REGISTRY: Dict[str, Type] = {
    "User": User,
    "UserLinks": UserLinks,
    "PaymentData": PaymentData
}

# Модели с уникальным user_id
UNIQUE_USER_ID_MODELS = {User, UserLinks}

async def notifyer_of_down_wrk(service: str):
    text = f"Service {service} is down for 10 minutes"

    await bot.send_message(
        chat_id=s.ADMIN_ID,
        text=text
    )

async def check_marzban_available() -> bool:
    """Проверка доступности Marzban"""
    try:
        async with aiohttp.ClientSession() as client:
            async with client.request("GET", settings.M_DIGITAL_URL) as res:
                return res.status < 500
    except:
        return False
    

async def check_db_available() -> bool:
    """Проверяет доступность PostgreSQL"""
    async with async_session_maker() as session:
        try:
            await session.execute(text("SELECT 1"))
            return True
        except Exception:
            return False


async def is_cached(
    redis_cache: Redis,
    user_id: int,
    session: AsyncSession,
    force_refresh: bool = False
) -> UserModel | None:
    """
    Получает данные пользователя из кеша или БД
    
    Args:
        redis_cache: Redis клиент
        user_id: ID пользователя
        session: SQLAlchemy сессия
        force_refresh: Принудительное обновление из БД (для ночного воркера)
    
    TTL стратегия:
        - force_refresh=True: 25 часов (90000 сек) - ночное обновление
        - force_refresh=False: 1 час (3600 сек) - первое обращение
    
    Returns:
        UserModel или None если пользователь не найден
    """
    user_str = f"USER_DATA:{user_id}"
    lock_key = f"USER_DATA_LOCK:{user_id}"

    logger.debug(f"🔍 is_cached: user_id={user_id}, force_refresh={force_refresh}")
    
    # Если не force_refresh - пытаемся взять из кеша
    if not force_refresh:
        user = await redis_cache.get(user_str)
        if user is not None:
            logger.info(f"✅ Cache HIT: user_id={user_id}")
            return _parse_user(user)
        logger.debug(f"⚠️  Cache MISS: user_id={user_id}")
    else:
        logger.debug(f"🔄 Force refresh: user_id={user_id}")
    
    # Нет кеша или force_refresh - берём lock
    acquired = await redis_cache.set(lock_key, "1", nx=True, ex=5)
    
    if acquired:
        logger.debug(f"🔒 Lock acquired: user_id={user_id}")
        try:
            # Double-check (если не force_refresh)
            if not force_refresh:
                user = await redis_cache.get(user_str)
                if user is not None:
                    logger.debug(f"✅ Cache filled by another task: user_id={user_id}")
                    return _parse_user(user)
            
            # Загружаем из БД
            logger.debug(f"📊 Loading from DB: user_id={user_id}")
            repo = BaseRepository(session=session, model=User)
            user_data = await repo.get_one(user_id=user_id)
            
            if not user_data:
                logger.warning(f"❌ User NOT FOUND in DB: user_id={user_id}")
                return None
            
            await session.refresh(user_data)

            # Сериализуем
            json_user_data = json.dumps(user_data.as_dict(), default=str)
            
            # Определяем TTL
            ttl = 90000 if force_refresh else 3600
            
            # Сохраняем в кеш
            await redis_cache.set(user_str, json_user_data, ex=ttl)
            logger.info(f"💾 Cached: user_id={user_id}, ttl={ttl}s, source={'nightly' if force_refresh else 'miss'}")
            
            return _parse_user(json_user_data)
            
        except Exception as e:
            logger.error(f"❌ DB error: user_id={user_id}, error={e}")
            return None
        finally:
            await redis_cache.delete(lock_key)
            logger.debug(f"🔓 Lock released: user_id={user_id}")
    else:
        # Другой task заполняет кеш, ждём его
        logger.debug(f"⏳ Waiting for lock: user_id={user_id}")
        for attempt in range(50):
            await asyncio.sleep(0.1)
            user = await redis_cache.get(user_str)
            if user is not None:
                logger.debug(f"✅ Cache ready (attempt {attempt+1}): user_id={user_id}")
                return _parse_user(user)
        
        logger.warning(f"⏱️  Timeout waiting for cache: user_id={user_id}")
        return None


def _parse_user(user_json: str) -> UserModel | None:
    """Парсит JSON строку в UserModel"""
    try:
        user_dict = json.loads(user_json)
        return UserModel(**user_dict)
    except Exception as e:
        logger.error(f"❌ JSON parse error: {e}")
        return None


async def cache_popular_pay_time(redis_cache: Redis, user_id: int) -> str | None:
    """Получить или создать платёж для популярной суммы (50₽)"""
    
    pay_str = f"POP_PAY_CHOOSE:{user_id}"
    lock_key = f"POP_PAY_LOCK:{user_id}"
    
    logger.debug(f"💰 Payment request: user_id={user_id}")

    # Проверяем кэш
    pay_data = await redis_cache.get(pay_str)
    
    if pay_data is None:
        # Атомарно берём lock
        acquired = await redis_cache.set(lock_key, "1", nx=True, ex=60)
        
        if acquired:
            logger.debug(f"🔒 Payment lock acquired: user_id={user_id}")
            try:
                # Double-check после получения lock
                pay_data = await redis_cache.get(pay_str)
                
                if pay_data is None:
                    # Только мы создаём платёж
                    payment_data = {
                        'user_id': user_id,
                        'amount': 50,
                    }
                    
                    await redis_cache.lpush("PAYMENT_QUEUE", json.dumps(payment_data))  # type: ignore # type: ignore
                    logger.info(f"📤 Payment queued: user_id={user_id}, amount=50₽")
                    
                    # Ждём обработки (максимум 10 секунд)
                    for _ in range(100):
                        await asyncio.sleep(0.1)
                        pay_data = await redis_cache.get(pay_str)
                        if pay_data:
                            logger.debug(f"✅ Payment processed: user_id={user_id}")
                            break
                    
                    if pay_data is None:
                        logger.warning(f"⏱️  Payment timeout: user_id={user_id}")
                        return None
            finally:
                await redis_cache.delete(lock_key)
                logger.debug(f"🔓 Payment lock released: user_id={user_id}")
        else:
            # Другой task создаёт платёж, ждём результата
            logger.debug(f"⏳ Waiting for payment: user_id={user_id}")
            for _ in range(100):
                await asyncio.sleep(0.1)
                pay_data = await redis_cache.get(pay_str)
                if pay_data:
                    break

            if pay_data is None:
                logger.warning(f"⏱️  Payment wait timeout: user_id={user_id}")
                return None
    else:
        logger.debug(f"✅ Payment cache HIT: user_id={user_id}")
    
    # Парсим и возвращаем URL
    pay_res = json.loads(pay_data)
    return pay_res['payment_url']

@queue_worker(
    queue_name="PAYMENT_QUEUE",
    timeout=5,
    max_retries=3
)
async def pub_listner(redis_cli: Redis, data: dict):
                """Воркер для обработки платежей из очереди"""
                
                # try:
                #     while True:
                #         result = await redis_cli.brpop("PAYMENT_QUEUE", timeout=5) # type: ignore # type: ignore
                        
                #         if not result:
                #             continue
                        
                #         _, message = result
                #         logger.debug("📥 Payment task received")
                        
                #         try:
                #             data = json.loads(message)
                yoo_handl = YooPay()
                logger.info("🚀 Payment worker started")
                user_id = data['user_id']
                amount = data['amount']
                pay_str = f"POP_PAY_CHOOSE:{user_id}"
                
                # Проверяем что платёж ещё не создан (идемпотентность)
                existing = await redis_cli.get(pay_str)
                if existing:
                    logger.debug(f"⏭️  Payment exists: user_id={user_id}")
                    raise SkipTask
                
                # Создаём платёж
                logger.info(f"💳 Creating payment: user_id={user_id}, amount={amount}₽")
                res = await yoo_handl.create_payment(
                    amount=amount,
                    email="saivel.mezencev1@gmail.com",
                    plan="1+9210"
                )
                
                if res is None:
                    logger.error(f"❌ Payment creation failed: user_id={user_id}")
                    await asyncio.sleep(5)
                    raise TimeoutError
                
                # Сохраняем результат
                data_for_load = {
                    "payment_url": res[0],
                    "payment_id": res[1]
                }

                data_for_webhook = {
                    "user_id": user_id,
                    "amount": amount
                }

                web_wrk_label = f"YOO:{res[1]}"
                await redis_cli.set(pay_str, json.dumps(data_for_load), ex=600)
                await redis_cli.set(web_wrk_label, json.dumps(data_for_webhook), ex=700)
                logger.info(f"✅ Payment created: user_id={user_id}, payment_id={res[1]}")
                
    #         except Exception as e:
    #             logger.error(f"❌ Payment task error: {e}")
    #             await redis_cli.lpush("PAYMENT_QUEUE", message) # type: ignore
    #             await asyncio.sleep(5)
                
    # except asyncio.CancelledError:
    #     logger.info("🛑 Payment worker stopped")
    #     raise


async def is_cached_payment(
    redis_cache: Redis,
    user_id: int,
    amount: int | None = None
) -> PayDataModel | None:
    """Проверить наличие платежа в кэше"""
    pay_str = f"POP_PAY_CHOOSE:{user_id}"
    pay_reg = f"PAY:{user_id}:{amount}" if amount else None
    
    logger.debug(f"🔍 Checking payment cache: user_id={user_id}, amount={amount}")
    
    pay = await redis_cache.get(pay_str)
    pay_c = await redis_cache.get(pay_reg) if pay_reg else None
    
    if pay is None and pay_c is None:
        logger.debug(f"❌ Payment not cached: user_id={user_id}")
        return None
    
    res_json: dict = {}
    
    if pay and (amount == 50 or amount is None):
        res_json = json.loads(pay)
        logger.debug(f"✅ Popular payment found: user_id={user_id}")
    elif pay_c:
        res_json = json.loads(pay_c)
        logger.debug(f"✅ Custom payment found: user_id={user_id}")
    
    if not res_json:
        return None
    
    res = PayDataModel(
        user_id=user_id,
        payment_url=res_json['payment_url']
    )
    
    return res


async def worker_exsists(
    redis_cli: Redis,
    worker: str,
    data: dict
) -> bool:
    """Проверка существования задачи с lock на конкретного пользователя"""
    user_id = data.get("user_id")
    lock_key = f"{worker}_CHECK_LOCK:{user_id}" if user_id else f"{worker}_CHECK_LOCK"
    
    logger.debug(f"🔍 Checking task existence: worker={worker}, user_id={user_id}")
    
    for attempt in range(20):
        acquired = await redis_cli.set(lock_key, "1", nx=True, ex=2)
        
        if acquired:
            try:
                all_items = await redis_cli.lrange(worker, 0, -1) # type: ignore
                search_value = json.dumps(data, sort_keys=True, default=str)
                
                result = search_value in all_items
                logger.debug(f"{'✅' if result else '❌'} Task {'exists' if result else 'not found'}: worker={worker}")
                return result
            finally:
                await redis_cli.delete(lock_key)
        
        await asyncio.sleep(0.1)
    
    logger.warning(f"⏱️  Lock timeout: worker={worker}, user_id={user_id}")
    return False


@queue_worker(
    queue_name="TRIAL_ACTIVATION",
    timeout=5,
    max_retries=3
)
async def trial_activation_worker(
    redis_cli: Redis,
    session: AsyncSession,
    data: dict
):
    # wrk_label = "TRIAL_ACTIVATION"
    # logger.info("🚀 Trial activation worker started")

    # while True:
    #     result = await redis_cli.brpop(wrk_label, timeout=5) # type: ignore

    #     if not result:
    #         continue 
        
    #     _, message = result
    #     data = json.loads(message)
    #     logger.info(f"📥 Trial task received: user_id={data.get('user_id')}")
        
    #     try:
            repo = BaseRepository(session=session, model=User)
            user = await repo.get_one(user_id=int(data["user_id"]))
            
            if not user:
                user = await repo.create(user_id=int(data['user_id']))
                logger.info(f"➕ User created: user_id={data['user_id']}")
            else:
                logger.debug(f"✅ User found: user_id={data['user_id']}")
            
            if user.trial_used:
                logger.warning(f"⏭️  Trial already used: user_id={data['user_id']}")
                raise SkipTask(f"⏭️  Trial already used: user_id={data['user_id']}")

            user_id = str(data['user_id'])
            
            logger.debug(f"🔍 Checking Marzban: username={user_id}")
            async with MarzbanClient() as client:
                user_marz = await client.get_user(username=user_id)
            
            sub_end_marz: int = 0

            if user_marz == 404:
                logger.debug(f"➕ New user in Marzban: {user_id}")
                data_marz: dict[str, Any] = {"type": "create", "user_id": user_id}
            elif user_marz is None:
                logger.error(f"❌ Marzban timeout: {user_id}")
                raise TimeoutError
            elif type(user_marz) == dict:
                logger.debug(f"🔄 Existing user in Marzban: {user_id}")
                data_marz: dict[str, Any] = {"type": "modify", "user_id": user_id}
                sub_end_marz = user_marz['expire']
            else:
                raise TimeoutError
            
            sub_end_marz = sub_end_marz if sub_end_marz > 0 else 0
            date_now = datetime.now()

            if user.subscription_end is None and sub_end_marz < int(date_now.timestamp()):
                max_val: datetime = date_now
            elif user.subscription_end:
                max_val: datetime = max(datetime.fromtimestamp(sub_end_marz), date_now, user.subscription_end)
            else:
                max_val: datetime = max(datetime.fromtimestamp(sub_end_marz), date_now)

            new_expire: datetime = max_val + timedelta(days=s.TRIAL_DAYS)
            data_marz['expire'] = int(new_expire.timestamp())

            logger.info(f"📤 Queueing Marzban task: user_id={user_id}, expire={new_expire}")
            await redis_cli.lpush("MARZBAN", json.dumps(data_marz, sort_keys=True, default=str)) # type: ignore

            data_for_cache = {
                "user_id": user_id,
                "username": user.username,
                "subscription_end": datetime.fromtimestamp(data_marz['expire']),
                "trial_used": True
            }

            await redis_cli.lpush("DB", json.dumps({
                "user_id": user_id,
                "trial_used": True,
                "model": "User",
                "type": "create"
            }, default=str, sort_keys=True)) # type: ignore # type: ignore

            await redis_cli.set(f"USER_DATA:{user_id}", json.dumps(data_for_cache, default=str), ex=7200)
            logger.info(f"✅ Trial activated: user_id={user_id}")

            await bot.send_message(
                chat_id=user_id,
                text="Пробный период активирован"
            )
            
        # except Exception as e:
        #     logger.error(f"❌ Trial activation error: {e}")
        #     await redis_cli.lpush(wrk_label, message) # type: ignore
        #     await asyncio.sleep(10)


@queue_worker(
    queue_name="MARZBAN",
    timeout=5,
    max_retries=3,
    check_availability=check_marzban_available
)
async def marzban_worker(
    redis_cli: Redis,
    data: dict,
    panel_url: str | None = None,
):
            """
            Воркер для обработки запросов к Marzban API
            data = 
            type: create | modify 
            user_id: str | int
            expire: int

            ADDITIONAL 
            id: uuid from marzban
            panel: custom panel to request
            """

    # wrk_label = 'MARZBAN'
    # logger.info("🚀 Marzban worker started")

    # cnt = 0
    # while True:
    #     # Ждём пока сервис станет доступен
    #     while not await check_marzban_available():
    #         logger.debug("⏳ Marzban unavailable, waiting 10s...")
    #         await asyncio.sleep(10)
    #         cnt += 1
    #         if cnt == 60:
    #             logger.error("🚨 Marzban unavailable for 10 minutes!")
    #             await notifyer_of_down_wrk(service="Marzban")
    #             cnt = 0
        
    #     result = await redis_cli.brpop(wrk_label, timeout=5) # type: ignore
    #     cnt = 0
        
    #     if not result:
    #         continue 
        
    #     _, message = result
    #     data = json.loads(message)
    #     logger.info(f"📥 Marzban task: type={data.get('type')}, user_id={data.get('user_id')}")
        
    #     try:
            if data.get('panel'): 
                panel_url = data['panel']
                logger.debug(f"🎯 Using panel: {panel_url}")

            async with MarzbanClient(base_url=panel_url if panel_url else s.M_DIGITAL_URL) as client:
                marz_data: dict = {}

                marz_data['username'] = str(data['user_id'])
                marz_data['expire'] = data['expire']
                
                db_data: dict = {"model": "User"}
                db_data_panels: dict = {"model": "UserLinks"}

                if data['type'] == "create":
                    logger.debug(f"➕ Creating user in Marzban: {marz_data['username']}")
                    if data.get("id"): 
                        marz_data['id'] = data['id']
                    create_data = CreateUserMarzbanModel(**marz_data)
                    res = await client.create(data=create_data)

                    db_data['type'] = 'create'
                    db_data['user_id'] = int(data['user_id'])
                    db_data['subscription_end'] = datetime.fromtimestamp(data['expire'])

                    db_data_panels['type'] = 'create'
                    db_data_panels['user_id'] = int(data['user_id'])
                    db_data_panels['uuid'] = str(uuid.uuid4())
                
                elif data["type"] == "modify":
                    logger.debug(f"🔄 Modifying user in Marzban: {marz_data['username']}")
                    res = await client.modify(**marz_data)

                    db_data['type'] = 'update'
                    db_data['filter'] = {"user_id": int(data['user_id'])}
                    db_data['subscription_end'] = datetime.fromtimestamp(data['expire'])

                    db_data_panels['type'] = 'update'
                    db_data_panels['filter'] = {"user_id": int(data['user_id'])}

                if res == 409:
                    logger.warning(f"⚠️  User exists (409), converting to modify: {marz_data['username']}")
                    res = await client.modify(**marz_data)

                    db_data['type'] = 'update'
                    db_data['filter'] = {"user_id": int(data['user_id'])}
                    db_data['subscription_end'] = datetime.fromtimestamp(data['expire'])

                    db_data_panels['type'] = 'update'
                    db_data_panels['filter'] = {"user_id": int(data['user_id'])}

                if type(res) != dict:
                    logger.error(f"❌ Unexpected Marzban response type: {type(res)}")
                    raise TimeoutError(f"Returns {type(res)} - {res}")

                url: str = res['subscription_url']

                if "dns1" in url:
                    db_data_panels['panel1'] = url
                    logger.debug(f"🔗 Panel1 link: user_id={data['user_id']}")
                elif "dns2" in url:
                    db_data_panels['panel2'] = url
                    logger.debug(f"🔗 Panel2 link: user_id={data['user_id']}")
                else:
                    logger.error(f"❌ Unknown panel in URL: {url}")
                    raise ValueError(f"Unknown panel {url}")
                
                logger.info(f"📤 Queueing DB tasks: user_id={data['user_id']}")
                for db_op in (db_data_panels, db_data):
                    await redis_cli.lpush("DB", json.dumps(db_op, sort_keys=True, default=str)) # type: ignore

                logger.info(f"✅ Marzban task completed: user_id={data['user_id']}")
                # return res
                
        # except Exception as e:
        #     logger.error(f"❌ Marzban worker error: {e}")
        #     await redis_cli.lpush(wrk_label, message) # type: ignore
        #     await asyncio.sleep(10)


def deserialize_data(data: dict) -> dict:
    """Конвертирует строковые datetime обратно в объекты datetime"""
    result = {}
    for key, value in data.items():
        if key in ('model', 'type', 'filter'):
            result[key] = value
            continue
        
        if isinstance(value, str):
            try:
                result[key] = datetime.fromisoformat(value)
            except (ValueError, AttributeError):
                result[key] = value
        elif isinstance(value, dict):
            result[key] = deserialize_data(value)
        else:
            result[key] = value
    
    return result

@queue_worker(
    queue_name="DB",
    timeout=5,
    max_retries=3,
    check_availability=check_db_available
)
async def db_worker(
    redis_cli: Redis,
    session: AsyncSession,
    data: dict,
    process_once: bool = False
):

    # wrk_label = 'DB'
    # cnt = 0
    
    # logger.info(f"🚀 DB Worker started (process_once={process_once})")
    
    # while True:
    #     # Проверка доступности БД
    #     while not await check_db_available(session):
    #         logger.debug("⏳ DB unavailable, waiting 10s...")
    #         await asyncio.sleep(10)
    #         cnt += 1
    #         if cnt == 60:
    #             logger.error("🚨 DB unavailable for 10 minutes!")
    #             await notifyer_of_down_wrk(service="DB")
    #             cnt = 0
        
    #     result = await redis_cli.brpop(wrk_label, timeout=5) # type: ignore
    #     cnt = 0
        
    #     if not result:
    #         if process_once:
    #             logger.debug("✅ No tasks, exiting")
    #             return None
    #         continue
        
    #     _, message = result
        
    #     try:
    #         data = json.loads(message)
            """
            Воркер для обработки операций с БД из очереди
            Типы: Create, Update
            Автоматическая конвертация операций при необходимости
            """
            logger.info(f"📥 DB task: model={data.get('model')}, type={data.get('type')}")
            
            data = deserialize_data(data)
            
            model = MODEL_REGISTRY.get(data['model'])
            if not model:
                logger.error(f"❌ Unknown model: {data['model']}")
                raise ValueError(f"Unknown model: {data['model']}")
            
            repo = BaseRepository(session=session, model=model)
            data_type: str = data['type'].lower()
            
            db_data = {
                k: v for k, v in data.items() 
                if k not in ("model", "type", "filter")
            }
            
            # Проверка существования для моделей с user_id
            if model in UNIQUE_USER_ID_MODELS:
                user_id = data.get('user_id') or data.get('filter', {}).get('user_id')
                
                if not user_id:
                    logger.error(f"❌ Missing user_id for {model.__name__}")
                    raise ValueError(f"{model.__name__} requires 'user_id' field")
                
                user_id = int(user_id)
                logger.debug(f"🔍 Checking: user_id={user_id}")
                
                existing = await repo.get_one(user_id=user_id)
                
                if existing is not None:
                    logger.debug(f"📌 Record exists: user_id={user_id}")
                    
                    current_data = {
                        k: v for k, v in existing.as_dict().items() 
                        if v is not None
                    }
                    
                    new_data = {
                        k: v for k, v in db_data.items()
                        if k != 'user_id'
                    }
                    
                    has_changes = False
                    for key, new_value in new_data.items():
                        current_value = current_data.get(key)
                        
                        if isinstance(new_value, datetime):
                            new_value = new_value.isoformat()
                        if isinstance(current_value, datetime):
                            current_value = current_value.isoformat()
                        
                        if new_value != current_value:
                            has_changes = True
                            logger.debug(f"📝 Change: {key}={current_value}→{new_value}")
                            break
                    
                    if not has_changes:
                        logger.debug(f"⏭️  No changes: user_id={user_id}")
                        if process_once:
                            return 'skipped'
                        raise SkipTask
                    
                    if data_type == "create":
                        logger.info(f"🔄 CREATE→UPDATE: user_id={user_id}")
                        data_type = 'update'
                        data['filter'] = {'user_id': user_id}
                        db_data = {k: v for k, v in db_data.items() if k != 'user_id'}
                
                else:
                    logger.debug(f"✨ No record: user_id={user_id}")
                    
                    # Конвертируем UPDATE → CREATE
                    if data_type == "update":
                        logger.info(f"🔄 UPDATE→CREATE: user_id={user_id}")
                        data_type = 'create'
                        
                        if 'filter' in data and 'user_id' in data['filter']:
                            db_data['user_id'] = user_id
                        
                        if model == UserLinks:
                            db_data['uuid'] = str(uuid.uuid4())

                        data.pop('filter', None)
            
            # Выполняем операцию
            if data_type == "create":
                logger.info(f"➕ Creating {model.__name__}")
                res = await repo.create(**db_data)
                logger.info(f"✅ Created: {res}")
                result_type = "create"

                # if model == UserLinks:
                #     user_id_int: int = int(db_data["user_id"])
                #     logger.debug(f"🔗 Fetching UserLinks: user_id={user_id_int}")
                    
                #     links_cache = await repo.get_one(user_id=user_id_int)
                #     if not links_cache:
                #         logger.error(f"❌ UserLinks not found after creation: user_id={user_id_int}")
                #         raise ValueError(f"UserLinks not found: user_id={user_id_int}")
                    
                #     logger.debug(f"✅ UserLinks fetched: user_id={user_id_int}")
                
            elif data_type == "update":
                filter_data = data.get('filter', {})
                
                if not filter_data:
                    logger.error("❌ Update requires filter")
                    raise ValueError("Update requires 'filter' parameter")
                
                update_data = {k: v for k, v in db_data.items() if k != 'user_id'}
                
                logger.info(f"🔄 Updating {model.__name__}: {filter_data}")
                res = await repo.update(data=update_data, **filter_data)
                logger.info(f"✅ Updated: {res} rows")
                result_type = 'update'
            
            else:
                logger.error(f"❌ Unknown type: {data_type}")
                raise ValueError(f"Unknown operation type: {data_type}")
            

            if model == User:
                user_id = db_data.get('user_id') or data.get('filter', {}).get('user_id')
                user: User | None = await repo.get_one(user_id=int(user_id))
                
                if user is None:
                    raise ValueError
                
                user_data = user.as_dict()
                await redis_cli.set(f"USER_DATA:{user_id}", json.dumps(user_data, default=str), ex=3600)

            elif model == UserLinks:
                user_id = db_data.get('user_id') or data.get('filter', {}).get('user_id')
                user_links: UserLinks | None = await repo.get_one(user_id=int(user_id))
                
                if user_links is None:
                    raise ValueError
                
                user_data = user_links.as_dict()
                await redis_cli.set(f"USER_UUID:{user_id}", json.dumps(user_data['uuid'], default=str), ex=3600)

            if process_once:
                return result_type
                
        # except Exception as e:
        #     logger.error(f"❌ DB worker error: {e}")
        #     await redis_cli.lpush(wrk_label, message) # type: ignore
            
        #     if process_once:
        #         raise
            
        #     await asyncio.sleep(1)


# ============================   Payment WRK   ======================================

async def payment_wrk(
    redis_cli: Redis
):
    logger.info("🚀 YOO PAYMENT Worker started")
    wrk_label = 'YOO:PROCEED'
    cnt = 0
    
    while True:
        # Проверка доступности Marzban
        while not await check_marzban_available():
            logger.debug("⏳ Marzban unavailable, waiting 10s...")
            await asyncio.sleep(10)
            cnt += 1
            if cnt == 60:
                logger.error("🚨 Marzban unavailable for 10 minutes!")
                await notifyer_of_down_wrk(service="Marzban")
                cnt = 0
        
        # Получаем задачу из очереди
        result = await redis_cli.brpop(wrk_label, timeout=5) #type: ignore
        cnt = 0
        
        if not result:
            continue
        
        _, message = result
        logger.info(f"📥 Payment task received: {message[:200]}...")
        
        try:
            data = json.loads(message)
            logger.info(f"💰 Processing payment: user_id={data.get('user_id')}, amount={data.get('amount')}₽")
            
            # Задача в Marzban
            mrzb_data: dict = {
                "user_id": data['user_id']
            }
            
            logger.debug(f"🔍 Checking user in Marzban: {data['user_id']}")
            async with MarzbanClient() as client:
                user = await client.get_user(username=data['user_id'])
            
            if isinstance(user, dict):
                logger.debug(f"✅ User exists in Marzban: {data['user_id']}")
                mrzb_data['type'] = 'modify'
                raw_expire: int = user['expire']
                obj_expire: datetime = datetime.fromtimestamp(raw_expire)
                logger.debug(f"📅 Current expire: {obj_expire} (timestamp={raw_expire})")
                
            elif user == 404:
                logger.debug(f"➕ New user in Marzban: {data['user_id']}")
                mrzb_data['type'] = 'create'
                obj_expire: datetime = datetime.now()
                
            else:
                logger.error(f"❌ Unexpected Marzban response: {type(user)}")
                raise TimeoutError
            
            # Вычисляем новый expire
            if obj_expire < datetime.now():
                logger.debug(f"⚠️  Expire in past, using now: {obj_expire} → {datetime.now()}")
                obj_expire = datetime.now()
            
            days = int(data['amount']) // PRICE_PER_MONTH * 30
            inc_expire: datetime = obj_expire + timedelta(days=days)
            
            logger.info(f"📆 Subscription extended: +{days} days, new expire={inc_expire}")
            
            mrzb_data['expire'] = int(inc_expire.timestamp())
            
            # Отправляем в Marzban воркер
            logger.debug(f"📤 Queueing Marzban task: type={mrzb_data['type']}, expire={inc_expire}")
            await redis_cli.lpush(
                "MARZBAN",
                json.dumps(mrzb_data, sort_keys=True, default=str)
            ) #type: ignore

            queue_size = await redis_cli.llen("MARZBAN") #type: ignore
            logger.info(f"📊 MARZBAN queue size after push: {queue_size}")

            # ✅ Проверка содержимого задачи
            last_task = await redis_cli.lindex("MARZBAN", 0) #type: ignore
            logger.debug(f"📝 Last MARZBAN task: {last_task}")
            
            # Задачи в БД
            user_db: dict = {
                'model': "User",
                "type": "create",
                "user_id": data['user_id'],
                "subscription_end": inc_expire
            }
            
            payment_db: dict = {
                'model': "PaymentData",
                "type": "create",
                "payment_id": data['order_id'],
                'user_id': data['user_id'],
                "amount": data['amount']
            }
            
            logger.debug(f"📤 Queueing DB tasks: User + PaymentData for user_id={data['user_id']}")
            for db_op in (user_db, payment_db):
                await redis_cli.lpush(
                    "DB",
                    json.dumps(db_op, sort_keys=True, default=str)
                ) #type: ignore
            
            logger.info(f"✅ Payment processed: user_id={data['user_id']}, amount={data['amount']}₽, order_id={data['order_id']}")

            # сообщение что обработан

            await bot.send_message(
                chat_id=int(data['user_id']),
                text=f"Оплата прошла успешно на сумму {data['amount']}"
            )

            await bot.send_message(
                chat_id=int(s.ADMIN_ID),
                text=f"Оплата прошла успешно на сумму {data['amount']} для пользователя `{data['user_id']}`",
                parse_mode='MARKDOWN'
            )
            
        except Exception as e:
            logger.error(f"❌ Payment worker error: {e}")
            logger.error(f"📋 Failed message: {message}")
            import traceback
            logger.error(f"🔍 Traceback:\n{traceback.format_exc()}")
            
            logger.warning(f"♻️  Re-queuing failed payment task")
            await redis_cli.lpush(wrk_label, message) #type: ignore
            
            await asyncio.sleep(5)  # Пауза перед повтором


# ============================   Payment WRK   ======================================           


def normalize_for_comparison(data: dict) -> dict:
    """Нормализует данные для корректного сравнения"""
    normalized = {}
    
    for k, v in data.items():
        if v is None:
            continue
        
        if isinstance(v, datetime):
            normalized[k] = v.isoformat()
        elif isinstance(v, bool):
            normalized[k] = int(v)
        else:
            normalized[k] = v
    
    return normalized


async def nightly_cache_refresh_worker(
    redis_cache: Redis,
    session_maker
):
    """Воркер для ночного обновления кешей всех пользователей"""
    logger.info("🌙 Nightly refresh worker started")
    
    while True:
        now = datetime.now()
        target = now.replace(hour=3, minute=0, second=0, microsecond=0)
        
        if target <= now:
            target += timedelta(days=1)
        
        sleep_seconds = (target - datetime.now()).total_seconds()
        logger.info(f"🌙 Next refresh: {target} (in {sleep_seconds/3600:.1f}h)")
        
        await asyncio.sleep(sleep_seconds)
        
        logger.info("🌙 Starting nightly cache refresh...")
        
        try:
            async with session_maker() as session:
                repo = BaseRepository(session=session, model=User)
                
                offset = 0
                batch_size = 100
                total_refreshed = 0
                
                while True:
                    stmt = select(User).offset(offset).limit(batch_size)
                    result = await session.execute(stmt)
                    users = result.scalars().all()
                    
                    if not users:
                        break
                    
                    for user in users:
                        try:
                            await is_cached(
                                redis_cache=redis_cache,
                                user_id=user.user_id,
                                session=session,
                                force_refresh=True
                            )
                            total_refreshed += 1
                            
                            if total_refreshed % 100 == 0:
                                logger.info(f"📊 Progress: {total_refreshed} users")
                                
                        except Exception as e:
                            logger.error(f"❌ Refresh failed: user_id={user.user_id}, error={e}")
                            continue
                    
                    offset += batch_size
                    await asyncio.sleep(0.5)
                
                logger.info(f"✅ Nightly refresh complete: {total_refreshed} users")
                
        except Exception as e:
            logger.error(f"❌ Nightly refresh error: {e}")


async def to_link(lst_data: dict):
    """Извлекает названия из ссылок"""
    from urllib.parse import unquote
    links = lst_data.get("links")
    
    if links is None:
        logger.debug("❌ No links provided")
        return None
    
    titles = []
    for link in links:
        sta = link.find("#")
        encoded = link[sta+1:]
        text = unquote(encoded)
        titles.append(text)

    logger.debug(f"✅ Extracted {len(titles)} titles")
    return titles