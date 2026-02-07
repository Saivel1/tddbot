# ============================================================================
# IMPORTS
# ============================================================================

# Database / ORM
import asyncio
from keyboards.markup import MainKeyboard

# Stdlib
import json
import uuid
from contextlib import suppress

# Date & time
from datetime import datetime, timedelta

# Typing
from typing import Any, Dict, Type

import aiohttp

# Redis
from redis.asyncio import Redis
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

# Bot
from bot_in import bot

# Config
from config import settings
from config import settings as s
from core.mails.client import create_user_mailbox
from core.marzban.Client import MarzbanClient

# External services / clients
from core.yoomoney.payment import YooPay
from db.database import async_session_maker
from db.models import PaymentData, User, UserLinks

# Logging
from logger_setup import logger

# Decorators
from misc.decorators import SkipTask, queue_worker
from repositories.base import BaseRepository

# Schemas
from schemas.schem import (
    CreateUserMarzbanModel,
    PayDataModel,
    UserModel,
)

# ============================================================================
# CONSTANTS
# ============================================================================

PRICE_PER_MONTH: int = 50

MODEL_REGISTRY: Dict[str, Type] = {
    "User": User,
    "UserLinks": UserLinks,
    "PaymentData": PaymentData
}

UNIQUE_USER_ID_MODELS = {User, UserLinks}


# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

async def notifyer_of_down_wrk(service: str):
    """Уведомление админа о падении сервиса"""
    text = f"Service {service} is down for 10 minutes"
    await bot.send_message(chat_id=s.ADMIN_ID, text=text)


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


def _parse_user(user_json: str) -> UserModel | None:
    """Парсит JSON строку в UserModel"""
    try:
        user_dict = json.loads(user_json)
        return UserModel(**user_dict)
    except Exception as e:
        logger.error(f"❌ JSON parse error: {e}")
        return None


# ============================================================================
# HEALTH CHECK FUNCTIONS
# ============================================================================

async def check_marzban_available() -> bool:
    """Проверка доступности Marzban"""
    try:
        async with aiohttp.ClientSession() as client:
            async with client.request("GET", settings.M_DIGITAL_URL) as res:
                return res.status < 500
    except Exception as e:
        print(e)
        return False


async def check_db_available() -> bool:
    """Проверяет доступность PostgreSQL"""
    async with async_session_maker() as session:
        try:
            await session.execute(text("SELECT 1"))
            return True
        except Exception:
            return False


# ============================================================================
# CACHE FUNCTIONS
# ============================================================================

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
                    
                    await redis_cache.lpush("PAYMENT_QUEUE", json.dumps(payment_data)) # type: ignore
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


# ============================================================================
# WORKERS
# ============================================================================

# --- Payment Creation Worker ---

async def create_order(amount: int, user_id):
    mail = await create_user_mailbox(user_id)
    logger.debug(mail)
    if not isinstance(mail, str):
        mail = 'saivel.mezencev1@gmail.com'
    yoo = YooPay()
    res = await yoo.create_payment(amount=amount, 
                                        plan=f"Подписка на {str((amount/50))} мес. {user_id}", 
                                        email=mail
    )
    logger.debug(res)
    return res


@queue_worker(
    queue_name="PAYMENT_QUEUE",
    timeout=5,
    max_retries=3
)
async def pub_listner(redis_cli: Redis, data: dict):
    """Воркер для обработки создания платежей"""
    
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
    res = await create_order(
        amount=amount,
        user_id=user_id
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


# --- Trial Activation Worker ---

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
    """Воркер для активации пробного периода"""
    from keyboards.deps import BackButton
    
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
    
    # Проверяем пользователя в Marzban
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
    elif type(user_marz) is dict:
        logger.debug(f"🔄 Existing user in Marzban: {user_id}")
        data_marz: dict[str, Any] = {"type": "modify", "user_id": user_id}
        sub_end_marz = user_marz['expire']
    else:
        raise TimeoutError
    
    # Вычисляем новую дату окончания
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

    # Отправляем задачи
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
    }, default=str, sort_keys=True)) # type: ignore

    await redis_cli.set(f"USER_DATA:{user_id}", json.dumps(data_for_cache, default=str), ex=7200)
    logger.info(f"✅ Trial activated: user_id={user_id}")

    TEXT = ("<b>Пробный период активирован ✅</b>\n\n"
            "Чтобы начать пользоваться, перейдите в раздел с инструкцией и "
            "посмотрите короткую инструкцию по установке:")

    # Уведомление пользователя
    await bot.send_message(
        chat_id=int(user_id),
        text=TEXT,
        reply_markup=MainKeyboard.main_keyboard_without_pay_back(),
        parse_mode="HTML"
    )


# --- Marzban Worker ---

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
    
    Expected data format:
        type: create | modify 
        user_id: str | int
        expire: int
        id: (optional) uuid from marzban
        panel: (optional) custom panel to request
    """
    
    if data.get('panel'): 
        panel_url = data['panel']
        logger.info(f"🎯 Using custom panel: {panel_url}")
    else:
        panel_url = s.M_DIGITAL_URL
        logger.info(f"🎯 Using default panel: {panel_url}")

    logger.debug(f"📦 Task data: {json.dumps(data, indent=2, default=str)}")

    async with MarzbanClient(base_url=panel_url) as client: #type: ignore
        record = await client.get_user(
            username=str(data['user_id'])
        )

    if isinstance(record, dict):
        data['type'] = "modify"
    elif record == 404:
        data['type'] = "create"
    else:
        logger.error("Что-то не так с запросом в воркер")
        await notifyer_of_down_wrk(
            service=json.dumps(data, default=str)
        )
        raise SkipTask("Где-то что-то наебнулось")
    
    marz_data: dict = {
        "username": str(data['user_id']),
        'expire': data['expire']
    }


    async with MarzbanClient(base_url=panel_url) as client: #type: ignore
        if data['type'] == 'create':

            with suppress(KeyError):
                marz_data['id'] = data['id']

            cr_data = CreateUserMarzbanModel(
                **marz_data
            )

            res = await client.create(
                cr_data
            )

        else:
            res = await client.modify(
                username=marz_data['username'],
                expire=marz_data['expire']
            )

    if res == 409:
        await redis_cli.lpush(
            "MARZBAN",
            json.dumps(data, sort_keys=True, default=str)
        ) #type: ignore
        raise SkipTask("Заводим обратно для повторной проверки на существование")
    elif not isinstance(res, dict):
        logger.warning("Возникла непредвиденная ошибка")
        status = res if res is int else "None"
        await notifyer_of_down_wrk(
            f"Marzban  {json.dumps(data, default=str):<10} + {str(status):<5}"
        )
        raise SkipTask("Нужно пересмотреть проблему")
    
    db_data: dict = {
        "model": "User",
        "type": "create",
        "user_id": int(data['user_id']),
        "subscription_end": datetime.fromtimestamp(data['expire'])
    }

    db_data_panels: dict = {
        "model": "UserLinks",
        "type": "create",
        "user_id": int(data['user_id']),
    }


    url: str = res['subscription_url']

    # Определяем panel по URL
    if "dns1" in url:
        db_data_panels['panel1'] = url
        logger.info(f"✅ Panel1 (DNS1) link saved for user_id={data['user_id']}")
        logger.debug(f"  └─ URL: {url}")
    elif "dns2" in url:
        db_data_panels['panel2'] = url
        logger.info(f"✅ Panel2 (DNS2) link saved for user_id={data['user_id']}")
        logger.debug(f"  └─ URL: {url}")
    else:
        logger.error("❌ Unknown panel in subscription URL!")
        logger.error(f"  ├─ URL: {url}")
        logger.error("  ├─ Expected: 'dns1' or 'dns2' in URL")
        logger.error("  └─ Got: neither")
        raise ValueError(f"Unknown panel in URL: {url}")
    
    # ========== ОТПРАВКА ЗАДАЧ В DB ==========
    logger.info("📤 Preparing to queue DB operations...")
    logger.debug(f"  ├─ db_data: {db_data}")
    logger.debug(f"  └─ db_data_panels: {db_data_panels}")
    
    for idx, db_op in enumerate((db_data, db_data_panels), 1):
        operation_name = "User" if idx == 1 else "UserLinks"
        logger.info(f"📤 Queueing DB operation {idx}/2: {operation_name}")
        
        task_json = json.dumps(db_op, sort_keys=True, default=str)
        logger.debug(f"  └─ Task JSON: {task_json}")
        
        await redis_cli.lpush("DB", task_json) # type: ignore
        logger.debug("  └─ Task pushed to Redis DB queue")
        
        logger.debug("😴 Sleeping 1s between DB tasks...")
        await asyncio.sleep(1)

    logger.info("✅ Marzban task completed successfully!")
    logger.info(f"  ├─ User ID: {data['user_id']}")
    logger.info(f"  ├─ Operation: {data['type']}")
    logger.info(f"  ├─ Panel: {panel_url}")
    logger.info(f"  └─ Subscription: {'dns1' if 'dns1' in url else 'dns2'}")


# --- Database Worker ---

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
        """
        Воркер для обработки операций с БД из очереди
        
        Типы операций: Create, Update
        Автоматическая конвертация операций при необходимости
        """
    
        logger.info(f"📥 DB task: model={data.get('model')}, type={data.get('type')}")
    
        data = deserialize_data(data)
    
        # ═══════════════════════════════════════════════════════════════
        # ЭТАП 1: Инициализация и валидация
        # ═══════════════════════════════════════════════════════════════

        logger.info(f"🚀 Starting DB operation: {data.get('type', 'UNKNOWN').upper()}")
        logger.debug(f"📦 Raw data: {json.dumps(data, default=str, ensure_ascii=False)[:500]}...")

        model = MODEL_REGISTRY.get(data['model'])
        if not model:
            logger.error(f"❌ Unknown model: {data['model']}")
            logger.error(f"📋 Available models: {list(MODEL_REGISTRY.keys())}")
            raise SkipTask(f"Unknown model: {data['model']}")

        logger.info(f"✅ Model resolved: {model.__name__}")

        repo = BaseRepository(session=session, model=model)
        data_type: str = data['type'].lower()

        logger.debug(f"📌 Operation type: {data_type}")

        # Извлекаем данные для БД (без служебных полей)
        db_data = {
            k: v for k, v in data.items() 
            if k not in ("model", "type", "filter")
        }

        logger.debug(f"🗃️  DB data fields: {list(db_data.keys())}")
        logger.debug(f"🗃️  DB data values: {json.dumps(db_data, default=str, ensure_ascii=False)[:300]}...")

        # ═══════════════════════════════════════════════════════════════
        # ЭТАП 2: Проверка существования для моделей с user_id
        # ═══════════════════════════════════════════════════════════════

        if model in UNIQUE_USER_ID_MODELS:
            logger.info(f"🔑 Model {model.__name__} requires user_id uniqueness check")
            
            user_id = data.get('user_id') or data.get('filter', {}).get('user_id')
            
            if not user_id:
                logger.error(f"❌ Missing user_id for {model.__name__}")
                logger.error(f"📦 Available fields: {list(data.keys())}")
                raise SkipTask(f"{model.__name__} requires 'user_id' field")
            
            user_id = int(user_id)
            logger.info(f"🔍 Checking existence: model={model.__name__}, user_id={user_id}")
            
            existing = await repo.get_one(user_id=user_id)
            
            # ───────────────────────────────────────────────────────────
            # Случай A: Запись СУЩЕСТВУЕТ
            # ───────────────────────────────────────────────────────────
            
            if existing is not None:
                logger.info(f"📌 Record EXISTS: model={model.__name__}, user_id={user_id}")
                
                current_data = {
                    k: v for k, v in existing.as_dict().items() 
                    if v is not None
                }
                logger.debug(f"📊 Current data fields: {list(current_data.keys())}")
                logger.debug(f"📊 Current data: {json.dumps(current_data, default=str, ensure_ascii=False)[:300]}...")
                
                new_data = {
                    k: v for k, v in db_data.items()
                    if k != 'user_id'
                }
                logger.debug(f"🆕 New data fields: {list(new_data.keys())}")
                logger.debug(f"🆕 New data: {json.dumps(new_data, default=str, ensure_ascii=False)[:300]}...")
                
                # Сравнение данных
                has_changes = False
                changes_log = []
                
                for key, new_value in new_data.items():
                    current_value = current_data.get(key)
                    
                    # Нормализация для сравнения
                    new_value_normalized = new_value
                    current_value_normalized = current_value
                    
                    if isinstance(new_value, datetime):
                        new_value_normalized = new_value.isoformat()
                    if isinstance(current_value, datetime):
                        current_value_normalized = current_value.isoformat()
                    
                    if new_value_normalized != current_value_normalized:
                        has_changes = True
                        change_msg = f"{key}: {current_value_normalized} → {new_value_normalized}"
                        changes_log.append(change_msg)
                        logger.debug(f"📝 Change detected: {change_msg}")
                
                if not has_changes:
                    logger.info(f"⏭️  No changes detected: model={model.__name__}, user_id={user_id}")
                    logger.debug(f"✓ All {len(new_data)} fields match existing record")
                    
                    if process_once:
                        logger.debug("🔄 Returning 'skipped' (process_once=True)")
                        return 'skipped'
                    
                    logger.debug("⏭️  Skipping task (no changes)")
                    raise SkipTask
                
                logger.info(f"📝 Changes found: {len(changes_log)} field(s)")
                for change in changes_log:
                    logger.info(f"  ↳ {change}")
                
                # Конвертация CREATE → UPDATE
                if data_type == "create":
                    logger.warning(f"🔄 Converting CREATE → UPDATE: model={model.__name__}, user_id={user_id}")
                    logger.debug("   Reason: Record already exists")
                    
                    data_type = 'update'
                    data['filter'] = {'user_id': user_id}
                    db_data = {k: v for k, v in db_data.items() if k != 'user_id'}
                    
                    logger.debug(f"✓ Updated operation type: {data_type}")
                    logger.debug(f"✓ Filter set: {data['filter']}")
                    logger.debug(f"✓ Update data: {list(db_data.keys())}")
            
            # ───────────────────────────────────────────────────────────
            # Случай B: Запись НЕ СУЩЕСТВУЕТ
            # ───────────────────────────────────────────────────────────
            
            else:
                logger.info(f"✨ Record NOT FOUND: model={model.__name__}, user_id={user_id}")
                # Генерация UUID для UserLinks
                if model == UserLinks and ('uuid' not in db_data or not db_data.get('uuid')):
                    generated_uuid = str(uuid.uuid4())
                    db_data['uuid'] = generated_uuid
                    logger.info(f"🆔 Generated UUID for UserLinks: {generated_uuid}")
                
                # Конвертация UPDATE → CREATE
                if data_type == "update":
                    logger.warning(f"🔄 Converting UPDATE → CREATE: model={model.__name__}, user_id={user_id}")
                    logger.debug("   Reason: Record does not exist")
                    
                    data_type = 'create'
                    
                    if 'filter' in data and 'user_id' in data['filter']:
                        db_data['user_id'] = user_id
                        logger.debug(f"✓ Added user_id to db_data: {user_id}")
                    
                    data.pop('filter', None)
                    logger.debug("✓ Removed filter from data")
                    logger.debug(f"✓ Final db_data: {list(db_data.keys())}")

        else:
            logger.debug(f"⏭️  Model {model.__name__} does not require user_id uniqueness check")

        # ═══════════════════════════════════════════════════════════════
        # ЭТАП 3: Выполнение операции
        # ═══════════════════════════════════════════════════════════════

        logger.info(f"⚙️  Executing operation: {data_type.upper()}")
        logger.debug("📋 Final operation details:")
        logger.debug(f"   Model: {model.__name__}")
        logger.debug(f"   Type: {data_type}")
        logger.debug(f"   Data fields: {list(db_data.keys())}")

        # ───────────────────────────────────────────────────────────
        # CREATE
        # ───────────────────────────────────────────────────────────

        if data_type == "create":
            logger.info(f"➕ Creating new {model.__name__} record")
            logger.debug(f"📦 Create data: {json.dumps(db_data, default=str, ensure_ascii=False)[:500]}...")
            
            try:
                res = await repo.create(**db_data)
                logger.info(f"✅ Successfully created {model.__name__}")
                logger.debug(f"📊 Created record: {res}")
                result_type = "create"
            except Exception as e:
                logger.error(f"❌ Failed to create {model.__name__}: {type(e).__name__}: {e}")
                logger.error(f"📦 Data that caused error: {json.dumps(db_data, default=str, ensure_ascii=False)}")
                raise

        # ───────────────────────────────────────────────────────────
        # UPDATE
        # ───────────────────────────────────────────────────────────

        elif data_type == "update":
            filter_data = data.get('filter', {})
            
            if not filter_data:
                logger.error(f"❌ Update requires filter for {model.__name__}")
                logger.error(f"📦 Available data keys: {list(data.keys())}")
                raise ValueError("Update requires 'filter' parameter")
            
            logger.info(f"🔄 Updating {model.__name__} record(s)")
            logger.debug(f"🔍 Filter: {filter_data}")
            
            update_data = {k: v for k, v in db_data.items() if k != 'user_id'}
            logger.debug(f"📦 Update data fields: {list(update_data.keys())}")
            logger.debug(f"📦 Update data: {json.dumps(update_data, default=str, ensure_ascii=False)[:500]}...")
            
            try:
                res = await repo.update(data=update_data, **filter_data)
                logger.info(f"✅ Successfully updated {model.__name__}: {res} row(s) affected")
                logger.debug(f"📊 Update result: {res}")
                result_type = 'update'
            except Exception as e:
                logger.error(f"❌ Failed to update {model.__name__}: {type(e).__name__}: {e}")
                logger.error(f"🔍 Filter: {filter_data}")
                logger.error(f"📦 Update data: {json.dumps(update_data, default=str, ensure_ascii=False)}")
                raise

        # ───────────────────────────────────────────────────────────
        # UNKNOWN
        # ───────────────────────────────────────────────────────────

        else:
            logger.error(f"❌ Unknown operation type: {data_type}")
            logger.error("📋 Expected: 'create' or 'update'")
            logger.error(f"📦 Full data: {json.dumps(data, default=str, ensure_ascii=False)}")
            raise SkipTask(f"Unknown operation type: {data_type}")

        # ═══════════════════════════════════════════════════════════════
        # ЭТАП 4: Обновление кеша
        # ═══════════════════════════════════════════════════════════════

        logger.debug(f"💾 Checking cache update requirements for {model.__name__}")

        if model == User:
            user_id = db_data.get('user_id') or data.get('filter', {}).get('user_id')
            logger.info(f"💾 Updating User cache: user_id={user_id}")
            
            user: User | None = await repo.get_one(user_id=int(user_id))
            
            if user is None:
                logger.error(f"❌ User not found after {result_type}: user_id={user_id}")
                raise SkipTask(f"User {user_id} not found after operation")
            
            user_data = user.as_dict()
            cache_key = f"USER_DATA:{user_id}"
            await redis_cli.set(cache_key, json.dumps(user_data, default=str), ex=3600)
            logger.debug(f"✅ Cached User data: key={cache_key}, ttl=3600s")

        elif model == UserLinks:
            user_id = db_data.get('user_id') or data.get('filter', {}).get('user_id')
            logger.info(f"💾 Updating UserLinks cache: user_id={user_id}")
            
            user_links: UserLinks | None = await repo.get_one(user_id=int(user_id))
            
            if user_links is None:
                logger.error(f"❌ UserLinks not found after {result_type}: user_id={user_id}")
                raise SkipTask(f"UserLinks for user {user_id} not found after operation")
            
            user_data = user_links.as_dict()
            uuid_value = user_data['uuid']
            cache_key = f"USER_UUID:{user_id}"
            await redis_cli.set(cache_key, json.dumps(uuid_value, default=str), ex=3600)
            logger.debug(f"✅ Cached UserLinks UUID: key={cache_key}, uuid={uuid_value}, ttl=3600s")

        else:
            logger.debug(f"⏭️  No cache update needed for {model.__name__}")

        # ═══════════════════════════════════════════════════════════════
        # Завершение
        # ═══════════════════════════════════════════════════════════════

        logger.info("🎉 DB operation completed successfully")
        logger.info(f"📊 Summary: model={model.__name__}, operation={result_type}, user_id={db_data.get('user_id', 'N/A')}")

        if process_once:
            logger.debug(f"🔄 Returning result_type: {result_type}")
            return result_type    


# --- Payment Processing Worker ---

@queue_worker(
    queue_name="YOO:PROCEED",
    timeout=5,
    max_retries=3,
    check_availability=check_marzban_available
)
async def payment_wrk(
    redis_cli: Redis,
    data: dict
):
    """Воркер для обработки успешных платежей"""
    from keyboards.deps import BackButton
    
    logger.info(f"💰 Processing payment: user_id={data.get('user_id')}, amount={data.get('amount')}₽")
    
    # Формируем данные для Marzban
    mrzb_data: dict = {
        "user_id": data['user_id']
    }
    
    # Проверяем пользователя в Marzban
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
    ) # type: ignore

    queue_size = await redis_cli.llen("MARZBAN") # type: ignore
    logger.info(f"📊 MARZBAN queue size after push: {queue_size}")

    last_task = await redis_cli.lindex("MARZBAN", 0) # type: ignore
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
        ) # type: ignore
    
    logger.info(f"✅ Payment processed: user_id={data['user_id']}, amount={data['amount']}₽, order_id={data['order_id']}")

    SUCCESS_PAYMENT_TXT = (
        "✅ <b>Оплата прошла успешно!</b>\n\n"
        f"Счет на сумму <b>{data['amount']} ₽</b> подтвержден. "
        "Ваша подписка обновлена и готова к работе.\n\n"
        "<b>Как подключиться:</b>\n"
        "1. Перейдите в <b>🔗 Подписки и ссылки</b>\n"
        "2. Скопируйте персональный ключ\n"
        "3. Если возникли вопросы — загляните в <b>📱 Инструкция</b>"
    )   

    # Уведомления
    await bot.send_message(
        chat_id=int(data['user_id']),
        text=SUCCESS_PAYMENT_TXT,
        reply_markup=MainKeyboard.main_keyboard_without_pay_back(),
        parse_mode="HTML"
    )

    await bot.send_message(
        chat_id=int(s.ADMIN_ID),
        text=f"Оплата прошла успешно на сумму {data['amount']} для пользователя `{data['user_id']}`",
        parse_mode='MARKDOWN'
    )


# --- Nightly Cache Refresh Worker ---

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


async def get_links_of_panels(uuid: str) -> list | None:
    '''
    Эта функция принимает на вход uuid строку. 
    И возвращает списоков подписок для обеих панелей, 
    которые есть в таблице links для этого uuid.
    '''
    async with async_session_maker() as session:
        user_repo = BaseRepository(session=session, model=UserLinks)
        res = await user_repo.get_one(uuid=uuid)
        logger.debug(res)

        if res is None:
            return None
        
        return [res.panel1, res.panel2]