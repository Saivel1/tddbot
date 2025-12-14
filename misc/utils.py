from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession
from repositories.base import BaseRepository
from db.models import User, UserLinks
import json
from schemas.schem import UserModel, PayDataModel
import asyncio
from core.yoomoney.payment import YooPay
import aiohttp
from config import settings
from typing import Any
from core.marzban.Client import MarzbanClient
from datetime import datetime, timedelta
from config import settings as s
from schemas.schem import CreateUserMarzbanModel
from sqlalchemy import text
from typing import Type, Dict
import uuid
from sqlalchemy import select
from logger_setup import logger

#TODO: Обработчик для платежей


MODEL_REGISTRY: Dict[str, Type] = {
    "User": User,
    "UserLinks": UserLinks
}

# Модели с уникальным user_id
UNIQUE_USER_ID_MODELS = {User, UserLinks}


async def is_cached(
    redis_cache: Redis,
    user_id: int,
    session: AsyncSession,
    force_refresh: bool = False  # Для ночного обновления
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

    logger.debug(f"is_cached called: user_id={user_id}, force_refresh={force_refresh}")
    
    # Если не force_refresh - пытаемся взять из кеша
    if not force_refresh:
        user = await redis_cache.get(user_str)
        if user is not None:
            logger.info(f"Cache HIT: user_id={user_id}")
            return _parse_user(user)

        logger.debug(f"Cache MISS: user_id={user_id}, acquiring lock")
    else:
        logger.debug(f"Force refresh: user_id={user_id}, skipping cache check")
    
    # Нет кеша или force_refresh - берём lock
    acquired = await redis_cache.set(lock_key, "1", nx=True, ex=5)
    
    if acquired:
        logger.debug(f"Lock ACQUIRED: user_id={user_id}")
        try:
            # Double-check (если не force_refresh)
            if not force_refresh:
                user = await redis_cache.get(user_str)
                if user is not None:
                    logger.debug(f"Cache filled by another task: user_id={user_id}")
                    return _parse_user(user)
            
            # Загружаем из БД
            logger.debug(f"Loading from DB: user_id={user_id}")
            repo = BaseRepository(session=session, model=User)
            user_data = await repo.get_one(user_id=user_id)
            
            if not user_data:
                logger.info(f"User NOT FOUND in DB: user_id={user_id}")
                return None
            
            user_dict = user_data.as_dict()
            logger.debug(f"User data loaded: user_id={user_id}, fields={list(user_dict.keys())}")

            # Сериализуем
            json_user_data = json.dumps(user_data.as_dict(), default=str)
            
            # Определяем TTL
            # Ночное обновление: 25 часов (90000 сек)
            # Первое обращение: 1 час (3600 сек)
            ttl = 90000 if force_refresh else 3600
            
            # Сохраняем в кеш
            await redis_cache.set(user_str, json_user_data, ex=ttl)
            logger.info(
                f"User cached: user_id={user_id}, "
                f"ttl={ttl}s, "
                f"source={'force_refresh' if force_refresh else 'cache_miss'}"
            )
            
            return _parse_user(json_user_data)
            
        except Exception as e:
            logger.error(
                f"DB error while loading user: user_id={user_id}, error={e}",
                exc_info=True
            )
            return None
        finally:
            # Освобождаем lock
            await redis_cache.delete(lock_key)
            logger.debug(f"Lock RELEASED: user_id={user_id}")
    else:
        # Другой task заполняет кеш, ждём его
        logger.debug(f"Lock held by another task, waiting: user_id={user_id}")
        for attempt in range(50):  # Максимум 5 секунд
            await asyncio.sleep(0.1)
            user = await redis_cache.get(user_str)
            if user is not None:
                logger.debug(f"Cache filled while waiting (attempt {attempt+1}): user_id={user_id}")
                return _parse_user(user)
        
        # Если так и не дождались
        logger.warning(
            f"Timeout waiting for cache: user_id={user_id}, "
            f"waited 5s, cache still empty"
        )
        return None


def _parse_user(user_json: str) -> UserModel | None:
    """
    Парсит JSON строку в UserModel
    
    Args:
        user_json: JSON строка с данными пользователя
    
    Returns:
        UserModel или None при ошибке парсинга
    """
    try:
        user_dict = json.loads(user_json)
        logger.debug(f"Parsing user data: {user_dict}")
        return UserModel(**user_dict)
    except Exception as e:
        print(f"JSON parse error: {e}")
        return None


async def cache_popular_pay_time(redis_cache: Redis, user_id: int) -> str | None:
    """
    Получить или создать платёж для популярной суммы (50₽)
    Возвращает payment_url или None если платёж в процессе создания
    """
    pay_str = f"POP_PAY_CHOOSE:{user_id}"
    lock_key = f"POP_PAY_LOCK:{user_id}"
    
    logger.debug(f"Entered function with {pay_str}")
    logger.debug(f"Entered function with {lock_key}")

    # Проверяем кэш
    pay_data = await redis_cache.get(pay_str)
    
    logger.debug(f"Pay data: {pay_data}")
    if pay_data is None:
        # Атомарно берём lock
        acquired = await redis_cache.set(lock_key, "1", nx=True, ex=60)
        
        if acquired:
            logger.debug(f"Acquired data: {acquired}")
            try:
                # Double-check после получения lock
                pay_data = await redis_cache.get(pay_str)
                
                if pay_data is None:
                    # Только мы создаём платёж
                    payment_data = {
                        'user_id': user_id,
                        'amount': 50,
                    }
                    
                    # Публикуем в очередь (используем lpush вместо publish для гарантии)
                    await redis_cache.lpush("PAYMENT_QUEUE", json.dumps(payment_data)) #type: ignore
                    logger.debug(f"Отдали задачу {payment_data}")
                    
                    # Ждём обработки (максимум 10 секунд)
                    for _ in range(100):
                        await asyncio.sleep(0.1)
                        pay_data = await redis_cache.get(pay_str)
                        if pay_data:
                            break
                    
                    logger.debug(f"Получили pay_data {pay_data}")
                    if pay_data is None:
                        # Timeout - платёж в процессе создания
                        return None
            finally:
                # Освобождаем lock
                await redis_cache.delete(lock_key)
                logger.debug(f"Lock_Key удалён {lock_key}")
        else:
            # Другой task создаёт платёж, ждём результата
            for _ in range(100):
                await asyncio.sleep(0.1)
                pay_data = await redis_cache.get(pay_str)
                if pay_data:
                    break

            logger.debug(f"Получили другой pay_data {pay_data}")
            if pay_data is None:
                return None
    
    # Парсим и возвращаем URL
    pay_res = json.loads(pay_data)
    return pay_res['payment_url']


async def pub_listner(redis_cli: Redis):
    """
    Воркер для обработки платежей из очереди
    Переименован в payment_worker для ясности, но оставлен старый name для обратной совместимости
    """
    yoo_handl = YooPay()
    
    try:
        while True:
            # brpop - только один воркер получит задачу
            result = await redis_cli.brpop("PAYMENT_QUEUE", timeout=5) #type: ignore
            
            if not result:
                continue
            
            _, message = result
            
            logger.debug("Приняли заказ")
            
            try:
                data = json.loads(message)
                user_id = data['user_id']
                amount = data['amount']
                pay_str = f"POP_PAY_CHOOSE:{user_id}"
                
                # Проверяем что платёж ещё не создан (идемпотентность)
                existing = await redis_cli.get(pay_str)
                if existing:
                    logger.debug(f"Платёж для {user_id} уже создан, пропускаем")
                    continue
                
                # Создаём платёж
                res = await yoo_handl.create_payment(
                    amount=amount,
                    email="saivel.mezencev1@gmail.com",
                    plan="1+9210"
                )
                
                if res is None:
                    # Ошибка создания - возвращаем задачу в очередь
                    await redis_cli.lpush("PAYMENT_QUEUE", message) #type: ignore
                    logger.debug(f"❌ Ошибка создания платежа для {user_id}, задача возвращена")
                    await asyncio.sleep(5)  # Пауза перед retry
                    continue
                
                # Сохраняем результат
                data_for_load = {
                    "payment_url": res[0],
                    "payment_id": res[1]
                }
                await redis_cli.set(pay_str, json.dumps(data_for_load), ex=600)
                logger.debug(f"✅ Платёж обработан! {user_id}")
                
            except Exception as e:
                logger.error(f"❌ Ошибка обработки задачи: {e}")
                # Возвращаем задачу в очередь
                await redis_cli.lpush("PAYMENT_QUEUE", message) #type: ignore
                await asyncio.sleep(5)
                
    except asyncio.CancelledError:
        logger.info("Payment worker остановлен")
        raise


async def is_cached_payment(
    redis_cache: Redis,
    user_id: int,
    amount: int | None = None
) -> PayDataModel | None:
    """
    Проверить наличие платежа в кэше
    amount=50 -> проверяет популярный платёж
    amount=другое -> проверяет кастомный платёж
    amount=None -> проверяет оба
    """
    pay_str = f"POP_PAY_CHOOSE:{user_id}"
    pay_reg = f"PAY:{user_id}:{amount}" if amount else None
    
    # Проверяем оба кэша
    pay = await redis_cache.get(pay_str)
    pay_c = await redis_cache.get(pay_reg) if pay_reg else None
    
    if pay is None and pay_c is None:
        return None
    
    res_json: dict = {}
    
    # Приоритет: если запросили amount=50 или не указали - проверяем популярный
    if pay and (amount == 50 or amount is None):
        res_json = json.loads(pay)
    # Иначе проверяем кастомный
    elif pay_c:
        res_json = json.loads(pay_c)
    
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
    """
    Проверка существования задачи с lock на конкретного пользователя
    """
    user_id = data.get("user_id")
    if not user_id:
        # Fallback если нет user_id
        lock_key = f"{worker}_CHECK_LOCK"
    else:
        lock_key = f"{worker}_CHECK_LOCK:{user_id}"  # ← Уникальный на пользователя
    
    # Пытаемся получить lock
    for attempt in range(20):
        acquired = await redis_cli.set(lock_key, "1", nx=True, ex=2)
        
        if acquired:
            try:
                all_items = await redis_cli.lrange(worker, 0, -1) #type: ignore
                search_value = json.dumps(data, sort_keys=True, default=str)
                
                start = datetime.now()
                result = search_value in all_items
                end = datetime.now()
                
                logger.debug(f"Start: {start} ||| End: {end}")
                return result
            finally:
                await redis_cli.delete(lock_key)
        
        await asyncio.sleep(0.1)
    
    return False


async def trial_activation_worker(
    redis_cli: Redis,
    session: AsyncSession
):
    wrk_label = "TRIAL_ACTIVATION"

    while True:
        result = await redis_cli.brpop(wrk_label, timeout=5) #type: ignore

        if not result:
            continue 
        
        _, message = result
        data = json.loads(message)
        
        try:

            repo = BaseRepository(session=session, model=User)
            user = await repo.get_one(user_id = int(data["user_id"]))
            
            if not user:
                user = await repo.create(
                    user_id = int(data['user_id'])
                )
            
            if user.trial_used:
                # обновить кэш 
                continue

            user_id = str(data['user_id'])
            async with MarzbanClient() as client:
                user_marz = await client.get_user(username=user_id)
            
            sub_end_marz: int = 0

            if user_marz == 404:
                data_marz: dict[str, Any] = {
                    "type": "create",
                    "user_id": user_id
                }
            elif user_marz is None:
                raise TimeoutError
            elif type(user_marz) == dict:
                data_marz: dict[str, Any] = {
                    "type": "modify",
                    "user_id": user_id
                }
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


            await redis_cli.lpush(
                "MARZBAN",
                json.dumps(data_marz, sort_keys=True, default=str)
            ) #type: ignore
            
        except Exception as e:
            logger.error(f"❌ Ошибка: {e}")
            await redis_cli.lpush(wrk_label, message) #type: ignore
            await asyncio.sleep(10)


async def marzban_worker(
    redis_cli: Redis,
    panel_url: str | None = None
):
    """
    На вход принимаем клиент редис и обрабатываем тип запроса 

    типы запросов:
        1. Create
        2. Modify

    данные для запроса
    username: str
    expire: int
    id: uuid from mazban like {"id": "aljfk-asfg-saadsg-g352", "protocol": "xtls-rs-fla"}
    """


    wrk_label = 'MARZBAN'

    cnt = 0
    while True:
        # Ждём пока сервис станет доступен
        while not await check_marzban_available():
            logger.debug("⏳ Сервис недоступен, ждём 10 сек...")
            await asyncio.sleep(10)
            cnt += 1
            if cnt == 6*10:
                #send message
                pass
        
        # Берём задачу из очереди (блокирующий вызов)
        result = await redis_cli.brpop(wrk_label, timeout=5) #type: ignore
        cnt = 0
        if not result:
            continue 
        
        _, message = result
        data = json.loads(message)
        
        try:
            async with MarzbanClient(base_url=panel_url if panel_url else s.M_DIGITAL_URL) as client:
                marz_data:dict = {}

                marz_data['username'] = str(data['user_id'])
                marz_data['expire'] = data['expire']
                if data.get("id"): marz_data['id'] = data['id']

                db_data: dict = {
                    "model": "User"
                }
                db_data_panels: dict = {
                    "model": "UserLinks"
                }

                if data['type'] == "create":
                    create_data = CreateUserMarzbanModel(
                        **marz_data
                    )
                    res = await client.create(data=create_data)

                    #User
                    db_data['type'] = 'create'
                    db_data['user_id'] = int(data['user_id'])
                    db_data['subscription_end'] = datetime.fromtimestamp(data['expire'])

                    #UserLinks
                    db_data_panels['type'] = 'create'
                    db_data_panels['user_id'] = int(data['user_id'])
                    db_data_panels['uuid'] = str(uuid.uuid4())
                
                elif data["type"] == "modify":
                    res = await client.modify(**marz_data)

                    #User
                    db_data['type'] = 'update'
                    db_data['filter'] = {"user_id": int(data['user_id'])}
                    db_data['subscription_end'] = datetime.fromtimestamp(data['expire'])

                    #UserLinks
                    db_data_panels['type'] = 'update'
                    db_data_panels['filter'] = {"user_id": int(data['user_id'])}


                if res == 409:
                    res = await client.modify(**marz_data)

                    #User
                    db_data['type'] = 'update'
                    db_data['filter'] = {"user_id": int(data['user_id'])}
                    db_data['subscription_end'] = datetime.fromtimestamp(data['expire'])

                    #UserLinks
                    db_data_panels['type'] = 'update'
                    db_data_panels['filter'] = {"user_id": int(data['user_id'])}

                if type(res) != dict:
                    raise TimeoutError(f"Returns {type(res)} - {res}")

                url: str = res['subscription_url']

                if "dns1" in url:
                    db_data_panels['panel1'] = url
                elif "dns2" in url:
                    db_data_panels['panel2'] = url
                else:
                    raise ValueError(f"Unkown panel {url}")
                
                for db_op in (db_data_panels, db_data):
                    await redis_cli.lpush(
                        "DB",
                        json.dumps(db_op, sort_keys=True, default=str)
                    ) #type:ignore

                # Данные?
                await redis_cli.set(
                    f"USER_DATA:{data['user_id']}",
                    "",
                    ex=7200
                )

                # Для тестов
                return res
        except Exception as e:
            logger.error(f"❌ Ошибка: {e}")
            await redis_cli.lpush(wrk_label, message) #type: ignore
            await asyncio.sleep(10)

def deserialize_data(data: dict) -> dict:
    """
    Конвертирует строковые datetime обратно в объекты datetime
    """
    result = {}
    for key, value in data.items():
        # Пропускаем служебные поля
        if key in ('model', 'type', 'filter'):
            result[key] = value
            continue
        
        # Пытаемся распарсить datetime
        if isinstance(value, str):
            try:
                # Пробуем ISO format datetime
                result[key] = datetime.fromisoformat(value)
            except (ValueError, AttributeError):
                # Не datetime - оставляем как есть
                result[key] = value
        elif isinstance(value, dict):
            # Рекурсивно обрабатываем вложенные словари (например, filter)
            result[key] = deserialize_data(value)
        else:
            result[key] = value
    
    return result


async def db_worker(
    redis_cli: Redis,
    session: AsyncSession,
    process_once: bool = False
):
    """
    На вход принимаем клиент редис и обрабатываем тип запроса 
    Типы запросов:
        1. Create - создание записи
        2. Update - обновление записи
    
    Особенности:
    - Автоматически превращает Create в Update если запись существует
    - Пропускает дубликаты (если данные не изменились)
    - Для User/UserLinks проверяет существование по user_id
    """
    wrk_label = 'DB'
    cnt = 0
    
    while True:
        # Проверка доступности БД
        while not await check_db_available(session):
            logger.debug("⏳ DB unavailable, waiting 10 sec...")
            await asyncio.sleep(10)
            cnt += 1
            if cnt == 60:
                logger.error("🚨 DB unavailable for 10 minutes!")
                cnt = 0
        
        # Берём задачу из очереди
        result = await redis_cli.brpop(wrk_label, timeout=5) #type: ignore
        cnt = 0
        
        if not result:
            if process_once:
                return None
            continue
        
        _, message = result
        
        try:
            data = json.loads(message)
            
            data = deserialize_data(data)
            # Получаем модель
            model = MODEL_REGISTRY.get(data['model'])
            if not model:
                raise ValueError(f"Unknown model: {data['model']}")
            
            repo = BaseRepository(session=session, model=model)
            data_type: str = data['type'].lower()
            
            # Извлекаем данные для записи (исключаем служебные поля)
            db_data = {
                k: v for k, v in data.items() 
                if k not in ("model", "type", "filter")
            }
            
            # Проверка существования для моделей с user_id
            if model in UNIQUE_USER_ID_MODELS:
                # ✅ ИСПРАВЛЕНО: Ищем user_id в data или в filter
                user_id = data.get('user_id') or data.get('filter', {}).get('user_id')
                
                if not user_id:
                    raise ValueError(f"{model.__name__} requires 'user_id' field")
                
                existing = await repo.get_one(user_id=user_id)
                
                if existing is not None:
                    # Получаем текущие данные из БД (без None)
                    current_data = {
                        k: v for k, v in existing.as_dict().items() 
                        if v is not None
                    }
                    
                    # Новые данные (только те поля, которые передаём)
                    new_data = {
                        k: v for k, v in db_data.items()
                        if k != 'user_id'  # Исключаем user_id из сравнения
                    }
                    
                    # ✅ ИСПРАВЛЕНО: Сравниваем только переданные поля
                    has_changes = False
                    for key, new_value in new_data.items():
                        current_value = current_data.get(key)
                        
                        # Нормализуем для сравнения
                        if isinstance(new_value, datetime):
                            new_value = new_value.isoformat()
                        if isinstance(current_value, datetime):
                            current_value = current_value.isoformat()
                        
                        if new_value != current_value:
                            has_changes = True
                            break
                    
                    # Если нет изменений - пропускаем
                    if not has_changes:
                        logger.debug(f"⏭️  Skipping duplicate for user_id={user_id}")
                        if process_once:
                            return 'skipped'
                        continue
                    
                    # Если это CREATE, но запись существует - превращаем в UPDATE
                    if data_type == "create":
                        logger.debug(f"🔄 Converting CREATE to UPDATE for user_id={user_id}")
                        data_type = 'update'
                        data['filter'] = {'user_id': user_id}
                        # ✅ Убираем user_id из db_data для UPDATE
                        db_data = {k: v for k, v in db_data.items() if k != 'user_id'}
            
            # Выполняем операцию
            if data_type == "create":
                res = await repo.create(**db_data)
                logger.debug(f"✅ Created {model.__name__}: {res}")
                result_type = "create"
                
            elif data_type == "update":
                filter_data = data.get('filter', {})
                
                if not filter_data:
                    raise ValueError("Update requires 'filter' parameter")
                
                # ✅ Для UPDATE user_id должен быть ТОЛЬКО в filter
                update_data = {k: v for k, v in db_data.items() if k != 'user_id'}
                
                res = await repo.update(data=update_data, **filter_data)
                logger.debug(f"✅ Updated {model.__name__}: {res} rows")
                result_type = 'update'
            
            else:
                raise ValueError(f"Unknown operation type: {data_type}")
            
            if process_once:
                return result_type
                
        except Exception as e:
            logger.error(f"❌ Ошибка: {e}")
            import traceback
            traceback.print_exc()
            
            await redis_cli.lpush(wrk_label, message) #type: ignore
            
            if process_once:
                raise
            
            await asyncio.sleep(1)


def normalize_for_comparison(data: dict) -> dict:
    """
    Нормализует данные для корректного сравнения
    
    Примеры:
    - datetime -> ISO string
    - None -> удаляется
    - bool -> int (для SQLite)
    """
    normalized = {}
    
    for k, v in data.items():
        if v is None:
            continue
        
        # Datetime -> string
        if isinstance(v, datetime):
            normalized[k] = v.isoformat()
        # Bool -> int (SQLite хранит как 0/1)
        elif isinstance(v, bool):
            normalized[k] = int(v)
        else:
            normalized[k] = v
    
    return normalized


async def nightly_cache_refresh_worker(
    redis_cache: Redis,
    session_maker  # async_sessionmaker
):
    """
    Воркер для ночного обновления кешей всех пользователей
    
    Запускается каждую ночь в 03:00 и обновляет кеш всех пользователей
    с TTL 25 часов (с запасом до следующего обновления)
    """
    
    while True:
        # Вычисляем время до 03:00
        now = datetime.now()
        target = now.replace(hour=3, minute=0, second=0, microsecond=0)
        
        # Если 03:00 уже прошло сегодня - берём завтра
        if target <= now:
            target += timedelta(days=1)
        
        sleep_seconds = (target - datetime.now()).total_seconds()
        logger.debug(f"🌙 Nightly cache refresh scheduled for {target} (in {sleep_seconds/3600:.1f} hours)")
        
        await asyncio.sleep(sleep_seconds)
        
        logger.debug("🌙 Starting nightly cache refresh...")
        
        try:
            async with session_maker() as session:
                repo = BaseRepository(session=session, model=User)
                
                # Обрабатываем пользователей пачками
                offset = 0
                batch_size = 100
                total_refreshed = 0
                
                while True:
                    # Получаем пачку пользователей
                    stmt = (
                        select(User)
                        .offset(offset)
                        .limit(batch_size)
                    )
                    result = await session.execute(stmt)
                    users = result.scalars().all()
                    
                    if not users:
                        break
                    
                    # Обновляем кеш для каждого пользователя
                    for user in users:
                        try:
                            await is_cached(
                                redis_cache=redis_cache,
                                user_id=user.user_id,
                                session=session,
                                force_refresh=True  # ← Принудительное обновление
                            )
                            total_refreshed += 1
                            
                            # Логируем прогресс каждые 100 пользователей
                            if total_refreshed % 100 == 0:
                                logger.debug(f"📊 Progress: {total_refreshed} users refreshed")
                                
                        except Exception as e:
                            logger.error(f"❌ Error refreshing cache for user {user.user_id}: {e}")
                            continue
                    
                    offset += batch_size
                    
                    # Небольшая пауза между пачками (чтобы не перегрузить Redis/БД)
                    await asyncio.sleep(0.5)
                
                logger.debug(f"✅ Nightly cache refresh completed: {total_refreshed} users")
                
        except Exception as e:
            logger.error(f"❌ Nightly cache refresh failed: {e}")
            import traceback
            traceback.print_exc()
            # Не падаем, попробуем снова завтра


async def check_db_available(session: AsyncSession) -> bool:
    """
    Проверяет доступность PostgreSQL
    """
    try:
        await session.execute(text("SELECT 1"))
        return True
    except Exception as e:
        print(f"DB unavailable: {e}")
        return False


async def check_marzban_available() -> bool:
    """
    Проверка доступности Marzban
    """
    try:
        async with aiohttp.ClientSession() as client:
            async with client.request("GET", settings.M_DIGITAL_URL) as res:
                return res.status < 500
    except:
        return False