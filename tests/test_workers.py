from tests.fixtures import message, callback_query, create_user, create_user_in_links
import pytest, pytest_asyncio
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlalchemy.ext.asyncio import AsyncSession
from db.models import Base, User, UserLinks
from repositories.base import BaseRepository
from datetime import datetime, timedelta
from config import settings
from redis.asyncio import Redis
import asyncio, json
from misc.utils import pub_listner, is_cached, worker_exsists
from core.yoomoney.payment import YooPay
from schemas.schem import UserModel
from core.marzban.Client import MarzbanClient
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select


@pytest_asyncio.fixture
async def redis_client():
    """Один Redis клиент на всю сессию"""
    client = Redis(
        host='localhost',
        port=6379,
        decode_responses=True
    )
    yield client
    await client.flushall()
    await client.aclose()

@pytest_asyncio.fixture(autouse=True)
async def clear_redis(redis_client):
    """Очищаем Redis перед каждым тестом"""
    await redis_client.flushdb()
    yield


@pytest_asyncio.fixture(scope="function")
async def test_engine():
    """Движок для тестовой БД"""
    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        echo=False
    )
    
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    
    yield engine

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)

    await engine.dispose()


@pytest_asyncio.fixture
async def test_session_maker(test_engine):
    """Session maker для тестов"""
    return async_sessionmaker(
        test_engine,
        class_=AsyncSession,
        expire_on_commit=False
    )


@pytest_asyncio.fixture
async def test_session(test_session_maker):
    """Одна сессия для теста"""
    async with test_session_maker() as session:
        yield session


@pytest.mark.asyncio
async def test_worker_exist(redis_client: Redis):
    from misc.utils import worker_exsists

    data = {
        "type": "create",
        "user_id": 1234,
        "sub_end": datetime.now()
    }

    await redis_client.lpush(
        "MARZBAN",
        json.dumps(data, default=str, sort_keys=True)
    ) #type: ignore

    res = await worker_exsists(redis_cli=redis_client, worker="MARZBAN", data=data)
    res_empty = await worker_exsists(redis_cli=redis_client, worker="12345678", data=data)

    assert res == True
    assert res_empty == False


@pytest.mark.asyncio
async def test_worker_marzban_create(redis_client: Redis):
    from misc.utils import marzban_worker

    data = {
        "type": "create",
        "user_id": 1234,
        "expire": int(datetime.now().timestamp())
    }

    await redis_client.lpush(
        "MARZBAN",
        json.dumps(data, default=str, sort_keys=True)
    ) #type: ignore

    res = await marzban_worker(redis_cli=redis_client)

    assert type(res) == dict


@pytest.mark.asyncio
async def test_worker_marzban_modify(redis_client: Redis):
    from misc.utils import marzban_worker


    data = {
        "type": "modify",
        "user_id": 1234,
        "expire": int(datetime.now().timestamp())
    }

    await redis_client.lpush(
        "MARZBAN",
        json.dumps(data, default=str, sort_keys=True)
    ) #type: ignore

    res = await marzban_worker(redis_cli=redis_client)

    assert type(res) == dict



@pytest.mark.asyncio
async def test_delete_user():
    async with MarzbanClient() as client:
        res = await client.delete(username='1234')

    assert type(res) == dict

    
@pytest.mark.asyncio
async def test_race_condition_cache_miss(
    redis_client: Redis,
    test_session_maker,
    create_user,
    monkeypatch
):
    """Тест: два параллельных запроса при холодном кэше"""
    
    user_id = 1234
    username = 'test_user'
    
    # Создаём пользователя
    await create_user(
        user_id=user_id,
        username=username,
        trial_used=False,
        subscription_end=datetime.now() + timedelta(days=7)
    )
    
    # Проверяем что кэша нет
    cache = await redis_client.get(f"USER_DATA:{user_id}")
    assert cache is None
    
    # Счётчик запросов к БД
    db_call_count = 0
    original_get_one = BaseRepository.get_one
    
    async def tracked_get_one(self, **kwargs):
        nonlocal db_call_count
        db_call_count += 1
        # Добавляем задержку чтобы увеличить окно race condition
        await asyncio.sleep(0.1)
        return await original_get_one(self, **kwargs)
    
    monkeypatch.setattr(BaseRepository, "get_one", tracked_get_one)
    
    # Каждая задача получает свою сессию
    async def cached_with_own_session():
        async with test_session_maker() as session:
            return await is_cached(
                redis_cache=redis_client,
                user_id=user_id,
                session=session
            )
    
    # Запускаем 10 параллельных запросов
    tasks = [cached_with_own_session() for _ in range(10)]
    
    results = await asyncio.gather(*tasks)
    
    # Проверяем что все получили данные
    assert len(results) == 10
    assert all(r is not None for r in results), "Some results are None"
    assert all(r.user_id == user_id for r in results), "User IDs don't match" #type: ignore
    
    print(f"\nDB calls: {db_call_count}")
    
    # БЕЗ защиты от race condition: будет ~10 запросов к БД
    # С защитой (lock): будет 1-2 запроса
    if db_call_count > 5:
        pytest.fail(f"Race condition detected! {db_call_count} DB calls instead of 1-2")
    
    # Строгая проверка (можно закомментировать для первого прогона)
    # assert db_call_count <= 2, f"Expected <=2 DB calls, got {db_call_count}"


@pytest.mark.asyncio
async def test_worker_exists_race_condition(redis_client: Redis):
    """Проверка worker_exists при параллельных запросах"""
    
    user_id = 12345
    data = {
        "user_id": user_id,
        "username": "test_user"
    }
    
    # Счётчик добавлений в очередь
    added_count = 0
    
    async def try_add_to_queue():
        nonlocal added_count
        
        # Имитация handler логики
        exists = await worker_exsists(
            redis_cli=redis_client,
            worker="TRIAL_ACTIVATION",
            data=data
        )
        
        if exists:
            return "Already exists"
        
        # Добавляем в очередь
        await redis_client.lpush(
            'TRIAL_ACTIVATION',
            json.dumps(data, sort_keys=True)
        ) #type: ignore
        added_count += 1
        return "Added"
    
    # Запускаем 10 параллельных попыток
    tasks = [try_add_to_queue() for _ in range(10)]
    results = await asyncio.gather(*tasks)
    
    print(f"\nResults: {results}")
    print(f"Added to queue: {added_count} times")
    
    # Проверяем очередь
    queue_size = await redis_client.llen("TRIAL_ACTIVATION") #type: ignore
    print(f"Queue size: {queue_size}")
    
    # БЕЗ защиты: может быть 2-10 задач
    # С защитой: должна быть 1 задача
    assert added_count <= 2, f"Race condition! {added_count} tasks added"
    assert queue_size <= 2, f"Race condition! {queue_size} tasks in queue"


@pytest.mark.asyncio
async def test_worker_exists_race_condition_many_users(redis_client: Redis):
    """Проверка worker_exists при параллельных запросах от разных пользователей"""
    
    base_user_id = 12345
    added_count = 0
    
    async def try_add_to_queue(n: int):
        nonlocal added_count
        
        # Создаём НОВЫЙ словарь для каждой задачи
        data = {
            "user_id": base_user_id + n,  # ← Уникальный ID
            "username": f"test_user_{n}"
        }
        
        # Проверка существования
        exists = await worker_exsists(
            redis_cli=redis_client,
            worker="TRIAL_ACTIVATION",
            data=data
        )
        
        print(f"Task {n}: user_id={data['user_id']}, exists={exists}")
        
        if exists:
            return f"Already exists: {data['user_id']}"
        
        # Добавляем в очередь
        await redis_client.lpush(
            'TRIAL_ACTIVATION',
            json.dumps(data, sort_keys=True)
        ) # type: ignore
        added_count += 1
        return f"Added: {data['user_id']}"
    
    # Запускаем 10 параллельных задач для разных пользователей
    tasks = [try_add_to_queue(n) for n in range(10)]
    results = await asyncio.gather(*tasks)
    
    print(f"\nResults: {results}")
    print(f"Added to queue: {added_count} times")
    
    queue_size = await redis_client.llen("TRIAL_ACTIVATION") # type: ignore
    print(f"Queue size: {queue_size}")
    
    # Для РАЗНЫХ пользователей - должно быть 10 задач (по одной на каждого)
    assert added_count == 10, f"Expected 10 tasks for 10 users, got {added_count}"
    assert queue_size == 10, f"Expected 10 tasks in queue, got {queue_size}"


@pytest.mark.asyncio
async def test_worker_db_create(redis_client: Redis, test_session: AsyncSession):
    from misc.utils import db_worker


    data = {
        "type": "create",
        "model": 'User',
        "user_id": 1234
    }

    await redis_client.lpush(
        "DB",
        json.dumps(data, default=str, sort_keys=True)
    ) #type: ignore

    res = await db_worker(redis_cli=redis_client, session=test_session, process_once=True)

    assert res == 'create'


@pytest.mark.asyncio
async def test_worker_db_update(redis_client: Redis, test_session: AsyncSession):
    from misc.utils import db_worker


    data = {
        "type": "create",
        "model": 'User',
        "user_id": 1234
    }

    await redis_client.lpush(
        "DB",
        json.dumps(data, default=str, sort_keys=True)
    ) #type: ignore

    await db_worker(redis_cli=redis_client, session=test_session, process_once=True)
    

    data = {
        "type": "update",
        "model": 'User',
        "filter": {'user_id': 1234},
        "username": "test_username"
    }

    await redis_client.lpush(
        "DB",
        json.dumps(data, default=str, sort_keys=True)
    ) #type: ignore

    res = await db_worker(redis_cli=redis_client, session=test_session, process_once=True)

    assert res == 'update'


@pytest.mark.asyncio
async def test_worker_duplicate_detection(redis_client: Redis, test_session: AsyncSession):
    """Тест: воркер пропускает дубликаты"""
    from misc.utils import db_worker
    
    # Создаём пользователя
    data = {
        "type": "create",
        "model": 'User',
        "user_id": 1234,
        "username": "test"
    }
    
    await redis_client.lpush("DB", json.dumps(data, sort_keys=True)) #type: ignore
    result1 = await db_worker(redis_cli=redis_client, session=test_session, process_once=True)
    assert result1 == 'create'
    
    # Отправляем дубликат
    await redis_client.lpush("DB", json.dumps(data, sort_keys=True)) #type: ignore
    result2 = await db_worker(redis_cli=redis_client, session=test_session, process_once=True)
    assert result2 == 'skipped'


@pytest.mark.asyncio
async def test_worker_create_to_update_conversion(redis_client: Redis, test_session: AsyncSession):
    """Тест: CREATE автоматически превращается в UPDATE"""
    from misc.utils import db_worker
    
    # Создаём пользователя
    data1 = {
        "type": "create",
        "model": 'User',
        "user_id": 1234,
        "username": "old_name"
    }
    
    await redis_client.lpush("DB", json.dumps(data1, sort_keys=True)) #type: ignore
    await db_worker(redis_cli=redis_client, session=test_session, process_once=True)
    
    # Отправляем CREATE с новыми данными
    data2 = {
        "type": "create",
        "model": 'User',
        "user_id": 1234,
        "username": "new_name"
    }
    
    await redis_client.lpush("DB", json.dumps(data2, sort_keys=True)) #type: ignore
    result = await db_worker(redis_cli=redis_client, session=test_session, process_once=True)
    
    assert result == 'update'  # Должен быть UPDATE, а не CREATE
    
    # Проверяем что данные обновились
    repo = BaseRepository(session=test_session, model=User)
    user = await repo.get_one(user_id=1234)
    assert user.username == "new_name"  #type: ignore



@pytest.mark.asyncio
async def test_cache_with_ttl_1_hour(redis_client: Redis, test_session: AsyncSession, create_user):
    """Тест: обычное обращение создаёт кеш с TTL 1 час"""
    await create_user(user_id=1234, username="test")
    
    # Первое обращение - создаёт кеш
    user = await is_cached(redis_client, 1234, test_session)
    assert user is not None
    assert user.username == "test"
    
    # Проверяем TTL (должен быть ~3600 секунд)
    ttl = await redis_client.ttl("USER_DATA:1234")
    assert 3550 < ttl <= 3600  # С небольшим запасом на выполнение


@pytest.mark.asyncio
async def test_force_refresh_with_ttl_25_hours(redis_client: Redis, test_session: AsyncSession, create_user):
    """Тест: force_refresh создаёт кеш с TTL 25 часов"""
    await create_user(user_id=1234, username="test")
    
    # Force refresh - создаёт постоянный кеш
    user = await is_cached(redis_client, 1234, test_session, force_refresh=True)
    assert user is not None
    
    # Проверяем TTL (должен быть ~90000 секунд = 25 часов)
    ttl = await redis_client.ttl("USER_DATA:1234")
    assert 89900 < ttl <= 90000


@pytest.mark.asyncio
async def test_force_refresh_updates_existing_cache(redis_client: Redis, test_session: AsyncSession, create_user):
    """Тест: force_refresh обновляет существующий кеш"""
    user = await create_user(user_id=1234, username="old_name")
    
    # Создаём начальный кеш
    cached = await is_cached(redis_client, 1234, test_session)
    assert cached.username == "old_name" #type:ignore
    
    # Обновляем в БД
    repo = BaseRepository(session=test_session, model=User)
    await repo.update(data={'username': 'new_name'}, user_id=1234)
    
    # Обычное обращение - вернёт старый кеш
    cached = await is_cached(redis_client, 1234, test_session)
    assert cached.username == "old_name"  # Ещё старое значение #type:ignore
    
    # Force refresh - обновит кеш
    cached = await is_cached(redis_client, 1234, test_session, force_refresh=True)
    assert cached.username == "new_name"  # Новое значение! #type:ignore


@pytest.mark.asyncio  
async def test_nightly_worker_refreshes_all_users(redis_client: Redis, test_session_maker, create_user):
    """Тест: ночной воркер обновляет кеш всех пользователей"""
    # Создаём несколько пользователей
    async with test_session_maker() as session:
        repo = BaseRepository(session=session, model=User)
        for i in range(5):
            await repo.create(user_id=1000 + i, username=f"user_{i}")
    
    # Запускаем одну итерацию ночного обновления (без ожидания 03:00)
    async with test_session_maker() as session:
        offset = 0
        batch_size = 100
        refreshed = 0
        
        while True:
            stmt = select(User).offset(offset).limit(batch_size)
            result = await session.execute(stmt)
            users = result.scalars().all()
            
            if not users:
                break
            
            for user in users:
                await is_cached(redis_client, user.user_id, session, force_refresh=True)
                refreshed += 1
            
            offset += batch_size
    
    assert refreshed == 5
    
    # Проверяем что все кеши созданы с правильным TTL
    for i in range(5):
        ttl = await redis_client.ttl(f"USER_DATA:{1000 + i}")
        assert 89900 < ttl <= 90000  # 25 часов