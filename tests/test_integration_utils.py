import pytest
import json
import asyncio
from datetime import datetime, timedelta
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession
from db.models import User, UserLinks
from repositories.base import BaseRepository
from misc.utils import (
    trial_activation_worker,
    marzban_worker,
    db_worker,
    is_cached,
    worker_exsists
)
from unittest.mock import AsyncMock, patch, MagicMock


@pytest.mark.asyncio
async def test_full_trial_activation_flow(
    redis_client: Redis,
    test_session: AsyncSession,
    monkeypatch
):
    """
    Полный флоу активации триала:
    1. Пользователь запрашивает триал
    2. trial_activation_worker обрабатывает запрос
    3. Отправляет задачу в MARZBAN очередь
    4. marzban_worker создаёт пользователя в Marzban
    5. Отправляет задачи в DB очередь
    6. db_worker сохраняет в БД
    7. Проверяем финальное состояние
    """
    user_id = 12345
    
    # Mock Marzban API
    mock_marzban_response = {
        'subscription_url': 'https://dns1.example.com/sub/test',
        'expire': int((datetime.now() + timedelta(days=3)).timestamp())
    }
    
    async def mock_get_user(username):
        return 404  # Пользователь не существует
    
    async def mock_create(data):
        return mock_marzban_response
    
    # Патчим MarzbanClient
    with patch('misc.utils.MarzbanClient') as MockClient:
        mock_instance = MagicMock()
        mock_instance.__aenter__.return_value = mock_instance
        mock_instance.__aexit__.return_value = None
        mock_instance.get_user = mock_get_user
        mock_instance.create = mock_create
        mock_instance.modify = AsyncMock(return_value=mock_marzban_response)
        MockClient.return_value = mock_instance
        
        # 1. Отправляем запрос на активацию триала
        trial_data = {
            "user_id": user_id,
            "username": f"user_{user_id}"
        }
        
        await redis_client.lpush(
            "TRIAL_ACTIVATION",
            json.dumps(trial_data, sort_keys=True)
        ) #type: ignore
        
        # 2. Обрабатываем trial_activation_worker (один раз)
        task1 = asyncio.create_task(
            trial_activation_worker(redis_client, test_session)
        )
        
        # Даём время на обработку
        await asyncio.sleep(0.5)
        task1.cancel()
        
        try:
            await task1
        except asyncio.CancelledError:
            pass
        
        # 3. Проверяем что задача попала в MARZBAN очередь
        marzban_queue_size = await redis_client.llen("MARZBAN") #type: ignore
        assert marzban_queue_size == 1, "Задача не попала в MARZBAN очередь"
        
        # 4. Обрабатываем marzban_worker
        marzban_result = await marzban_worker(redis_client)
        
        assert marzban_result == mock_marzban_response
        
        # 5. Проверяем что задачи попали в DB очередь (User + UserLinks)
        db_queue_size = await redis_client.llen("DB") #type: ignore
        assert db_queue_size == 2, f"Expected 2 tasks in DB queue, got {db_queue_size}"
        
        # 6. Обрабатываем db_worker (два раза - для User и UserLinks)
        result1 = await db_worker(redis_client, test_session, process_once=True)
        result2 = await db_worker(redis_client, test_session, process_once=True)
        
        assert result1 in ['create', 'update']
        assert result2 in ['create', 'update']
        
        # 7. Проверяем финальное состояние в БД
        user_repo = BaseRepository(session=test_session, model=User)
        user = await user_repo.get_one(user_id=user_id)
        
        assert user is not None, "User не создан в БД"
        assert user.subscription_end is not None, "subscription_end не установлен"
        assert user.subscription_end > datetime.now(), "subscription_end в прошлом"
        
        links_repo = BaseRepository(session=test_session, model=UserLinks)
        links = await links_repo.get_one(user_id=user_id)
        
        assert links is not None, "UserLinks не создан в БД"
        assert links.panel1 == mock_marzban_response['subscription_url']


@pytest.mark.asyncio
async def test_subscription_extension_flow(
    redis_client: Redis,
    test_session: AsyncSession,
    create_user,
    monkeypatch
):
    """
    Флоу продления подписки:
    1. Пользователь уже существует с подпиской
    2. Приходит запрос на продление
    3. marzban_worker делает modify
    4. db_worker обновляет subscription_end
    5. Кеш пользователя обновляется
    """
    user_id = 12345
    current_expire = datetime.now() + timedelta(days=5)
    new_expire = datetime.now() + timedelta(days=35)
    
    # Создаём существующего пользователя
    await create_user(
        user_id=user_id,
        username="existing_user",
        subscription_end=current_expire,
        trial_used=True
    )
    
    # Mock Marzban API
    mock_marzban_response = {
        'subscription_url': 'https://dns1.example.com/sub/test',
        'expire': int(new_expire.timestamp())
    }
    
    with patch('misc.utils.MarzbanClient') as MockClient:
        mock_instance = MagicMock()
        mock_instance.__aenter__.return_value = mock_instance
        mock_instance.__aexit__.return_value = None
        mock_instance.modify = AsyncMock(return_value=mock_marzban_response)
        MockClient.return_value = mock_instance
        
        # 1. Отправляем запрос на продление в MARZBAN
        extend_data = {
            "type": "modify",
            "user_id": user_id,
            "expire": int(new_expire.timestamp())
        }
        
        await redis_client.lpush(
            "MARZBAN",
            json.dumps(extend_data, sort_keys=True, default=str)
        ) #type: ignore
        
        # 2. Обрабатываем marzban_worker
        marzban_result = await marzban_worker(redis_client)
        assert marzban_result == mock_marzban_response
        
        # 3. Обрабатываем db_worker для обновления User
        db_result = await db_worker(redis_client, test_session, process_once=True)
        assert db_result == 'update'
        
        # Обрабатываем UserLinks
        await db_worker(redis_client, test_session, process_once=True)
        
        # 4. Проверяем что подписка обновилась
        user_repo = BaseRepository(session=test_session, model=User)
        user = await user_repo.get_one(user_id=user_id)
        
        assert user.subscription_end is not None #type: ignore
        # Проверяем что новая дата больше старой
        assert user.subscription_end > current_expire #type: ignore
        
        # 5. Проверяем обновление кеша
        cached_user = await is_cached(redis_client, user_id, test_session, force_refresh=True)
        assert cached_user is not None
        assert cached_user.subscription_end > current_expire #type: ignore


@pytest.mark.asyncio
async def test_duplicate_prevention_flow(
    redis_client: Redis,
    test_session: AsyncSession
):
    """
    Флоу предотвращения дубликатов:
    1. Пользователь отправляет запрос
    2. worker_exsists проверяет очередь
    3. Если дубликат - не добавляем
    4. Если новый - добавляем
    """
    user_id = 12345
    
    task_data = {
        "user_id": user_id,
        "type": "create"
    }
    
    # 1. Добавляем первую задачу
    await redis_client.lpush(
        "TRIAL_ACTIVATION",
        json.dumps(task_data, sort_keys=True)
    ) #type: ignore
    
    # 2. Проверяем что задача существует
    exists = await worker_exsists(
        redis_cli=redis_client,
        worker="TRIAL_ACTIVATION",
        data=task_data
    )
    
    assert exists is True, "Задача должна существовать в очереди"
    
    # 3. Пытаемся добавить дубликат (как в реальном handler)
    if not exists:
        await redis_client.lpush(
            "TRIAL_ACTIVATION",
            json.dumps(task_data, sort_keys=True)
        ) #type: ignore
    
    # 4. Проверяем что в очереди только одна задача
    queue_size = await redis_client.llen("TRIAL_ACTIVATION") #type: ignore
    assert queue_size == 1, f"Expected 1 task, got {queue_size}"
    
    # 5. Добавляем задачу для другого пользователя
    other_task = {
        "user_id": 99999,
        "type": "create"
    }
    
    exists_other = await worker_exsists(
        redis_cli=redis_client,
        worker="TRIAL_ACTIVATION",
        data=other_task
    )
    
    assert exists_other is False, "Другая задача не должна существовать"
    
    # Добавляем её
    await redis_client.lpush(
        "TRIAL_ACTIVATION",
        json.dumps(other_task, sort_keys=True)
    ) #type: ignore
    
    # Проверяем что теперь 2 задачи
    queue_size = await redis_client.llen("TRIAL_ACTIVATION") #type: ignore
    assert queue_size == 2, f"Expected 2 tasks, got {queue_size}"


@pytest.mark.asyncio
async def test_create_to_update_conversion_flow(
    redis_client: Redis,
    test_session: AsyncSession,
    create_user
):
    """
    Флоу автоматической конвертации CREATE → UPDATE:
    1. Пользователь существует в БД
    2. Приходит CREATE запрос
    3. db_worker конвертирует в UPDATE
    4. Данные обновляются вместо ошибки
    """
    user_id = 12345
    
    # 1. Создаём существующего пользователя
    await create_user(
        user_id=user_id,
        username="old_name",
        subscription_end=datetime.now() + timedelta(days=5)
    )
    
    # 2. Отправляем CREATE с новыми данными
    create_task = {
        "model": "User",
        "type": "create",
        "user_id": user_id,
        "username": "new_name",
        "subscription_end": datetime.now() + timedelta(days=10)
    }
    
    await redis_client.lpush(
        "DB",
        json.dumps(create_task, sort_keys=True, default=str)
    ) #type: ignore
    
    # 3. Обрабатываем db_worker
    result = await db_worker(redis_client, test_session, process_once=True)
    
    # 4. Должен быть UPDATE, а не CREATE
    assert result == 'update', f"Expected 'update', got '{result}'"
    
    # 5. Проверяем что данные обновились
    user_repo = BaseRepository(session=test_session, model=User)
    user = await user_repo.get_one(user_id=user_id)
    
    assert user.username == "new_name", "Username не обновился" #type: ignore


@pytest.mark.asyncio
async def test_marzban_409_conflict_handling(
    redis_client: Redis,
    test_session: AsyncSession
):
    """
    Флоу обработки конфликта 409 от Marzban:
    1. Отправляем CREATE запрос
    2. Marzban возвращает 409 (пользователь существует)
    3. Автоматически повторяем как MODIFY
    4. Данные обновляются
    """
    user_id = 12345
    new_expire = datetime.now() + timedelta(days=30)
    
    mock_marzban_response = {
        'subscription_url': 'https://dns1.example.com/sub/test',
        'expire': int(new_expire.timestamp())
    }
    
    # Mock: первый вызов create возвращает 409, потом modify работает
    create_called = False
    
    async def mock_create(data):
        nonlocal create_called
        create_called = True
        return 409  # Конфликт
    
    async def mock_modify(**kwargs):
        return mock_marzban_response
    
    with patch('misc.utils.MarzbanClient') as MockClient:
        mock_instance = MagicMock()
        mock_instance.__aenter__.return_value = mock_instance
        mock_instance.__aexit__.return_value = None
        mock_instance.create = mock_create
        mock_instance.modify = mock_modify
        MockClient.return_value = mock_instance
        
        # Отправляем CREATE запрос
        create_task = {
            "type": "create",
            "user_id": user_id,
            "expire": int(new_expire.timestamp())
        }
        
        await redis_client.lpush(
            "MARZBAN",
            json.dumps(create_task, sort_keys=True, default=str)
        ) #type: ignore
        
        # Обрабатываем
        result = await marzban_worker(redis_client)
        
        # Проверяем что create был вызван
        assert create_called is True, "create не был вызван"
        
        # Проверяем что modify сработал и вернул правильный результат
        assert result == mock_marzban_response, "modify не сработал после 409"
        
        # Проверяем что задачи попали в DB с типом UPDATE
        db_tasks = []
        while True:
            task = await redis_client.rpop("DB") #type: ignore
            if not task:
                break
            db_tasks.append(json.loads(task)) #type: ignore
        
        assert len(db_tasks) == 2, f"Expected 2 DB tasks, got {len(db_tasks)}"
        
        # Обе задачи должны быть UPDATE (после конвертации)
        for task in db_tasks:
            assert task['type'] == 'update', f"Expected type='update', got '{task['type']}'"


@pytest.mark.asyncio
async def test_cache_update_after_subscription_change(
    redis_client: Redis,
    test_session: AsyncSession,
    create_user
):
    """
    Флоу обновления кеша после изменения подписки:
    1. Пользователь в кеше с старой датой
    2. Подписка продлевается через воркеры
    3. Кеш обновляется через force_refresh
    4. Пользователь получает актуальные данные
    """
    user_id = 12345
    old_expire = datetime.now() + timedelta(days=5)
    new_expire = datetime.now() + timedelta(days=35)
    
    # 1. Создаём пользователя
    await create_user(
        user_id=user_id,
        username="test_user",
        subscription_end=old_expire
    )
    
    # 2. Кешируем данные
    cached = await is_cached(redis_client, user_id, test_session)
    assert cached is not None
    assert cached.subscription_end.date() == old_expire.date() #type: ignore
    
    # 3. Обновляем подписку в БД (имитация работы воркеров)
    user_repo = BaseRepository(session=test_session, model=User)
    await user_repo.update(
        data={'subscription_end': new_expire},
        user_id=user_id
    )
    
    # 4. Обычное обращение - вернёт старый кеш
    cached_old = await is_cached(redis_client, user_id, test_session)
    assert cached_old.subscription_end.date() == old_expire.date() #type: ignore
    
    # 5. Force refresh - обновит кеш
    cached_new = await is_cached(redis_client, user_id, test_session, force_refresh=True)
    assert cached_new.subscription_end.date() == new_expire.date() #type: ignore
    
    # 6. Теперь обычное обращение вернёт новый кеш
    cached_final = await is_cached(redis_client, user_id, test_session)
    assert cached_final.subscription_end.date() == new_expire.date() #type: ignore


@pytest.mark.asyncio
async def test_concurrent_workers_same_task(
    redis_client: Redis,
    test_session: AsyncSession
):
    """
    Флоу: несколько воркеров конкурируют за одну задачу
    1. Один воркер получает задачу (brpop)
    2. Другие воркеры ждут следующей задачи
    3. Только один воркер обрабатывает задачу
    """
    user_id = 12345
    
    task_data = {
        "model": "User",
        "type": "create",
        "user_id": user_id,
        "username": "test"
    }
    
    # Добавляем одну задачу
    await redis_client.lpush(
        "DB",
        json.dumps(task_data, sort_keys=True, default=str)
    ) #type: ignore
    
    # Запускаем 3 воркера параллельно
    results = []
    
    async def worker_wrapper():
        result = await db_worker(redis_client, test_session, process_once=True)
        return result
    
    tasks = [
        asyncio.create_task(worker_wrapper()),
        asyncio.create_task(worker_wrapper()),
        asyncio.create_task(worker_wrapper())
    ]
    
    # Даём время на обработку
    await asyncio.sleep(1)
    
    # Собираем результаты
    for task in tasks:
        if not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                results.append(None)
        else:
            results.append(task.result())
    
    # Только один воркер должен обработать задачу
    processed = [r for r in results if r is not None]
    assert len(processed) == 1, f"Expected 1 worker to process, got {len(processed)}"
    assert processed[0] == 'create'
    
    # Проверяем что пользователь создан один раз
    user_repo = BaseRepository(session=test_session, model=User)
    user = await user_repo.get_one(user_id=user_id)
    assert user is not None


@pytest.mark.asyncio
async def test_error_retry_flow(
    redis_client: Redis,
    test_session: AsyncSession,
    monkeypatch
):
    """
    Флоу обработки ошибок с retry:
    1. Задача вызывает ошибку
    2. Задача возвращается в очередь
    3. При повторной попытке успешно обрабатывается
    """
    user_id = 12345
    
    task_data = {
        "model": "User",
        "type": "create",
        "user_id": user_id,
        "username": "test"
    }
    
    await redis_client.lpush(
        "DB",
        json.dumps(task_data, sort_keys=True, default=str)
    ) #type: ignore
    
    # Mock: первый вызов выдаёт ошибку, второй работает
    attempt_count = 0
    original_create = BaseRepository.create
    
    async def failing_create(self, **data):
        nonlocal attempt_count
        attempt_count += 1
        if attempt_count == 1:
            raise Exception("Temporary DB error")
        return await original_create(self, **data)
    
    monkeypatch.setattr(BaseRepository, "create", failing_create)
    
    # Первая попытка - ошибка
    with pytest.raises(Exception, match="Temporary DB error"):
        await db_worker(redis_client, test_session, process_once=True)
    
    # Проверяем что задача вернулась в очередь
    queue_size = await redis_client.llen("DB") #type: ignore
    assert queue_size == 1, "Задача не вернулась в очередь после ошибки"
    
    # Вторая попытка - успех
    result = await db_worker(redis_client, test_session, process_once=True)
    assert result == 'create'
    
    # Проверяем что пользователь создан
    user_repo = BaseRepository(session=test_session, model=User)
    user = await user_repo.get_one(user_id=user_id)
    assert user is not None
    assert attempt_count == 2