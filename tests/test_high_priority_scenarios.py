"""
Тесты для сценариев высокого приоритета:
1. Latency explosion при росте очередей
2. Zombie tasks при крэше воркера
3. Data inconsistency при partial failure
4. Redis memory limits
"""

import pytest
import json
import asyncio
from datetime import datetime, timedelta
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession
from db.models import User, UserLinks
from repositories.base import BaseRepository
from misc.utils import worker_exsists, db_worker, marzban_worker
from unittest.mock import AsyncMock, patch, MagicMock
import time


# ============================================================================
# 1. LATENCY EXPLOSION - Тесты производительности дедупликации
# ============================================================================

@pytest.mark.asyncio
async def test_worker_exists_performance_degradation(redis_client: Redis):
    """
    Тест: worker_exsists деградирует при росте очереди
    
    Демонстрирует O(N) сложность текущей реализации
    """
    queue = "TEST_QUEUE"
    
    # Заполняем очередь разными задачами
    for i in range(1000):
        task = {
            "user_id": 10000 + i,
            "type": "test"
        }
        await redis_client.lpush(queue, json.dumps(task, sort_keys=True)) #type: ignore
    
    # Проверяем задачу которой НЕТ в очереди (worst case)
    search_task = {
        "user_id": 99999,
        "type": "test"
    }
    
    # Замеряем время
    start = time.time()
    exists = await worker_exsists(redis_client, queue, search_task)
    elapsed = time.time() - start
    
    print(f"\n⏱️  Time to check in 1000 items: {elapsed*1000:.2f}ms")
    
    assert exists is False
    # При 1000 элементов ожидаем > 50ms
    assert elapsed > 0.05, f"Too fast: {elapsed*1000:.2f}ms - возможно кеширование"


@pytest.mark.asyncio
async def test_worker_exists_scales_linearly(redis_client: Redis):
    """
    Тест: worker_exsists масштабируется линейно O(N)
    
    При удвоении размера очереди время удваивается
    """
    queue = "TEST_QUEUE"
    
    times = []
    
    for queue_size in [100, 500, 1000]:
        # Очищаем
        await redis_client.delete(queue)
        
        # Заполняем
        for i in range(queue_size):
            task = {"user_id": 10000 + i, "type": "test"}
            await redis_client.lpush(queue, json.dumps(task, sort_keys=True)) #type: ignore
        
        # Ищем несуществующую задачу
        search_task = {"user_id": 99999, "type": "test"}
        
        start = time.time()
        await worker_exsists(redis_client, queue, search_task)
        elapsed = time.time() - start
        
        times.append(elapsed)
        print(f"\n📊 Queue size {queue_size}: {elapsed*1000:.2f}ms")
    
    # Проверяем линейный рост (с погрешностью)
    # time(1000) / time(100) ≈ 10
    ratio = times[2] / times[0]
    print(f"\n📈 Growth ratio (1000/100): {ratio:.2f}x")
    
    assert 5 < ratio < 15, f"Expected ~10x growth, got {ratio:.2f}x"


@pytest.mark.asyncio
async def test_concurrent_duplicate_checks_create_bottleneck(redis_client: Redis):
    """
    Тест: множественные параллельные проверки создают bottleneck
    
    100 одновременных проверок в очереди из 1000 элементов
    """
    queue = "TEST_QUEUE"
    
    # Заполняем очередь
    for i in range(1000):
        task = {"user_id": 10000 + i, "type": "test"}
        await redis_client.lpush(queue, json.dumps(task, sort_keys=True)) #type: ignore
    
    # 100 параллельных проверок
    async def check_task(user_id: int):
        task = {"user_id": user_id, "type": "test"}
        start = time.time()
        await worker_exsists(redis_client, queue, task)
        return time.time() - start
    
    start_total = time.time()
    tasks = [check_task(90000 + i) for i in range(100)]
    results = await asyncio.gather(*tasks)
    total_time = time.time() - start_total
    
    avg_time = sum(results) / len(results)
    
    print(f"\n⏱️  100 concurrent checks:")
    print(f"   Total time: {total_time:.2f}s")
    print(f"   Avg per check: {avg_time*1000:.2f}ms")
    print(f"   Throughput: {100/total_time:.2f} checks/sec")
    
    # При O(N) сложности throughput будет низкий
    assert total_time > 1.0, "Should take >1 second for 100 checks in 1000 items"


# ============================================================================
# 2. ZOMBIE TASKS - Тесты потери задач при крэше
# ============================================================================

@pytest.mark.asyncio
async def test_task_lost_on_worker_crash(redis_client: Redis, test_session: AsyncSession):
    """
    Тест: задача теряется при крэше воркера после brpop
    
    Демонстрирует потерю данных без processing queue
    """
    queue = "DB"
    
    task_data = {
        "model": "User",
        "type": "create",
        "user_id": 12345,
        "username": "test_user"
    }
    
    # Добавляем задачу
    await redis_client.lpush(queue, json.dumps(task_data, sort_keys=True, default=str)) #type: ignore
    
    # Проверяем что задача в очереди
    queue_size = await redis_client.llen(queue) #type: ignore
    assert queue_size == 1
    
    # Извлекаем задачу (имитация brpop)
    result = await redis_client.brpop(queue, timeout=1) #type: ignore
    assert result is not None
    
    # Проверяем что задача больше не в очереди
    queue_size = await redis_client.llen(queue) #type: ignore
    assert queue_size == 0
    
    # ❌ Имитация крэша - задача не обработана и не вернулась в очередь
    # В реальности здесь был бы крэш процесса
    
    # Проверяем что задача потеряна
    queue_size = await redis_client.llen(queue) #type: ignore
    assert queue_size == 0, "Task is lost forever"
    
    # Проверяем что User НЕ создан в БД
    repo = BaseRepository(session=test_session, model=User)
    user = await repo.get_one(user_id=12345)
    assert user is None, "User should not exist (task was lost)"
    
    print("\n💥 Task lost on crash - no recovery mechanism")


@pytest.mark.asyncio
async def test_processing_queue_prevents_task_loss(redis_client: Redis):
    """
    Тест: processing queue защищает от потери задач
    
    Демонстрирует как RPOPLPUSH предотвращает потерю данных
    """
    queue = "DB"
    processing_queue = "DB:processing"
    
    task_data = {
        "model": "User",
        "type": "create",
        "user_id": 12345
    }
    task_json = json.dumps(task_data, sort_keys=True, default=str)
    
    # Добавляем задачу
    await redis_client.lpush(queue, task_json) #type: ignore
    
    # ✅ Используем RPOPLPUSH вместо BRPOP
    message = await redis_client.brpoplpush(queue, processing_queue, timeout=1) #type: ignore
    assert message is not None
    
    # Задача переместилась из queue в processing_queue
    main_queue_size = await redis_client.llen(queue) #type: ignore
    processing_queue_size = await redis_client.llen(processing_queue) #type: ignore
    
    assert main_queue_size == 0, "Task moved from main queue"
    assert processing_queue_size == 1, "Task now in processing queue"
    
    # ❌ Имитация крэша
    # Задача осталась в processing_queue - можем восстановить!
    
    # Recovery: перемещаем обратно
    recovered = await redis_client.rpoplpush(processing_queue, queue) #type: ignore
    assert recovered == message
    
    # Проверяем восстановление
    main_queue_size = await redis_client.llen(queue) #type: ignore
    processing_queue_size = await redis_client.llen(processing_queue) #type: ignore
    
    assert main_queue_size == 1, "Task recovered to main queue"
    assert processing_queue_size == 0, "Processing queue is clean"
    
    print("\n✅ Task recovered after crash using processing queue")


@pytest.mark.asyncio
async def test_zombie_task_detection_and_recovery(redis_client: Redis):
    """
    Тест: обнаружение и восстановление зависших задач через TTL
    """
    queue = "DB"
    processing_queue = "DB:processing"
    
    task_json = json.dumps({"model": "User", "type": "create", "user_id": 12345})
    
    # Перемещаем в processing
    await redis_client.lpush(queue, task_json) #type: ignore
    await redis_client.brpoplpush(queue, processing_queue, timeout=1) #type: ignore
    
    # Устанавливаем TTL маркер (короткий для теста)
    task_id = f"{processing_queue}:{task_json}"
    await redis_client.setex(task_id, 2, "1")  # 2 секунды
    
    # Проверяем что маркер существует
    exists = await redis_client.exists(task_id)
    assert exists == 1
    
    # Ждём истечения TTL
    await asyncio.sleep(3)
    
    # TTL истёк - задача "зависла"
    exists = await redis_client.exists(task_id)
    assert exists == 0, "TTL expired - task is zombie"
    
    # Cleanup worker обнаруживает и восстанавливает
    tasks_in_processing = await redis_client.lrange(processing_queue, 0, -1) #type: ignore
    
    for task in tasks_in_processing:
        task_marker = f"{processing_queue}:{task}"
        marker_exists = await redis_client.exists(task_marker)
        
        if not marker_exists:
            # Зависшая задача - восстанавливаем
            await redis_client.lrem(processing_queue, 1, task) #type: ignore
            await redis_client.lpush(queue, task) #type: ignore
            print(f"\n♻️ Recovered zombie task")
    
    # Проверяем восстановление
    main_queue_size = await redis_client.llen(queue) #type: ignore
    processing_queue_size = await redis_client.llen(processing_queue) #type: ignore
    
    assert main_queue_size == 1, "Zombie task recovered"
    assert processing_queue_size == 0, "Processing queue cleaned"


@pytest.mark.asyncio
async def test_multiple_worker_crashes_preserve_tasks(redis_client: Redis):
    """
    Тест: несколько крэшей воркеров не приводят к потере задач
    """
    queue = "DB"
    processing_queue = "DB:processing"
    
    # Добавляем 10 задач
    for i in range(10):
        task = {"model": "User", "type": "create", "user_id": 10000 + i}
        await redis_client.lpush(queue, json.dumps(task, sort_keys=True)) #type: ignore
    
    # Воркер 1: обрабатывает 3 задачи и крашится
    for _ in range(3):
        await redis_client.brpoplpush(queue, processing_queue, timeout=1) #type: ignore
    
    # Воркер 2: обрабатывает 2 задачи и крашится
    for _ in range(2):
        await redis_client.brpoplpush(queue, processing_queue, timeout=1) #type: ignore
    
    # Состояние после крэшей
    main_queue_size = await redis_client.llen(queue) #type: ignore
    processing_queue_size = await redis_client.llen(processing_queue) #type: ignore
    
    print(f"\n📊 After crashes:")
    print(f"   Main queue: {main_queue_size}")
    print(f"   Processing queue: {processing_queue_size}")
    
    assert main_queue_size == 5, "5 tasks not yet processed"
    assert processing_queue_size == 5, "5 tasks stuck in processing"
    
    # Recovery: возвращаем все из processing в main
    while True:
        task = await redis_client.rpoplpush(processing_queue, queue) #type: ignore
        if not task:
            break
    
    # Проверяем полное восстановление
    main_queue_size = await redis_client.llen(queue) #type: ignore
    processing_queue_size = await redis_client.llen(processing_queue) #type: ignore
    
    assert main_queue_size == 10, "All 10 tasks recovered"
    assert processing_queue_size == 0, "Processing queue empty"
    
    print(f"✅ All {main_queue_size} tasks recovered - zero data loss")


# ============================================================================
# 3. PARTIAL FAILURES - Тесты несогласованности данных
# ============================================================================

@pytest.mark.asyncio
async def test_partial_failure_creates_inconsistency(
    redis_client: Redis,
    test_session: AsyncSession,
    monkeypatch
):
    """
    Тест: частичный сбой создаёт несогласованность между User и UserLinks
    
    User создан, но UserLinks нет → subscription_url потерян
    """
    user_id = 12345
    
    # Mock Marzban API
    mock_response = {
        'subscription_url': 'https://dns1.example.com/sub/test',
        'expire': int((datetime.now() + timedelta(days=30)).timestamp())
    }
    
    # Счётчик lpush вызовов
    lpush_count = 0
    original_lpush = redis_client.lpush
    
    async def failing_lpush(*args, **kwargs):
        nonlocal lpush_count
        lpush_count += 1
        
        # Второй lpush (UserLinks) падает
        if lpush_count == 2:
            raise ConnectionError("Redis connection lost")
        
        return await original_lpush(*args, **kwargs) #type: ignore
    
    monkeypatch.setattr(redis_client, "lpush", failing_lpush)
    
    with patch('misc.utils.MarzbanClient') as MockClient:
        mock_instance = MagicMock()
        mock_instance.__aenter__.return_value = mock_instance
        mock_instance.__aexit__.return_value = None
        mock_instance.create = AsyncMock(return_value=mock_response)
        MockClient.return_value = mock_instance
        
        # Отправляем задачу
        task_data = {
            "type": "create",
            "user_id": user_id,
            "expire": mock_response['expire']
        }
        
        await redis_client.lpush("MARZBAN", json.dumps(task_data, sort_keys=True, default=str)) #type: ignore
        
        # Обрабатываем - должен упасть на втором lpush
        with pytest.raises(ConnectionError):
            await marzban_worker(redis_client)
    
    # Проверяем что первый lpush (User) прошёл
    db_queue_size = await redis_client.llen("DB") #type: ignore
    assert db_queue_size == 1, "Only User task made it to queue"
    
    # Обрабатываем User
    await db_worker(redis_client, test_session, process_once=True)
    
    # Проверяем несогласованность
    user_repo = BaseRepository(session=test_session, model=User)
    links_repo = BaseRepository(session=test_session, model=UserLinks)
    
    user = await user_repo.get_one(user_id=user_id)
    links = await links_repo.get_one(user_id=user_id)
    
    assert user is not None, "User created"
    assert links is None, "❌ UserLinks NOT created - subscription_url lost!"
    
    print("\n💥 Data inconsistency: User exists but UserLinks missing")
    print(f"   User: {user.user_id}")
    print(f"   Links: None")
    print(f"   subscription_url: LOST")


@pytest.mark.asyncio
async def test_transactional_outbox_prevents_inconsistency(redis_client: Redis):
    """
    Тест: transactional outbox предотвращает потерю данных при partial failure
    
    Все задачи сохраняются атомарно перед отправкой
    """
    transaction_id = "tx_12345"
    
    # Подготавливаем транзакцию
    transaction = {
        "id": transaction_id,
        "marzban_result": {"subscription_url": "https://example.com"},
        "tasks": [
            {"model": "User", "type": "create", "user_id": 12345},
            {"model": "UserLinks", "type": "create", "user_id": 12345}
        ]
    }
    
    # Сохраняем транзакцию атомарно
    tx_key = f"transaction:{transaction_id}"
    await redis_client.setex(
        tx_key,
        3600,
        json.dumps(transaction, default=str)
    )
    
    # Пытаемся отправить задачи (может упасть на второй)
    sent_count = 0
    try:
        for task in transaction["tasks"]:
            await redis_client.lpush("DB", json.dumps(task, default=str)) #type: ignore
            sent_count += 1
            
            if sent_count == 1:
                # Имитация крэша после первой задачи
                raise ConnectionError("Crash after first lpush")
    except ConnectionError:
        pass
    
    # Проверяем что отправлена только одна задача
    queue_size = await redis_client.llen("DB") #type: ignore
    assert queue_size == 1, "Only 1 task sent before crash"
    
    # ✅ Но транзакция сохранена - можем восстановить!
    tx_data = await redis_client.get(tx_key)
    assert tx_data is not None, "Transaction preserved"
    
    saved_tx = json.loads(tx_data)
    assert len(saved_tx["tasks"]) == 2, "All tasks recorded"
    
    # Recovery: проверяем committed статус
    committed = await redis_client.exists(f"{tx_key}:committed")
    
    if not committed:
        # Транзакция не закоммичена - повторяем отправку ВСЕХ задач
        print(f"\n♻️ Recovering incomplete transaction {transaction_id}")
        
        for task in saved_tx["tasks"]:
            await redis_client.lpush("DB", json.dumps(task, default=str)) #type: ignore
        
        # Коммитим
        await redis_client.setex(f"{tx_key}:committed", 3600, "1")
    
    # Проверяем полное восстановление
    # Первая задача была отправлена дважды, но это OK (идемпотентность)
    queue_size = await redis_client.llen("DB") #type: ignore
    assert queue_size >= 2, "All tasks in queue after recovery"
    
    print(f"✅ Transaction recovered: {queue_size} tasks in queue")


@pytest.mark.asyncio
async def test_marzban_success_but_db_tasks_lost(
    redis_client: Redis,
    test_session: AsyncSession
):
    """
    Тест: пользователь создан в Marzban, но DB задачи потеряны
    
    Worst case: деньги списаны, Marzban создан, но БД не обновлена
    """
    user_id = 12345
    
    mock_response = {
        'subscription_url': 'https://dns1.example.com/sub/test',
        'expire': int((datetime.now() + timedelta(days=30)).timestamp())
    }
    
    with patch('misc.utils.MarzbanClient') as MockClient:
        mock_instance = MagicMock()
        mock_instance.__aenter__.return_value = mock_instance
        mock_instance.__aexit__.return_value = None
        mock_instance.create = AsyncMock(return_value=mock_response)
        MockClient.return_value = mock_instance
        
        # Отправляем задачу
        task_data = {
            "type": "create",
            "user_id": user_id,
            "expire": mock_response['expire']
        }
        
        await redis_client.lpush("MARZBAN", json.dumps(task_data, sort_keys=True, default=str)) #type: ignore
        
        # Обрабатываем Marzban
        result = await marzban_worker(redis_client) #type: ignore
        assert result == mock_response, "Marzban success"
    
    # ❌ Имитация: Redis падает ПОСЛЕ marzban_worker
    # DB задачи потеряны (не записались в очередь или Redis упал)
    await redis_client.flushdb()
    
    # Проверяем последствия
    db_queue_size = await redis_client.llen("DB") #type: ignore
    assert db_queue_size == 0, "DB queue is empty - tasks lost"
    
    # Marzban создан (в реальности), но БД не обновлена
    user_repo = BaseRepository(session=test_session, model=User)
    user = await user_repo.get_one(user_id=user_id)
    
    assert user is None, "User NOT in database"
    
    print("\n💥 Critical inconsistency:")
    print(f"   Marzban: ✅ User created")
    print(f"   Database: ❌ User missing")
    print(f"   subscription_url: {mock_response['subscription_url']}")
    print(f"   User cannot access - support ticket incoming!")


# ============================================================================
# 4. REDIS MEMORY LIMITS - Тесты переполнения памяти
# ============================================================================

@pytest.mark.asyncio
async def test_queue_growth_without_limit(redis_client: Redis):
    """
    Тест: неограниченный рост очереди при недоступности воркера
    
    Демонстрирует накопление задач когда воркер не обрабатывает
    """
    queue = "DB"
    
    # Имитация: воркер недоступен, но задачи продолжают поступать
    for i in range(1000):
        task = {
            "model": "User",
            "type": "create",
            "user_id": 10000 + i
        }
        await redis_client.lpush(queue, json.dumps(task, sort_keys=True)) #type: ignore
    
    # Проверяем размер
    queue_size = await redis_client.llen(queue) #type: ignore
    memory_info = await redis_client.info('memory')
    
    used_memory = memory_info.get('used_memory_human', 'unknown')
    
    print(f"\n📊 Queue statistics:")
    print(f"   Size: {queue_size} tasks")
    print(f"   Memory: {used_memory}")
    
    assert queue_size == 1000
    
    # Если каждая задача ~500 байт, 1000 задач = ~500KB
    # При 1,000,000 задач = ~500MB
    # Без maxmemory-policy Redis может упасть!
    
    print(f"\n⚠️  Without limits, 1M tasks would use ~500MB")
    print(f"   Current: {queue_size} tasks = ~{queue_size * 500 / 1024:.2f}KB")


@pytest.mark.asyncio
async def test_dedup_set_grows_unbounded_without_ttl(redis_client: Redis):
    """
    Тест: SET для дедупликации растёт бесконечно без TTL
    
    Каждая задача добавляется в SET и остаётся там навсегда
    """
    dedup_set = "QUEUE:dedup"
    
    # Добавляем 10,000 уникальных задач
    for i in range(10000):
        task = json.dumps({"user_id": 10000 + i, "type": "create"}, sort_keys=True)
        await redis_client.sadd(dedup_set, task) #type: ignore
    
    # Проверяем размер
    set_size = await redis_client.scard(dedup_set) #type: ignore
    
    print(f"\n📊 Dedup SET size: {set_size} items")
    
    # Проверяем TTL
    ttl = await redis_client.ttl(dedup_set)
    print(f"   TTL: {ttl} seconds")
    
    if ttl == -1:
        print(f"\n⚠️  WARNING: No TTL - set will grow forever!")
        print(f"   Memory leak: each task stays in SET permanently")
        print(f"   After 1M tasks: ~50MB of memory never freed")
    
    assert set_size == 10000


@pytest.mark.asyncio
async def test_memory_leak_from_failed_tasks(redis_client: Redis):
    """
    Тест: утечка памяти от постоянно падающих задач
    
    Задача падает → возвращается в очередь → падает снова → цикл
    """
    queue = "DB"
    
    # Задача которая всегда будет падать
    poison_task = {
        "model": "InvalidModel",  # Не существует в MODEL_REGISTRY
        "type": "create",
        "user_id": 12345
    }
    
    # Добавляем poison task
    await redis_client.lpush(queue, json.dumps(poison_task, sort_keys=True)) #type: ignore
    
    # Имитация: воркер пытается обработать и возвращает в очередь
    for attempt in range(100):
        # Берём задачу
        result = await redis_client.brpop(queue, timeout=1) #type: ignore
        if not result:
            break
        
        _, message = result
        
        # "Обработка" падает
        # Возвращаем в очередь
        await redis_client.lpush(queue, message) #type: ignore
    
    # Проверяем что задача всё ещё в очереди
    queue_size = await redis_client.llen(queue) #type: ignore
    assert queue_size == 1, "Poison task still in queue"
    
    print(f"\n💥 Poison task processed {100} times")
    print(f"   Still in queue: {queue_size}")
    print(f"   Without max_retries: infinite loop + log spam")
    
    # В реальности: нужен счётчик попыток и Dead Letter Queue


@pytest.mark.asyncio  
async def test_massive_duplicate_detection_memory_spike(redis_client: Redis):
    """
    Тест: множественные одновременные проверки дубликатов создают memory spike
    
    100 воркеров × LRANGE(10000 элементов) = 1MB × 100 = 100MB spike
    """
    queue = "TEST_QUEUE"
    
    # Заполняем очередь
    for i in range(10000):
        task = {"user_id": 10000 + i, "type": "test"}
        await redis_client.lpush(queue, json.dumps(task, sort_keys=True)) #type: ignore
    
    # Имитация: 100 одновременных проверок
    async def check_duplicate():
        # worker_exsists делает LRANGE
        all_items = await redis_client.lrange(queue, 0, -1) #type: ignore
        # В памяти Python: список из 10000 элементов × ~500 байт = ~5MB
        return len(all_items)
    
    memory_before = await redis_client.info('memory')
    
    # 100 параллельных вызовов
    tasks = [check_duplicate() for _ in range(100)]
    await asyncio.gather(*tasks)
    
    memory_after = await redis_client.info('memory')
    
    print(f"\n📊 Memory spike from duplicate checks:")
    print(f"   Before: {memory_before.get('used_memory_human')}")
    print(f"   After: {memory_after.get('used_memory_human')}")
    print(f"   100 × 10K items = 100 × 5MB = 500MB potential spike")


# ============================================================================
# SUMMARY TEST - Комплексный сценарий
# ============================================================================

@pytest.mark.asyncio
async def test_cascading_failure_scenario(
    redis_client: Redis,
    test_session: AsyncSession
):
    """
    Комплексный тест: каскадный отказ системы
    
    1. Очередь растёт (воркер медленный)
    2. Проверки дубликатов замедляются
    3. Пользователи retry → ещё больше дубликатов
    4. Memory spike
    5. Redis OOM
    """
    queue = "TRIAL_ACTIVATION"
    
    # Этап 1: Накопление задач (воркер медленный/недоступен)
    print("\n📊 Stage 1: Queue accumulation")
    for i in range(5000):
        task = {"user_id": 10000 + i, "type": "trial"}
        await redis_client.lpush(queue, json.dumps(task, sort_keys=True)) #type: ignore
    
    queue_size = await redis_client.llen(queue) #type: ignore
    print(f"   Queue size: {queue_size}")
    
    # Этап 2: Проверка дубликатов замедляется
    print(f"\n📊 Stage 2: Duplicate checking slowdown")
    start = time.time()
    test_task = {"user_id": 99999, "type": "trial"}
    await worker_exsists(redis_client, queue, test_task)
    elapsed = time.time() - start
    print(f"   Check time: {elapsed*1000:.2f}ms (would be ~50ms with 1000 items)")
    
    # Этап 3: Пользователи retry (дубликаты не детектятся вовремя)
    print(f"\n📊 Stage 3: User retries add duplicates")
    # 100 пользователей × 5 retry = 500 дубликатов
    for i in range(100):
        for _ in range(5):
            task = {"user_id": 20000 + i, "type": "trial"}
            await redis_client.lpush(queue, json.dumps(task, sort_keys=True)) #type: ignore
    
    queue_size = await redis_client.llen(queue) #type: ignore
    print(f"   Queue size: {queue_size} (+500 duplicates)")
    
    # Этап 4: Memory spike от массовых проверок
    print(f"\n📊 Stage 4: Memory spike from concurrent checks")
    memory_info = await redis_client.info('memory')
    used_memory = memory_info.get('used_memory', 0)
    print(f"   Memory: {memory_info.get('used_memory_human')}")
    
    # Этап 5: Critical state
    print(f"\n💥 System state: CRITICAL")
    print(f"   Queue: {queue_size} tasks")
    print(f"   Duplicates: ~500 (9%)")
    print(f"   Check latency: {elapsed*1000:.2f}ms")
    print(f"   Memory: {memory_info.get('used_memory_human')}")
    print(f"\n   Without protection:")
    print(f"   - Redis OOM risk")
    print(f"   - User experience degraded")
    print(f"   - Data loss on crash")
    print(f"   - Manual intervention required")
    
    # Assertion: система в критическом состоянии
    assert queue_size > 5000
    assert elapsed > 0.1  # Проверка дубликатов замедлилась


if __name__ == "__main__":
    print("Run with: pytest tests/test_high_priority_scenarios.py -v -s")