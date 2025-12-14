import pytest
import json
from datetime import datetime, timedelta
from redis.asyncio import Redis
from litestar.testing import AsyncTestClient


@pytest.mark.asyncio
async def test_marzban_webhook_user_created(
    test_client: AsyncTestClient,
    redis_client: Redis
):
    """
    Тест: webhook от Marzban при создании пользователя
    """
    username = "test_user_123"
    
    webhook_data = [
        {
            "username": username,
            "action": "user_created",
            "expire": int((datetime.now() + timedelta(days=30)).timestamp()),
            "proxies": {
                "vless": {
                    "id": "abc-123-def-456"
                }
            }
        }
    ]
    
    # AsyncTestClient синхронный, но можно использовать в async тесте
    response = await test_client.post("/marzban", json=webhook_data)
    
    assert response.status_code == 200
    assert response.json() == {"ok": True}
    
    # Redis операции async
    queue_size = await redis_client.llen("MARZBAN") #type: ignore
    assert queue_size == 1
    
    task_json = await redis_client.rpop("MARZBAN") #type: ignore
    task = json.loads(task_json) #type: ignore
    
    assert task["user_id"] == username
    assert "expire" in task
    assert task["id"] == "abc-123-def-456"
    
    cache_key = f"marzban:{username}:user_created"
    exists = await redis_client.exists(cache_key)
    assert exists == 1


@pytest.mark.asyncio
async def test_marzban_webhook_user_updated(
    test_client: AsyncTestClient,
    redis_client: Redis
):
    """
    Тест: webhook от Marzban при обновлении пользователя
    """
    username = "test_user_456"
    
    webhook_data = [
        {
            "username": username,
            "action": "user_updated",
            "expire": int((datetime.now() + timedelta(days=60)).timestamp())
        }
    ]
    
    response = await test_client.post("/marzban", json=webhook_data)
    
    assert response.status_code == 200
    assert response.json() == {"ok": True}
    
    task_json = await redis_client.rpop("MARZBAN") #type: ignore
    task = json.loads(task_json) #type: ignore
    
    assert task["user_id"] == username
    assert "expire" in task
    assert "id" not in task


@pytest.mark.asyncio
async def test_marzban_webhook_duplicate_prevention(
    test_client: AsyncTestClient,
    redis_client: Redis
):
    """
    Тест: дедупликация webhooks
    """
    username = "duplicate_test_user"
    
    webhook_data = [
        {
            "username": username,
            "action": "user_created",
            "expire": int((datetime.now() + timedelta(days=30)).timestamp()),
            "proxies": {"vless": {"id": "test-id"}}
        }
    ]
    
    # Первый запрос
    response1 = await test_client.post("/marzban", json=webhook_data)
    assert response1.status_code == 200
    assert response1.json() == {"ok": True}
    
    queue_size = await redis_client.llen("MARZBAN") #type: ignore
    assert queue_size == 1
    
    # Второй запрос (дубликат)
    response2 = await test_client.post("/marzban", json=webhook_data)
    assert response2.status_code == 200
    assert response2.json() == {'msg': 'operation for user been'}
    
    queue_size = await redis_client.llen("MARZBAN") #type: ignore
    assert queue_size == 1


@pytest.mark.asyncio
async def test_marzban_webhook_different_actions(
    test_client: AsyncTestClient,
    redis_client: Redis
):
    """
    Тест: разные actions для одного пользователя
    """
    username = "multi_action_user"
    
    webhook_created = [
        {
            "username": username,
            "action": "user_created",
            "expire": int((datetime.now() + timedelta(days=30)).timestamp()),
            "proxies": {"vless": {"id": "id-123"}}
        }
    ]
    
    response1 = await test_client.post("/marzban", json=webhook_created)
    assert response1.status_code == 200
    
    webhook_updated = [
        {
            "username": username,
            "action": "user_updated",
            "expire": int((datetime.now() + timedelta(days=60)).timestamp())
        }
    ]
    
    response2 = await test_client.post("/marzban", json=webhook_updated)
    assert response2.status_code == 200
    assert response2.json() == {"ok": True}
    
    queue_size = await redis_client.llen("MARZBAN") #type: ignore
    assert queue_size == 2


@pytest.mark.asyncio
async def test_marzban_webhook_ttl_by_action(
    test_client: AsyncTestClient,
    redis_client: Redis
):
    """
    Тест: разные TTL для разных actions
    """
    username = "ttl_test_user"
    
    # reached_days_left -> 3600s
    webhook1 = [{
        "username": f"{username}_1",
        "action": "reached_days_left",
        "expire": int(datetime.now().timestamp())
    }]
    await test_client.post("/marzban", json=webhook1)
    
    key1 = f"marzban:{username}_1:reached_days_left"
    ttl1 = await redis_client.ttl(key1)
    assert 3550 < ttl1 <= 3600, f"TTL should be ~3600s, got {ttl1}s"
    
    # user_expired -> 300s
    webhook2 = [{
        "username": f"{username}_2",
        "action": "user_expired",
        "expire": int(datetime.now().timestamp())
    }]
    await test_client.post("/marzban", json=webhook2)
    
    key2 = f"marzban:{username}_2:user_expired"
    ttl2 = await redis_client.ttl(key2)
    assert 290 < ttl2 <= 300, f"TTL should be ~300s, got {ttl2}s"
    
    # user_created -> 60s
    webhook3 = [{
        "username": f"{username}_3",
        "action": "user_created",
        "expire": int(datetime.now().timestamp()),
        "proxies": {"vless": {"id": "test"}}
    }]
    await test_client.post("/marzban", json=webhook3)
    
    key3 = f"marzban:{username}_3:user_created"
    ttl3 = await redis_client.ttl(key3)
    assert 55 < ttl3 <= 60, f"TTL should be ~60s, got {ttl3}s"


@pytest.mark.asyncio
async def test_marzban_webhook_concurrent_requests(
    test_client: AsyncTestClient,
    redis_client: Redis
):
    """
    Тест: параллельные webhook запросы
    """
    username = "concurrent_user"
    
    webhook_data = [
        {
            "username": username,
            "action": "user_created",
            "expire": int((datetime.now() + timedelta(days=30)).timestamp()),
            "proxies": {"vless": {"id": "concurrent-id"}}
        }
    ]
    
    # 10 параллельных запросов (синхронный client)
    responses = [
        await test_client.post("/marzban", json=webhook_data)
        for _ in range(10)
    ]
    
    ok_count = sum(1 for r in responses if r.json() == {"ok": True})
    duplicate_count = sum(1 for r in responses if r.json() == {'msg': 'operation for user been'})
    
    print(f"\n📊 Concurrent requests: OK={ok_count}, Duplicates={duplicate_count}")
    
    # Первый запрос успешен
    assert ok_count == 1
    # Остальные - дубликаты
    assert duplicate_count == 9
    
    queue_size = await redis_client.llen("MARZBAN") #type: ignore
    assert queue_size == 1


@pytest.mark.asyncio
async def test_marzban_webhook_queue_data_format(
    test_client: AsyncTestClient,
    redis_client: Redis
):
    """
    Тест: формат данных в очереди
    """
    username = "format_test_user"
    expire_timestamp = int((datetime.now() + timedelta(days=30)).timestamp())
    proxy_id = "test-proxy-id-123"
    
    webhook_data = [
        {
            "username": username,
            "action": "user_created",
            "expire": expire_timestamp,
            "proxies": {"vless": {"id": proxy_id}}
        }
    ]
    
    await test_client.post("/marzban", json=webhook_data)
    
    task_json = await redis_client.rpop("MARZBAN") #type: ignore
    task = json.loads(task_json) #type: ignore
    
    assert "user_id" in task
    assert "expire" in task
    assert task["user_id"] == username
    assert task["expire"] == expire_timestamp
    assert task["id"] == proxy_id
    assert isinstance(task["expire"], int)
    
    print(f"\n✅ Queue data: {json.dumps(task, indent=2)}")


@pytest.mark.asyncio  
async def test_marzban_webhook_malformed_data(
    test_client: AsyncTestClient,
    redis_client: Redis
):
    """
    Тест: обработка невалидных данных
    """
    # Пустой массив
    response1 = await test_client.post("/marzban", json=[])
    assert response1.status_code in [400, 422, 500]
    
    # Отсутствует username
    webhook_no_username = [
        {
            "action": "user_created",
            "expire": int(datetime.now().timestamp())
        }
    ]
    response2 = await test_client.post("/marzban", json=webhook_no_username)
    assert response2.status_code in [400, 422, 500]
    
    # Отсутствует action
    webhook_no_action = [
        {
            "username": "test_user",
            "expire": int(datetime.now().timestamp())
        }
    ]
    response3 = await test_client.post("/marzban", json=webhook_no_action)
    assert response3.status_code in [400, 422, 500]


if __name__ == "__main__":
    print("Run with: pytest tests/test_webhooks.py -v -s")