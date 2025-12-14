from redis.asyncio import Redis
from config import settings
from typing import Optional

redis_client: Optional[Redis] = None


async def init_redis() -> Redis:
    """Инициализация Redis подключения"""
    return Redis(
        host=settings.REDIS_HOST,
        port=settings.REDIS_PORT,
        password=settings.REDIS_PASS,
        db=0,  # база данных (0-15)
        decode_responses=True,  # автоматически декодирует байты в строки
        socket_connect_timeout=5,
        socket_keepalive=True,
        health_check_interval=30,
    )

async def close_redis():
    """Закрытие Redis"""
    global redis_client
    if redis_client:
        await redis_client.aclose()