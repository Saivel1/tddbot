from functools import wraps
from typing import Awaitable, Callable, Any
from redis.asyncio import Redis
import asyncio
import json
from logger_setup import logger


class SkipTask(Exception):
    """Пропустить задачу без retry"""
    pass


def queue_worker(
    queue_name: str,
    timeout: int = 5,
    max_retries: int = 3,
    retry_delay: int = 1,
    check_availability: Callable[[], Awaitable[bool]] | None = None,
):
    """
    Декоратор для создания воркеров из очередей
    
    Usage:
        @queue_worker(queue_name="DB", timeout=5)
        async def handle_db_task(data: dict, redis_cli: Redis, session: AsyncSession):
            # обработка задачи
            return result
    """
    def decorator(handler: Callable):
        @wraps(handler)
        async def wrapper(
            redis_cli: Redis,
            process_once: bool = False,
            **handler_kwargs
        ):
            worker_name = handler.__name__
            logger.info(f"🚀 {worker_name} started (queue={queue_name})")
            
            cnt = 0
            
            while True:
                # Проверка доступности
                if check_availability:
                    while not await check_availability(): #type: ignore
                        logger.debug(f"⏳ {worker_name}: service unavailable, waiting 10s...")
                        await asyncio.sleep(10)
                        cnt += 1
                        if cnt == 60:
                            logger.error(f"🚨 {worker_name}: unavailable for 10 minutes!")
                            cnt = 0
                
                # Получаем задачу
                result = await redis_cli.brpop(queue_name, timeout=timeout) # type: ignore
                cnt = 0
                
                if not result:
                    if process_once:
                        logger.debug(f"✅ {worker_name}: no tasks, exiting")
                        return None
                    continue
                
                _, message = result
                logger.info(f"📥 {worker_name}: task received")
                
                # Обработка с retry
                for attempt in range(max_retries):
                    try:
                        data = json.loads(message)
                        
                        # Вызываем обработчик
                        result = await handler(
                            data=data,
                            redis_cli=redis_cli,
                            **handler_kwargs
                        )
                        
                        logger.info(f"✅ {worker_name}: task completed")
                        
                        if process_once:
                            return result
                        
                        break  # Успех
                    
                    except SkipTask as e:
                        # ✅ Пропускаем задачу без retry и re-queue
                        logger.info(f"⏭️  {worker_name}: task skipped - {e}")
                        if process_once:
                            return 'skipped'
                        break 

                        
                    except Exception as e:
                        logger.error(f"❌ {worker_name}: error (attempt {attempt + 1}/{max_retries}): {e}")
                        
                        if attempt < max_retries - 1:
                            await asyncio.sleep(retry_delay)
                            continue
                        else:
                            # Возвращаем в очередь
                            logger.warning(f"♻️  {worker_name}: re-queuing failed task")
                            await redis_cli.lpush(queue_name, message) # type: ignore
                            
                            if process_once:
                                raise
                            
                            await asyncio.sleep(retry_delay)
        
        return wrapper
    return decorator