from repositories.base import BaseRepository
from db.models import UserLinks
from db.database import async_session_maker
from redis.asyncio import Redis
from logger_setup import logger


async def get_uuid_cache(
    redis_cache: Redis,
    user_id
):
    links_str_uuid = f"USER_UUID:{user_id}"
    uuid_cache = await redis_cache.get(links_str_uuid)

    if not uuid_cache:
        async with async_session_maker() as session:
            repo = BaseRepository(session=session, model=UserLinks)
            uuid_data = await repo.get_one(user_id=int(user_id))
            if uuid_data is None:
                # await callback.answer()
                return None

            uuid = uuid_data.uuid
        
        await redis_cache.set(
            links_str_uuid,
            uuid
        )
        uuid_cache = uuid
    
    logger.info(uuid_cache)
    uuid_cache = uuid_cache.replace('"', "")
    return uuid_cache