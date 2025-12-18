from aiogram.types import Message, CallbackQuery
from keyboards.markup import MainKeyboard
from keyboards.builder import SubMenu
from sqlalchemy.ext.asyncio import AsyncSession
from repositories.base import BaseRepository
from db.models import User, UserLinks
from db.database import async_session_maker
from misc.utils import is_cached, to_link
from redis.asyncio import Redis
from bot_in import dp
from aiogram import F
from aiogram.filters import Command
from schemas.schem import UserLinksModel
from logger_setup import logger
import json
from core.marzban.Client import MarzbanClient
from config import settings as s


@dp.callback_query(F.data == 'subs')
async def sub_n_links(
    callback: CallbackQuery,
    redis_cache: Redis
):
    user_id = callback.from_user.id
    links_str = f"LINKS:{user_id}"
    links_str_uuid = f"USER_UUID:{user_id}"
    cache = await redis_cache.get(links_str)
    uuid_cache = await redis_cache.get(links_str_uuid)

    if cache:
        links = _parse_links(cache)

        if links is None:
            return "???"
        
    else:
        async with MarzbanClient() as client:
            user = await client.get_user(username=str(user_id))

            if not isinstance(user, dict):
                return "??? Osibka"
            
        marz_links: list = user.get('links', [])
        links = _parse_links(json.dumps({
            "user_id": user_id,
            "links": marz_links
        }))

        if links is None:
            return "???"
        
        await redis_cache.set(
            links_str,
            json.dumps(
                links.model_dump(),
                default=str
            )
        )

    if not uuid_cache:
        async with async_session_maker() as session:
            repo = BaseRepository(session=session, model=UserLinks)
            uuid_data = await repo.get_one(user_id=int(user_id))
            if uuid_data is None:
                await callback.answer()
                return

            uuid = uuid_data.uuid
        
        await redis_cache.set(
            links_str_uuid,
            uuid
        )
        uuid_cache = uuid

    uuid_cache = uuid_cache.replace('"', "")

    link_titles = await to_link({"links": links.links})
        
    await callback.message.edit_text( #type:ignore
        text=f'Something like link {s.IN_SUB_LINK}{uuid_cache}',
        reply_markup=SubMenu.links_keyboard(links=link_titles) #type: ignore
    )
    
    
@dp.callback_query(F.data.startswith("sub_"))
async def links(
    callback: CallbackQuery,
    redis_cache: Redis
):
    prev = await redis_cache.get("PREV")

    if prev == callback.data:
        await callback.answer()
        return
    
    await redis_cache.set("PREV", callback.data) #type: ignore

    user_id = callback.from_user.id
    links_str = f"LINKS:{user_id}"
    cache = await redis_cache.get(links_str)

    links_str_uuid = f"USER_UUID:{user_id}"
    uuid_cache = await redis_cache.get(links_str_uuid)

    if cache:
        links = _parse_links(cache)

        if links is None:
            return "???"
        
    else:
        async with MarzbanClient() as client:
            user = await client.get_user(username=str(user_id))

            if not isinstance(user, dict):
                return "??? Osibka"
            
        marz_links: list = user.get('links', [])
        links = _parse_links(json.dumps({
            "user_id": user_id,
            "links": marz_links
        }))

        if links is None:
            return "???"
        
        await redis_cache.set(
            links_str,
            json.dumps(
                links.model_dump(),
                default=str
            )
        )

    if not uuid_cache:
        async with async_session_maker() as session:
            repo = BaseRepository(session=session, model=UserLinks)
            uuid_data = await repo.get_one(user_id=int(user_id))
            if uuid_data is None:
                await callback.answer()
                return

            uuid = uuid_data.uuid
        
        await redis_cache.set(
            links_str_uuid,
            uuid
        )
        uuid_cache = uuid

    uuid_cache = uuid_cache.replace('"', "")

    index = callback.data.replace("sub_", "") #type: ignore
    link_titles = await to_link({"links": links.links})
    if link_titles is None:
        return "Error"

    text = f"""
Something like link {s.IN_SUB_LINK}{uuid_cache}

```{links.links[int(index)]}```
"""
    
    await callback.message.edit_text( #type: ignore
        text=text,
        reply_markup=SubMenu.links_keyboard(links=link_titles),
        parse_mode="MARKDOWN"
    )


def _parse_links(user_json: str) -> UserLinksModel | None:
    """
    Парсит JSON строку в UserModel
    
    Args:
        user_json: JSON строка с данными пользователя
    
    Returns:
        UserLinksModel или None при ошибке парсинга
    """
    logger.debug(user_json)
    try:
        user_dict = json.loads(user_json)
        logger.debug(f"Parsing user data: {user_dict}")
        return UserLinksModel(**user_dict)
    except Exception as e:
        logger.error(f"JSON parse error: {e}")
        return None