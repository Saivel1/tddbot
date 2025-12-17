from aiogram.types import Message, CallbackQuery
from keyboards.markup import MainKeyboard
from keyboards.builder import SubMenu
from sqlalchemy.ext.asyncio import AsyncSession
from repositories.base import BaseRepository
from db.models import User
from misc.utils import is_cached
from redis.asyncio import Redis
from bot_in import dp
from aiogram import F
from aiogram.filters import Command
from schemas.schem import UserLinksModel
from logger_setup import logger
import json
from core.marzban.Client import MarzbanClient


@dp.callback_query(F.data == 'subs')
async def sub_n_links(
    callback: CallbackQuery,
    redis_cache: Redis
):
    user_id = callback.from_user.id
    links_str = f"LINKS:{user_id}"
    cache = await redis_cache.get(links_str)

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
        marz_link_set = set(marz_links)

        links = _parse_links(json.dumps(marz_link_set))

        if links is None:
            return "???"
        
        await redis_cache.set(
            links_str,
            json.dumps(
                links.model_dump(),
                default=str
            )
        )
        
    await callback.message.edit_text( #type:ignore
        text='Something like link',
        reply_markup=SubMenu.links_keyboard(links=links.links)
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