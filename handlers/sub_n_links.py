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
from handlers.deps import get_uuid_cache

text_pattern = """
🔐 **Ваши подписки IV VPN**

📋 Универсальная ссылка подписки:
(Нажмите для копирования)
"""

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


async def get_links_cache(
    redis_cache: Redis,
    user_id
) -> UserLinksModel | None:
    links_str = f"LINKS:{user_id}"
    cache = await redis_cache.get(links_str)

    if cache:
        links = _parse_links(cache)

        if links is None:
            return None
        
    else:
        async with MarzbanClient() as client:
            user = await client.get_user(username=str(user_id))

            if not isinstance(user, dict):
                logger.error(f"Ошибка Marzban {user}")
                return None
            
        marz_links: list = user.get('links', [])
        links = _parse_links(json.dumps({
            "user_id": user_id,
            "links": marz_links
        }))

        if links is None:
            return None
        
        await redis_cache.set(
            links_str,
            json.dumps(
                links.model_dump(),
                default=str
            )
        )
    
    return links


@dp.callback_query(F.data == 'subs')
async def sub_n_links(
    callback: CallbackQuery,
    redis_cache: Redis
):
    user_id = callback.from_user.id

    uuid_cache = await get_uuid_cache(
        redis_cache=redis_cache,
        user_id=user_id
    )

    links = await get_links_cache(
        redis_cache=redis_cache,
        user_id=user_id
    )

    if links is None or uuid_cache is None:
        await callback.answer()
        return
    
    text_reponse = text_pattern
    text_reponse += "\n" + f"`{s.IN_SUB_LINK}{uuid_cache}`" #type: ignore

    link_titles = await to_link({"links": links.links})
        
    await callback.message.edit_text( #type:ignore
        text=text_reponse,
        reply_markup=SubMenu.links_keyboard(links=link_titles), #type: ignore
        parse_mode="MARKDOWN"
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

    uuid_cache = await get_uuid_cache(
        redis_cache=redis_cache,
        user_id=user_id
    )
    
    links = await get_links_cache(
        redis_cache=redis_cache,
        user_id=user_id
    )

    if links is None or uuid_cache is None:
        await callback.answer()
        return

    index = callback.data.replace("sub_", "") #type: ignore
    link_titles = await to_link({"links": links.links})
    if link_titles is None:
        return "Error"
    
    text_response = f"""🔐 <b>Ваши подписки IV VPN</b>

📋 <b>Универсальная ссылка:</b>
<code>{s.IN_SUB_LINK}{uuid_cache}</code>

🔑 <b>Ключ конфигурации:</b>
<code>{links.links[int(index)]}</code>

💡 <i>Используйте универсальную ссылку для автоматического обновления серверов, или ключ для ручной настройки.</i>
"""

    
    await callback.message.edit_text( #type: ignore
        text=text_response,
        reply_markup=SubMenu.links_keyboard(links=link_titles),
        parse_mode="HTML"
    )
