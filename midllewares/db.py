from sqlalchemy.ext.asyncio import async_sessionmaker
from aiogram import BaseMiddleware
from app.redis_client import redis_client

class DatabaseMiddleware(BaseMiddleware):
    def __init__(self, session_maker: async_sessionmaker):
        self.session_maker = session_maker
        super().__init__()
    
    async def __call__(self, handler, event, data):
        async with self.session_maker() as session:
            data["session"] = session
            data["redis_cache"] = redis_client
            try:
                return await handler(event, data)
            except Exception:
                await session.rollback()
                raise