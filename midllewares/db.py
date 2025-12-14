from sqlalchemy.ext.asyncio import async_sessionmaker
from aiogram import BaseMiddleware
import app.redis_client as redis_module 

class DatabaseMiddleware(BaseMiddleware):
    def __init__(self, session_maker: async_sessionmaker):
        self.session_maker = session_maker
        super().__init__()
    
    async def __call__(self, handler, event, data):
        async with self.session_maker() as session:
            data["session"] = session
            redis_cli = redis_module.redis_client
            data["redis_cache"]  = redis_cli

            if redis_cli is None:
                print("❌ ERROR: redis_client is None in middleware!")
                
            try:
                return await handler(event, data)
            except Exception:
                await session.rollback()
                raise