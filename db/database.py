from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from config import settings

engine = create_async_engine(
    url=settings.DATABASE_URL_aiosqlite
)

async_session_maker = async_sessionmaker(
    bind=engine
)