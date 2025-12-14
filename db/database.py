from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from config import settings

engine = create_async_engine(
    url=settings.DATABASE_URL
)

async_session_maker = async_sessionmaker(
    bind=engine,
    expire_on_commit=False
)