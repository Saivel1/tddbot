from typing import TypeVar, Generic
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update


T = TypeVar('T', bound=DeclarativeBase)

class BaseRepository(Generic[T]):
    def __init__(self, session: AsyncSession, model: type[T]):
        self.session = session
        self.model = model
    

    async def get_one(self, **filters):
        stmt = (
            select(self.model)
            .filter_by(**filters)
        )
        res = await self.session.execute(stmt)
        await self.session.commit()
        return res.scalar_one_or_none()
    

    async def create(self, **data):            
        ins_data = self.model(**data)
        self.session.add(ins_data)
        await self.session.commit()
        await self.session.refresh(ins_data)
        return ins_data
    

    async def update(self, data: dict, **filter):
        stmt = (
            update(self.model)
            .values(**data)
            .filter_by(**filter)
        )
        res = await self.session.execute(stmt)
        await self.session.commit()
        return res.rowcount #type: ignore