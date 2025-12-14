from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from datetime import datetime
from sqlalchemy import DateTime
from logger_setup import logger


class Base(DeclarativeBase):
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)


class User(Base):
    __tablename__ = "users"

    user_id:          Mapped[int] = mapped_column(unique=True)
    username:         Mapped[str | None]
    trial_used:       Mapped[bool] = mapped_column(default=False)
    subscription_end: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)

    def as_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}


class UserLinks(Base):
    __tablename__ = "links"

    user_id:  Mapped[int] = mapped_column(unique=True)
    uuid:     Mapped[str]
    panel1:   Mapped[str | None]
    panel2:   Mapped[str | None]

    def as_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}