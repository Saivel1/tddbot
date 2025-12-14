from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from datetime import datetime
from sqlalchemy import DateTime
from logger_setup import logger
from sqlalchemy.inspection import inspect


class Base(DeclarativeBase):
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)

    def as_dict(self):
        """
        Безопасное преобразование в dict - только загруженные атрибуты
        """
        # ✅ Используем inspect чтобы получить только загруженные атрибуты
        result = {}
        for c in self.__table__.columns:
            # Проверяем что атрибут загружен (не вызовет lazy load)
            state = inspect(self)
            if c.key in state.dict:
                result[c.name] = state.dict[c.key]
            else:
                # Если не загружен - используем прямой доступ (может быть None)
                result[c.name] = getattr(self, c.key, None)
        return result


class User(Base):
    __tablename__ = "users"

    user_id:          Mapped[int] = mapped_column(unique=True)
    username:         Mapped[str | None]
    trial_used:       Mapped[bool] = mapped_column(default=False)
    subscription_end: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)


class UserLinks(Base):
    __tablename__ = "links"

    user_id:  Mapped[int] = mapped_column(unique=True)
    uuid:     Mapped[str]
    panel1:   Mapped[str | None]
    panel2:   Mapped[str | None]