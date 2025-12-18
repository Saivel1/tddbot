from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
from datetime import datetime
from sqlalchemy import DateTime
from sqlalchemy.inspection import inspect
from sqlalchemy import ForeignKey, func


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

    payments: Mapped[list["PaymentData"]] = relationship(back_populates="user")

    links: Mapped['UserLinks'] = relationship(back_populates='user')


class UserLinks(Base):
    __tablename__ = "links"

    user_id:  Mapped[int] = mapped_column(ForeignKey('users.user_id'))
    uuid:     Mapped[str]
    panel1:   Mapped[str | None]
    panel2:   Mapped[str | None]

    user: Mapped['User'] = relationship(back_populates='links')

class PaymentData(Base):
    __tablename__ = 'payment_data'
    
    payment_id: Mapped[str] = mapped_column(index=True)
    user_id: Mapped[str] = mapped_column(ForeignKey('users.user_id'))
    status: Mapped[str] = mapped_column(server_default='succeeded')
    amount: Mapped[int]
    created_at: Mapped[datetime] = mapped_column(server_default=func.now())
    
    # Опционально: relationship для удобства
    user: Mapped["User"] = relationship(back_populates="payments")