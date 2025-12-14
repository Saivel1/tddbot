from pydantic import BaseModel
from datetime import datetime

class UserModel(BaseModel):
    user_id: int
    username: str
    trial_used: bool
    subscription_end: datetime | None = None


class PayDataModel(BaseModel):
    user_id: int
    payment_url: str


class CreateUserMarzbanModel(BaseModel):
    username: str
    id: str | None = None
    expire: int | None = None