from pydantic import BaseModel, ConfigDict
from datetime import datetime

class UserModel(BaseModel):
    user_id: int
    username: str | None = None
    trial_used: bool
    subscription_end: datetime | None = None


class PayDataModel(BaseModel):
    user_id: int
    payment_url: str


class CreateUserMarzbanModel(BaseModel):
    model_config = ConfigDict(extra='ignore', from_attributes=True)

    username: str
    id: str | None = None
    expire: int | None = None


class UpdateUserMarzbanModel(BaseModel):
    model_config = ConfigDict(extra='ignore', from_attributes=True)

    username: str
    expire: int


class UserLinksModel(BaseModel):
    user_id: int
    links: list[str]


class WRKDBUserInput(BaseModel):
    pass


class WRKDBUserLinksInput(BaseModel):
    pass


class WRKDBPaymentDataInput(BaseModel):
    pass


class WRKMarzbanInput(BaseModel):
    username: str
    id: str | None = None
    expire: int | None = None



class WRKPaymentInput(BaseModel):
    pass