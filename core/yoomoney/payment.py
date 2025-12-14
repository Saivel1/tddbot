import uuid
from yookassa import Payment, Configuration
from config import settings as s
from logger_setup import logger

Configuration.account_id = s.ACCOUNT_ID
Configuration.secret_key = s.SECRET_KEY

class YooPay:
    def __init__(self):
        self.id: str
        self.url: str
    
    async def create_payment(self, amount: int, email: str, plan: str) -> tuple | None:
        payment_id = uuid.uuid4()
        try:
            data = {
                "amount": {
                    "value": amount,
                    "currency": "RUB"
                },
                "confirmation": {
                    "type": "redirect",
                    "return_url": "https://t.me/ivvpnbot"
                },
                "capture": True,
                "description": "Подписка на VPN. В боте @ivvpnbot",
                "receipt": {
                    "customer": {
                        "email": email # Обязательно для отправки чека
                        },
                        "items": [
                            {
                                "description": plan,
                                "quantity": 1.0,
                                "amount": {
                                    "value": amount,
                                    "currency": "RUB"
                                },
                                "vat_code": "2" # Код НДС, например "2" для "без НДС"
                            }
                        ]
                }
            }
            payment = Payment.create(data, payment_id)
            self.id = payment.id # type: ignore
            self.link = payment.confirmation.confirmation_url # type: ignore
            logger.debug(f"Data {data}")
            logger.debug(f"Payment Obj {payment}")
            return (self.link, self.id)
        except Exception as e:
            logger.error(f'Ошибка создания платежа: {e}')

        return None
