# tests/locust/locustfile.py

from locust import HttpUser, task, between, events
import json
import random
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


class VPNBotUser(HttpUser):
    """Симуляция поведения обычного пользователя"""
    
    wait_time = between(1, 5)
    
    def on_start(self):
        """Инициализация при старте пользователя"""
        # ✅ Безопасный диапазон (не существует в Telegram)
        self.user_id = random.randint(10_000_000_000, 10_000_999_999)
        self.has_trial = False
        logger.info(f"👤 User {self.user_id} started")
    
    @task(10)
    def check_subscription(self):
        """Проверка статуса подписки"""
        update = self._create_telegram_update(
            text="/status",
            user_id=self.user_id
        )
        
        with self.client.post(
            "/bot-webhook",
            json=update,
            name="📊 Check Subscription",
            catch_response=True
        ) as response:
            if response.status_code == 200:
                response.success()
            elif response.status_code == 422:
                response.failure("422 Invalid format")
            else:
                response.failure(f"Status: {response.status_code}")
    
    @task(5)
    def activate_trial(self):
        """Активация пробного периода"""
        if self.has_trial:
            return
        
        update = self._create_telegram_update(
            text="/trial",
            user_id=self.user_id
        )
        
        with self.client.post(
            "/bot-webhook",
            json=update,
            name="🆓 Activate Trial",
            catch_response=True
        ) as response:
            if response.status_code == 200:
                self.has_trial = True
                response.success()
                logger.info(f"✅ Trial activated for {self.user_id}")
            else:
                response.failure(f"Status: {response.status_code}")
    
    @task(3)
    def get_payment_link(self):
        """Получение ссылки на оплату (через callback)"""
        amount = random.choice([50, 100, 200, 600])
        
        # ✅ Используем callback_query вместо текста
        update = self._create_callback_update(
            callback_data=f"pay:{amount}",
            user_id=self.user_id
        )
        
        with self.client.post(
            "/bot-webhook",
            json=update,
            name=f"💳 Get Payment Link ({amount}₽)",
            catch_response=True
        ) as response:
            if response.status_code == 200:
                response.success()
            else:
                response.failure(f"Status: {response.status_code}")
    
    @task(2)
    def get_vpn_links(self):
        """Получение VPN ссылок"""
        update = self._create_telegram_update(
            text="/links",
            user_id=self.user_id
        )
        
        with self.client.post(
            "/bot-webhook",
            json=update,
            name="🔗 Get VPN Links",
            catch_response=True
        ) as response:
            if response.status_code == 200:
                response.success()
            else:
                response.failure(f"Status: {response.status_code}")
    
    @task(1)
    def help_command(self):
        """Команда помощи"""
        update = self._create_telegram_update(
            text="/help",
            user_id=self.user_id
        )
        
        with self.client.post(
            "/bot-webhook",
            json=update,
            name="❓ Help",
            catch_response=True
        ) as response:
            if response.status_code == 200:
                response.success()
            else:
                response.failure(f"Status: {response.status_code}")
    
    def _create_telegram_update(self, text: str, user_id: int) -> dict:
        """
        Создать валидный Telegram Update с message
        https://core.telegram.org/bots/api#update
        """
        return {
            "update_id": random.randint(1, 10_000_000),
            "message": {
                "message_id": random.randint(1, 10_000_000),
                "from": {
                    "id": user_id,
                    "is_bot": False,
                    "first_name": f"TestUser",
                    "username": f"testuser{user_id}",
                    "language_code": "ru"
                },
                "chat": {
                    "id": user_id,
                    "first_name": f"TestUser",
                    "username": f"testuser{user_id}",
                    "type": "private"
                },
                "date": int(datetime.now().timestamp()),
                "text": text
            }
        }
    
    def _create_callback_update(self, callback_data: str, user_id: int) -> dict:
        """
        Создать валидный Telegram Update с callback_query
        https://core.telegram.org/bots/api#callbackquery
        """
        message_id = random.randint(1, 10_000_000)
        
        return {
            "update_id": random.randint(1, 10_000_000),
            "callback_query": {
                "id": str(random.randint(1_000_000_000, 9_999_999_999)),
                "from": {
                    "id": user_id,
                    "is_bot": False,
                    "first_name": f"TestUser",
                    "username": f"testuser{user_id}",
                    "language_code": "ru"
                },
                "message": {
                    "message_id": message_id,
                    "from": {
                        "id": 6155909199,  # Bot ID
                        "is_bot": True,
                        "first_name": "Your Bot",
                        "username": "your_bot"
                    },
                    "chat": {
                        "id": user_id,
                        "first_name": f"TestUser",
                        "username": f"testuser{user_id}",
                        "type": "private"
                    },
                    "date": int(datetime.now().timestamp()),
                    "text": "Выберите сумму:"
                },
                "chat_instance": str(random.randint(1_000_000_000, 9_999_999_999)),  # ✅ Обязательное поле!
                "data": callback_data
            }
        }


class PaymentWebhookUser(HttpUser):
    """Симуляция webhook'ов от YooMoney"""
    
    wait_time = between(5, 15)  # Платежи приходят редко
    
    def on_start(self):
        self.payment_counter = 0
    
    @task(1)
    def payment_succeeded(self):
        """Webhook успешного платежа"""
        # ✅ Используем безопасный диапазон user_id
        user_id = random.randint(10_000_000_000, 10_000_999_999)
        order_id = f"locust-test-{random.randint(10000, 99999)}-{int(datetime.now().timestamp())}"
        amount = random.choice([50, 100, 200, 600])
        
        payload = {
            "type": "notification",
            "event": "payment.succeeded",
            "object": {
                "id": order_id,
                "status": "succeeded",
                "paid": True,
                "amount": {
                    "value": f"{amount}.00",
                    "currency": "RUB"
                },
                "created_at": datetime.now().isoformat(),
                "metadata": {
                    "user_id": str(user_id)
                }
            }
        }
        
        with self.client.post(
            "/pay-test",  # ✅ Используем тестовый эндпоинт
            json=payload,
            name="💰 Payment Webhook",
            catch_response=True
        ) as response:
            if response.status_code in [200, 201]:
                self.payment_counter += 1
                response.success()
                logger.info(f"✅ Payment {order_id} (total: {self.payment_counter})")
            elif response.status_code == 403:
                response.failure("403 Forbidden")
            else:
                response.failure(f"Status: {response.status_code}")


# ============================================================================
# СОБЫТИЯ
# ============================================================================

@events.test_start.add_listener
def on_test_start(environment, **kwargs):
    """Вызывается при старте теста"""
    print("\n" + "="*70)
    print("🧪 LOCUST LOAD TEST STARTED")
    print("="*70)
    print(f"⚠️  WARNING: Using FAKE user_ids (10,000,000,000+)")
    print(f"⚠️  Ensure TESTING_MODE=true in your app!")
    print(f"Target: {environment.host}")
    print("="*70 + "\n")


@events.test_stop.add_listener
def on_test_stop(environment, **kwargs):
    """Вызывается при остановке теста"""
    print("\n" + "="*70)
    print("✅ LOCUST LOAD TEST STOPPED")
    print("="*70)
    
    stats = environment.stats
    if stats.total.num_requests > 0:
        print(f"\n📊 SUMMARY:")
        print(f"Total requests: {stats.total.num_requests}")
        print(f"Total failures: {stats.total.num_failures}")
        print(f"Success rate: {(1 - stats.total.num_failures/stats.total.num_requests)*100:.2f}%")
        print(f"Median response time: {stats.total.median_response_time}ms")
        print(f"95th percentile: {stats.total.get_response_time_percentile(0.95)}ms")
        print(f"RPS: {stats.total.total_rps:.2f}")
    print("="*70 + "\n")