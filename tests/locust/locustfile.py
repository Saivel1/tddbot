# tests/locust/locustfile.py

from locust import HttpUser, task, between, events
from locust.env import Environment
import json
import random
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


class VPNBotUser(HttpUser):
    """Симуляция поведения обычного пользователя"""
    
    wait_time = between(1, 5)  # Пауза между действиями 1-5 сек
    
    def on_start(self):
        """Инициализация при старте пользователя"""
        self.user_id = random.randint(100000, 999999)
        self.has_trial = False
        self.has_subscription = False
        logger.info(f"👤 User {self.user_id} started")
    
    @task(10)  # Вес 10 - самая частая задача
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
            else:
                response.failure(f"Status code: {response.status_code}")
    
    @task(5)
    def activate_trial(self):
        """Активация пробного периода"""
        if self.has_trial:
            return  # Уже активирован
        
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
                response.failure(f"Failed: {response.status_code}")
    
    @task(3)
    def get_payment_link(self):
        """Получение ссылки на оплату"""
        amount = random.choice([50, 100, 200, 600])
        
        update = self._create_telegram_update(
            text=f"/pay_{amount}",
            user_id=self.user_id,
            callback_data=f"pay:{amount}"
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
                response.failure(f"Status code: {response.status_code}")
    
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
                response.failure(f"Status code: {response.status_code}")
    
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
            name="❓ Help Command",
            catch_response=True
        ) as response:
            if response.status_code == 200:
                response.success()
            else:
                response.failure(f"Status code: {response.status_code}")
    
    def _create_telegram_update(self, text: str, user_id: int, callback_data: str | None = None):
        """Создать Telegram Update"""
        update = {
            "update_id": random.randint(1, 1000000),
            "message": {
                "message_id": random.randint(1, 1000000),
                "from": {
                    "id": user_id,
                    "is_bot": False,
                    "first_name": f"User{user_id}",
                    "username": f"user{user_id}"
                },
                "chat": {
                    "id": user_id,
                    "type": "private",
                    "first_name": f"User{user_id}"
                },
                "date": int(datetime.now().timestamp()),
                "text": text
            }
        }
        
        if callback_data:
            update["callback_query"] = {
                "id": str(random.randint(1, 1000000)),
                "from": update["message"]["from"],
                "message": update["message"],
                "data": callback_data
            }
        
        return update


class PaymentWebhookUser(HttpUser):
    """Симуляция webhook'ов от YooMoney"""
    
    wait_time = between(2, 10)  # Платежи приходят реже
    
    def on_start(self):
        """Инициализация"""
        self.payment_counter = 0
    
    @task(1)
    def payment_succeeded(self):
        """Webhook успешного платежа"""
        user_id = random.randint(100000, 999999)
        order_id = f"test-{random.randint(10000, 99999)}-{int(datetime.now().timestamp())}"
        amount = random.choice([50, 100, 200, 600])
        
        payload = {
            "type": "notification",
            "event": "payment.succeeded",
            "object": {
                "id": order_id,
                "status": "succeeded",
                "paid": True,
                "amount": {
                    "value": str(amount),
                    "currency": "RUB"
                },
                "metadata": {
                    "user_id": str(user_id)
                },
                "created_at": datetime.now().isoformat()
            }
        }
        
        with self.client.post(
            "/pay",
            json=payload,
            name="💰 Payment Webhook (succeeded)",
            catch_response=True
        ) as response:
            if response.status_code in [200, 201]:
                self.payment_counter += 1
                response.success()
                logger.info(f"✅ Payment webhook {order_id} processed (total: {self.payment_counter})")
            else:
                response.failure(f"Status code: {response.status_code}")


# ============================================================================
# СОБЫТИЯ И ХУКИ
# ============================================================================

@events.test_start.add_listener
def on_test_start(environment, **kwargs):
    """Вызывается при старте теста"""
    print("\n" + "="*70)
    print("🚀 LOCUST STRESS TEST STARTED")
    print("="*70)
    print(f"Target: {environment.host}")
    print(f"Users: {environment.runner.target_user_count if hasattr(environment.runner, 'target_user_count') else 'N/A'}")
    print("="*70 + "\n")


@events.test_stop.add_listener
def on_test_stop(environment, **kwargs):
    """Вызывается при остановке теста"""
    print("\n" + "="*70)
    print("🛑 LOCUST STRESS TEST STOPPED")
    print("="*70)
    
    # Выводим статистику
    stats = environment.stats
    print(f"\n📊 SUMMARY:")
    print(f"Total requests: {stats.total.num_requests}")
    print(f"Total failures: {stats.total.num_failures}")
    print(f"Success rate: {(1 - stats.total.num_failures/stats.total.num_requests)*100:.2f}%")
    print(f"Median response time: {stats.total.median_response_time}ms")
    print(f"95th percentile: {stats.total.get_response_time_percentile(0.95)}ms")
    print(f"99th percentile: {stats.total.get_response_time_percentile(0.99)}ms")
    print(f"RPS: {stats.total.total_rps:.2f}")
    print("="*70 + "\n")


@events.request.add_listener
def on_request(request_type, name, response_time, response_length, exception, **kwargs):
    """Логирование каждого запроса (опционально)"""
    if exception:
        logger.error(f"❌ {name} failed: {exception}")