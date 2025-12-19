# tests/locust/config.py

from dataclasses import dataclass

@dataclass
class LoadTestConfig:
    """Конфигурация нагрузочного теста"""
    
    # Целевой хост
    host: str = "https://your-domain.com"
    
    # Параметры нагрузки
    users: int = 100  # Количество пользователей
    spawn_rate: int = 10  # Скорость создания пользователей/сек
    run_time: str = "5m"  # Длительность теста
    
    # Веса пользователей (какой % каких задач)
    user_weight: int = 80  # 80% обычные пользователи
    payment_weight: int = 20  # 20% webhook'и
    
    # Результаты
    html_report: str = "tests/results/report.html"
    csv_prefix: str = "tests/results/stats"


# Предустановленные сценарии
SCENARIOS = {
    "smoke": LoadTestConfig(
        users=10,
        spawn_rate=2,
        run_time="2m"
    ),
    "load": LoadTestConfig(
        users=100,
        spawn_rate=10,
        run_time="5m"
    ),
    "stress": LoadTestConfig(
        users=500,
        spawn_rate=50,
        run_time="10m"
    ),
    "spike": LoadTestConfig(
        users=1000,
        spawn_rate=1000,
        run_time="2m"
    ),
    "soak": LoadTestConfig(
        users=200,
        spawn_rate=20,
        run_time="60m"
    ),
}