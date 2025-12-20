FROM python:3.13-slim

# Переменные окружения
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    UV_SYSTEM_PYTHON=1 \
    UV_COMPILE_BYTECODE=1 \
    PATH="/root/.cargo/bin:$PATH"

# Устанавливаем системные зависимости
RUN apt-get update && apt-get install -y \
    curl \
    git \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Устанавливаем uv
RUN curl -LsSf https://astral.sh/uv/install.sh | sh && \
    mv /root/.cargo/bin/uv /usr/local/bin/uv && \
    uv --version

# Создаём рабочую директорию
WORKDIR /app

# Копируем файлы зависимостей
COPY pyproject.toml uv.lock ./

# Устанавливаем зависимости через uv
RUN uv sync --frozen --no-dev

# Копируем весь проект
COPY . .

# Создаём непривилегированного пользователя
RUN useradd -m -u 1000 botuser && \
    chown -R botuser:botuser /app

USER botuser

# Healthcheck для контейнера
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import asyncio; from app.redis_client import init_redis; asyncio.run(init_redis()).ping()" || exit 1

# Запуск приложения
CMD ["uv", "run", "run.py"]