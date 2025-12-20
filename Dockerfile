FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim

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


# Запуск приложения
CMD ["uv", "run", "run.py"]