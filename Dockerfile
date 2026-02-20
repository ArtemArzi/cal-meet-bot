FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

RUN useradd --create-home --uid 10001 appuser

COPY pyproject.toml requirements.txt ./
COPY src ./src

RUN pip install --upgrade pip \
    && pip install .

RUN mkdir -p /data \
    && chown -R appuser:appuser /app /data

USER appuser

ENV BOT_VSTRECHI_DB_PATH=/data/local.db \
    LOG_LEVEL=INFO \
    LOG_FORMAT=json

EXPOSE 8000

CMD ["uvicorn", "bot_vstrechi.asgi:app", "--host", "0.0.0.0", "--port", "8000", "--proxy-headers", "--forwarded-allow-ips", "*"]
