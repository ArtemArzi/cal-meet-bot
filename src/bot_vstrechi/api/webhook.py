from __future__ import annotations

from collections.abc import Mapping
from contextlib import AbstractAsyncContextManager
from dataclasses import asdict
from datetime import datetime, timezone
import logging
from typing import Callable, cast

from fastapi import FastAPI, HTTPException, Request

from bot_vstrechi.telegram.adapter import TelegramWebhookAdapter


logger = logging.getLogger(__name__)


def create_webhook_app(
    *,
    adapter: TelegramWebhookAdapter,
    now_provider: Callable[[], datetime] | None = None,
    lifespan: Callable[[FastAPI], AbstractAsyncContextManager[None]] | None = None,
    secret_token: str | None = None,
    google_webhook_handler: Callable[[Mapping[str, str], datetime], None] | None = None,
    google_channel_token: str | None = None,
    readiness_probe: Callable[[], bool] | None = None,
) -> FastAPI:
    clock = now_provider or (lambda: datetime.now(tz=timezone.utc))
    probe = readiness_probe or (lambda: True)
    app = FastAPI(
        title="bot-vstrechi-webhook",
        version="0.1.0",
        lifespan=lifespan,
    )

    async def telegram_webhook(request: Request) -> dict[str, object]:
        if secret_token is not None:
            header_token = request.headers.get("X-Telegram-Bot-Api-Secret-Token")
            if header_token != secret_token:
                raise HTTPException(status_code=403, detail="forbidden")

        payload_obj = cast(object, await request.json())
        if not isinstance(payload_obj, dict):
            raise HTTPException(status_code=400, detail="Invalid update payload")
        payload = cast(Mapping[str, object], payload_obj)

        logger.info(
            "webhook request received",
            extra={"update_id": payload.get("update_id")},
        )

        result = adapter.handle_update(update=payload, now=clock())
        result_map = asdict(result)
        logger.info(
            "webhook response",
            extra={
                "outcome": result_map["outcome"],
                "reason_code": result_map["reason_code"],
            },
        )
        return {
            "ok": True,
            "outcome": result_map["outcome"],
            "reason_code": result_map["reason_code"],
            "message": result_map["message"],
        }

    async def health() -> dict[str, str]:
        return {"status": "ok"}

    async def google_webhook(request: Request) -> dict[str, object]:
        if google_channel_token is not None:
            header_token = request.headers.get("X-Goog-Channel-Token")
            if header_token != google_channel_token:
                raise HTTPException(status_code=403, detail="forbidden")

        if google_webhook_handler is None:
            return {"ok": True, "accepted": False}

        headers = {key: value for key, value in request.headers.items()}
        google_webhook_handler(headers, clock())
        return {"ok": True, "accepted": True}

    async def readiness() -> dict[str, str]:
        if not probe():
            raise HTTPException(status_code=503, detail="db_not_ready")
        return {"status": "ok", "db": "ok"}

    app.add_api_route("/telegram/webhook", telegram_webhook, methods=["POST"])
    app.add_api_route("/calendar/webhook", google_webhook, methods=["POST"])
    app.add_api_route("/health", health, methods=["GET"])
    app.add_api_route("/readiness", readiness, methods=["GET"])

    return app
