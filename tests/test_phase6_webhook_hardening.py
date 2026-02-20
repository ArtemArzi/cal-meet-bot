from __future__ import annotations
from unittest.mock import MagicMock

from datetime import datetime
from pathlib import Path

from fastapi.testclient import TestClient

from bot_vstrechi.db.repository import SQLiteRepository
from bot_vstrechi.application.service import MeetingWorkflowService
from bot_vstrechi.telegram.adapter import TelegramWebhookAdapter
from bot_vstrechi.api.webhook import create_webhook_app
from bot_vstrechi.infrastructure.runtime import create_application
from bot_vstrechi.domain.models import InboundEventSource, OutboxStatus


def _app(
    tmp_path: Path,
    *,
    secret_token: str | None,
    google_channel_token: str | None = None,
) -> TestClient:
    repository = SQLiteRepository(str(tmp_path / "phase6-webhook.db"))
    repository.initialize_schema()
    service = MeetingWorkflowService(repository, calendar_gateway=MagicMock())
    adapter = TelegramWebhookAdapter(repository=repository, workflow_service=service)
    app = create_webhook_app(
        adapter=adapter,
        now_provider=lambda: datetime(2026, 2, 12, 14, 0, 0),
        secret_token=secret_token,
        google_channel_token=google_channel_token,
        readiness_probe=repository.check_connection,
    )
    return TestClient(app)


def test_webhook_rejects_request_without_secret_header(tmp_path: Path) -> None:
    client = _app(tmp_path, secret_token="secret-1")

    response = client.post("/telegram/webhook", json={"update_id": 1})

    assert response.status_code == 403
    assert response.json() == {"detail": "forbidden"}


def test_webhook_rejects_request_with_wrong_secret(tmp_path: Path) -> None:
    client = _app(tmp_path, secret_token="secret-1")

    response = client.post(
        "/telegram/webhook",
        json={"update_id": 2},
        headers={"X-Telegram-Bot-Api-Secret-Token": "wrong"},
    )

    assert response.status_code == 403
    assert response.json() == {"detail": "forbidden"}


def test_webhook_accepts_request_with_correct_secret(tmp_path: Path) -> None:
    client = _app(tmp_path, secret_token="secret-1")

    response = client.post(
        "/telegram/webhook",
        json={"update_id": 3},
        headers={"X-Telegram-Bot-Api-Secret-Token": "secret-1"},
    )

    assert response.status_code == 200
    assert response.json()["ok"] is True


def test_webhook_passes_through_when_no_secret_configured(tmp_path: Path) -> None:
    client = _app(tmp_path, secret_token=None)

    response = client.post("/telegram/webhook", json={"update_id": 4})

    assert response.status_code == 200
    assert response.json()["ok"] is True


def test_health_returns_ok(tmp_path: Path) -> None:
    client = _app(tmp_path, secret_token="secret-1")

    response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_readiness_returns_ok_with_db_check(tmp_path: Path) -> None:
    client = _app(tmp_path, secret_token="secret-1")

    response = client.get("/readiness")

    assert response.status_code == 200
    assert response.json() == {"status": "ok", "db": "ok"}


def test_calendar_webhook_rejects_wrong_secret(tmp_path: Path) -> None:
    client = _app(
        tmp_path,
        secret_token="secret-1",
        google_channel_token="google-secret",
    )

    response = client.post(
        "/calendar/webhook",
        headers={"X-Goog-Channel-Token": "wrong"},
    )

    assert response.status_code == 403
    assert response.json() == {"detail": "forbidden"}


def test_calendar_webhook_accepts_and_records_signal(tmp_path: Path) -> None:
    db_path = tmp_path / "phase6-google-webhook.db"
    app = create_application(
        db_path=str(db_path),
        secret_token="secret-1",
        google_channel_token="google-secret",
        run_background_workers=False,
    )

    with TestClient(app) as client:
        response = client.post(
            "/calendar/webhook",
            headers={
                "X-Goog-Channel-Token": "google-secret",
                "X-Goog-Channel-ID": "channel-1",
                "X-Goog-Message-Number": "11",
                "X-Goog-Resource-State": "exists",
                "X-Goog-Resource-ID": "resource-1",
            },
        )

        assert response.status_code == 200
        assert response.json() == {"ok": True, "accepted": True}

    repository = SQLiteRepository(str(db_path))
    assert repository.count_calendar_sync_signals(status=OutboxStatus.PENDING) == 1
    duplicate = repository.register_inbound_event(
        source=InboundEventSource.GOOGLE_WEBHOOK,
        external_event_id="channel-1:11:exists:resource-1",
        received_at=datetime(2026, 2, 12, 14, 0, 0),
    )
    assert duplicate is False
    repository.close()
