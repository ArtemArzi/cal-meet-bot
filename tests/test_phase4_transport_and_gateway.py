from __future__ import annotations
from unittest.mock import MagicMock

import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import cast, override

import httpx
from fastapi.testclient import TestClient

from bot_vstrechi.telegram.callback_tokens import CallbackTokenService
from bot_vstrechi.domain import (
    CallbackActionType,
    Decision,
    JobType,
    Meeting,
    MeetingParticipant,
    MeetingState,
    ScheduledJobSpec,
)
from bot_vstrechi.domain.policies import DEADLINE_GRACE_WINDOW
from bot_vstrechi.calendar.client import (
    GoogleServiceAccountCalendarClient,
    GoogleServiceAccountCredentials,
)
from bot_vstrechi.calendar.gateway import CalendarApiClient, GoogleCalendarGateway
from bot_vstrechi.db.repository import SQLiteRepository
from bot_vstrechi.application.service import MeetingWorkflowService
from bot_vstrechi.telegram.adapter import TelegramWebhookAdapter
from bot_vstrechi.api.webhook import create_webhook_app
from bot_vstrechi.workers.scheduler import SchedulerWorker


def _repo(tmp_path: Path) -> SQLiteRepository:
    repository = SQLiteRepository(str(tmp_path / "bot_vstrechi_phase4.db"))
    repository.initialize_schema()
    return repository


def _meeting(
    now: datetime,
    *,
    meeting_id: str,
    state: MeetingState = MeetingState.PENDING,
    confirmation_deadline_at: datetime | None = None,
) -> Meeting:
    return Meeting(
        meeting_id=meeting_id,
        initiator_telegram_user_id=100,
        chat_id=100,
        state=state,
        scheduled_start_at=now + timedelta(hours=1),
        scheduled_end_at=now + timedelta(hours=2),
        confirmation_deadline_at=confirmation_deadline_at,
        participants=(
            MeetingParticipant(
                telegram_user_id=100,
                is_required=False,
                decision=Decision.NONE,
            ),
            MeetingParticipant(
                telegram_user_id=200,
                is_required=True,
                decision=Decision.NONE,
            ),
        ),
    )


def test_callback_token_service_persists_token_and_callback_data(
    tmp_path: Path,
) -> None:
    now = datetime(2026, 2, 11, 18, 0, 0)
    repository = _repo(tmp_path)
    meeting = _meeting(
        now,
        meeting_id="m-4-token",
        confirmation_deadline_at=now + timedelta(minutes=20),
    )
    repository.insert_meeting(meeting, now=now)

    token_service = CallbackTokenService(repository)
    button = token_service.issue_callback_button(
        meeting=meeting,
        action_type=CallbackActionType.PARTICIPANT_CONFIRM,
        allowed_user_id=200,
        now=now,
        text="Подтвердить",
    )

    assert button.callback_data.startswith("act:")
    token = button.callback_data[4:]
    stored = repository.get_callback_action_token(token)
    assert stored is not None
    assert stored.meeting_id == meeting.meeting_id
    assert stored.allowed_user_id == 200
    repository.close()


def test_fastapi_webhook_endpoint_processes_callback(tmp_path: Path) -> None:
    now = datetime(2026, 2, 11, 18, 0, 0)
    repository = _repo(tmp_path)
    meeting = _meeting(
        now,
        meeting_id="m-4-webhook",
        confirmation_deadline_at=now + timedelta(minutes=20),
    )
    repository.insert_meeting(meeting, now=now)

    workflow = MeetingWorkflowService(repository, calendar_gateway=MagicMock())
    token_service = CallbackTokenService(repository)
    button = token_service.issue_callback_button(
        meeting=meeting,
        action_type=CallbackActionType.PARTICIPANT_CONFIRM,
        allowed_user_id=200,
        now=now,
        text="Подтвердить",
    )

    adapter = TelegramWebhookAdapter(repository=repository, workflow_service=workflow)
    app = create_webhook_app(adapter=adapter, now_provider=lambda: now)
    client = TestClient(app)

    response = client.post(
        "/telegram/webhook",
        json={
            "update_id": 77,
            "callback_query": {
                "id": "cb-77",
                "from": {"id": 200},
                "data": button.callback_data,
            },
        },
    )

    assert response.status_code == 200
    body = response.json()
    assert isinstance(body, dict)
    assert body["outcome"] == "ok"
    reloaded = repository.get_meeting(meeting.meeting_id)
    assert reloaded is not None
    assert reloaded.state == MeetingState.CONFIRMED
    repository.close()


def test_google_service_account_calendar_client_retries_and_idempotency() -> None:
    token_calls = 0
    patch_calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal token_calls, patch_calls
        if request.method == "POST" and request.url.path == "/token":
            token_calls += 1
            return httpx.Response(
                200, json={"access_token": "token-1", "expires_in": 3600}
            )

        if request.method == "PATCH":
            patch_calls += 1
            if patch_calls == 1:
                return httpx.Response(503, text="temporary")
            return httpx.Response(200, json={"id": "evt-1"})

        return httpx.Response(404)

    transport = httpx.MockTransport(handler)
    http_client = httpx.Client(transport=transport, base_url="https://example.test")

    class TestClientImpl(GoogleServiceAccountCalendarClient):
        @override
        def _build_assertion(self, *, subject: str, now_epoch: int) -> str:
            del subject, now_epoch
            return "assertion"

    client = TestClientImpl(
        credentials=GoogleServiceAccountCredentials(
            client_email="sa@example.iam.gserviceaccount.com",
            private_key="dummy",
            token_uri="https://example.test/token",
        ),
        impersonation_subject="admin@example.com",
        http_client=http_client,
        max_attempts=3,
        backoff_base_seconds=0.0,
    )

    client.patch_event(
        google_event_id="evt-1",
        initiator_google_email="initiator@example.com",
        payload={"summary": "X"},
        idempotency_key="idem-1",
    )
    client.patch_event(
        google_event_id="evt-1",
        initiator_google_email="initiator@example.com",
        payload={"summary": "X"},
        idempotency_key="idem-1",
    )

    assert token_calls == 1
    assert patch_calls == 2


def test_google_service_account_calendar_client_patch_send_updates_override() -> None:
    token_calls = 0
    patch_requests: list[dict[str, object]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal token_calls
        if request.method == "POST" and request.url.path == "/token":
            token_calls += 1
            return httpx.Response(
                200, json={"access_token": "token-1", "expires_in": 3600}
            )

        if request.method == "PATCH":
            body_obj: object = json.loads(request.content.decode("utf-8"))
            assert isinstance(body_obj, dict)
            patch_requests.append(
                {
                    "send_updates": request.url.params.get("sendUpdates"),
                    "payload": body_obj,
                }
            )
            return httpx.Response(200, json={"id": "evt-1"})

        return httpx.Response(404)

    transport = httpx.MockTransport(handler)
    http_client = httpx.Client(transport=transport, base_url="https://example.test")

    class TestClientImpl(GoogleServiceAccountCalendarClient):
        @override
        def _build_assertion(self, *, subject: str, now_epoch: int) -> str:
            del subject, now_epoch
            return "assertion"

    client = TestClientImpl(
        credentials=GoogleServiceAccountCredentials(
            client_email="sa@example.iam.gserviceaccount.com",
            private_key="dummy",
            token_uri="https://example.test/token",
        ),
        impersonation_subject="admin@example.com",
        http_client=http_client,
        max_attempts=1,
        backoff_base_seconds=0.0,
    )

    client.patch_event(
        google_event_id="evt-1",
        initiator_google_email="initiator@example.com",
        payload={"summary": "X", "_send_updates": "none"},
        idempotency_key="idem-override",
    )
    client.patch_event(
        google_event_id="evt-2",
        initiator_google_email="initiator@example.com",
        payload={"summary": "Y"},
        idempotency_key="idem-default",
    )

    assert token_calls == 1
    assert len(patch_requests) == 2
    assert patch_requests[0]["send_updates"] == "none"
    first_payload_obj = patch_requests[0]["payload"]
    assert isinstance(first_payload_obj, dict)
    assert "_send_updates" not in first_payload_obj
    assert patch_requests[1]["send_updates"] == "all"


def test_google_service_account_calendar_client_falls_back_when_attendee_scoped_omitted_error() -> (
    None
):
    token_calls = 0
    patch_requests: list[dict[str, object]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal token_calls
        if request.method == "POST" and request.url.path == "/token":
            token_calls += 1
            return httpx.Response(
                200, json={"access_token": "token-1", "expires_in": 3600}
            )

        if request.method == "PATCH":
            body_obj: object = json.loads(request.content.decode("utf-8"))
            assert isinstance(body_obj, dict)
            patch_requests.append(
                {
                    "send_updates": request.url.params.get("sendUpdates"),
                    "payload": body_obj,
                }
            )

            if len(patch_requests) == 1:
                return httpx.Response(
                    400,
                    json={
                        "error": {
                            "errors": [
                                {
                                    "domain": "calendar",
                                    "reason": "omittedAttendeesSpecified",
                                    "message": "The request specified attendees that should have been omitted.",
                                }
                            ],
                            "code": 400,
                            "message": "The request specified attendees that should have been omitted.",
                        }
                    },
                )

            return httpx.Response(200, json={"id": "evt-1"})

        return httpx.Response(404)

    transport = httpx.MockTransport(handler)
    http_client = httpx.Client(transport=transport, base_url="https://example.test")

    class TestClientImpl(GoogleServiceAccountCalendarClient):
        @override
        def _build_assertion(self, *, subject: str, now_epoch: int) -> str:
            del subject, now_epoch
            return "assertion"

    client = TestClientImpl(
        credentials=GoogleServiceAccountCredentials(
            client_email="sa@example.iam.gserviceaccount.com",
            private_key="dummy",
            token_uri="https://example.test/token",
        ),
        impersonation_subject="admin@example.com",
        http_client=http_client,
        max_attempts=1,
        backoff_base_seconds=0.0,
    )

    client.patch_event(
        google_event_id="evt-1",
        initiator_google_email="initiator@example.com",
        payload={
            "attendeesOmitted": True,
            "attendees": [{"email": "petya@example.com", "responseStatus": "accepted"}],
            "_send_updates": "none",
        },
        idempotency_key="idem-omitted-fallback",
    )

    assert token_calls == 1
    assert len(patch_requests) == 2
    assert patch_requests[0]["send_updates"] == "none"
    assert patch_requests[1]["send_updates"] == "none"

    first_payload_obj = patch_requests[0]["payload"]
    assert isinstance(first_payload_obj, dict)
    assert first_payload_obj.get("attendeesOmitted") is True

    second_payload_obj = patch_requests[1]["payload"]
    assert isinstance(second_payload_obj, dict)
    assert "attendeesOmitted" not in second_payload_obj


def test_e2e_smoke_webhook_command_scheduler_gateway(tmp_path: Path) -> None:
    now = datetime(2026, 2, 11, 18, 0, 0)
    repository = _repo(tmp_path)
    workflow = MeetingWorkflowService(repository, calendar_gateway=MagicMock())

    meeting_webhook = _meeting(
        now,
        meeting_id="m-4-a",
        confirmation_deadline_at=now + timedelta(minutes=20),
    )
    repository.insert_meeting(meeting_webhook, now=now)

    token_service = CallbackTokenService(repository)
    confirm_button = token_service.issue_callback_button(
        meeting=meeting_webhook,
        action_type=CallbackActionType.PARTICIPANT_CONFIRM,
        allowed_user_id=200,
        now=now,
        text="Подтвердить",
    )

    adapter = TelegramWebhookAdapter(repository=repository, workflow_service=workflow)
    app = create_webhook_app(adapter=adapter, now_provider=lambda: now)
    http = TestClient(app)
    callback_response = http.post(
        "/telegram/webhook",
        json={
            "update_id": 100,
            "callback_query": {
                "id": "cb-e2e-1",
                "from": {"id": 200},
                "data": confirm_button.callback_data,
            },
        },
    )
    assert callback_response.status_code == 200
    assert callback_response.json()["outcome"] == "ok"

    meeting_scheduler = _meeting(
        now,
        meeting_id="m-4-b",
        confirmation_deadline_at=now - DEADLINE_GRACE_WINDOW - timedelta(seconds=1),
    )
    repository.insert_meeting(meeting_scheduler, now=now)
    repository.enqueue_jobs(
        (
            ScheduledJobSpec(
                job_type=JobType.CONFIRM_DEADLINE,
                meeting_id=meeting_scheduler.meeting_id,
                round=meeting_scheduler.confirmation_round,
                run_at=now - timedelta(seconds=1),
            ),
        ),
        now=now,
    )

    worker = SchedulerWorker(repository=repository, service=workflow)
    tick = worker.run_once(now=now)
    assert tick.processed is True

    class FakeCalendarClient:
        def __init__(self) -> None:
            self.calls: list[dict[str, object]] = []

        def query_free_busy(
            self,
            *,
            emails: tuple[str, ...],
            time_min: datetime,
            time_max: datetime,
        ) -> dict[str, list[tuple[datetime, datetime]]]:
            del time_min, time_max
            return {email: [] for email in emails}

        def insert_event(
            self,
            *,
            organizer_email: str,
            payload: dict[str, object],
            idempotency_key: str | None = None,
        ) -> str:
            del organizer_email, payload, idempotency_key
            return "evt-fake"

        def patch_event(
            self,
            *,
            google_event_id: str,
            initiator_google_email: str,
            payload: dict[str, object],
            idempotency_key: str | None = None,
        ) -> None:
            self.calls.append(
                {
                    "google_event_id": google_event_id,
                    "initiator_google_email": initiator_google_email,
                    "payload": payload,
                    "idempotency_key": idempotency_key,
                }
            )

        def list_events(
            self,
            *,
            email: str,
            time_min: datetime,
            time_max: datetime,
            max_results: int = 100,
        ) -> list[dict[str, object]]:
            del email, time_min, time_max, max_results
            return []

    calendar_client = FakeCalendarClient()
    gateway = GoogleCalendarGateway(  # pyright: ignore[reportArgumentType]
        api_client=cast(CalendarApiClient, calendar_client)
    )

    confirmed = repository.get_meeting(meeting_webhook.meeting_id)
    assert confirmed is not None
    assert confirmed.state == MeetingState.CONFIRMED

    gateway_result = gateway.patch_event_for_meeting(
        meeting=confirmed,
        google_event_id="evt-42",
        initiator_google_email="initiator@example.com",
        payload={"summary": "Synced"},
        idempotency_key="m-4-a:r1:confirmed",
    )

    assert gateway_result.outcome.value == "ok"
    assert len(calendar_client.calls) == 1

    transitioned = repository.get_meeting(meeting_scheduler.meeting_id)
    assert transitioned is not None
    assert transitioned.state == MeetingState.NEEDS_INITIATOR_DECISION
    repository.close()
