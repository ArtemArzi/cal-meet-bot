from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
import threading
import time
from types import SimpleNamespace

from fastapi.testclient import TestClient
from pytest import MonkeyPatch
import bot_vstrechi.infrastructure.runtime as runtime_module

from bot_vstrechi.domain import (
    Decision,
    JobStatus,
    JobType,
    Meeting,
    MeetingParticipant,
    MeetingState,
    OutboxEffectType,
    OutboxStatus,
    Outcome,
    ReasonCode,
    ScheduledJobSpec,
)
from bot_vstrechi.db.repository import SQLiteRepository
from bot_vstrechi.infrastructure.runtime import create_application, create_runtime
from bot_vstrechi.telegram.adapter import TelegramAdapterResult, TelegramWebhookAdapter


def _meeting(now: datetime, *, meeting_id: str) -> Meeting:
    return Meeting(
        meeting_id=meeting_id,
        initiator_telegram_user_id=100,
        chat_id=100,
        state=MeetingState.PENDING,
        scheduled_start_at=now + timedelta(hours=1),
        scheduled_end_at=now + timedelta(hours=2),
        confirmation_deadline_at=now + timedelta(minutes=20),
        participants=(
            MeetingParticipant(
                telegram_user_id=100,
                is_required=True,
                decision=Decision.NONE,
            ),
            MeetingParticipant(
                telegram_user_id=200,
                is_required=True,
                decision=Decision.NONE,
            ),
        ),
    )


def test_runtime_wires_core_components(tmp_path: Path) -> None:
    now = datetime(2026, 2, 12, 9, 0, 0)
    runtime = create_runtime(
        db_path=str(tmp_path / "phase5-runtime.db"),
        now_provider=lambda: now,
    )

    assert isinstance(runtime.repository, SQLiteRepository)
    assert isinstance(runtime.telegram_adapter, TelegramWebhookAdapter)

    recovered = runtime.startup()
    assert recovered == 0

    runtime.shutdown()


def test_application_lifespan_reconciles_stale_running_jobs(tmp_path: Path) -> None:
    base_now = datetime(2026, 2, 12, 9, 0, 0)
    db_path = str(tmp_path / "phase5-app.db")

    repository = SQLiteRepository(db_path)
    repository.initialize_schema()

    meeting = _meeting(base_now, meeting_id="m-5-runtime")
    meeting = replace(meeting, confirmation_deadline_at=base_now - timedelta(seconds=1))
    repository.insert_meeting(meeting, now=base_now)
    repository.enqueue_jobs(
        (
            ScheduledJobSpec(
                job_type=JobType.CONFIRM_DEADLINE,
                meeting_id=meeting.meeting_id,
                round=meeting.confirmation_round,
                run_at=base_now - timedelta(seconds=1),
            ),
        ),
        now=base_now,
    )
    claimed = repository.claim_due_job(now=base_now)
    assert claimed is not None
    assert (
        repository.count_jobs(
            meeting_id=meeting.meeting_id,
            status=JobStatus.RUNNING,
            job_type=JobType.CONFIRM_DEADLINE,
        )
        == 1
    )

    _ = repository.enqueue_outbox(
        effect_type=OutboxEffectType.TELEGRAM_SEND_MESSAGE,
        payload={"telegram_user_id": 200, "text": "ping"},
        now=base_now,
    )
    claimed_outbox = repository.claim_due_outbox(now=base_now)
    assert claimed_outbox is not None
    assert repository.count_outbox(status=OutboxStatus.RUNNING) == 1
    repository.close()

    app = create_application(
        db_path=db_path,
        now_provider=lambda: base_now + timedelta(minutes=6),
    )

    with TestClient(app) as client:
        response = client.post("/telegram/webhook", json={"update_id": 501})
        assert response.status_code == 200

    post_startup_repo = SQLiteRepository(db_path)
    assert (
        post_startup_repo.count_jobs(
            meeting_id=meeting.meeting_id,
            status=JobStatus.RUNNING,
            job_type=JobType.CONFIRM_DEADLINE,
        )
        == 0
    )
    assert (
        post_startup_repo.count_jobs(
            meeting_id=meeting.meeting_id,
            status=JobStatus.PENDING,
            job_type=JobType.CONFIRM_DEADLINE,
        )
        == 1
    )
    assert post_startup_repo.count_outbox(status=OutboxStatus.RUNNING) == 0
    assert post_startup_repo.count_outbox(status=OutboxStatus.PENDING) == 1
    post_startup_repo.close()


def test_application_lifespan_reconciles_stale_running_calendar_sync_signals(
    tmp_path: Path,
) -> None:
    base_now = datetime(2026, 2, 12, 9, 0, 0)
    db_path = str(tmp_path / "phase5-app-calendar-sync.db")

    repository = SQLiteRepository(db_path)
    repository.initialize_schema()
    _ = repository.enqueue_calendar_sync_signal(
        calendar_id="init@example.com",
        external_event_id="sync-signal-1",
        resource_state="poll",
        message_number=77,
        now=base_now,
    )
    claimed_signal = repository.claim_due_calendar_sync_signal(now=base_now)
    assert claimed_signal is not None
    assert repository.count_calendar_sync_signals(status=OutboxStatus.RUNNING) == 1
    repository.close()

    app = create_application(
        db_path=db_path,
        now_provider=lambda: base_now + timedelta(minutes=6),
    )

    with TestClient(app) as client:
        response = client.post("/telegram/webhook", json={"update_id": 777})
        assert response.status_code == 200

    post_startup_repo = SQLiteRepository(db_path)
    assert (
        post_startup_repo.count_calendar_sync_signals(status=OutboxStatus.RUNNING) == 0
    )
    assert (
        post_startup_repo.count_calendar_sync_signals(status=OutboxStatus.PENDING) == 1
    )
    post_startup_repo.close()


def test_runtime_uses_injected_telegram_client_for_outbox(tmp_path: Path) -> None:
    now = datetime(2026, 2, 12, 9, 0, 0)
    sent_messages: list[tuple[int, str, str | None]] = []

    class FakeTelegramClient:
        def send_message(
            self,
            *,
            telegram_user_id: int,
            text: str,
            buttons: list[dict[str, str] | list[dict[str, str]]] | None = None,
            keyboard: list[list[str]] | None = None,
            idempotency_key: str | None = None,
        ) -> None:
            del buttons, keyboard
            sent_messages.append((telegram_user_id, text, idempotency_key))

        def edit_message(
            self,
            *,
            telegram_user_id: int,
            message_id: int,
            text: str,
            buttons: list[dict[str, str] | list[dict[str, str]]] | None = None,
            idempotency_key: str | None = None,
        ) -> None:
            del telegram_user_id, message_id, text, buttons, idempotency_key

        def answer_callback_query(
            self,
            *,
            callback_query_id: str,
            text: str | None = None,
            idempotency_key: str | None = None,
        ) -> None:
            del callback_query_id, text, idempotency_key

    runtime = create_runtime(
        db_path=str(tmp_path / "phase5-runtime-live-client.db"),
        now_provider=lambda: now,
        telegram_client=FakeTelegramClient(),
    )
    _ = runtime.startup()

    _ = runtime.repository.enqueue_outbox(
        effect_type=OutboxEffectType.TELEGRAM_SEND_MESSAGE,
        payload={"telegram_user_id": 200, "text": "hello"},
        idempotency_key="notify:1",
        now=now,
    )

    tick = runtime.run_outbox_once()
    assert tick.processed is True
    assert sent_messages == [(200, "hello", "notify:1")]

    runtime.shutdown()


def test_runtime_startup_configures_telegram_ui_when_supported(tmp_path: Path) -> None:
    now = datetime(2026, 2, 12, 9, 0, 0)
    ui_setup_calls = 0

    class FakeTelegramClient:
        def configure_bot_ui(self) -> None:
            nonlocal ui_setup_calls
            ui_setup_calls += 1

        def send_message(
            self,
            *,
            telegram_user_id: int,
            text: str,
            buttons: list[dict[str, str] | list[dict[str, str]]] | None = None,
            keyboard: list[list[str]] | None = None,
            idempotency_key: str | None = None,
        ) -> None:
            del telegram_user_id, text, buttons, keyboard, idempotency_key

        def edit_message(
            self,
            *,
            telegram_user_id: int,
            message_id: int,
            text: str,
            buttons: list[dict[str, str] | list[dict[str, str]]] | None = None,
            idempotency_key: str | None = None,
        ) -> None:
            del telegram_user_id, message_id, text, buttons, idempotency_key

        def answer_callback_query(
            self,
            *,
            callback_query_id: str,
            text: str | None = None,
            idempotency_key: str | None = None,
        ) -> None:
            del callback_query_id, text, idempotency_key

    runtime = create_runtime(
        db_path=str(tmp_path / "phase5-runtime-ui.db"),
        now_provider=lambda: now,
        telegram_client=FakeTelegramClient(),
    )

    _ = runtime.startup()
    assert ui_setup_calls == 1
    runtime.shutdown()


def test_background_polling_loop_yields_under_continuous_work(
    monkeypatch: MonkeyPatch, tmp_path: Path
) -> None:
    class FakeRepository:
        def check_connection(self) -> bool:
            return True

    class FakeAdapter:
        def handle_update(
            self, *, update: object, now: datetime
        ) -> TelegramAdapterResult:
            del update, now
            return TelegramAdapterResult(
                outcome=Outcome.OK,
                reason_code=ReasonCode.UPDATED,
                message="ok",
            )

    class FakeRuntime:
        repository: FakeRepository
        telegram_adapter: FakeAdapter

        def __init__(self) -> None:
            self.repository = FakeRepository()
            self.telegram_adapter = FakeAdapter()

        def now(self) -> datetime:
            return datetime(2026, 2, 12, 9, 0, 0, tzinfo=timezone.utc)

        def startup(self) -> int:
            return 0

        def shutdown(self) -> None:
            return None

        def run_worker_once(self):
            return SimpleNamespace(processed=True)

        def run_outbox_once(self):
            return SimpleNamespace(processed=False)

    runtime = FakeRuntime()

    def _create_runtime_stub(**_: object) -> FakeRuntime:
        return runtime

    monkeypatch.setattr(runtime_module, "create_runtime", _create_runtime_stub)

    app = create_application(
        db_path=str(tmp_path / "phase5-app-polling.db"),
        run_background_workers=True,
        background_poll_interval_seconds=0.1,
    )

    errors: list[Exception] = []
    response_status: list[int] = []

    def _run_client() -> None:
        try:
            with TestClient(app) as client:
                response = client.get("/health")
                response_status.append(response.status_code)
        except Exception as error:  # pragma: no cover - assertion below checks this
            errors.append(error)

    thread = threading.Thread(target=_run_client, daemon=True)
    thread.start()
    thread.join(timeout=1.5)

    assert not thread.is_alive(), "background polling loop blocked request handling"
    assert errors == []
    assert response_status == [200]


def test_background_polling_loop_runs_retention_tasks(
    monkeypatch: MonkeyPatch, tmp_path: Path
) -> None:
    class FakeRepository:
        def __init__(self) -> None:
            self.cleanup_calls = 0
            self.checkpoint_calls = 0
            self.vacuum_calls = 0

        def check_connection(self) -> bool:
            return True

        def get_all_active_users(self) -> list[dict[str, object]]:
            return []

        def enqueue_calendar_sync_signal(self, **_: object) -> bool:
            return True

        def cleanup_retention(self, **_: object):
            self.cleanup_calls += 1
            return SimpleNamespace(
                calendar_sync_signals_deleted=0,
                outbox_deleted=0,
                jobs_deleted=0,
                audit_logs_deleted=0,
                inbound_events_deleted=0,
            )

        def wal_checkpoint(self, **_: object):
            self.checkpoint_calls += 1
            return (0, 0, 0)

        def vacuum(self) -> None:
            self.vacuum_calls += 1

    class FakeAdapter:
        def handle_update(
            self, *, update: object, now: datetime
        ) -> TelegramAdapterResult:
            del update, now
            return TelegramAdapterResult(
                outcome=Outcome.OK,
                reason_code=ReasonCode.UPDATED,
                message="ok",
            )

    class FakeRuntime:
        repository: FakeRepository
        telegram_adapter: FakeAdapter

        def __init__(self) -> None:
            self.repository = FakeRepository()
            self.telegram_adapter = FakeAdapter()

        def now(self) -> datetime:
            return datetime.now(tz=timezone.utc)

        def startup(self) -> int:
            return 0

        def shutdown(self) -> None:
            return None

        def run_worker_once(self):
            return SimpleNamespace(processed=False)

        def run_outbox_once(self):
            return SimpleNamespace(processed=False)

        def run_calendar_sync_once(self):
            return SimpleNamespace(processed=False)

    runtime = FakeRuntime()

    def _create_runtime_stub(**_: object) -> FakeRuntime:
        return runtime

    monkeypatch.setattr(runtime_module, "create_runtime", _create_runtime_stub)

    app = create_application(
        db_path=str(tmp_path / "phase5-app-retention-loop.db"),
        run_background_workers=True,
        background_poll_interval_seconds=0.05,
        calendar_poll_interval_seconds=60,
        retention_cleanup_interval_seconds=0.05,
        retention_checkpoint_interval_seconds=0.05,
        retention_vacuum_interval_seconds=0.05,
    )

    with TestClient(app) as client:
        response = client.get("/health")
        assert response.status_code == 200
        time.sleep(0.2)

    assert runtime.repository.cleanup_calls >= 1
    assert runtime.repository.checkpoint_calls >= 1
    assert runtime.repository.vacuum_calls >= 1
