from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

from bot_vstrechi.db.repository import SQLiteRepository
from bot_vstrechi.domain.models import (
    Decision,
    JobStatus,
    JobType,
    Meeting,
    MeetingParticipant,
    MeetingState,
    OutboxEffectType,
    ScheduledJobSpec,
)


def _repo(tmp_path: Path) -> SQLiteRepository:
    repository = SQLiteRepository(str(tmp_path / "bot_vstrechi_calendar_first.db"))
    repository.initialize_schema()
    return repository


def _meeting(now: datetime, *, meeting_id: str = "m-calendar-first") -> Meeting:
    return Meeting(
        meeting_id=meeting_id,
        initiator_telegram_user_id=100,
        chat_id=100,
        state=MeetingState.PENDING,
        scheduled_start_at=now + timedelta(hours=4),
        scheduled_end_at=now + timedelta(hours=5),
        confirmation_deadline_at=now + timedelta(hours=2),
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
            MeetingParticipant(
                telegram_user_id=300,
                is_required=True,
                decision=Decision.CONFIRM,
                decision_received_at=now,
            ),
        ),
    )


def test_new_tables_and_columns_exist(tmp_path: Path) -> None:
    repository = _repo(tmp_path)

    table_rows = repository._conn.execute(  # noqa: SLF001
        "SELECT name FROM sqlite_master WHERE type = 'table'"
    ).fetchall()
    table_names = {str(row[0]) for row in table_rows}
    assert "manager_role" in table_names
    assert "calendar_sync_state" in table_names

    meeting_columns = {
        str(row[1])
        for row in repository._conn.execute("PRAGMA table_info(meeting)").fetchall()  # noqa: SLF001
    }
    assert "series_event_id" in meeting_columns
    assert "occurrence_start_at" in meeting_columns
    assert "group_status_message_id" in meeting_columns
    assert "recurring_confirmation_mode" in meeting_columns
    repository.close()


def test_cannot_revoke_last_manager(tmp_path: Path) -> None:
    now = datetime(2026, 2, 20, 10, 0, 0)
    repository = _repo(tmp_path)

    repository.grant_manager_role(telegram_user_id=100, granted_by=None, now=now)
    assert repository.list_active_manager_ids() == (100,)

    revoked_last = repository.revoke_manager_role(
        telegram_user_id=100,
        revoked_by=100,
        now=now + timedelta(seconds=1),
    )
    assert revoked_last is False

    repository.grant_manager_role(
        telegram_user_id=200,
        granted_by=100,
        now=now + timedelta(seconds=2),
    )
    revoked = repository.revoke_manager_role(
        telegram_user_id=100,
        revoked_by=200,
        now=now + timedelta(seconds=3),
    )
    assert revoked is True
    assert repository.list_active_manager_ids() == (200,)
    repository.close()


def test_cancel_all_jobs_for_round(tmp_path: Path) -> None:
    now = datetime(2026, 2, 20, 10, 0, 0)
    repository = _repo(tmp_path)
    meeting = _meeting(now)
    repository.insert_meeting(meeting, now=now)

    repository.enqueue_jobs(
        (
            ScheduledJobSpec(
                job_type=JobType.REMINDER,
                meeting_id=meeting.meeting_id,
                round=meeting.confirmation_round,
                run_at=now + timedelta(minutes=5),
            ),
            ScheduledJobSpec(
                job_type=JobType.CONFIRM_DEADLINE,
                meeting_id=meeting.meeting_id,
                round=meeting.confirmation_round,
                run_at=now + timedelta(minutes=10),
            ),
            ScheduledJobSpec(
                job_type=JobType.REMINDER,
                meeting_id=meeting.meeting_id,
                round=meeting.confirmation_round + 1,
                run_at=now + timedelta(minutes=15),
            ),
        ),
        now=now,
    )

    cancelled = repository.cancel_jobs_for_meeting_round(
        meeting_id=meeting.meeting_id,
        round=meeting.confirmation_round,
        now=now + timedelta(seconds=1),
    )
    assert cancelled == 2

    cancelled_rows = repository._conn.execute(  # noqa: SLF001
        """
        SELECT COUNT(*)
        FROM job
        WHERE meeting_id = ? AND round = ? AND status = ?
        """,
        (meeting.meeting_id, meeting.confirmation_round, JobStatus.CANCELLED),
    ).fetchone()
    assert cancelled_rows is not None
    assert int(cancelled_rows[0]) == 2
    repository.close()


def test_unresolved_required_participant_ids(tmp_path: Path) -> None:
    now = datetime(2026, 2, 20, 10, 0, 0)
    repository = _repo(tmp_path)
    meeting = _meeting(now)
    repository.insert_meeting(meeting, now=now)

    unresolved = repository.get_unresolved_required_participant_ids(
        meeting_id=meeting.meeting_id
    )
    assert unresolved == (200,)
    repository.close()


def test_calendar_sync_state_roundtrip(tmp_path: Path) -> None:
    now = datetime(2026, 2, 20, 10, 0, 0)
    repository = _repo(tmp_path)

    repository.upsert_calendar_sync_state(
        calendar_id="primary",
        sync_token="sync-1",
        watch_channel_id="ch-1",
        watch_resource_id="res-1",
        watch_expiration_at=now + timedelta(days=7),
        last_message_number=42,
        now=now,
    )

    state = repository.get_calendar_sync_state(calendar_id="primary")
    assert state is not None
    assert state["sync_token"] == "sync-1"
    assert state["watch_channel_id"] == "ch-1"
    assert state["last_message_number"] == 42
    repository.close()


def test_has_open_meeting_for_series(tmp_path: Path) -> None:
    now = datetime(2026, 2, 20, 10, 0, 0)
    repository = _repo(tmp_path)
    meeting = Meeting(
        meeting_id="m-series-open",
        initiator_telegram_user_id=100,
        chat_id=100,
        state=MeetingState.PENDING,
        title="Daily",
        scheduled_start_at=now + timedelta(hours=1),
        scheduled_end_at=now + timedelta(hours=2),
        series_event_id="series-abc",
        participants=(
            MeetingParticipant(
                telegram_user_id=100, is_required=False, decision=Decision.NONE
            ),
        ),
    )
    repository.insert_meeting(meeting, now=now)

    assert repository.has_open_meeting_for_series(series_event_id="series-abc", now=now)
    assert not repository.has_open_meeting_for_series(
        series_event_id="series-missing", now=now
    )
    repository.close()


def test_outbox_claim_prioritizes_callback_and_edit(tmp_path: Path) -> None:
    now = datetime(2026, 2, 20, 10, 0, 0)
    repository = _repo(tmp_path)

    _ = repository.enqueue_outbox(
        effect_type=OutboxEffectType.TELEGRAM_SEND_MESSAGE,
        payload={"telegram_user_id": 100, "text": "send"},
        idempotency_key="prio-send",
        now=now,
    )
    _ = repository.enqueue_outbox(
        effect_type=OutboxEffectType.TELEGRAM_EDIT_MESSAGE,
        payload={"telegram_user_id": 100, "message_id": 1, "text": "edit"},
        idempotency_key="prio-edit",
        now=now,
    )
    _ = repository.enqueue_outbox(
        effect_type=OutboxEffectType.TELEGRAM_ANSWER_CALLBACK,
        payload={"callback_query_id": "cb-1"},
        idempotency_key="prio-answer",
        now=now,
    )

    first = repository.claim_due_outbox(now=now)
    second = repository.claim_due_outbox(now=now)
    third = repository.claim_due_outbox(now=now)

    assert first is not None
    assert second is not None
    assert third is not None
    assert first.effect_type == OutboxEffectType.TELEGRAM_ANSWER_CALLBACK
    assert second.effect_type == OutboxEffectType.TELEGRAM_EDIT_MESSAGE
    assert third.effect_type == OutboxEffectType.TELEGRAM_SEND_MESSAGE
    repository.close()
