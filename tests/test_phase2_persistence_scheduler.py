from __future__ import annotations
from unittest.mock import MagicMock

from dataclasses import replace
from datetime import datetime, timedelta
from pathlib import Path
import sqlite3

from bot_vstrechi.domain import (
    Decision,
    InboundEventSource,
    JobStatus,
    JobType,
    Meeting,
    MeetingParticipant,
    MeetingState,
    ScheduledJobSpec,
)
from bot_vstrechi.domain.policies import DEADLINE_GRACE_WINDOW
from bot_vstrechi.db.repository import SQLiteRepository
from bot_vstrechi.application.service import MeetingWorkflowService
from bot_vstrechi.workers.scheduler import SchedulerWorker


def _repo(tmp_path: Path) -> SQLiteRepository:
    repo = SQLiteRepository(str(tmp_path / "bot_vstrechi.db"))
    repo.initialize_schema()
    return repo


def _meeting(now: datetime, *, state: MeetingState = MeetingState.PENDING) -> Meeting:
    return Meeting(
        meeting_id="m-2",
        initiator_telegram_user_id=100,
        chat_id=100,
        state=state,
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


def test_inbound_event_dedup(tmp_path: Path) -> None:
    now = datetime(2026, 2, 11, 12, 0, 0)
    repo = _repo(tmp_path)

    first = repo.register_inbound_event(
        source=InboundEventSource.TELEGRAM_CALLBACK,
        external_event_id="cb-1",
        received_at=now,
    )
    second = repo.register_inbound_event(
        source=InboundEventSource.TELEGRAM_CALLBACK,
        external_event_id="cb-1",
        received_at=now,
    )

    assert first is True
    assert second is False
    repo.close()


def test_job_claiming_and_completion(tmp_path: Path) -> None:
    now = datetime(2026, 2, 11, 12, 0, 0)
    repo = _repo(tmp_path)
    meeting = _meeting(now)
    repo.insert_meeting(meeting, now=now)

    repo.enqueue_jobs(
        (
            ScheduledJobSpec(
                job_type=JobType.CONFIRM_DEADLINE,
                meeting_id=meeting.meeting_id,
                round=meeting.confirmation_round,
                run_at=now - timedelta(seconds=1),
            ),
        ),
        now=now,
    )

    claimed = repo.claim_due_job(now=now)
    assert claimed is not None
    assert claimed.job_type == JobType.CONFIRM_DEADLINE

    repo.mark_job_done(job_id=claimed.job_id)
    assert (
        repo.count_jobs(
            meeting_id=meeting.meeting_id,
            status=JobStatus.DONE,
            job_type=JobType.CONFIRM_DEADLINE,
        )
        == 1
    )
    repo.close()


def test_scheduler_deadline_moves_to_needs_initiator_decision(tmp_path: Path) -> None:
    now = datetime(2026, 2, 11, 12, 0, 0)
    repo = _repo(tmp_path)
    meeting = _meeting(now)
    meeting = meeting.with_participants(
        (
            MeetingParticipant(
                telegram_user_id=100,
                is_required=True,
                decision=Decision.CONFIRM,
                decision_received_at=now - timedelta(minutes=1),
            ),
            MeetingParticipant(
                telegram_user_id=200,
                is_required=True,
                decision=Decision.NONE,
            ),
        )
    )
    meeting = replace(
        meeting,
        confirmation_deadline_at=now - DEADLINE_GRACE_WINDOW - timedelta(seconds=1),
    )
    repo.insert_meeting(meeting, now=now)

    repo.enqueue_jobs(
        (
            ScheduledJobSpec(
                job_type=JobType.CONFIRM_DEADLINE,
                meeting_id=meeting.meeting_id,
                round=meeting.confirmation_round,
                run_at=now - timedelta(seconds=1),
            ),
        ),
        now=now,
    )

    service = MeetingWorkflowService(repo, calendar_gateway=MagicMock())
    worker = SchedulerWorker(repo, service)
    tick = worker.run_once(now=now)

    updated = repo.get_meeting(meeting.meeting_id)
    assert tick.processed is True
    assert updated is not None
    assert updated.state == MeetingState.NEEDS_INITIATOR_DECISION
    assert updated.initiator_decision_deadline_at == now + timedelta(minutes=15)
    assert (
        repo.count_jobs(
            meeting_id=meeting.meeting_id,
            status=JobStatus.PENDING,
            job_type=JobType.INITIATOR_TIMEOUT,
        )
        == 1
    )
    repo.close()


def test_scheduler_initiator_timeout_cancels_meeting(tmp_path: Path) -> None:
    now = datetime(2026, 2, 11, 12, 0, 0)
    repo = _repo(tmp_path)
    meeting = _meeting(now, state=MeetingState.NEEDS_INITIATOR_DECISION)
    meeting = replace(
        meeting,
        initiator_decision_deadline_at=now - timedelta(seconds=1),
        confirmation_deadline_at=None,
    )
    repo.insert_meeting(meeting, now=now)

    repo.enqueue_jobs(
        (
            ScheduledJobSpec(
                job_type=JobType.INITIATOR_TIMEOUT,
                meeting_id=meeting.meeting_id,
                round=meeting.confirmation_round,
                run_at=now - timedelta(seconds=1),
            ),
        ),
        now=now,
    )

    service = MeetingWorkflowService(repo, calendar_gateway=MagicMock())
    worker = SchedulerWorker(repo, service)
    tick = worker.run_once(now=now)

    updated = repo.get_meeting(meeting.meeting_id)
    assert tick.processed is True
    assert updated is not None
    assert updated.state == MeetingState.CANCELLED
    assert (
        repo.count_jobs(
            meeting_id=meeting.meeting_id,
            status=JobStatus.DONE,
            job_type=JobType.INITIATOR_TIMEOUT,
        )
        == 1
    )
    repo.close()


def test_initialize_schema_migrates_legacy_db_columns(tmp_path: Path) -> None:
    now = datetime(2026, 2, 11, 12, 0, 0)
    db_path = tmp_path / "bot_vstrechi_legacy.db"

    conn = sqlite3.connect(str(db_path))
    _ = conn.executescript(
        """
        CREATE TABLE user_mapping (
            telegram_user_id INTEGER PRIMARY KEY,
            telegram_username TEXT,
            google_email TEXT NOT NULL UNIQUE,
            is_active INTEGER NOT NULL DEFAULT 1,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE meeting (
            meeting_id TEXT PRIMARY KEY,
            initiator_telegram_user_id INTEGER NOT NULL,
            state TEXT NOT NULL,
            state_updated_at TEXT NOT NULL,
            title TEXT NOT NULL DEFAULT '',
            scheduled_start_at TEXT NOT NULL,
            scheduled_end_at TEXT NOT NULL,
            confirmation_deadline_at TEXT,
            initiator_decision_deadline_at TEXT,
            confirmation_round INTEGER NOT NULL,
            created_by_bot INTEGER NOT NULL,
            updated_at TEXT NOT NULL
        );
        """
    )

    _ = conn.execute(
        """
        INSERT INTO user_mapping (
            telegram_user_id,
            telegram_username,
            google_email,
            is_active,
            updated_at
        ) VALUES (?, ?, ?, ?, ?)
        """,
        (100, "initiator", "initiator@4sell.ai", 1, now.isoformat(timespec="seconds")),
    )
    _ = conn.execute(
        """
        INSERT INTO meeting (
            meeting_id,
            initiator_telegram_user_id,
            state,
            state_updated_at,
            title,
            scheduled_start_at,
            scheduled_end_at,
            confirmation_deadline_at,
            initiator_decision_deadline_at,
            confirmation_round,
            created_by_bot,
            updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "legacy-m-1",
            100,
            "pending",
            now.isoformat(timespec="seconds"),
            "Legacy",
            (now + timedelta(hours=1)).isoformat(timespec="seconds"),
            (now + timedelta(hours=2)).isoformat(timespec="seconds"),
            (now + timedelta(minutes=20)).isoformat(timespec="seconds"),
            None,
            1,
            1,
            now.isoformat(timespec="seconds"),
        ),
    )
    conn.commit()
    conn.close()

    repo = SQLiteRepository(str(db_path))
    repo.initialize_schema()

    mapping = repo.get_user_mapping(100)
    assert mapping is not None
    assert mapping["timezone"] == "Asia/Yekaterinburg"

    meeting = repo.get_meeting("legacy-m-1")
    assert meeting is not None
    assert meeting.chat_id == 100
    assert meeting.google_event_id is None
    assert meeting.google_calendar_id is None
    repo.close()


def test_conversation_state_roundtrip_and_expiry(tmp_path: Path) -> None:
    now = datetime(2026, 2, 11, 12, 0, 0)
    repo = _repo(tmp_path)

    repo.upsert_conversation_state(
        chat_id=100,
        user_id=100,
        flow="meet",
        state={"mode": "await_manual_time", "meeting_id": "m-1"},
        expires_at=now + timedelta(minutes=5),
        now=now,
    )

    state = repo.get_conversation_state(
        chat_id=100,
        user_id=100,
        flow="meet",
        now=now + timedelta(minutes=1),
    )
    assert state is not None
    assert state["mode"] == "await_manual_time"

    expired = repo.get_conversation_state(
        chat_id=100,
        user_id=100,
        flow="meet",
        now=now + timedelta(minutes=10),
    )
    assert expired is None
    repo.close()
