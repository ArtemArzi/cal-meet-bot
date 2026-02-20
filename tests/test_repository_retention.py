from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

from bot_vstrechi.db.repository import SQLiteRepository
from bot_vstrechi.domain.models import (
    InboundEventSource,
    JobStatus,
    Meeting,
    MeetingState,
    OutboxStatus,
)


def _iso(value: datetime) -> str:
    return value.isoformat(timespec="seconds")


def test_repository_retention_cleanup_deletes_only_terminal_old_rows(
    tmp_path: Path,
) -> None:
    now = datetime(2026, 2, 21, 12, 0, 0)
    old = now - timedelta(days=20)
    recent = now - timedelta(days=1)

    repository = SQLiteRepository(str(tmp_path / "retention.db"))
    repository.initialize_schema()
    meeting = Meeting(
        meeting_id="m-retention",
        initiator_telegram_user_id=100,
        chat_id=100,
        state=MeetingState.PENDING,
        scheduled_start_at=now + timedelta(hours=1),
        scheduled_end_at=now + timedelta(hours=2),
        title="Retention",
    )
    repository.insert_meeting(meeting, now=now)

    _ = repository._conn.execute(
        """
        INSERT INTO calendar_sync_signal (
            calendar_id, external_event_id, resource_state, message_number,
            run_after, status, attempts, locked_at, last_error, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, 0, NULL, NULL, ?, ?)
        """,
        (
            "init@example.com",
            "ret-old-done",
            "poll",
            None,
            _iso(old),
            OutboxStatus.DONE,
            _iso(old),
            _iso(old),
        ),
    )
    _ = repository._conn.execute(
        """
        INSERT INTO calendar_sync_signal (
            calendar_id, external_event_id, resource_state, message_number,
            run_after, status, attempts, locked_at, last_error, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, 0, NULL, NULL, ?, ?)
        """,
        (
            "init@example.com",
            "ret-old-pending",
            "poll",
            None,
            _iso(old),
            OutboxStatus.PENDING,
            _iso(old),
            _iso(old),
        ),
    )

    _ = repository._conn.execute(
        """
        INSERT INTO outbox (
            effect_type, payload_json, idempotency_key, run_after, status,
            attempts, locked_at, last_error, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, 0, NULL, NULL, ?, ?)
        """,
        (
            "telegram_send_message",
            '{"telegram_user_id":100,"text":"old done"}',
            "outbox-old-done",
            _iso(old),
            OutboxStatus.DONE,
            _iso(old),
            _iso(old),
        ),
    )
    _ = repository._conn.execute(
        """
        INSERT INTO outbox (
            effect_type, payload_json, idempotency_key, run_after, status,
            attempts, locked_at, last_error, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, 0, NULL, NULL, ?, ?)
        """,
        (
            "telegram_send_message",
            '{"telegram_user_id":100,"text":"old pending"}',
            "outbox-old-pending",
            _iso(old),
            OutboxStatus.PENDING,
            _iso(old),
            _iso(old),
        ),
    )
    _ = repository._conn.execute(
        """
        INSERT INTO outbox (
            effect_type, payload_json, idempotency_key, run_after, status,
            attempts, locked_at, last_error, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, 0, NULL, NULL, ?, ?)
        """,
        (
            "telegram_send_message",
            '{"telegram_user_id":100,"text":"recent done"}',
            "outbox-recent-done",
            _iso(recent),
            OutboxStatus.DONE,
            _iso(recent),
            _iso(recent),
        ),
    )

    _ = repository._conn.execute(
        """
        INSERT INTO job (
            job_type, meeting_id, round, run_at, status, attempts, locked_at,
            last_error, created_at
        ) VALUES (?, ?, ?, ?, ?, 0, NULL, NULL, ?)
        """,
        (
            "reminder",
            meeting.meeting_id,
            1,
            _iso(old),
            JobStatus.CANCELLED,
            _iso(old),
        ),
    )
    _ = repository._conn.execute(
        """
        INSERT INTO job (
            job_type, meeting_id, round, run_at, status, attempts, locked_at,
            last_error, created_at
        ) VALUES (?, ?, ?, ?, ?, 0, NULL, NULL, ?)
        """,
        (
            "reminder",
            meeting.meeting_id,
            2,
            _iso(old),
            JobStatus.PENDING,
            _iso(old),
        ),
    )

    repository.insert_audit_log(
        meeting_id=meeting.meeting_id,
        round=1,
        actor_telegram_user_id=100,
        actor_type="system",
        action="old",
        details={"k": "v"},
        now=old,
    )
    repository.insert_audit_log(
        meeting_id=meeting.meeting_id,
        round=1,
        actor_telegram_user_id=100,
        actor_type="system",
        action="recent",
        details={"k": "v"},
        now=recent,
    )

    _ = repository.register_inbound_event(
        source=InboundEventSource.GOOGLE_POLL,
        external_event_id="inbound-old",
        received_at=old,
    )
    _ = repository.register_inbound_event(
        source=InboundEventSource.GOOGLE_POLL,
        external_event_id="inbound-recent",
        received_at=recent,
    )

    repository._conn.commit()

    result = repository.cleanup_retention(
        now=now,
        calendar_sync_signal_retention_days=7,
        outbox_retention_days=14,
        job_retention_days=14,
        audit_log_retention_days=14,
        inbound_event_retention_days=7,
    )

    assert result.calendar_sync_signals_deleted == 1
    assert result.outbox_deleted == 1
    assert result.jobs_deleted == 1
    assert result.audit_logs_deleted == 1
    assert result.inbound_events_deleted == 1

    calendar_signal_statuses = {
        row[0]
        for row in repository._conn.execute(
            "SELECT status FROM calendar_sync_signal"
        ).fetchall()
    }
    assert OutboxStatus.PENDING in calendar_signal_statuses

    outbox_statuses = {
        row[0]
        for row in repository._conn.execute("SELECT status FROM outbox").fetchall()
    }
    assert OutboxStatus.PENDING in outbox_statuses

    job_statuses = {
        row[0] for row in repository._conn.execute("SELECT status FROM job").fetchall()
    }
    assert JobStatus.PENDING in job_statuses

    audit_count = repository._conn.execute("SELECT COUNT(*) FROM audit_log").fetchone()[
        0
    ]
    assert audit_count == 1

    inbound_count = repository._conn.execute(
        "SELECT COUNT(*) FROM inbound_event_dedup"
    ).fetchone()[0]
    assert inbound_count == 1
    repository.close()


def test_repository_checkpoint_and_vacuum_commands(tmp_path: Path) -> None:
    repository = SQLiteRepository(str(tmp_path / "retention-maintenance.db"))
    repository.initialize_schema()

    checkpoint_result = repository.wal_checkpoint(mode="PASSIVE")
    assert checkpoint_result is None or len(checkpoint_result) == 3

    repository.vacuum()
    repository.close()
