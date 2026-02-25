from __future__ import annotations

from collections.abc import Mapping
import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import cast

from bot_vstrechi.domain.commands import CommandExecution
from bot_vstrechi.domain.models import (
    CallbackActionToken,
    CallbackActionType,
    Decision,
    DecisionSource,
    InboundEventSource,
    JobStatus,
    JobType,
    Meeting,
    MeetingParticipant,
    MeetingState,
    OutboxEffectType,
    OutboxStatus,
    Outcome,
    RecurringConfirmationMode,
    ScheduledJobSpec,
)


def _serialize_datetime(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.isoformat(timespec="seconds")


def _parse_datetime(value: str | None) -> datetime | None:
    if value is None:
        return None
    return datetime.fromisoformat(value)


def _parse_required_datetime(value: str | None, *, field: str) -> datetime:
    parsed = _parse_datetime(value)
    if parsed is None:
        raise ValueError(f"Missing required datetime field: {field}")
    return parsed


def _row_int(row: Mapping[str, object], key: str) -> int:
    value = row[key]
    if not isinstance(value, int):
        raise TypeError(f"Expected int for {key}, got {type(value).__name__}")
    return value


def _row_str(row: Mapping[str, object], key: str) -> str:
    value = row[key]
    if not isinstance(value, str):
        raise TypeError(f"Expected str for {key}, got {type(value).__name__}")
    return value


def _row_optional_str(row: Mapping[str, object], key: str) -> str | None:
    value = row[key]
    if value is None:
        return None
    if not isinstance(value, str):
        raise TypeError(f"Expected str|None for {key}, got {type(value).__name__}")
    return value


def _row_optional_int(row: Mapping[str, object], key: str) -> int | None:
    value = row[key]
    if value is None:
        return None
    if not isinstance(value, int):
        raise TypeError(f"Expected int|None for {key}, got {type(value).__name__}")
    return value


@dataclass(frozen=True)
class ClaimedJob:
    job_id: int
    job_type: JobType
    meeting_id: str
    round: int
    run_at: datetime
    attempts: int


@dataclass(frozen=True)
class ClaimedOutbox:
    outbox_id: int
    effect_type: OutboxEffectType
    payload: dict[str, object]
    idempotency_key: str | None
    run_after: datetime
    attempts: int


@dataclass(frozen=True)
class ClaimedCalendarSyncSignal:
    signal_id: int
    calendar_id: str
    external_event_id: str
    resource_state: str
    message_number: int | None
    run_after: datetime
    attempts: int


@dataclass(frozen=True)
class RetentionCleanupResult:
    calendar_sync_signals_deleted: int
    outbox_deleted: int
    jobs_deleted: int
    audit_logs_deleted: int
    inbound_events_deleted: int


class SQLiteRepository:
    def __init__(self, db_path: str) -> None:
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self._conn: sqlite3.Connection = sqlite3.connect(
            db_path,
            check_same_thread=False,
        )
        self._conn.row_factory = sqlite3.Row
        self._in_transaction: bool = False
        self._configure_connection()

    def _configure_connection(self) -> None:
        _ = self._conn.execute("PRAGMA foreign_keys = ON")
        _ = self._conn.execute("PRAGMA journal_mode = WAL")
        _ = self._conn.execute("PRAGMA busy_timeout = 5000")
        self._commit()

    def _begin_immediate(self) -> None:
        if not getattr(self, "_in_transaction", False):
            self._conn.execute("BEGIN IMMEDIATE")

    def _commit(self) -> None:
        if not getattr(self, "_in_transaction", False):
            self._conn.commit()

    def _rollback(self) -> None:
        if not getattr(self, "_in_transaction", False):
            self._conn.rollback()

    from contextlib import contextmanager
    from collections.abc import Iterator

    @contextmanager
    def atomic(self) -> Iterator[None]:
        if getattr(self, "_in_transaction", False):
            yield
            return

        self._conn.execute("BEGIN IMMEDIATE")
        self._in_transaction = True
        try:
            yield
            self._conn.commit()
        except Exception:
            self._conn.rollback()
            raise
        finally:
            self._in_transaction = False

    def close(self) -> None:
        self._conn.close()

    def check_connection(self) -> bool:
        try:
            _ = self._conn.execute("SELECT 1")
            return True
        except sqlite3.Error:
            return False

    def initialize_schema(self) -> None:
        _ = self._conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS user_mapping (
                telegram_user_id INTEGER PRIMARY KEY,
                telegram_username TEXT,
                google_email TEXT NOT NULL UNIQUE,
                full_name TEXT,
                timezone TEXT NOT NULL DEFAULT 'Asia/Yekaterinburg',
                is_active INTEGER NOT NULL DEFAULT 1,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS manager_role (
                telegram_user_id INTEGER PRIMARY KEY,
                is_active INTEGER NOT NULL DEFAULT 1,
                granted_by INTEGER,
                revoked_by INTEGER,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS calendar_sync_state (
                calendar_id TEXT PRIMARY KEY,
                sync_token TEXT,
                watch_channel_id TEXT,
                watch_resource_id TEXT,
                watch_expiration_at TEXT,
                last_message_number INTEGER,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS calendar_sync_signal (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                calendar_id TEXT NOT NULL,
                external_event_id TEXT NOT NULL,
                resource_state TEXT NOT NULL,
                message_number INTEGER,
                run_after TEXT NOT NULL,
                status TEXT NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 0,
                locked_at TEXT,
                last_error TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                UNIQUE(external_event_id)
            );

            CREATE TABLE IF NOT EXISTS meeting (
                meeting_id TEXT PRIMARY KEY,
                initiator_telegram_user_id INTEGER NOT NULL,
                chat_id INTEGER NOT NULL DEFAULT 0,
                state TEXT NOT NULL,
                state_updated_at TEXT NOT NULL,
                title TEXT NOT NULL DEFAULT '',
                google_event_id TEXT,
                google_calendar_id TEXT,
                series_event_id TEXT,
                occurrence_start_at TEXT,
                group_status_message_id INTEGER,
                scheduled_start_at TEXT NOT NULL,
                scheduled_end_at TEXT NOT NULL,
                confirmation_deadline_at TEXT,
                initiator_decision_deadline_at TEXT,
                recurring_confirmation_mode TEXT NOT NULL DEFAULT 'strict',
                confirmation_round INTEGER NOT NULL,
                created_by_bot INTEGER NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS meeting_participant (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                meeting_id TEXT NOT NULL,
                telegram_user_id INTEGER NOT NULL,
                is_required INTEGER NOT NULL,
                decision_source TEXT NOT NULL,
                decision TEXT NOT NULL,
                decision_received_at TEXT,
                FOREIGN KEY (meeting_id) REFERENCES meeting(meeting_id) ON DELETE CASCADE,
                UNIQUE(meeting_id, telegram_user_id)
            );

            CREATE TABLE IF NOT EXISTS job (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_type TEXT NOT NULL,
                meeting_id TEXT NOT NULL,
                round INTEGER NOT NULL,
                run_at TEXT NOT NULL,
                status TEXT NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 0,
                locked_at TEXT,
                last_error TEXT,
                created_at TEXT NOT NULL,
                FOREIGN KEY (meeting_id) REFERENCES meeting(meeting_id) ON DELETE CASCADE,
                UNIQUE(job_type, meeting_id, round, run_at)
            );

            CREATE TABLE IF NOT EXISTS inbound_event_dedup (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source TEXT NOT NULL,
                external_event_id TEXT NOT NULL,
                received_at TEXT NOT NULL,
                UNIQUE(source, external_event_id)
            );

            CREATE TABLE IF NOT EXISTS callback_action_token (
                token TEXT PRIMARY KEY,
                meeting_id TEXT NOT NULL,
                round INTEGER NOT NULL,
                action_type TEXT NOT NULL,
                allowed_user_id INTEGER NOT NULL,
                expires_at TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (meeting_id) REFERENCES meeting(meeting_id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS outbox (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                effect_type TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                idempotency_key TEXT,
                run_after TEXT NOT NULL,
                status TEXT NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 0,
                locked_at TEXT,
                last_error TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                UNIQUE(idempotency_key)
            );

            CREATE TABLE IF NOT EXISTS audit_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                meeting_id TEXT,
                round INTEGER,
                actor_telegram_user_id INTEGER,
                actor_type TEXT NOT NULL,
                action TEXT NOT NULL,
                details_json TEXT,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS conversation_state (
                chat_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                flow TEXT NOT NULL,
                state_json TEXT NOT NULL,
                expires_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY(chat_id, user_id, flow)
            );

            CREATE INDEX IF NOT EXISTS idx_meeting_state ON meeting(state);
            CREATE INDEX IF NOT EXISTS idx_job_due ON job(status, run_at);
            CREATE INDEX IF NOT EXISTS idx_participant_meeting ON meeting_participant(meeting_id);
            CREATE INDEX IF NOT EXISTS idx_callback_token_meeting ON callback_action_token(meeting_id);
            CREATE INDEX IF NOT EXISTS idx_outbox_due ON outbox(status, run_after);
            CREATE INDEX IF NOT EXISTS idx_conversation_expires ON conversation_state(expires_at);
            CREATE INDEX IF NOT EXISTS idx_manager_role_active ON manager_role(is_active);
            CREATE INDEX IF NOT EXISTS idx_calendar_sync_signal_due ON calendar_sync_signal(status, run_after);
            """
        )
        self._apply_schema_migrations()
        self._commit()

    def _table_columns(self, table_name: str) -> set[str]:
        rows = self._conn.execute(f"PRAGMA table_info({table_name})").fetchall()
        columns: set[str] = set()
        for row_obj in rows:
            row = cast(Mapping[str, object], row_obj)
            columns.add(_row_str(row, "name"))
        return columns

    def _ensure_column(
        self, *, table_name: str, column_name: str, definition_sql: str
    ) -> bool:
        if column_name in self._table_columns(table_name):
            return False
        _ = self._conn.execute(
            f"ALTER TABLE {table_name} ADD COLUMN {column_name} {definition_sql}"
        )
        return True

    def _apply_schema_migrations(self) -> None:
        _ = self._ensure_column(
            table_name="user_mapping",
            column_name="timezone",
            definition_sql="TEXT NOT NULL DEFAULT 'Asia/Yekaterinburg'",
        )
        _ = self._ensure_column(
            table_name="user_mapping",
            column_name="full_name",
            definition_sql="TEXT",
        )
        _ = self._ensure_column(
            table_name="user_mapping",
            column_name="preferred_chat_id",
            definition_sql="INTEGER",
        )

        added_chat_id = self._ensure_column(
            table_name="meeting",
            column_name="chat_id",
            definition_sql="INTEGER NOT NULL DEFAULT 0",
        )
        _ = self._ensure_column(
            table_name="meeting",
            column_name="google_event_id",
            definition_sql="TEXT",
        )
        _ = self._ensure_column(
            table_name="meeting",
            column_name="google_calendar_id",
            definition_sql="TEXT",
        )
        _ = self._ensure_column(
            table_name="meeting",
            column_name="series_event_id",
            definition_sql="TEXT",
        )
        _ = self._ensure_column(
            table_name="meeting",
            column_name="occurrence_start_at",
            definition_sql="TEXT",
        )
        _ = self._ensure_column(
            table_name="meeting",
            column_name="group_status_message_id",
            definition_sql="INTEGER",
        )
        _ = self._ensure_column(
            table_name="meeting",
            column_name="recurring_confirmation_mode",
            definition_sql="TEXT NOT NULL DEFAULT 'strict'",
        )

        if added_chat_id:
            _ = self._conn.execute(
                """
                UPDATE meeting
                SET chat_id = initiator_telegram_user_id
                WHERE chat_id = 0
                """
            )

    def insert_meeting(self, meeting: Meeting, *, now: datetime) -> None:
        self._begin_immediate()
        try:
            _ = self._conn.execute(
                """
                INSERT INTO meeting (
                    meeting_id,
                    initiator_telegram_user_id,
                    chat_id,
                    state,
                    state_updated_at,
                    title,
                    google_event_id,
                    google_calendar_id,
                    series_event_id,
                    occurrence_start_at,
                    group_status_message_id,
                    scheduled_start_at,
                    scheduled_end_at,
                    confirmation_deadline_at,
                    initiator_decision_deadline_at,
                    recurring_confirmation_mode,
                    confirmation_round,
                    created_by_bot,
                    updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    meeting.meeting_id,
                    meeting.initiator_telegram_user_id,
                    meeting.chat_id,
                    meeting.state,
                    _serialize_datetime(now),
                    meeting.title,
                    meeting.google_event_id,
                    meeting.google_calendar_id,
                    meeting.series_event_id,
                    _serialize_datetime(meeting.occurrence_start_at),
                    meeting.group_status_message_id,
                    _serialize_datetime(meeting.scheduled_start_at),
                    _serialize_datetime(meeting.scheduled_end_at),
                    _serialize_datetime(meeting.confirmation_deadline_at),
                    _serialize_datetime(meeting.initiator_decision_deadline_at),
                    meeting.recurring_confirmation_mode,
                    meeting.confirmation_round,
                    1 if meeting.created_by_bot else 0,
                    _serialize_datetime(now),
                ),
            )
            self._replace_participants(meeting)
            self._commit()
        except Exception:
            self._rollback()
            raise

    def get_meeting(self, meeting_id: str) -> Meeting | None:
        maybe_row_obj: object = self._conn.execute(
            """
            SELECT
                meeting_id,
                initiator_telegram_user_id,
                chat_id,
                state,
                title,
                google_event_id,
                google_calendar_id,
                series_event_id,
                occurrence_start_at,
                group_status_message_id,
                scheduled_start_at,
                scheduled_end_at,
                created_by_bot,
                confirmation_round,
                confirmation_deadline_at,
                initiator_decision_deadline_at,
                recurring_confirmation_mode
            FROM meeting
            WHERE meeting_id = ?
            """,
            (meeting_id,),
        ).fetchone()
        if maybe_row_obj is None:
            return None
        meeting_row = cast(Mapping[str, object], maybe_row_obj)

        participant_rows = self._conn.execute(
            """
            SELECT
                telegram_user_id,
                is_required,
                decision_source,
                decision,
                decision_received_at
            FROM meeting_participant
            WHERE meeting_id = ?
            ORDER BY telegram_user_id ASC
            """,
            (meeting_id,),
        ).fetchall()

        participants = tuple(
            MeetingParticipant(
                telegram_user_id=_row_int(
                    cast(Mapping[str, object], row), "telegram_user_id"
                ),
                is_required=bool(
                    _row_int(cast(Mapping[str, object], row), "is_required")
                ),
                decision_source=DecisionSource(
                    _row_str(cast(Mapping[str, object], row), "decision_source")
                ),
                decision=Decision(
                    _row_str(cast(Mapping[str, object], row), "decision")
                ),
                decision_received_at=_parse_datetime(
                    _row_optional_str(
                        cast(Mapping[str, object], row), "decision_received_at"
                    )
                ),
            )
            for row in participant_rows
        )

        return Meeting(
            meeting_id=_row_str(meeting_row, "meeting_id"),
            initiator_telegram_user_id=_row_int(
                meeting_row,
                "initiator_telegram_user_id",
            ),
            chat_id=_row_int(meeting_row, "chat_id"),
            state=MeetingState(_row_str(meeting_row, "state")),
            title=_row_str(meeting_row, "title"),
            google_event_id=_row_optional_str(meeting_row, "google_event_id"),
            google_calendar_id=_row_optional_str(meeting_row, "google_calendar_id"),
            series_event_id=_row_optional_str(meeting_row, "series_event_id"),
            occurrence_start_at=_parse_datetime(
                _row_optional_str(meeting_row, "occurrence_start_at")
            ),
            group_status_message_id=_row_optional_int(
                meeting_row,
                "group_status_message_id",
            ),
            scheduled_start_at=_parse_required_datetime(
                _row_optional_str(meeting_row, "scheduled_start_at"),
                field="scheduled_start_at",
            ),
            scheduled_end_at=_parse_required_datetime(
                _row_optional_str(meeting_row, "scheduled_end_at"),
                field="scheduled_end_at",
            ),
            created_by_bot=bool(_row_int(meeting_row, "created_by_bot")),
            confirmation_round=_row_int(meeting_row, "confirmation_round"),
            confirmation_deadline_at=_parse_datetime(
                _row_optional_str(meeting_row, "confirmation_deadline_at")
            ),
            initiator_decision_deadline_at=_parse_datetime(
                _row_optional_str(meeting_row, "initiator_decision_deadline_at")
            ),
            participants=participants,
            recurring_confirmation_mode=RecurringConfirmationMode(
                _row_str(meeting_row, "recurring_confirmation_mode")
            ),
        )

    def find_meeting_by_google_event_id(
        self, *, google_event_id: str
    ) -> Meeting | None:
        normalized = google_event_id.strip()
        if not normalized:
            return None

        maybe_row_obj: object = self._conn.execute(
            """
            SELECT meeting_id
            FROM meeting
            WHERE google_event_id = ?
            ORDER BY updated_at DESC
            LIMIT 1
            """,
            (normalized,),
        ).fetchone()
        if maybe_row_obj is None:
            return None

        row = cast(Mapping[str, object], maybe_row_obj)
        meeting_id = _row_str(row, "meeting_id")
        return self.get_meeting(meeting_id)

    def find_meeting_by_occurrence_identity(
        self,
        *,
        series_event_id: str,
        occurrence_start_at: datetime,
    ) -> Meeting | None:
        normalized_series = series_event_id.strip()
        if not normalized_series:
            return None

        maybe_row_obj: object = self._conn.execute(
            """
            SELECT meeting_id
            FROM meeting
            WHERE series_event_id = ? AND occurrence_start_at = ?
            ORDER BY updated_at DESC
            LIMIT 1
            """,
            (
                normalized_series,
                _serialize_datetime(occurrence_start_at),
            ),
        ).fetchone()
        if maybe_row_obj is None:
            return None

        row = cast(Mapping[str, object], maybe_row_obj)
        meeting_id = _row_str(row, "meeting_id")
        return self.get_meeting(meeting_id)

    def has_open_meeting_for_series(
        self,
        *,
        series_event_id: str,
        now: datetime,
    ) -> bool:
        normalized_series_id = series_event_id.strip()
        if not normalized_series_id:
            return False

        maybe_row_obj: object = self._conn.execute(
            """
            SELECT meeting_id
            FROM meeting
            WHERE
                series_event_id = ?
                AND state IN (?, ?, ?)
                AND scheduled_end_at >= ?
            LIMIT 1
            """,
            (
                normalized_series_id,
                MeetingState.DRAFT,
                MeetingState.PENDING,
                MeetingState.NEEDS_INITIATOR_DECISION,
                _serialize_datetime(now),
            ),
        ).fetchone()
        return maybe_row_obj is not None

    def apply_execution(
        self,
        *,
        before: Meeting,
        execution: CommandExecution,
        now: datetime,
    ) -> bool:
        if execution.result.outcome != Outcome.OK:
            return True

        updated = cast(Meeting, execution.meeting)
        jobs = cast(tuple[ScheduledJobSpec, ...], execution.jobs)
        self._begin_immediate()
        try:
            update_result = self._conn.execute(
                """
                UPDATE meeting
                SET
                    chat_id = ?,
                    state = ?,
                    state_updated_at = ?,
                    title = ?,
                    google_event_id = ?,
                    google_calendar_id = ?,
                    series_event_id = ?,
                    occurrence_start_at = ?,
                    group_status_message_id = CASE
                        WHEN ? IS NULL THEN group_status_message_id
                        ELSE ?
                    END,
                    scheduled_start_at = ?,
                    scheduled_end_at = ?,
                    confirmation_deadline_at = ?,
                    initiator_decision_deadline_at = ?,
                    recurring_confirmation_mode = ?,
                    confirmation_round = ?,
                    updated_at = ?
                WHERE
                    meeting_id = ?
                    AND state = ?
                    AND confirmation_round = ?
                """,
                (
                    updated.chat_id,
                    updated.state,
                    _serialize_datetime(now),
                    updated.title,
                    updated.google_event_id,
                    updated.google_calendar_id,
                    updated.series_event_id,
                    _serialize_datetime(updated.occurrence_start_at),
                    updated.group_status_message_id,
                    updated.group_status_message_id,
                    _serialize_datetime(updated.scheduled_start_at),
                    _serialize_datetime(updated.scheduled_end_at),
                    _serialize_datetime(updated.confirmation_deadline_at),
                    _serialize_datetime(updated.initiator_decision_deadline_at),
                    updated.recurring_confirmation_mode,
                    updated.confirmation_round,
                    _serialize_datetime(now),
                    updated.meeting_id,
                    before.state,
                    before.confirmation_round,
                ),
            )

            if update_result.rowcount != 1:
                self._rollback()
                return False

            self._replace_participants(updated)
            self._enqueue_jobs(jobs, now=now)
            self._cancel_stale_jobs_on_transition(before=before, after=updated)

            self._commit()
            return True
        except Exception:
            self._rollback()
            raise

    def enqueue_jobs(
        self, jobs: tuple[ScheduledJobSpec, ...], *, now: datetime
    ) -> None:
        self._begin_immediate()
        try:
            self._enqueue_jobs(jobs, now=now)
            self._commit()
        except Exception:
            self._rollback()
            raise

    def _enqueue_jobs(
        self, jobs: tuple[ScheduledJobSpec, ...], *, now: datetime
    ) -> None:
        for job in jobs:
            _ = self._conn.execute(
                """
                INSERT INTO job (
                    job_type,
                    meeting_id,
                    round,
                    run_at,
                    status,
                    attempts,
                    locked_at,
                    last_error,
                    created_at
                ) VALUES (?, ?, ?, ?, ?, 0, NULL, NULL, ?)
                ON CONFLICT(job_type, meeting_id, round, run_at) DO NOTHING
                """,
                (
                    job.job_type,
                    job.meeting_id,
                    job.round,
                    _serialize_datetime(job.run_at),
                    JobStatus.PENDING,
                    _serialize_datetime(now),
                ),
            )

    def _replace_participants(self, meeting: Meeting) -> None:
        _ = self._conn.execute(
            "DELETE FROM meeting_participant WHERE meeting_id = ?",
            (meeting.meeting_id,),
        )
        for participant in meeting.participants:
            _ = self._conn.execute(
                """
                INSERT INTO meeting_participant (
                    meeting_id,
                    telegram_user_id,
                    is_required,
                    decision_source,
                    decision,
                    decision_received_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    meeting.meeting_id,
                    participant.telegram_user_id,
                    1 if participant.is_required else 0,
                    participant.decision_source,
                    participant.decision,
                    _serialize_datetime(participant.decision_received_at),
                ),
            )

    def _cancel_stale_jobs_on_transition(
        self, *, before: Meeting, after: Meeting
    ) -> None:
        if after.confirmation_round != before.confirmation_round:
            _ = self._conn.execute(
                """
                UPDATE job
                SET status = ?
                WHERE
                    meeting_id = ?
                    AND round != ?
                    AND status IN (?, ?)
                """,
                (
                    JobStatus.CANCELLED,
                    after.meeting_id,
                    after.confirmation_round,
                    JobStatus.PENDING,
                    JobStatus.RUNNING,
                ),
            )

        if after.state in {
            MeetingState.CONFIRMED,
            MeetingState.CANCELLED,
            MeetingState.EXPIRED,
        }:
            _ = self._conn.execute(
                """
                UPDATE job
                SET status = ?
                WHERE
                    meeting_id = ?
                    AND status IN (?, ?)
                """,
                (
                    JobStatus.CANCELLED,
                    after.meeting_id,
                    JobStatus.PENDING,
                    JobStatus.RUNNING,
                ),
            )

        if after.state == MeetingState.NEEDS_INITIATOR_DECISION:
            _ = self._conn.execute(
                """
                UPDATE job
                SET status = ?
                WHERE
                    meeting_id = ?
                    AND round = ?
                    AND job_type IN (?, ?)
                    AND status IN (?, ?)
                """,
                (
                    JobStatus.CANCELLED,
                    after.meeting_id,
                    after.confirmation_round,
                    JobType.REMINDER,
                    JobType.CONFIRM_DEADLINE,
                    JobStatus.PENDING,
                    JobStatus.RUNNING,
                ),
            )

    def claim_due_job(self, *, now: datetime) -> ClaimedJob | None:
        self._begin_immediate()
        try:
            maybe_row_obj: object = self._conn.execute(
                """
                SELECT id, job_type, meeting_id, round, run_at, attempts
                FROM job
                WHERE status = ? AND run_at <= ?
                ORDER BY run_at ASC, id ASC
                LIMIT 1
                """,
                (JobStatus.PENDING, _serialize_datetime(now)),
            ).fetchone()

            if maybe_row_obj is None:
                self._commit()
                return None
            row = cast(Mapping[str, object], maybe_row_obj)

            claimed = self._conn.execute(
                """
                UPDATE job
                SET
                    status = ?,
                    locked_at = ?,
                    attempts = attempts + 1
                WHERE id = ? AND status = ?
                """,
                (
                    JobStatus.RUNNING,
                    _serialize_datetime(now),
                    _row_int(row, "id"),
                    JobStatus.PENDING,
                ),
            )
            if claimed.rowcount != 1:
                self._rollback()
                return None

            self._commit()
            return ClaimedJob(
                job_id=_row_int(row, "id"),
                job_type=JobType(_row_str(row, "job_type")),
                meeting_id=_row_str(row, "meeting_id"),
                round=_row_int(row, "round"),
                run_at=_parse_required_datetime(
                    _row_optional_str(row, "run_at"),
                    field="job.run_at",
                ),
                attempts=_row_int(row, "attempts") + 1,
            )
        except Exception:
            self._rollback()
            raise

    def mark_job_done(self, *, job_id: int) -> None:
        _ = self._conn.execute(
            "UPDATE job SET status = ?, locked_at = NULL WHERE id = ?",
            (JobStatus.DONE, job_id),
        )
        self._commit()

    def mark_job_failed(self, *, job_id: int, error: str) -> None:
        _ = self._conn.execute(
            "UPDATE job SET status = ?, last_error = ?, locked_at = NULL WHERE id = ?",
            (JobStatus.FAILED, error, job_id),
        )
        self._commit()

    def reconcile_stale_running_jobs(self, *, stale_before: datetime) -> int:
        result = self._conn.execute(
            """
            UPDATE job
            SET status = ?, locked_at = NULL
            WHERE status = ? AND locked_at IS NOT NULL AND locked_at < ?
            """,
            (
                JobStatus.PENDING,
                JobStatus.RUNNING,
                _serialize_datetime(stale_before),
            ),
        )
        self._commit()
        return result.rowcount

    def enqueue_outbox(
        self,
        *,
        effect_type: OutboxEffectType,
        payload: Mapping[str, object],
        now: datetime,
        run_after: datetime | None = None,
        idempotency_key: str | None = None,
    ) -> bool:
        target_run_after = run_after or now
        payload_json = json.dumps(dict(payload), separators=(",", ":"), sort_keys=True)
        try:
            _ = self._conn.execute(
                """
                INSERT INTO outbox (
                    effect_type,
                    payload_json,
                    idempotency_key,
                    run_after,
                    status,
                    attempts,
                    locked_at,
                    last_error,
                    created_at,
                    updated_at
                ) VALUES (?, ?, ?, ?, ?, 0, NULL, NULL, ?, ?)
                """,
                (
                    effect_type,
                    payload_json,
                    idempotency_key,
                    _serialize_datetime(target_run_after),
                    OutboxStatus.PENDING,
                    _serialize_datetime(now),
                    _serialize_datetime(now),
                ),
            )
            self._commit()
            return True
        except sqlite3.IntegrityError:
            self._rollback()
            return False

    def claim_due_outbox(self, *, now: datetime) -> ClaimedOutbox | None:
        self._begin_immediate()
        try:
            maybe_row_obj: object = self._conn.execute(
                """
                SELECT id, effect_type, payload_json, idempotency_key, run_after, attempts
                FROM outbox
                WHERE status = ? AND run_after <= ?
                ORDER BY
                    run_after ASC,
                    CASE effect_type
                        WHEN ? THEN 0
                        WHEN ? THEN 1
                        WHEN ? THEN 2
                        ELSE 3
                    END ASC,
                    id ASC
                LIMIT 1
                """,
                (
                    OutboxStatus.PENDING,
                    _serialize_datetime(now),
                    OutboxEffectType.TELEGRAM_ANSWER_CALLBACK,
                    OutboxEffectType.TELEGRAM_EDIT_MESSAGE,
                    OutboxEffectType.TELEGRAM_SEND_MESSAGE,
                ),
            ).fetchone()

            if maybe_row_obj is None:
                self._commit()
                return None
            row = cast(Mapping[str, object], maybe_row_obj)

            claimed = self._conn.execute(
                """
                UPDATE outbox
                SET
                    status = ?,
                    locked_at = ?,
                    attempts = attempts + 1,
                    updated_at = ?
                WHERE id = ? AND status = ?
                """,
                (
                    OutboxStatus.RUNNING,
                    _serialize_datetime(now),
                    _serialize_datetime(now),
                    _row_int(row, "id"),
                    OutboxStatus.PENDING,
                ),
            )
            if claimed.rowcount != 1:
                self._rollback()
                return None

            payload_obj = json.loads(_row_str(row, "payload_json"))
            if not isinstance(payload_obj, dict):
                self._rollback()
                raise TypeError("outbox payload_json must be JSON object")

            self._commit()
            return ClaimedOutbox(
                outbox_id=_row_int(row, "id"),
                effect_type=OutboxEffectType(_row_str(row, "effect_type")),
                payload=cast(dict[str, object], payload_obj),
                idempotency_key=_row_optional_str(row, "idempotency_key"),
                run_after=_parse_required_datetime(
                    _row_optional_str(row, "run_after"),
                    field="outbox.run_after",
                ),
                attempts=_row_int(row, "attempts") + 1,
            )
        except Exception:
            self._rollback()
            raise

    def mark_outbox_done(self, *, outbox_id: int, now: datetime) -> None:
        _ = self._conn.execute(
            """
            UPDATE outbox
            SET status = ?, locked_at = NULL, last_error = NULL, updated_at = ?
            WHERE id = ?
            """,
            (OutboxStatus.DONE, _serialize_datetime(now), outbox_id),
        )
        self._commit()

    def mark_outbox_retry(
        self,
        *,
        outbox_id: int,
        run_after: datetime,
        error: str,
        now: datetime,
    ) -> None:
        _ = self._conn.execute(
            """
            UPDATE outbox
            SET
                status = ?,
                run_after = ?,
                locked_at = NULL,
                last_error = ?,
                updated_at = ?
            WHERE id = ?
            """,
            (
                OutboxStatus.PENDING,
                _serialize_datetime(run_after),
                error,
                _serialize_datetime(now),
                outbox_id,
            ),
        )
        self._commit()

    def mark_outbox_failed(self, *, outbox_id: int, error: str, now: datetime) -> None:
        _ = self._conn.execute(
            """
            UPDATE outbox
            SET status = ?, locked_at = NULL, last_error = ?, updated_at = ?
            WHERE id = ?
            """,
            (OutboxStatus.FAILED, error, _serialize_datetime(now), outbox_id),
        )
        self._commit()

    def suppress_pending_outbox_by_keys(
        self,
        *,
        keys: tuple[str, ...],
        reason: str,
        now: datetime,
    ) -> int:
        if not keys:
            return 0
        placeholders = ", ".join(["?"] * len(keys))
        query = (
            "UPDATE outbox "
            + "SET status = ?, locked_at = NULL, last_error = ?, updated_at = ? "
            + "WHERE status = ? AND idempotency_key IN ("
            + placeholders
            + ")"
        )
        result = self._conn.execute(
            query,
            (
                OutboxStatus.FAILED,
                reason,
                _serialize_datetime(now),
                OutboxStatus.PENDING,
                *keys,
            ),
        )
        self._commit()
        return result.rowcount

    def suppress_pending_group_progress_outbox(
        self,
        *,
        meeting_id: str,
        round: int,
        now: datetime,
    ) -> int:
        key_prefix = f"group_status:{meeting_id}:r{round}:pending_progress:"
        query = (
            "UPDATE outbox "
            + "SET status = ?, locked_at = NULL, last_error = ?, updated_at = ? "
            + "WHERE status = ? AND idempotency_key LIKE ?"
        )
        result = self._conn.execute(
            query,
            (
                OutboxStatus.FAILED,
                "suppressed: meeting is no longer pending for this round",
                _serialize_datetime(now),
                OutboxStatus.PENDING,
                f"{key_prefix}%",
            ),
        )
        self._commit()
        return result.rowcount

    def reconcile_stale_running_outbox(
        self, *, stale_before: datetime, now: datetime
    ) -> int:
        result = self._conn.execute(
            """
            UPDATE outbox
            SET status = ?, locked_at = NULL, updated_at = ?
            WHERE status = ? AND locked_at IS NOT NULL AND locked_at < ?
            """,
            (
                OutboxStatus.PENDING,
                _serialize_datetime(now),
                OutboxStatus.RUNNING,
                _serialize_datetime(stale_before),
            ),
        )
        self._commit()
        return result.rowcount

    def count_outbox(
        self,
        *,
        status: OutboxStatus | None = None,
        effect_type: OutboxEffectType | None = None,
    ) -> int:
        query = "SELECT COUNT(*) AS c FROM outbox WHERE 1=1"
        params: list[object] = []
        if status is not None:
            query += " AND status = ?"
            params.append(status)
        if effect_type is not None:
            query += " AND effect_type = ?"
            params.append(effect_type)
        maybe_row_obj: object = self._conn.execute(query, tuple(params)).fetchone()
        if maybe_row_obj is None:
            return 0
        row = cast(Mapping[str, object], maybe_row_obj)
        return _row_int(row, "c")

    def count_calendar_sync_signals(
        self,
        *,
        status: OutboxStatus | None = None,
    ) -> int:
        query = "SELECT COUNT(*) AS c FROM calendar_sync_signal WHERE 1=1"
        params: list[object] = []
        if status is not None:
            query += " AND status = ?"
            params.append(status)
        maybe_row_obj: object = self._conn.execute(query, tuple(params)).fetchone()
        if maybe_row_obj is None:
            return 0
        row = cast(Mapping[str, object], maybe_row_obj)
        return _row_int(row, "c")

    def insert_audit_log(
        self,
        *,
        meeting_id: str | None,
        round: int | None,
        actor_telegram_user_id: int | None,
        actor_type: str,
        action: str,
        details: dict[str, object] | None = None,
        now: datetime,
    ) -> None:
        details_json = None
        if details is not None:
            details_json = json.dumps(details, separators=(",", ":"), sort_keys=True)

        _ = self._conn.execute(
            """
            INSERT INTO audit_log (
                meeting_id,
                round,
                actor_telegram_user_id,
                actor_type,
                action,
                details_json,
                created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                meeting_id,
                round,
                actor_telegram_user_id,
                actor_type,
                action,
                details_json,
                _serialize_datetime(now),
            ),
        )
        self._commit()

    def get_audit_logs(self, meeting_id: str) -> list[dict[str, object]]:
        rows = self._conn.execute(
            """
            SELECT
                id,
                meeting_id,
                round,
                actor_telegram_user_id,
                actor_type,
                action,
                details_json,
                created_at
            FROM audit_log
            WHERE meeting_id = ?
            ORDER BY id ASC
            """,
            (meeting_id,),
        ).fetchall()
        return [cast(dict[str, object], dict(row)) for row in rows]

    def cancel_jobs_for_meeting_round(
        self,
        *,
        meeting_id: str,
        round: int,
        now: datetime,
    ) -> int:
        del now
        result = self._conn.execute(
            """
            UPDATE job
            SET status = ?, locked_at = NULL
            WHERE
                meeting_id = ?
                AND round = ?
                AND status IN (?, ?)
            """,
            (
                JobStatus.CANCELLED,
                meeting_id,
                round,
                JobStatus.PENDING,
                JobStatus.RUNNING,
            ),
        )
        self._commit()
        return result.rowcount

    def get_unresolved_required_participant_ids(
        self, *, meeting_id: str
    ) -> tuple[int, ...]:
        rows = self._conn.execute(
            """
            SELECT telegram_user_id
            FROM meeting_participant
            WHERE meeting_id = ? AND is_required = 1 AND decision = ?
            ORDER BY telegram_user_id ASC
            """,
            (meeting_id, Decision.NONE),
        ).fetchall()
        return tuple(
            _row_int(cast(Mapping[str, object], row), "telegram_user_id")
            for row in rows
        )

    def list_active_manager_ids(self) -> tuple[int, ...]:
        rows = self._conn.execute(
            """
            SELECT telegram_user_id
            FROM manager_role
            WHERE is_active = 1
            ORDER BY telegram_user_id ASC
            """
        ).fetchall()
        return tuple(
            _row_int(cast(Mapping[str, object], row), "telegram_user_id")
            for row in rows
        )

    def is_manager(self, *, telegram_user_id: int) -> bool:
        maybe_row_obj: object = self._conn.execute(
            """
            SELECT telegram_user_id
            FROM manager_role
            WHERE telegram_user_id = ? AND is_active = 1
            """,
            (telegram_user_id,),
        ).fetchone()
        return maybe_row_obj is not None

    def grant_manager_role(
        self,
        *,
        telegram_user_id: int,
        granted_by: int | None,
        now: datetime,
    ) -> None:
        _ = self._conn.execute(
            """
            INSERT INTO manager_role (
                telegram_user_id,
                is_active,
                granted_by,
                revoked_by,
                updated_at
            ) VALUES (?, 1, ?, NULL, ?)
            ON CONFLICT(telegram_user_id) DO UPDATE SET
                is_active = 1,
                granted_by = excluded.granted_by,
                revoked_by = NULL,
                updated_at = excluded.updated_at
            """,
            (
                telegram_user_id,
                granted_by,
                _serialize_datetime(now),
            ),
        )
        self._commit()

    def revoke_manager_role(
        self,
        *,
        telegram_user_id: int,
        revoked_by: int | None,
        now: datetime,
    ) -> bool:
        self._begin_immediate()
        try:
            active_count_obj: object = self._conn.execute(
                "SELECT COUNT(*) AS c FROM manager_role WHERE is_active = 1"
            ).fetchone()
            if active_count_obj is None:
                self._commit()
                return False
            active_count = _row_int(cast(Mapping[str, object], active_count_obj), "c")

            target_obj: object = self._conn.execute(
                """
                SELECT is_active
                FROM manager_role
                WHERE telegram_user_id = ?
                """,
                (telegram_user_id,),
            ).fetchone()
            if target_obj is None:
                self._commit()
                return False

            target_row = cast(Mapping[str, object], target_obj)
            target_is_active = _row_int(target_row, "is_active") == 1
            if not target_is_active:
                self._commit()
                return False

            if active_count <= 1:
                self._rollback()
                return False

            result = self._conn.execute(
                """
                UPDATE manager_role
                SET
                    is_active = 0,
                    revoked_by = ?,
                    updated_at = ?
                WHERE telegram_user_id = ? AND is_active = 1
                """,
                (
                    revoked_by,
                    _serialize_datetime(now),
                    telegram_user_id,
                ),
            )
            self._commit()
            return result.rowcount == 1
        except Exception:
            self._rollback()
            raise

    def upsert_calendar_sync_state(
        self,
        *,
        calendar_id: str,
        sync_token: str | None,
        watch_channel_id: str | None,
        watch_resource_id: str | None,
        watch_expiration_at: datetime | None,
        last_message_number: int | None,
        now: datetime,
    ) -> None:
        _ = self._conn.execute(
            """
            INSERT INTO calendar_sync_state (
                calendar_id,
                sync_token,
                watch_channel_id,
                watch_resource_id,
                watch_expiration_at,
                last_message_number,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(calendar_id) DO UPDATE SET
                sync_token = excluded.sync_token,
                watch_channel_id = excluded.watch_channel_id,
                watch_resource_id = excluded.watch_resource_id,
                watch_expiration_at = excluded.watch_expiration_at,
                last_message_number = excluded.last_message_number,
                updated_at = excluded.updated_at
            """,
            (
                calendar_id,
                sync_token,
                watch_channel_id,
                watch_resource_id,
                _serialize_datetime(watch_expiration_at),
                last_message_number,
                _serialize_datetime(now),
            ),
        )
        self._commit()

    def get_calendar_sync_state(self, *, calendar_id: str) -> dict[str, object] | None:
        maybe_row_obj: object = self._conn.execute(
            """
            SELECT
                calendar_id,
                sync_token,
                watch_channel_id,
                watch_resource_id,
                watch_expiration_at,
                last_message_number,
                updated_at
            FROM calendar_sync_state
            WHERE calendar_id = ?
            """,
            (calendar_id,),
        ).fetchone()
        if maybe_row_obj is None:
            return None
        return cast(dict[str, object], dict(cast(Mapping[str, object], maybe_row_obj)))

    def enqueue_calendar_sync_signal(
        self,
        *,
        calendar_id: str,
        external_event_id: str,
        resource_state: str,
        message_number: int | None,
        now: datetime,
        run_after: datetime | None = None,
    ) -> bool:
        target_run_after = run_after or now
        try:
            _ = self._conn.execute(
                """
                INSERT INTO calendar_sync_signal (
                    calendar_id,
                    external_event_id,
                    resource_state,
                    message_number,
                    run_after,
                    status,
                    attempts,
                    locked_at,
                    last_error,
                    created_at,
                    updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, 0, NULL, NULL, ?, ?)
                """,
                (
                    calendar_id,
                    external_event_id,
                    resource_state,
                    message_number,
                    _serialize_datetime(target_run_after),
                    OutboxStatus.PENDING,
                    _serialize_datetime(now),
                    _serialize_datetime(now),
                ),
            )
            self._commit()
            return True
        except sqlite3.IntegrityError:
            self._rollback()
            return False

    def claim_due_calendar_sync_signal(
        self,
        *,
        now: datetime,
    ) -> ClaimedCalendarSyncSignal | None:
        self._begin_immediate()
        try:
            maybe_row_obj: object = self._conn.execute(
                """
                SELECT
                    id,
                    calendar_id,
                    external_event_id,
                    resource_state,
                    message_number,
                    run_after,
                    attempts
                FROM calendar_sync_signal
                WHERE status = ? AND run_after <= ?
                  AND calendar_id NOT IN (
                      SELECT calendar_id FROM calendar_sync_signal WHERE status = ?
                  )
                ORDER BY run_after ASC, id ASC
                LIMIT 1
                """,
                (OutboxStatus.PENDING, _serialize_datetime(now), OutboxStatus.RUNNING),
            ).fetchone()

            if maybe_row_obj is None:
                self._commit()
                return None
            row = cast(Mapping[str, object], maybe_row_obj)

            claimed = self._conn.execute(
                """
                UPDATE calendar_sync_signal
                SET
                    status = ?,
                    locked_at = ?,
                    attempts = attempts + 1,
                    updated_at = ?
                WHERE id = ? AND status = ?
                """,
                (
                    OutboxStatus.RUNNING,
                    _serialize_datetime(now),
                    _serialize_datetime(now),
                    _row_int(row, "id"),
                    OutboxStatus.PENDING,
                ),
            )
            if claimed.rowcount != 1:
                self._rollback()
                return None

            self._commit()
            return ClaimedCalendarSyncSignal(
                signal_id=_row_int(row, "id"),
                calendar_id=_row_str(row, "calendar_id"),
                external_event_id=_row_str(row, "external_event_id"),
                resource_state=_row_str(row, "resource_state"),
                message_number=_row_optional_int(row, "message_number"),
                run_after=_parse_required_datetime(
                    _row_optional_str(row, "run_after"),
                    field="calendar_sync_signal.run_after",
                ),
                attempts=_row_int(row, "attempts") + 1,
            )
        except Exception:
            self._rollback()
            raise

    def mark_calendar_sync_signal_done(self, *, signal_id: int, now: datetime) -> None:
        _ = self._conn.execute(
            """
            UPDATE calendar_sync_signal
            SET status = ?, locked_at = NULL, last_error = NULL, updated_at = ?
            WHERE id = ?
            """,
            (OutboxStatus.DONE, _serialize_datetime(now), signal_id),
        )
        self._commit()

    def mark_calendar_sync_signal_retry(
        self,
        *,
        signal_id: int,
        run_after: datetime,
        error: str,
        now: datetime,
    ) -> None:
        _ = self._conn.execute(
            """
            UPDATE calendar_sync_signal
            SET
                status = ?,
                run_after = ?,
                locked_at = NULL,
                last_error = ?,
                updated_at = ?
            WHERE id = ?
            """,
            (
                OutboxStatus.PENDING,
                _serialize_datetime(run_after),
                error,
                _serialize_datetime(now),
                signal_id,
            ),
        )
        self._commit()

    def mark_calendar_sync_signal_failed(
        self,
        *,
        signal_id: int,
        error: str,
        now: datetime,
    ) -> None:
        _ = self._conn.execute(
            """
            UPDATE calendar_sync_signal
            SET status = ?, locked_at = NULL, last_error = ?, updated_at = ?
            WHERE id = ?
            """,
            (OutboxStatus.FAILED, error, _serialize_datetime(now), signal_id),
        )
        self._commit()

    def reconcile_stale_running_calendar_sync_signals(
        self,
        *,
        stale_before: datetime,
        now: datetime,
    ) -> int:
        result = self._conn.execute(
            """
            UPDATE calendar_sync_signal
            SET status = ?, locked_at = NULL, updated_at = ?
            WHERE status = ? AND locked_at IS NOT NULL AND locked_at < ?
            """,
            (
                OutboxStatus.PENDING,
                _serialize_datetime(now),
                OutboxStatus.RUNNING,
                _serialize_datetime(stale_before),
            ),
        )
        self._commit()
        return result.rowcount

    def cleanup_retention(
        self,
        *,
        now: datetime,
        calendar_sync_signal_retention_days: int,
        outbox_retention_days: int,
        job_retention_days: int,
        audit_log_retention_days: int,
        inbound_event_retention_days: int,
    ) -> RetentionCleanupResult:
        calendar_sync_cutoff = now - timedelta(
            days=max(calendar_sync_signal_retention_days, 1)
        )
        outbox_cutoff = now - timedelta(days=max(outbox_retention_days, 1))
        job_cutoff = now - timedelta(days=max(job_retention_days, 1))
        audit_cutoff = now - timedelta(days=max(audit_log_retention_days, 1))
        inbound_cutoff = now - timedelta(days=max(inbound_event_retention_days, 1))

        self._begin_immediate()
        try:
            calendar_deleted = self._conn.execute(
                """
                DELETE FROM calendar_sync_signal
                WHERE status IN (?, ?) AND created_at < ?
                """,
                (
                    OutboxStatus.DONE,
                    OutboxStatus.FAILED,
                    _serialize_datetime(calendar_sync_cutoff),
                ),
            ).rowcount

            outbox_deleted = self._conn.execute(
                """
                DELETE FROM outbox
                WHERE status IN (?, ?) AND created_at < ?
                """,
                (
                    OutboxStatus.DONE,
                    OutboxStatus.FAILED,
                    _serialize_datetime(outbox_cutoff),
                ),
            ).rowcount

            jobs_deleted = self._conn.execute(
                """
                DELETE FROM job
                WHERE status IN (?, ?, ?) AND created_at < ?
                """,
                (
                    JobStatus.DONE,
                    JobStatus.CANCELLED,
                    JobStatus.FAILED,
                    _serialize_datetime(job_cutoff),
                ),
            ).rowcount

            audit_deleted = self._conn.execute(
                """
                DELETE FROM audit_log
                WHERE created_at < ?
                """,
                (_serialize_datetime(audit_cutoff),),
            ).rowcount

            inbound_deleted = self._conn.execute(
                """
                DELETE FROM inbound_event_dedup
                WHERE received_at < ?
                """,
                (_serialize_datetime(inbound_cutoff),),
            ).rowcount

            self._commit()
            return RetentionCleanupResult(
                calendar_sync_signals_deleted=calendar_deleted,
                outbox_deleted=outbox_deleted,
                jobs_deleted=jobs_deleted,
                audit_logs_deleted=audit_deleted,
                inbound_events_deleted=inbound_deleted,
            )
        except Exception:
            self._rollback()
            raise

    def wal_checkpoint(self, *, mode: str = "PASSIVE") -> tuple[int, int, int] | None:
        safe_mode = mode.strip().upper()
        if safe_mode not in {"PASSIVE", "FULL", "RESTART", "TRUNCATE"}:
            raise ValueError("Unsupported WAL checkpoint mode")
        row = self._conn.execute(f"PRAGMA wal_checkpoint({safe_mode})").fetchone()
        self._commit()
        if row is None or len(row) != 3:
            return None
        first, second, third = row
        if (
            not isinstance(first, int)
            or not isinstance(second, int)
            or not isinstance(third, int)
        ):
            return None
        return (first, second, third)

    def vacuum(self) -> None:
        _ = self._conn.execute("VACUUM")
        self._commit()

    def upsert_conversation_state(
        self,
        *,
        chat_id: int,
        user_id: int,
        flow: str,
        state: Mapping[str, object],
        expires_at: datetime,
        now: datetime,
    ) -> None:
        _ = self._conn.execute(
            """
            INSERT INTO conversation_state (
                chat_id,
                user_id,
                flow,
                state_json,
                expires_at,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(chat_id, user_id, flow) DO UPDATE SET
                state_json = excluded.state_json,
                expires_at = excluded.expires_at,
                updated_at = excluded.updated_at
            """,
            (
                chat_id,
                user_id,
                flow,
                json.dumps(dict(state), separators=(",", ":"), sort_keys=True),
                _serialize_datetime(expires_at),
                _serialize_datetime(now),
            ),
        )
        self._commit()

    def get_conversation_state(
        self,
        *,
        chat_id: int,
        user_id: int,
        flow: str,
        now: datetime,
    ) -> dict[str, object] | None:
        maybe_row_obj: object = self._conn.execute(
            """
            SELECT state_json, expires_at
            FROM conversation_state
            WHERE chat_id = ? AND user_id = ? AND flow = ?
            """,
            (chat_id, user_id, flow),
        ).fetchone()
        if maybe_row_obj is None:
            return None

        row = cast(Mapping[str, object], maybe_row_obj)
        expires_at = _parse_required_datetime(
            _row_optional_str(row, "expires_at"),
            field="conversation_state.expires_at",
        )
        if now > expires_at:
            self.clear_conversation_state(chat_id=chat_id, user_id=user_id, flow=flow)
            return None

        payload_obj = json.loads(_row_str(row, "state_json"))
        if not isinstance(payload_obj, dict):
            return None
        return cast(dict[str, object], payload_obj)

    def clear_conversation_state(
        self, *, chat_id: int, user_id: int, flow: str
    ) -> None:
        _ = self._conn.execute(
            "DELETE FROM conversation_state WHERE chat_id = ? AND user_id = ? AND flow = ?",
            (chat_id, user_id, flow),
        )
        self._commit()

    def get_user_mapping(self, telegram_user_id: int) -> dict[str, object] | None:
        maybe_row_obj: object = self._conn.execute(
            """
            SELECT telegram_user_id, telegram_username, google_email, full_name, timezone, is_active, preferred_chat_id
            FROM user_mapping
            WHERE telegram_user_id = ?
            """,
            (telegram_user_id,),
        ).fetchone()
        if maybe_row_obj is None:
            return None
        return cast(dict[str, object], dict(cast(Mapping[str, object], maybe_row_obj)))

    def get_preferred_chat_id(self, *, telegram_user_id: int) -> int | None:
        maybe_row_obj: object = self._conn.execute(
            """
            SELECT preferred_chat_id
            FROM user_mapping
            WHERE telegram_user_id = ?
            """,
            (telegram_user_id,),
        ).fetchone()
        if maybe_row_obj is None:
            return None
        row = cast(dict[str, object], dict(cast(Mapping[str, object], maybe_row_obj)))
        value = row.get("preferred_chat_id")
        return value if isinstance(value, int) else None

    def set_preferred_chat_id(
        self,
        *,
        telegram_user_id: int,
        preferred_chat_id: int | None,
        now: datetime,
    ) -> bool:
        cursor = self._conn.execute(
            """
            UPDATE user_mapping
            SET preferred_chat_id = ?, updated_at = ?
            WHERE telegram_user_id = ?
            """,
            (
                preferred_chat_id,
                _serialize_datetime(now),
                telegram_user_id,
            ),
        )
        self._commit()
        return cursor.rowcount > 0

    def upsert_user_mapping(
        self,
        *,
        telegram_user_id: int,
        google_email: str,
        now: datetime,
        telegram_username: str | None = None,
        full_name: str | None = None,
        timezone: str = "Asia/Yekaterinburg",
        is_active: bool = True,
    ) -> None:
        normalized_email = google_email.strip().lower()
        if not normalized_email:
            raise ValueError("google_email must be non-empty")

        normalized_username = None
        if telegram_username is not None:
            stripped_username = telegram_username.strip()
            if stripped_username:
                normalized_username = stripped_username.lstrip("@")

        normalized_full_name = None
        if full_name is not None:
            stripped_full_name = full_name.strip()
            if stripped_full_name:
                normalized_full_name = stripped_full_name

        normalized_timezone = timezone.strip()
        if not normalized_timezone:
            raise ValueError("timezone must be non-empty")

        timestamp = _serialize_datetime(now)
        _ = self._conn.execute(
            """
            INSERT INTO user_mapping (
                telegram_user_id,
                telegram_username,
                google_email,
                full_name,
                timezone,
                is_active,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(telegram_user_id) DO UPDATE SET
                telegram_username = excluded.telegram_username,
                google_email = excluded.google_email,
                full_name = excluded.full_name,
                timezone = excluded.timezone,
                is_active = excluded.is_active,
                updated_at = excluded.updated_at
            """,
            (
                telegram_user_id,
                normalized_username,
                normalized_email,
                normalized_full_name,
                normalized_timezone,
                1 if is_active else 0,
                timestamp,
            ),
        )
        self._commit()

    def resolve_usernames(self, usernames: tuple[str, ...]) -> list[dict[str, object]]:
        if not usernames:
            return []
        placeholders = ", ".join(["?"] * len(usernames))
        rows = self._conn.execute(
            f"""
            SELECT telegram_user_id, telegram_username, google_email, full_name, timezone, is_active, preferred_chat_id
            FROM user_mapping
            WHERE telegram_username IN ({placeholders}) AND is_active = 1
            """,
            usernames,
        ).fetchall()
        return [cast(dict[str, object], dict(row)) for row in rows]

    def get_all_active_users(self) -> list[dict[str, object]]:
        rows = self._conn.execute(
            """
            SELECT telegram_user_id, telegram_username, google_email, full_name, timezone, is_active, preferred_chat_id
            FROM user_mapping
            WHERE is_active = 1
            """
        ).fetchall()
        return [cast(dict[str, object], dict(row)) for row in rows]

    def list_user_mappings(
        self,
        *,
        include_inactive: bool = False,
        limit: int = 200,
    ) -> list[dict[str, object]]:
        safe_limit = max(1, min(limit, 500))
        where_clause = "" if include_inactive else "WHERE is_active = 1"
        rows = self._conn.execute(
            f"""
            SELECT telegram_user_id, telegram_username, google_email, full_name, timezone, is_active, preferred_chat_id
            FROM user_mapping
            {where_clause}
            ORDER BY
                is_active DESC,
                COALESCE(full_name, '') COLLATE NOCASE ASC,
                COALESCE(telegram_username, '') COLLATE NOCASE ASC,
                telegram_user_id ASC
            LIMIT ?
            """,
            (safe_limit,),
        ).fetchall()
        return [cast(dict[str, object], dict(row)) for row in rows]

    def search_user_mappings(
        self,
        *,
        query: str,
        active_only: bool = True,
        limit: int = 10,
    ) -> list[dict[str, object]]:
        raw_query = query.strip()
        normalized_query = raw_query.lstrip("@").strip().casefold()
        if not normalized_query:
            return []

        safe_limit = max(1, min(limit, 50))

        candidates = self.list_user_mappings(
            include_inactive=not active_only,
            limit=500,
        )
        matched: list[dict[str, object]] = []
        for mapping in candidates:
            if active_only and not bool(mapping.get("is_active")):
                continue

            values: list[str] = []

            username_obj = mapping.get("telegram_username")
            if isinstance(username_obj, str) and username_obj.strip():
                values.append(username_obj.strip().casefold())

            email_obj = mapping.get("google_email")
            if isinstance(email_obj, str) and email_obj.strip():
                values.append(email_obj.strip().casefold())

            full_name_obj = mapping.get("full_name")
            if isinstance(full_name_obj, str) and full_name_obj.strip():
                values.append(full_name_obj.strip().casefold())

            user_id_obj = mapping.get("telegram_user_id")
            if isinstance(user_id_obj, int):
                values.append(str(user_id_obj))

            if any(normalized_query in value for value in values):
                matched.append(mapping)
                if len(matched) >= safe_limit:
                    break

        return matched

    def get_user_mapping_by_email(self, google_email: str) -> dict[str, object] | None:
        normalized_email = google_email.strip().lower()
        if not normalized_email:
            return None

        maybe_row_obj: object = self._conn.execute(
            """
            SELECT telegram_user_id, telegram_username, google_email, full_name, timezone, is_active, preferred_chat_id
            FROM user_mapping
            WHERE LOWER(google_email) = ?
            """,
            (normalized_email,),
        ).fetchone()
        if maybe_row_obj is None:
            return None
        return cast(dict[str, object], dict(cast(Mapping[str, object], maybe_row_obj)))

    def set_user_mapping_active(
        self,
        *,
        telegram_user_id: int,
        is_active: bool,
        now: datetime,
    ) -> bool:
        cursor = self._conn.execute(
            """
            UPDATE user_mapping
            SET is_active = ?, updated_at = ?
            WHERE telegram_user_id = ?
            """,
            (
                1 if is_active else 0,
                _serialize_datetime(now),
                telegram_user_id,
            ),
        )
        self._commit()
        return cursor.rowcount > 0

    def list_initiator_meetings(
        self,
        *,
        initiator_telegram_user_id: int,
        now: datetime,
        states: tuple[MeetingState, ...],
        limit: int = 20,
    ) -> list[Meeting]:
        if not states:
            return []

        placeholders = ", ".join(["?"] * len(states))
        query = (
            """
            SELECT meeting_id
            FROM meeting
            WHERE initiator_telegram_user_id = ?
              AND scheduled_end_at >= ?
              AND state IN ("""
            + placeholders
            + """)
            ORDER BY scheduled_start_at ASC
            LIMIT ?
            """
        )
        params: list[object] = [
            initiator_telegram_user_id,
            _serialize_datetime(now),
            *states,
            limit,
        ]
        rows = self._conn.execute(query, tuple(params)).fetchall()

        meetings: list[Meeting] = []
        for row in rows:
            meeting_row = cast(Mapping[str, object], row)
            meeting_id = _row_str(meeting_row, "meeting_id")
            meeting = self.get_meeting(meeting_id)
            if meeting is not None:
                meetings.append(meeting)
        return meetings

    def update_initiator_open_meetings_chat(
        self,
        *,
        initiator_telegram_user_id: int,
        target_chat_id: int | None,
        now: datetime,
        states: tuple[MeetingState, ...] = (
            MeetingState.DRAFT,
            MeetingState.PENDING,
            MeetingState.NEEDS_INITIATOR_DECISION,
        ),
    ) -> int:
        if not states:
            return 0

        resolved_chat_id = (
            target_chat_id if target_chat_id is not None else initiator_telegram_user_id
        )
        placeholders = ", ".join(["?"] * len(states))
        query = (
            """
            UPDATE meeting
            SET chat_id = ?, group_status_message_id = NULL, updated_at = ?
            WHERE initiator_telegram_user_id = ?
              AND scheduled_end_at >= ?
              AND state IN ("""
            + placeholders
            + ")"
        )
        params: list[object] = [
            resolved_chat_id,
            _serialize_datetime(now),
            initiator_telegram_user_id,
            _serialize_datetime(now),
            *states,
        ]
        cursor = self._conn.execute(query, tuple(params))
        self._commit()
        return cursor.rowcount

    def register_inbound_event(
        self,
        *,
        source: InboundEventSource,
        external_event_id: str,
        received_at: datetime,
    ) -> bool:
        try:
            _ = self._conn.execute(
                """
                INSERT INTO inbound_event_dedup (source, external_event_id, received_at)
                VALUES (?, ?, ?)
                """,
                (source, external_event_id, _serialize_datetime(received_at)),
            )
            self._commit()
            return True
        except sqlite3.IntegrityError:
            self._rollback()
            return False

    def unregister_inbound_event(
        self,
        *,
        source: InboundEventSource,
        external_event_id: str,
    ) -> None:
        _ = self._conn.execute(
            """
            DELETE FROM inbound_event_dedup
            WHERE source = ? AND external_event_id = ?
            """,
            (source, external_event_id),
        )
        self._commit()

    def upsert_callback_action_token(
        self,
        *,
        callback_token: CallbackActionToken,
        now: datetime,
    ) -> None:
        _ = self._conn.execute(
            """
            INSERT INTO callback_action_token (
                token,
                meeting_id,
                round,
                action_type,
                allowed_user_id,
                expires_at,
                created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(token) DO UPDATE SET
                meeting_id = excluded.meeting_id,
                round = excluded.round,
                action_type = excluded.action_type,
                allowed_user_id = excluded.allowed_user_id,
                expires_at = excluded.expires_at,
                created_at = excluded.created_at
            """,
            (
                callback_token.token,
                callback_token.meeting_id,
                callback_token.round,
                callback_token.action_type,
                callback_token.allowed_user_id,
                _serialize_datetime(callback_token.expires_at),
                _serialize_datetime(now),
            ),
        )
        self._commit()

    def get_callback_action_token(self, token: str) -> CallbackActionToken | None:
        maybe_row_obj: object = self._conn.execute(
            """
            SELECT token, meeting_id, round, action_type, allowed_user_id, expires_at
            FROM callback_action_token
            WHERE token = ?
            """,
            (token,),
        ).fetchone()
        if maybe_row_obj is None:
            return None
        row = cast(Mapping[str, object], maybe_row_obj)
        return CallbackActionToken(
            token=_row_str(row, "token"),
            meeting_id=_row_str(row, "meeting_id"),
            round=_row_int(row, "round"),
            action_type=CallbackActionType(_row_str(row, "action_type")),
            allowed_user_id=_row_int(row, "allowed_user_id"),
            expires_at=_parse_required_datetime(
                _row_optional_str(row, "expires_at"),
                field="callback_action_token.expires_at",
            ),
        )

    def expire_callback_tokens_for_participants(
        self,
        *,
        meeting_id: str,
        round: int,
        user_ids: tuple[int, ...],
        now: datetime,
    ) -> int:
        if not user_ids:
            return 0
        placeholders = ", ".join(["?"] * len(user_ids))
        query = (
            "UPDATE callback_action_token "
            + "SET expires_at = ? "
            + "WHERE meeting_id = ? AND round = ? AND allowed_user_id IN ("
            + placeholders
            + ")"
        )
        result = self._conn.execute(
            query,
            (
                _serialize_datetime(now),
                meeting_id,
                round,
                *user_ids,
            ),
        )
        self._commit()
        return result.rowcount

    def count_jobs(
        self,
        *,
        meeting_id: str,
        status: JobStatus | None = None,
        job_type: JobType | None = None,
    ) -> int:
        query = "SELECT COUNT(*) AS c FROM job WHERE meeting_id = ?"
        params: list[object] = [meeting_id]
        if status is not None:
            query += " AND status = ?"
            params.append(status)
        if job_type is not None:
            query += " AND job_type = ?"
            params.append(job_type)
        maybe_row_obj: object = self._conn.execute(query, tuple(params)).fetchone()
        if maybe_row_obj is None:
            return 0
        row = cast(Mapping[str, object], maybe_row_obj)
        return _row_int(row, "c")
