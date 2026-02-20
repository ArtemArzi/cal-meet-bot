from __future__ import annotations
from unittest.mock import MagicMock

import json
import logging
from dataclasses import replace
from datetime import datetime, timedelta
from pathlib import Path
from typing import cast

import pytest
from bot_vstrechi.domain import Decision, Meeting, MeetingParticipant, MeetingState
from bot_vstrechi.infrastructure.logging import configure_logging
from bot_vstrechi.db.repository import SQLiteRepository
from bot_vstrechi.application.service import MeetingWorkflowService


def _repo(tmp_path: Path) -> SQLiteRepository:
    repository = SQLiteRepository(str(tmp_path / "bot_vstrechi_phase7_audit.db"))
    repository.initialize_schema()
    return repository


def _meeting(now: datetime, *, meeting_id: str = "m-7-audit") -> Meeting:
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


def test_insert_audit_log_creates_row(tmp_path: Path) -> None:
    now = datetime(2026, 2, 13, 11, 0, 0)
    repository = _repo(tmp_path)

    repository.insert_audit_log(
        meeting_id="m-7-audit-insert",
        round=1,
        actor_telegram_user_id=100,
        actor_type="user",
        action="manual_insert",
        details={"key": "value"},
        now=now,
    )

    logs = repository.get_audit_logs("m-7-audit-insert")
    assert len(logs) == 1
    assert logs[0]["action"] == "manual_insert"
    assert logs[0]["details_json"] == '{"key":"value"}'
    repository.close()


def test_audit_log_written_on_participant_decision(tmp_path: Path) -> None:
    now = datetime(2026, 2, 13, 11, 0, 0)
    repository = _repo(tmp_path)
    meeting = _meeting(now, meeting_id="m-7-audit-decision")
    repository.insert_meeting(meeting, now=now)
    service = MeetingWorkflowService(repository, calendar_gateway=MagicMock())

    _ = service.record_participant_decision(
        meeting_id=meeting.meeting_id,
        round=meeting.confirmation_round,
        actor_user_id=200,
        decision=Decision.CONFIRM,
        source="telegram",
        now=now,
    )

    logs = repository.get_audit_logs(meeting.meeting_id)
    assert len(logs) == 1
    assert logs[0]["action"] == "record_participant_decision"
    repository.close()


def test_audit_log_written_on_cancel(tmp_path: Path) -> None:
    now = datetime(2026, 2, 13, 11, 0, 0)
    repository = _repo(tmp_path)
    meeting = _meeting(now, meeting_id="m-7-audit-cancel")
    repository.insert_meeting(meeting, now=now)
    service = MeetingWorkflowService(repository, calendar_gateway=MagicMock())

    _ = service.cancel_meeting(
        meeting_id=meeting.meeting_id,
        actor_user_id=meeting.initiator_telegram_user_id,
        reason="manual",
        now=now,
    )

    logs = repository.get_audit_logs(meeting.meeting_id)
    assert len(logs) == 1
    assert logs[0]["action"] == "cancel_meeting"
    assert logs[0]["actor_type"] == "user"
    repository.close()


def test_audit_log_contains_state_transition_details(tmp_path: Path) -> None:
    now = datetime(2026, 2, 13, 11, 0, 0)
    repository = _repo(tmp_path)
    meeting = _meeting(now, meeting_id="m-7-audit-details")
    repository.insert_meeting(meeting, now=now)
    service = MeetingWorkflowService(repository, calendar_gateway=MagicMock())

    _ = service.cancel_meeting(
        meeting_id=meeting.meeting_id,
        actor_user_id=meeting.initiator_telegram_user_id,
        reason="manual",
        now=now,
    )

    logs = repository.get_audit_logs(meeting.meeting_id)
    assert len(logs) == 1
    details_json = logs[0]["details_json"]
    assert isinstance(details_json, str)
    details = cast(dict[str, object], json.loads(details_json))
    assert details["outcome"] == "ok"
    assert details["reason_code"] == "updated"
    assert details["state_before"] == "pending"
    assert details["state_after"] == "cancelled"
    repository.close()


def test_audit_log_actor_type_system_for_deadline(tmp_path: Path) -> None:
    now = datetime(2026, 2, 13, 11, 0, 0)
    repository = _repo(tmp_path)
    meeting = replace(
        _meeting(now, meeting_id="m-7-audit-system"),
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
        confirmation_deadline_at=now - timedelta(seconds=1),
    )
    repository.insert_meeting(meeting, now=now)
    service = MeetingWorkflowService(repository, calendar_gateway=MagicMock())

    _ = service.handle_confirm_deadline(
        meeting_id=meeting.meeting_id,
        round=meeting.confirmation_round,
        now=now,
    )

    logs = repository.get_audit_logs(meeting.meeting_id)
    assert len(logs) == 1
    assert logs[0]["action"] == "handle_confirm_deadline"
    assert logs[0]["actor_type"] == "system"
    repository.close()


def test_configure_logging_json_produces_valid_json(
    capsys: pytest.CaptureFixture[str],
) -> None:
    configure_logging("INFO", "json")
    logging.getLogger("phase7.json").info("hello-json", extra={"k": "v"})

    captured = capsys.readouterr()
    line = captured.err.strip().splitlines()[-1]
    payload = cast(dict[str, object], json.loads(line))
    assert payload["level"] == "INFO"
    assert payload["logger"] == "phase7.json"
    assert payload["msg"] == "hello-json"
    assert payload["k"] == "v"


def test_configure_logging_text_format(capsys: pytest.CaptureFixture[str]) -> None:
    configure_logging("INFO", "text")
    logging.getLogger("phase7.text").info("hello-text")

    captured = capsys.readouterr()
    line = captured.err.strip().splitlines()[-1]
    assert "INFO" in line
    assert "phase7.text" in line
    assert "hello-text" in line


def test_configure_logging_pretty_format_includes_context(
    capsys: pytest.CaptureFixture[str],
) -> None:
    configure_logging("INFO", "pretty")
    logging.getLogger("phase7.pretty").info(
        "hello-pretty",
        extra={"meeting_id": "m-1", "outcome": "ok"},
    )

    captured = capsys.readouterr()
    line = captured.err.strip().splitlines()[-1]
    assert "INFO" in line
    assert "phase7.pretty" in line
    assert "hello-pretty" in line
    assert "meeting_id=m-1" in line
    assert "outcome=ok" in line
