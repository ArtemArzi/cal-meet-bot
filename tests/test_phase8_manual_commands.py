from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock

from bot_vstrechi.application.service import MeetingWorkflowService
from bot_vstrechi.db.repository import SQLiteRepository
from bot_vstrechi.domain.models import Meeting, MeetingState, Outcome, ReasonCode
from bot_vstrechi.telegram.adapter import TelegramWebhookAdapter
from bot_vstrechi.telegram.presentation import (
    BUTTON_HELP,
    BUTTON_PEOPLE,
    main_menu_keyboard,
    telegram_commands_payload,
)


def _repo(tmp_path: Path) -> SQLiteRepository:
    repository = SQLiteRepository(str(tmp_path / "bot_vstrechi_phase8.db"))
    repository.initialize_schema()
    return repository


def _adapter(
    tmp_path: Path,
    *,
    manager_user_ids: tuple[int, ...] = (),
) -> TelegramWebhookAdapter:
    adapter, _ = _adapter_with_repo(tmp_path, manager_user_ids=manager_user_ids)
    return adapter


def _adapter_with_repo(
    tmp_path: Path,
    *,
    manager_user_ids: tuple[int, ...] = (),
) -> tuple[TelegramWebhookAdapter, SQLiteRepository]:
    repository = _repo(tmp_path)
    seeded_at = datetime(2026, 2, 20, 0, 0, 0)
    for manager_user_id in manager_user_ids:
        repository.grant_manager_role(
            telegram_user_id=manager_user_id,
            granted_by=None,
            now=seeded_at,
        )
    service = MeetingWorkflowService(repository, calendar_gateway=MagicMock())
    return TelegramWebhookAdapter(
        repository=repository, workflow_service=service
    ), repository


def test_start_and_help_commands_remain_supported(tmp_path: Path) -> None:
    now = datetime(2026, 2, 20, 12, 0, 0)
    adapter = _adapter(tmp_path)

    start_result = adapter.handle_update(
        update={
            "update_id": 9001,
            "message": {
                "text": "/start",
                "from": {"id": 100},
                "chat": {"id": 100},
            },
        },
        now=now,
    )
    assert start_result.outcome == Outcome.OK

    help_result = adapter.handle_update(
        update={
            "update_id": 9002,
            "message": {
                "text": "/help",
                "from": {"id": 100},
                "chat": {"id": 100},
            },
        },
        now=now,
    )
    assert help_result.outcome == Outcome.OK


def test_unsupported_commands_are_rejected(tmp_path: Path) -> None:
    now = datetime(2026, 2, 20, 12, 0, 0)
    adapter = _adapter(tmp_path)

    unsupported_commands = (
        "/meet",
        "/reschedule",
        "/cancel_meet",
        "/schedule",
        "/free",
    )

    for index, command in enumerate(unsupported_commands, start=1):
        result = adapter.handle_update(
            update={
                "update_id": 9100 + index,
                "message": {
                    "text": command,
                    "from": {"id": 100},
                    "chat": {"id": 100},
                },
            },
            now=now,
        )
        assert result.outcome == Outcome.NOOP
        assert result.reason_code == ReasonCode.INVALID_STATE


def test_supported_commands_and_buttons_have_no_legacy_entries() -> None:
    commands = {item["command"] for item in telegram_commands_payload()}
    assert commands == {"start", "help", "chat", "people"}

    keyboard = main_menu_keyboard()
    labels = {button for row in keyboard for button in row}
    assert labels == {BUTTON_PEOPLE, BUTTON_HELP}

    joined = " ".join(labels).lower()
    for legacy in ("meet", "reschedule", "cancel_meet", "schedule", "free"):
        assert legacy not in joined


def test_help_message_contains_clear_usage_flow(tmp_path: Path) -> None:
    now = datetime(2026, 2, 20, 12, 0, 0)
    adapter, repository = _adapter_with_repo(tmp_path)

    result = adapter.handle_update(
        update={
            "update_id": 9108,
            "message": {
                "text": "/help",
                "from": {"id": 100},
                "chat": {"id": 100},
            },
        },
        now=now,
    )
    assert result.outcome == Outcome.OK

    outbox = repository.claim_due_outbox(now=now)
    assert outbox is not None
    text_obj = outbox.payload.get("text")
    assert isinstance(text_obj, str)
    assert "как пользоваться ботом" in text_obj.lower()
    assert "1)" in text_obj
    assert "/start" in text_obj
    assert "/help" in text_obj
    assert "/chat" in text_obj
    assert "/people" in text_obj
    assert "/meet" not in text_obj


def test_start_message_for_active_user_contains_quick_instruction(
    tmp_path: Path,
) -> None:
    now = datetime(2026, 2, 20, 12, 0, 0)
    adapter, repository = _adapter_with_repo(tmp_path)
    repository.upsert_user_mapping(
        telegram_user_id=100,
        google_email="artem@company.com",
        telegram_username="artem",
        full_name="Artem",
        timezone="Asia/Yekaterinburg",
        now=now,
        is_active=True,
    )

    result = adapter.handle_update(
        update={
            "update_id": 9109,
            "message": {
                "text": "/start",
                "from": {"id": 100},
                "chat": {"id": 100},
            },
        },
        now=now,
    )
    assert result.outcome == Outcome.OK

    outbox = repository.claim_due_outbox(now=now)
    assert outbox is not None
    text_obj = outbox.payload.get("text")
    assert isinstance(text_obj, str)
    assert "аккаунт подключен" in text_obj.lower()
    assert "как это работает" in text_obj.lower()
    assert "google calendar" in text_obj.lower()
    assert "/help" in text_obj
    assert "/chat" in text_obj
    assert "/people" in text_obj


def test_chat_command_remains_supported_for_manager(tmp_path: Path) -> None:
    now = datetime(2026, 2, 20, 12, 0, 0)
    adapter = _adapter(tmp_path, manager_user_ids=(100,))

    result = adapter.handle_update(
        update={
            "update_id": 9210,
            "message": {
                "text": "/chat",
                "from": {"id": 100},
                "chat": {"id": 100},
            },
        },
        now=now,
    )

    assert result.outcome == Outcome.OK


def test_chat_command_is_rejected_for_non_manager(tmp_path: Path) -> None:
    now = datetime(2026, 2, 20, 12, 0, 0)
    adapter = _adapter(tmp_path)

    result = adapter.handle_update(
        update={
            "update_id": 9211,
            "message": {
                "text": "/chat",
                "from": {"id": 100},
                "chat": {"id": 100},
            },
        },
        now=now,
    )

    assert result.outcome == Outcome.REJECTED
    assert result.reason_code == ReasonCode.PERMISSION_DENIED


def test_chat_command_is_rejected_in_group_chat_for_manager(tmp_path: Path) -> None:
    now = datetime(2026, 2, 20, 12, 0, 0)
    adapter = _adapter(tmp_path, manager_user_ids=(100,))

    result = adapter.handle_update(
        update={
            "update_id": 9212,
            "message": {
                "text": "/chat",
                "from": {"id": 100},
                "chat": {"id": -100500},
            },
        },
        now=now,
    )

    assert result.outcome == Outcome.REJECTED
    assert result.reason_code == ReasonCode.PERMISSION_DENIED
    assert "личном чате" in result.message.lower()


def test_chat_clear_then_input_updates_open_meetings_chat_id(tmp_path: Path) -> None:
    now = datetime(2026, 2, 20, 12, 0, 0)
    adapter, repository = _adapter_with_repo(tmp_path, manager_user_ids=(100,))
    repository.upsert_user_mapping(
        telegram_user_id=100,
        telegram_username="manager",
        google_email="manager@example.com",
        now=now,
    )

    repository.insert_meeting(
        Meeting(
            meeting_id="m-chat-1",
            initiator_telegram_user_id=100,
            chat_id=100,
            state=MeetingState.DRAFT,
            scheduled_start_at=now + timedelta(hours=1),
            scheduled_end_at=now + timedelta(hours=2),
            title="Draft",
        ),
        now=now,
    )
    repository.insert_meeting(
        Meeting(
            meeting_id="m-chat-2",
            initiator_telegram_user_id=100,
            chat_id=100,
            state=MeetingState.PENDING,
            scheduled_start_at=now + timedelta(hours=3),
            scheduled_end_at=now + timedelta(hours=4),
            title="Pending",
        ),
        now=now,
    )

    open_result = adapter.handle_update(
        update={
            "update_id": 9213,
            "message": {
                "text": "/chat",
                "from": {"id": 100},
                "chat": {"id": 100},
            },
        },
        now=now,
    )
    assert open_result.outcome == Outcome.OK

    clear_result = adapter.handle_update(
        update={
            "update_id": 9214,
            "callback_query": {
                "id": "cb-chat-clear",
                "from": {"id": 100},
                "data": "chat_menu:clear",
                "message": {
                    "message_id": 81,
                    "chat": {"id": 100},
                    "text": "Настройка чата статусов",
                },
            },
        },
        now=now,
    )
    assert clear_result.outcome == Outcome.OK

    set_result = adapter.handle_update(
        update={
            "update_id": 9215,
            "message": {
                "text": "-1005151698406",
                "from": {"id": 100},
                "chat": {"id": 100},
            },
        },
        now=now,
    )
    assert set_result.outcome == Outcome.OK

    meeting_one = repository.get_meeting("m-chat-1")
    meeting_two = repository.get_meeting("m-chat-2")
    assert meeting_one is not None
    assert meeting_two is not None
    assert meeting_one.chat_id == -1005151698406
    assert meeting_two.chat_id == -1005151698406
    assert meeting_one.group_status_message_id is None
    assert meeting_two.group_status_message_id is None
    mapping = repository.get_user_mapping(100)
    assert mapping is not None
    assert mapping.get("preferred_chat_id") == -1005151698406


def test_people_command_remains_supported(tmp_path: Path) -> None:
    now = datetime(2026, 2, 20, 12, 0, 0)
    adapter = _adapter(tmp_path, manager_user_ids=(100,))

    result = adapter.handle_update(
        update={
            "update_id": 9201,
            "message": {
                "text": "/people",
                "from": {"id": 100},
                "chat": {"id": 100},
            },
        },
        now=now,
    )

    assert result.outcome == Outcome.OK


def test_people_command_is_rejected_for_non_manager(tmp_path: Path) -> None:
    now = datetime(2026, 2, 20, 12, 0, 0)
    adapter = _adapter(tmp_path)

    result = adapter.handle_update(
        update={
            "update_id": 9202,
            "message": {
                "text": "/people",
                "from": {"id": 100},
                "chat": {"id": 100},
            },
        },
        now=now,
    )

    assert result.outcome == Outcome.REJECTED
    assert result.reason_code == ReasonCode.PERMISSION_DENIED


def test_people_command_is_rejected_in_group_chat_for_manager(tmp_path: Path) -> None:
    now = datetime(2026, 2, 20, 12, 0, 0)
    adapter = _adapter(tmp_path, manager_user_ids=(100,))

    result = adapter.handle_update(
        update={
            "update_id": 9203,
            "message": {
                "text": "/people",
                "from": {"id": 100},
                "chat": {"id": -100500},
            },
        },
        now=now,
    )

    assert result.outcome == Outcome.REJECTED
    assert result.reason_code == ReasonCode.PERMISSION_DENIED
    assert "личном чате" in result.message.lower()


def test_people_menu_callback_is_rejected_in_group_chat(tmp_path: Path) -> None:
    now = datetime(2026, 2, 20, 12, 0, 0)
    adapter = _adapter(tmp_path, manager_user_ids=(100,))

    result = adapter.handle_update(
        update={
            "update_id": 9204,
            "callback_query": {
                "id": "cb-people-group",
                "from": {"id": 100},
                "data": "people_menu:list",
                "message": {
                    "message_id": 42,
                    "chat": {"id": -100500},
                    "text": "Участники",
                },
            },
        },
        now=now,
    )

    assert result.outcome == Outcome.REJECTED
    assert result.reason_code == ReasonCode.PERMISSION_DENIED
    assert "личном чате" in result.message.lower()
