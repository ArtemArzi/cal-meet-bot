from datetime import datetime, timedelta
from unittest.mock import MagicMock
from bot_vstrechi.db.repository import SQLiteRepository
from bot_vstrechi.application.service import MeetingWorkflowService
from bot_vstrechi.telegram.adapter import TelegramWebhookAdapter
from bot_vstrechi.domain.models import (
    ReasonCode,
    Outcome,
    CallbackActionToken,
    CallbackActionType,
    Meeting,
    MeetingState,
    MeetingParticipant,
)


def test_double_click_bypasses_dedup_and_triggers_twice() -> None:
    repository = SQLiteRepository(":memory:")
    repository.initialize_schema()

    # Insert a fake meeting so workflow service doesn't fail
    now = datetime(2026, 2, 11, 10, 0, 0)
    meeting = Meeting(
        meeting_id="m-1",
        initiator_telegram_user_id=10,
        chat_id=100,
        state=MeetingState.PENDING,
        scheduled_start_at=now + timedelta(hours=4),
        scheduled_end_at=now + timedelta(hours=5),
        confirmation_round=1,
        confirmation_deadline_at=now + timedelta(hours=1),
        participants=(MeetingParticipant(telegram_user_id=100, is_required=True),),
    )
    repository.insert_meeting(meeting=meeting, now=now)

    # Insert a token
    token = CallbackActionToken(
        token="tok1",
        meeting_id="m-1",
        round=1,
        action_type=CallbackActionType.PARTICIPANT_CONFIRM,
        allowed_user_id=100,
        expires_at=now + timedelta(hours=1),
    )
    repository.upsert_callback_action_token(callback_token=token, now=now)

    service = MeetingWorkflowService(repository, calendar_gateway=MagicMock())
    adapter = TelegramWebhookAdapter(repository=repository, workflow_service=service)

    update1 = {
        "update_id": 1,
        "callback_query": {
            "id": "cb1",
            "from": {"id": 100},
            "message": {"chat": {"id": 100}, "message_id": 1},
            "data": "act:tok1",
        },
    }

    update2 = {
        "update_id": 2,
        "callback_query": {
            "id": "cb2",
            "from": {"id": 100},
            "message": {"chat": {"id": 100}, "message_id": 1},
            "data": "act:tok1",
        },
    }

    # First click
    res1 = adapter.handle_update(update=update1, now=now)

    # Second click - should ideally be rejected by InboundEventDedup, but it isn't.
    # What happens? Since the token is valid, it calls service.record_participant_decision!
    # Let's see what res2 returns.
    res2 = adapter.handle_update(update=update2, now=now)

    print(f"res1: {res1}")
    print(f"res2: {res2}")

    # Let's count how many times outbox was created for telegram send message
    outbox_count = repository.count_outbox(status=None)
    print(f"Outbox count: {outbox_count}")

    assert res2.outcome == Outcome.NOOP
    assert res2.reason_code in (
        ReasonCode.LATE_RESPONSE_RECORDED,
        ReasonCode.INVALID_STATE,
    )
