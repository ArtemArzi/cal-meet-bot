from __future__ import annotations

import secrets
from dataclasses import dataclass
from datetime import datetime, timedelta

from bot_vstrechi.domain.models import CallbackActionToken, CallbackActionType, Meeting
from bot_vstrechi.db.repository import SQLiteRepository


DEFAULT_CALLBACK_TOKEN_TTL = timedelta(minutes=30)


@dataclass(frozen=True)
class InlineButton:
    text: str
    callback_data: str


def _build_callback_data(token: str) -> str:
    return f"act:{token}"


class CallbackTokenService:
    def __init__(
        self,
        repository: SQLiteRepository,
        *,
        ttl: timedelta = DEFAULT_CALLBACK_TOKEN_TTL,
    ) -> None:
        self._repository: SQLiteRepository = repository
        self._ttl: timedelta = ttl

    def issue_callback_button(
        self,
        *,
        meeting: Meeting,
        action_type: CallbackActionType,
        allowed_user_id: int,
        now: datetime,
        text: str,
    ) -> InlineButton:
        token = self._issue_token(
            meeting=meeting,
            action_type=action_type,
            allowed_user_id=allowed_user_id,
            now=now,
        )
        return InlineButton(text=text, callback_data=_build_callback_data(token.token))

    def build_participant_decision_buttons(
        self,
        *,
        meeting: Meeting,
        participant_user_id: int,
        now: datetime,
    ) -> tuple[InlineButton, InlineButton]:
        confirm = self.issue_callback_button(
            meeting=meeting,
            action_type=CallbackActionType.PARTICIPANT_CONFIRM,
            allowed_user_id=participant_user_id,
            now=now,
            text="✅ Участвую",
        )
        cancel = self.issue_callback_button(
            meeting=meeting,
            action_type=CallbackActionType.PARTICIPANT_CANCEL,
            allowed_user_id=participant_user_id,
            now=now,
            text="❌ Не смогу",
        )
        return confirm, cancel

    def build_initiator_decision_buttons(
        self,
        *,
        meeting: Meeting,
        now: datetime,
        allowed_user_id: int | None = None,
    ) -> tuple[InlineButton, InlineButton, InlineButton]:
        target_user_id = allowed_user_id or meeting.initiator_telegram_user_id
        replan = self.issue_callback_button(
            meeting=meeting,
            action_type=CallbackActionType.INITIATOR_REPLAN,
            allowed_user_id=target_user_id,
            now=now,
            text="🔁 Перепланировать",
        )
        cancel = self.issue_callback_button(
            meeting=meeting,
            action_type=CallbackActionType.INITIATOR_CANCEL,
            allowed_user_id=target_user_id,
            now=now,
            text="❌ Отменить",
        )
        proceed = self.issue_callback_button(
            meeting=meeting,
            action_type=CallbackActionType.INITIATOR_PROCEED_WITHOUT_SUBSET,
            allowed_user_id=target_user_id,
            now=now,
            text="✅ Провести без ожидающих ответа",
        )
        return replan, cancel, proceed

    def _issue_token(
        self,
        *,
        meeting: Meeting,
        action_type: CallbackActionType,
        allowed_user_id: int,
        now: datetime,
    ) -> CallbackActionToken:
        token = secrets.token_urlsafe(18)
        callback_token = CallbackActionToken(
            token=token,
            meeting_id=meeting.meeting_id,
            round=meeting.confirmation_round,
            action_type=action_type,
            allowed_user_id=allowed_user_id,
            expires_at=now + self._ttl,
        )
        self._repository.upsert_callback_action_token(
            callback_token=callback_token,
            now=now,
        )
        return callback_token
