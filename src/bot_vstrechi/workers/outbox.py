from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
import logging
from typing import Protocol, cast

from bot_vstrechi.calendar.gateway import CalendarApiClient
from bot_vstrechi.domain.commands import CommandExecution
from bot_vstrechi.db.repository import ClaimedOutbox, SQLiteRepository
from bot_vstrechi.domain.models import (
    CommandResult,
    OutboxEffectType,
    OutboxStatus,
    Outcome,
    ReasonCode,
)


DEFAULT_OUTBOX_STALE_RUNNING_AFTER = timedelta(minutes=5)
DEFAULT_OUTBOX_RETRY_BACKOFF_BASE = timedelta(seconds=5)


logger = logging.getLogger(__name__)


class RetryableOutboxError(Exception):
    pass


class TelegramApiClient(Protocol):
    def send_message(
        self,
        *,
        telegram_user_id: int,
        text: str,
        buttons: list[dict[str, str] | list[dict[str, str]]] | None = None,
        keyboard: list[list[str]] | None = None,
        idempotency_key: str | None = None,
    ) -> int | None: ...

    def edit_message(
        self,
        *,
        telegram_user_id: int,
        message_id: int,
        text: str,
        buttons: list[dict[str, str] | list[dict[str, str]]] | None = None,
        idempotency_key: str | None = None,
    ) -> None: ...

    def answer_callback_query(
        self,
        *,
        callback_query_id: str,
        text: str | None = None,
        idempotency_key: str | None = None,
    ) -> None: ...


@dataclass(frozen=True)
class OutboxTickResult:
    processed: bool
    outbox_id: int | None = None
    status: OutboxStatus | None = None


class OutboxDispatcher:
    def __init__(
        self,
        *,
        repository: SQLiteRepository,
        telegram_client: TelegramApiClient,
        calendar_client: CalendarApiClient,
    ) -> None:
        self._repository: SQLiteRepository = repository
        self._telegram_client: TelegramApiClient = telegram_client
        self._calendar_client: CalendarApiClient = calendar_client

    def dispatch(self, *, message: ClaimedOutbox) -> None:
        if message.effect_type == OutboxEffectType.TELEGRAM_SEND_MESSAGE:
            self._dispatch_telegram(message=message)
            return

        if message.effect_type == OutboxEffectType.TELEGRAM_EDIT_MESSAGE:
            self._dispatch_telegram_edit(message=message)
            return

        if message.effect_type == OutboxEffectType.TELEGRAM_ANSWER_CALLBACK:
            self._dispatch_telegram_callback_answer(message=message)
            return

        if message.effect_type == OutboxEffectType.CALENDAR_INSERT_EVENT:
            self._dispatch_calendar_insert(message=message)
            return

        if message.effect_type == OutboxEffectType.CALENDAR_PATCH_EVENT:
            self._dispatch_calendar_patch(message=message)
            return

        raise ValueError(f"Unsupported outbox effect type: {message.effect_type}")

    def _dispatch_telegram(self, *, message: ClaimedOutbox) -> None:
        user_id_obj = message.payload.get("telegram_user_id")
        text_obj = message.payload.get("text")
        buttons_obj = message.payload.get("buttons")
        keyboard_obj = message.payload.get("keyboard")
        is_group_status_obj = message.payload.get("_group_status_message")
        meeting_id_obj = message.payload.get("_meeting_id")
        is_group_status = isinstance(is_group_status_obj, bool) and is_group_status_obj
        if not isinstance(user_id_obj, int) or not isinstance(text_obj, str):
            raise ValueError("Invalid telegram outbox payload")

        buttons: list[dict[str, str] | list[dict[str, str]]] | None = None
        if isinstance(buttons_obj, list):
            parsed_buttons: list[dict[str, str] | list[dict[str, str]]] = []
            for item_obj in buttons_obj:
                if isinstance(item_obj, dict):
                    text = item_obj.get("text")
                    callback_data = item_obj.get("callback_data")
                    if isinstance(text, str) and isinstance(callback_data, str):
                        parsed_buttons.append(
                            {"text": text, "callback_data": callback_data}
                        )
                    continue

                if isinstance(item_obj, list):
                    row: list[dict[str, str]] = []
                    for button_obj in item_obj:
                        if not isinstance(button_obj, dict):
                            continue
                        text = button_obj.get("text")
                        callback_data = button_obj.get("callback_data")
                        if isinstance(text, str) and isinstance(callback_data, str):
                            row.append({"text": text, "callback_data": callback_data})
                    if row:
                        parsed_buttons.append(row)

            if parsed_buttons:
                buttons = parsed_buttons

        keyboard: list[list[str]] | None = None
        if isinstance(keyboard_obj, list):
            parsed_keyboard: list[list[str]] = []
            for row_obj in keyboard_obj:
                if not isinstance(row_obj, list):
                    continue
                parsed_row = [
                    cell for cell in row_obj if isinstance(cell, str) and cell.strip()
                ]
                if parsed_row:
                    parsed_keyboard.append(parsed_row)
            if parsed_keyboard:
                keyboard = parsed_keyboard

        sent_message_id: int | None = None
        if keyboard is None:
            sent_message_id = self._telegram_client.send_message(
                telegram_user_id=user_id_obj,
                text=text_obj,
                buttons=buttons,
                idempotency_key=message.idempotency_key,
            )
        else:
            sent_message_id = self._telegram_client.send_message(
                telegram_user_id=user_id_obj,
                text=text_obj,
                buttons=buttons,
                keyboard=keyboard,
                idempotency_key=message.idempotency_key,
            )

        if (
            is_group_status
            and isinstance(meeting_id_obj, str)
            and isinstance(sent_message_id, int)
        ):
            self._update_group_status_message_pointer(
                meeting_id=meeting_id_obj,
                message_id=sent_message_id,
            )

    def _dispatch_telegram_edit(self, *, message: ClaimedOutbox) -> None:
        user_id_obj = message.payload.get("telegram_user_id")
        message_id_obj = message.payload.get("message_id")
        text_obj = message.payload.get("text")
        buttons_obj = message.payload.get("buttons")
        is_group_status_obj = message.payload.get("_group_status_message")
        meeting_id_obj = message.payload.get("_meeting_id")
        if (
            not isinstance(user_id_obj, int)
            or not isinstance(message_id_obj, int)
            or not isinstance(text_obj, str)
        ):
            raise ValueError("Invalid telegram edit payload")

        buttons: list[dict[str, str] | list[dict[str, str]]] | None = None
        if isinstance(buttons_obj, list):
            parsed_buttons: list[dict[str, str] | list[dict[str, str]]] = []
            for item_obj in buttons_obj:
                if isinstance(item_obj, dict):
                    text = item_obj.get("text")
                    callback_data = item_obj.get("callback_data")
                    if isinstance(text, str) and isinstance(callback_data, str):
                        parsed_buttons.append(
                            {"text": text, "callback_data": callback_data}
                        )
                    continue

                if isinstance(item_obj, list):
                    row: list[dict[str, str]] = []
                    for button_obj in item_obj:
                        if not isinstance(button_obj, dict):
                            continue
                        text = button_obj.get("text")
                        callback_data = button_obj.get("callback_data")
                        if isinstance(text, str) and isinstance(callback_data, str):
                            row.append({"text": text, "callback_data": callback_data})
                    if row:
                        parsed_buttons.append(row)

            buttons = parsed_buttons

        try:
            self._telegram_client.edit_message(
                telegram_user_id=user_id_obj,
                message_id=message_id_obj,
                text=text_obj,
                buttons=buttons,
                idempotency_key=message.idempotency_key,
            )
        except RuntimeError as error:
            if self._is_edit_already_applied(error):
                return

            is_group_status = (
                isinstance(is_group_status_obj, bool) and is_group_status_obj
            )
            if (
                is_group_status
                and isinstance(meeting_id_obj, str)
                and self._can_fallback_group_edit(error)
            ):
                sent_message_id = self._telegram_client.send_message(
                    telegram_user_id=user_id_obj,
                    text=text_obj,
                    buttons=buttons,
                    idempotency_key=(
                        f"{message.idempotency_key}:fallback"
                        if isinstance(message.idempotency_key, str)
                        else None
                    ),
                )
                if isinstance(sent_message_id, int):
                    self._update_group_status_message_pointer(
                        meeting_id=meeting_id_obj,
                        message_id=sent_message_id,
                    )
                return

            raise

    def _is_edit_already_applied(self, error: RuntimeError) -> bool:
        text = str(error).lower()
        return "message is not modified" in text

    def _can_fallback_group_edit(self, error: RuntimeError) -> bool:
        text = str(error).lower()
        fallback_markers = (
            "message to edit not found",
            "message can't be edited",
            "non-retryable status 400",
        )
        return any(marker in text for marker in fallback_markers)

    def _update_group_status_message_pointer(
        self, *, meeting_id: str, message_id: int
    ) -> None:
        meeting = self._repository.get_meeting(meeting_id)
        if meeting is None:
            return
        if meeting.group_status_message_id == message_id:
            return

        updated = replace(meeting, group_status_message_id=message_id)
        _ = self._repository.apply_execution(
            before=meeting,
            execution=CommandExecution(
                result=CommandResult(Outcome.OK, ReasonCode.UPDATED),
                meeting=updated,
            ),
            now=datetime.now(tz=timezone.utc),
        )

    def _dispatch_telegram_callback_answer(self, *, message: ClaimedOutbox) -> None:
        callback_query_id_obj = message.payload.get("callback_query_id")
        text_obj = message.payload.get("text")
        if (
            not isinstance(callback_query_id_obj, str)
            or not callback_query_id_obj.strip()
        ):
            raise ValueError("Invalid telegram callback answer payload")

        callback_text: str | None = None
        if isinstance(text_obj, str) and text_obj.strip():
            callback_text = text_obj.strip()

        self._telegram_client.answer_callback_query(
            callback_query_id=callback_query_id_obj,
            text=callback_text,
            idempotency_key=message.idempotency_key,
        )

    def _dispatch_calendar_patch(self, *, message: ClaimedOutbox) -> None:
        event_id_obj = message.payload.get("google_event_id")
        email_obj = message.payload.get("initiator_google_email")
        payload_obj = message.payload.get("payload")
        if (
            not isinstance(event_id_obj, str)
            or not isinstance(email_obj, str)
            or not isinstance(payload_obj, dict)
        ):
            raise ValueError("Invalid calendar patch outbox payload")

        payload = cast(dict[str, object], payload_obj)

        self._calendar_client.patch_event(
            google_event_id=event_id_obj,
            initiator_google_email=email_obj,
            payload=payload,
            idempotency_key=message.idempotency_key,
        )

    def _dispatch_calendar_insert(self, *, message: ClaimedOutbox) -> None:
        email_obj = message.payload.get("organizer_email")
        payload_obj = message.payload.get("payload")
        meeting_id_obj = message.payload.get("meeting_id")
        if (
            not isinstance(email_obj, str)
            or not isinstance(payload_obj, dict)
            or not isinstance(meeting_id_obj, str)
        ):
            raise ValueError("Invalid calendar insert outbox payload")

        payload = cast(dict[str, object], payload_obj)
        event_id = self._calendar_client.insert_event(
            organizer_email=email_obj,
            payload=payload,
            idempotency_key=message.idempotency_key,
        )

        meeting = self._repository.get_meeting(meeting_id_obj)
        if meeting:
            updated = replace(
                meeting, google_event_id=event_id, google_calendar_id=email_obj
            )
            _ = self._repository.apply_execution(
                before=meeting,
                execution=CommandExecution(
                    result=CommandResult(Outcome.OK, ReasonCode.UPDATED),
                    meeting=updated,
                ),
                now=datetime.now(tz=timezone.utc),
            )


class OutboxWorker:
    def __init__(
        self,
        *,
        repository: SQLiteRepository,
        dispatcher: OutboxDispatcher,
        max_attempts: int = 5,
        retry_backoff_base: timedelta | None = None,
    ) -> None:
        self._repository: SQLiteRepository = repository
        self._dispatcher: OutboxDispatcher = dispatcher
        self._max_attempts: int = max_attempts
        self._retry_backoff_base: timedelta = (
            retry_backoff_base or DEFAULT_OUTBOX_RETRY_BACKOFF_BASE
        )

    def reconcile_on_startup(
        self,
        *,
        now: datetime,
        stale_running_after: timedelta | None = None,
    ) -> int:
        stale_after = stale_running_after or DEFAULT_OUTBOX_STALE_RUNNING_AFTER
        return self._repository.reconcile_stale_running_outbox(
            stale_before=now - stale_after,
            now=now,
        )

    def run_once(self, *, now: datetime) -> OutboxTickResult:
        message = self._repository.claim_due_outbox(now=now)
        if message is None:
            return OutboxTickResult(processed=False)

        logger.info(
            "outbox claimed",
            extra={
                "outbox_id": message.outbox_id,
                "effect_type": message.effect_type,
            },
        )

        try:
            self._dispatcher.dispatch(message=message)
            self._repository.mark_outbox_done(outbox_id=message.outbox_id, now=now)
            logger.info(
                "outbox dispatched",
                extra={"outbox_id": message.outbox_id},
            )
            return OutboxTickResult(
                processed=True,
                outbox_id=message.outbox_id,
                status=OutboxStatus.DONE,
            )
        except Exception as error:
            if (
                self._is_retryable(error=error)
                and message.attempts < self._max_attempts
            ):
                retry_after = now + self._backoff_for_attempt(message.attempts)
                self._repository.mark_outbox_retry(
                    outbox_id=message.outbox_id,
                    run_after=retry_after,
                    error=str(error),
                    now=now,
                )
                logger.warning(
                    "outbox retry",
                    extra={
                        "outbox_id": message.outbox_id,
                        "error": str(error),
                    },
                )
                return OutboxTickResult(
                    processed=True,
                    outbox_id=message.outbox_id,
                    status=OutboxStatus.PENDING,
                )

            self._notify_managers_on_undeliverable_dm(
                message=message,
                error=error,
                now=now,
            )

            self._repository.mark_outbox_failed(
                outbox_id=message.outbox_id,
                error=str(error),
                now=now,
            )
            logger.error(
                "outbox failed",
                extra={
                    "outbox_id": message.outbox_id,
                    "error": str(error),
                },
            )
            return OutboxTickResult(
                processed=True,
                outbox_id=message.outbox_id,
                status=OutboxStatus.FAILED,
            )

    def _notify_managers_on_undeliverable_dm(
        self,
        *,
        message: ClaimedOutbox,
        error: Exception,
        now: datetime,
    ) -> None:
        if message.effect_type != OutboxEffectType.TELEGRAM_SEND_MESSAGE:
            return

        payload = message.payload
        is_manager_alert_obj = payload.get("_manager_alert")
        if isinstance(is_manager_alert_obj, bool) and is_manager_alert_obj:
            return

        user_id_obj = payload.get("telegram_user_id")
        text_obj = payload.get("text")
        if not isinstance(user_id_obj, int) or not isinstance(text_obj, str):
            return

        manager_ids = self._repository.list_active_manager_ids()
        if not manager_ids:
            return

        excerpt = text_obj.strip().replace("\n", " ")
        if len(excerpt) > 120:
            excerpt = excerpt[:117] + "..."

        alert_text = (
            "⚠️ Не удалось доставить личное уведомление участнику.\n"
            f"User ID: {user_id_obj}\n"
            f"Причина: {error}\n"
            f"Фрагмент: {excerpt}"
        )

        for manager_id in manager_ids:
            _ = self._repository.enqueue_outbox(
                effect_type=OutboxEffectType.TELEGRAM_SEND_MESSAGE,
                payload={
                    "telegram_user_id": manager_id,
                    "text": alert_text,
                    "_manager_alert": True,
                },
                idempotency_key=(
                    f"manager_undeliverable:{message.outbox_id}:"
                    f"{manager_id}:{now.isoformat(timespec='seconds')}"
                ),
                now=now,
            )

    def _is_retryable(self, *, error: Exception) -> bool:
        if isinstance(error, (RetryableOutboxError, TimeoutError, ConnectionError)):
            return True

        if not isinstance(error, RuntimeError):
            return False

        message = str(error).lower()
        if "non-retryable status" in message:
            return False

        if "failed with status" in message:
            status_candidates = [
                token for token in message.replace(":", " ").split() if token.isdigit()
            ]
            if not status_candidates:
                return False
            status_code = int(status_candidates[-1])
            return status_code in {429, 500, 502, 503, 504}

        transient_markers = (
            "request failed",
            "failed after all attempts",
            "exhausted attempts",
            "timed out",
            "timeout",
        )
        return any(marker in message for marker in transient_markers)

    def _backoff_for_attempt(self, attempt: int) -> timedelta:
        seconds = self._retry_backoff_base.total_seconds() * float(
            2 ** max(attempt - 1, 0)
        )
        return timedelta(seconds=seconds)
