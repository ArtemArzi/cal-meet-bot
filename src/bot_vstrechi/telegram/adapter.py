from __future__ import annotations

import hashlib
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import cast

from bot_vstrechi.domain.models import (
    CallbackActionType,
    Decision,
    InboundEventSource,
    MeetingState,
    OutboxEffectType,
    Outcome,
    ReasonCode,
)
from bot_vstrechi.db.repository import SQLiteRepository
from bot_vstrechi.application.service import MeetingWorkflowService
from bot_vstrechi.telegram.callback_tokens import CallbackTokenService
from bot_vstrechi.telegram.presentation import (
    BUTTON_HELP,
    BUTTON_PEOPLE,
    main_menu_keyboard,
)


STALE_ACTION_MESSAGE = (
    "Действие устарело. Откройте актуальное сообщение или начните заново."
)
PEOPLE_FLOW_NAME = "people"
CHAT_FLOW_NAME = "chat_config"
PEOPLE_CONVERSATION_TTL = timedelta(minutes=30)
CHAT_CONVERSATION_TTL = timedelta(minutes=30)
PEOPLE_MANAGER_ONLY_MESSAGE = "Команда /people доступна только активным менеджерам."
PEOPLE_PRIVATE_CHAT_ONLY_MESSAGE = (
    "Команда /people доступна только в личном чате с ботом."
)
CHAT_MANAGER_ONLY_MESSAGE = "Команда /chat доступна только активным менеджерам."
CHAT_PRIVATE_CHAT_ONLY_MESSAGE = "Команда /chat доступна только в личном чате с ботом."
CHAT_INPUT_EXIT_HINT = (
    "Если не хотите менять чат сейчас, ничего не вводите: "
    "просто отправьте другую команду (/start, /help, /people, /chat)."
)
OPEN_CHAT_SCOPE_STATES: tuple[MeetingState, ...] = (
    MeetingState.DRAFT,
    MeetingState.PENDING,
    MeetingState.NEEDS_INITIATOR_DECISION,
)


MAIN_MENU_NORMALIZED_COMMANDS: dict[str, str] = {
    BUTTON_PEOPLE.lower(): "/people",
    BUTTON_HELP.lower(): "/help",
    "ℹ️ помощь": "/help",
}


@dataclass(frozen=True)
class TelegramAdapterResult:
    outcome: Outcome
    reason_code: ReasonCode
    message: str


def build_callback_data(token: str) -> str:
    return f"act:{token}"


def parse_callback_data(data: str) -> str | None:
    if not data.startswith("act:"):
        return None
    token = data[4:].strip()
    if not token:
        return None
    return token


class TelegramWebhookAdapter:
    _callback_tokens: CallbackTokenService

    def __init__(
        self,
        repository: SQLiteRepository,
        workflow_service: MeetingWorkflowService,
    ) -> None:
        self._repository: SQLiteRepository = repository
        self._workflow_service: MeetingWorkflowService = workflow_service
        self._callback_tokens = CallbackTokenService(repository)

    def handle_update(
        self,
        *,
        update: Mapping[str, object],
        now: datetime,
    ) -> TelegramAdapterResult:
        update_id = update.get("update_id")
        if update_id is None:
            return TelegramAdapterResult(
                outcome=Outcome.REJECTED,
                reason_code=ReasonCode.INVALID_CALLBACK_FORMAT,
                message=STALE_ACTION_MESSAGE,
            )

        event_id = str(update_id)
        accepted = self._repository.register_inbound_event(
            source=InboundEventSource.TELEGRAM_UPDATE,
            external_event_id=event_id,
            received_at=now,
        )
        if not accepted:
            return TelegramAdapterResult(
                outcome=Outcome.NOOP,
                reason_code=ReasonCode.DUPLICATE_INBOUND_EVENT,
                message="Повторное обновление проигнорировано",
            )

        try:
            callback_query = update.get("callback_query")
            if isinstance(callback_query, dict):
                return self._handle_callback_query(
                    callback_query=cast(Mapping[str, object], callback_query),
                    now=now,
                )

            return self._handle_registered_update(update=update, now=now)
        except Exception:
            self._repository.unregister_inbound_event(
                source=InboundEventSource.TELEGRAM_UPDATE,
                external_event_id=event_id,
            )
            raise

    def _handle_registered_update(
        self,
        *,
        update: Mapping[str, object],
        now: datetime,
    ) -> TelegramAdapterResult:
        message_obj = update.get("message")
        if isinstance(message_obj, dict):
            message = cast(Mapping[str, object], message_obj)
            chat_obj = message.get("chat")
            chat_id = None
            if isinstance(chat_obj, dict):
                chat_id = cast(Mapping[str, object], chat_obj).get("id")

            if not isinstance(chat_id, int):
                return TelegramAdapterResult(
                    outcome=Outcome.NOOP,
                    reason_code=ReasonCode.INVALID_CALLBACK_FORMAT,
                    message="Не удалось определить chat_id",
                )

            text_obj = message.get("text")
            if isinstance(text_obj, str):
                text = text_obj.strip()
                from_obj = message.get("from")
                actor_user_id = cast(Mapping[str, object], from_obj or {}).get("id")
                if not isinstance(actor_user_id, int):
                    return TelegramAdapterResult(
                        outcome=Outcome.REJECTED,
                        reason_code=ReasonCode.INVALID_CALLBACK_FORMAT,
                        message="Некорректный пользователь",
                    )

                normalized_command = MAIN_MENU_NORMALIZED_COMMANDS.get(text.lower())
                if self._is_global_navigation_input(
                    text=text,
                    normalized_command=normalized_command,
                ):
                    self._clear_user_conversation_states(
                        chat_id=chat_id,
                        user_id=actor_user_id,
                    )

                if not text.startswith("/") and normalized_command is None:
                    chat_conversation = self._repository.get_conversation_state(
                        chat_id=chat_id,
                        user_id=actor_user_id,
                        flow=CHAT_FLOW_NAME,
                        now=now,
                    )
                    if isinstance(chat_conversation, dict):
                        mode_obj = chat_conversation.get("mode")
                        if mode_obj == "await_chat_target_id":
                            return self._handle_chat_target_input(
                                chat_id=chat_id,
                                actor_user_id=actor_user_id,
                                text=text,
                                now=now,
                            )

                    people_conversation = self._repository.get_conversation_state(
                        chat_id=chat_id,
                        user_id=actor_user_id,
                        flow=PEOPLE_FLOW_NAME,
                        now=now,
                    )
                    if isinstance(people_conversation, dict):
                        mode_obj = people_conversation.get("mode")
                        if mode_obj == "await_people_add_fields":
                            return self._handle_people_add_fields_input(
                                chat_id=chat_id,
                                actor_user_id=actor_user_id,
                                text=text,
                                now=now,
                            )
                        if mode_obj == "await_people_remove_query":
                            return self._handle_people_remove_query_input(
                                chat_id=chat_id,
                                actor_user_id=actor_user_id,
                                text=text,
                                now=now,
                            )
                        if mode_obj in {
                            "await_people_add_confirm",
                            "await_people_remove_confirm",
                        }:
                            _ = self._repository.enqueue_outbox(
                                effect_type=OutboxEffectType.TELEGRAM_SEND_MESSAGE,
                                payload={
                                    "telegram_user_id": chat_id,
                                    "text": (
                                        "Подтвердите действие кнопкой в предыдущем сообщении "
                                        "или начните заново через /people."
                                    ),
                                },
                                idempotency_key=(
                                    f"people_waiting_confirm:{chat_id}:{actor_user_id}:{now.isoformat(timespec='seconds')}"
                                ),
                                now=now,
                            )
                            return TelegramAdapterResult(
                                outcome=Outcome.NOOP,
                                reason_code=ReasonCode.INVALID_STATE,
                                message="Ожидается подтверждение кнопкой",
                            )

                if text.startswith("/start"):
                    return self._handle_start_message(
                        message=message, chat_id=chat_id, now=now
                    )
                if text.startswith("/help") or normalized_command == "/help":
                    return self._handle_help_message(
                        message=message, chat_id=chat_id, now=now
                    )
                if text.startswith("/chat"):
                    return self._handle_chat_message(
                        message=message,
                        chat_id=chat_id,
                        text=text,
                        now=now,
                    )
                if text.startswith("/people") or normalized_command == "/people":
                    return self._handle_people_message(
                        message=message,
                        chat_id=chat_id,
                        now=now,
                    )

        return TelegramAdapterResult(
            outcome=Outcome.NOOP,
            reason_code=ReasonCode.INVALID_STATE,
            message="Неподдерживаемый тип обновления",
        )

    def _clear_user_conversation_states(
        self,
        *,
        chat_id: int,
        user_id: int,
    ) -> None:
        self._repository.clear_conversation_state(
            chat_id=chat_id,
            user_id=user_id,
            flow=PEOPLE_FLOW_NAME,
        )
        self._repository.clear_conversation_state(
            chat_id=chat_id,
            user_id=user_id,
            flow=CHAT_FLOW_NAME,
        )

    def _is_global_navigation_input(
        self,
        *,
        text: str,
        normalized_command: str | None,
    ) -> bool:
        if normalized_command is not None:
            return True

        return text.startswith(
            (
                "/start",
                "/help",
                "/chat",
                "/people",
            )
        )

    def _ensure_people_manager(
        self,
        *,
        actor_user_id: int,
    ) -> TelegramAdapterResult | None:
        if self._repository.is_manager(telegram_user_id=actor_user_id):
            return None
        return TelegramAdapterResult(
            outcome=Outcome.REJECTED,
            reason_code=ReasonCode.PERMISSION_DENIED,
            message=PEOPLE_MANAGER_ONLY_MESSAGE,
        )

    def _ensure_people_private_chat(
        self,
        *,
        chat_id: int,
        actor_user_id: int,
    ) -> TelegramAdapterResult | None:
        if chat_id == actor_user_id:
            return None
        return TelegramAdapterResult(
            outcome=Outcome.REJECTED,
            reason_code=ReasonCode.PERMISSION_DENIED,
            message=PEOPLE_PRIVATE_CHAT_ONLY_MESSAGE,
        )

    def _ensure_chat_manager(
        self,
        *,
        actor_user_id: int,
    ) -> TelegramAdapterResult | None:
        if self._repository.is_manager(telegram_user_id=actor_user_id):
            return None
        return TelegramAdapterResult(
            outcome=Outcome.REJECTED,
            reason_code=ReasonCode.PERMISSION_DENIED,
            message=CHAT_MANAGER_ONLY_MESSAGE,
        )

    def _ensure_chat_private_chat(
        self,
        *,
        chat_id: int,
        actor_user_id: int,
    ) -> TelegramAdapterResult | None:
        if chat_id == actor_user_id:
            return None
        return TelegramAdapterResult(
            outcome=Outcome.REJECTED,
            reason_code=ReasonCode.PERMISSION_DENIED,
            message=CHAT_PRIVATE_CHAT_ONLY_MESSAGE,
        )

    def _chat_menu_buttons(self) -> list[list[dict[str, str]]]:
        return [
            [{"text": "➕ Добавить чат", "callback_data": "chat_menu:add"}],
            [{"text": "🧹 Очистить чат", "callback_data": "chat_menu:clear"}],
        ]

    def _parse_chat_target_id(self, text: str) -> int | None:
        candidate = text.strip()
        if not candidate:
            return None

        lowered = candidate.casefold()
        for prefix in ("chat_id", "chatid", "id"):
            if lowered.startswith(prefix):
                candidate = candidate[len(prefix) :].strip()
                if candidate.startswith(":") or candidate.startswith("="):
                    candidate = candidate[1:].strip()
                break

        if not re.fullmatch(r"-?\d+", candidate):
            return None

        parsed = int(candidate)
        if parsed == 0:
            return None
        return parsed

    def _open_chat_status_text(self, *, actor_user_id: int, now: datetime) -> str:
        meetings = self._repository.list_initiator_meetings(
            initiator_telegram_user_id=actor_user_id,
            now=now,
            states=OPEN_CHAT_SCOPE_STATES,
            limit=200,
        )
        if not meetings:
            return "Открытых встреч нет. Настройка применится к следующим открытым встречам."

        chat_ids = sorted({meeting.chat_id for meeting in meetings})
        if len(chat_ids) == 1:
            return (
                "Текущий chat_id для открытых встреч: "
                f"{chat_ids[0]} (встреч: {len(meetings)})"
            )

        joined = ", ".join(str(chat_id) for chat_id in chat_ids)
        return (
            "Для открытых встреч найдены разные chat_id: "
            f"{joined} (встреч: {len(meetings)})."
        )

    def _handle_chat_message(
        self,
        *,
        message: Mapping[str, object],
        chat_id: int,
        text: str,
        now: datetime,
    ) -> TelegramAdapterResult:
        from_obj = message.get("from")
        actor_id = cast(Mapping[str, object], from_obj or {}).get("id")
        if not isinstance(actor_id, int):
            return TelegramAdapterResult(
                outcome=Outcome.REJECTED,
                reason_code=ReasonCode.INVALID_CALLBACK_FORMAT,
                message="Некорректный пользователь",
            )

        private_chat_only = self._ensure_chat_private_chat(
            chat_id=chat_id,
            actor_user_id=actor_id,
        )
        if private_chat_only is not None:
            return private_chat_only

        denied = self._ensure_chat_manager(actor_user_id=actor_id)
        if denied is not None:
            return denied

        self._repository.clear_conversation_state(
            chat_id=chat_id,
            user_id=actor_id,
            flow=CHAT_FLOW_NAME,
        )

        parts = text.split(maxsplit=1)
        if len(parts) > 1 and parts[1].strip():
            return self._handle_chat_target_input(
                chat_id=chat_id,
                actor_user_id=actor_id,
                text=parts[1],
                now=now,
            )

        _ = self._repository.enqueue_outbox(
            effect_type=OutboxEffectType.TELEGRAM_SEND_MESSAGE,
            payload={
                "telegram_user_id": chat_id,
                "text": (
                    "Настройка чата статусов\n"
                    f"{self._open_chat_status_text(actor_user_id=actor_id, now=now)}\n\n"
                    "Выберите действие кнопкой."
                ),
                "buttons": self._chat_menu_buttons(),
            },
            idempotency_key=(
                f"chat_menu:{chat_id}:{actor_id}:{now.isoformat(timespec='seconds')}"
            ),
            now=now,
        )
        return TelegramAdapterResult(
            outcome=Outcome.OK,
            reason_code=ReasonCode.UPDATED,
            message="Меню настройки чата отправлено",
        )

    def _handle_chat_target_input(
        self,
        *,
        chat_id: int,
        actor_user_id: int,
        text: str,
        now: datetime,
    ) -> TelegramAdapterResult:
        private_chat_only = self._ensure_chat_private_chat(
            chat_id=chat_id,
            actor_user_id=actor_user_id,
        )
        if private_chat_only is not None:
            self._repository.clear_conversation_state(
                chat_id=chat_id,
                user_id=actor_user_id,
                flow=CHAT_FLOW_NAME,
            )
            return private_chat_only

        denied = self._ensure_chat_manager(actor_user_id=actor_user_id)
        if denied is not None:
            self._repository.clear_conversation_state(
                chat_id=chat_id,
                user_id=actor_user_id,
                flow=CHAT_FLOW_NAME,
            )
            return denied

        parsed_chat_id = self._parse_chat_target_id(text)
        if parsed_chat_id is None:
            _ = self._repository.enqueue_outbox(
                effect_type=OutboxEffectType.TELEGRAM_SEND_MESSAGE,
                payload={
                    "telegram_user_id": chat_id,
                    "text": (
                        "Введите chat_id числом, например: -5151698406\n"
                        f"{CHAT_INPUT_EXIT_HINT}"
                    ),
                },
                idempotency_key=(
                    f"chat_set_retry:{chat_id}:{actor_user_id}:{now.isoformat(timespec='seconds')}"
                ),
                now=now,
            )
            return TelegramAdapterResult(
                outcome=Outcome.REJECTED,
                reason_code=ReasonCode.INVALID_CALLBACK_FORMAT,
                message="Некорректный chat_id",
            )

        updated_count = self._repository.update_initiator_open_meetings_chat(
            initiator_telegram_user_id=actor_user_id,
            target_chat_id=parsed_chat_id,
            now=now,
            states=OPEN_CHAT_SCOPE_STATES,
        )
        _ = self._repository.set_preferred_chat_id(
            telegram_user_id=actor_user_id,
            preferred_chat_id=parsed_chat_id,
            now=now,
        )
        self._repository.clear_conversation_state(
            chat_id=chat_id,
            user_id=actor_user_id,
            flow=CHAT_FLOW_NAME,
        )
        _ = self._repository.enqueue_outbox(
            effect_type=OutboxEffectType.TELEGRAM_SEND_MESSAGE,
            payload={
                "telegram_user_id": chat_id,
                "text": (
                    f"Готово. Новый chat_id: {parsed_chat_id}\n"
                    f"Обновлено открытых встреч: {updated_count}"
                ),
            },
            idempotency_key=(
                f"chat_set_done:{chat_id}:{actor_user_id}:{parsed_chat_id}:{now.isoformat(timespec='seconds')}"
            ),
            now=now,
        )
        return TelegramAdapterResult(
            outcome=Outcome.OK,
            reason_code=ReasonCode.UPDATED,
            message="chat_id обновлен",
        )

    def _handle_chat_menu_callback(
        self,
        *,
        callback_query: Mapping[str, object],
        chat_id: int,
        data: str,
        now: datetime,
    ) -> TelegramAdapterResult:
        action = data.replace("chat_menu:", "", 1).strip()
        from_obj = callback_query.get("from")
        actor_id = cast(Mapping[str, object], from_obj or {}).get("id")
        if not isinstance(actor_id, int):
            return TelegramAdapterResult(
                outcome=Outcome.REJECTED,
                reason_code=ReasonCode.INVALID_CALLBACK_FORMAT,
                message=STALE_ACTION_MESSAGE,
            )

        private_chat_only = self._ensure_chat_private_chat(
            chat_id=chat_id,
            actor_user_id=actor_id,
        )
        if private_chat_only is not None:
            return private_chat_only

        denied = self._ensure_chat_manager(actor_user_id=actor_id)
        if denied is not None:
            return denied

        if action == "add":
            self._repository.upsert_conversation_state(
                chat_id=chat_id,
                user_id=actor_id,
                flow=CHAT_FLOW_NAME,
                state={"mode": "await_chat_target_id"},
                expires_at=now + CHAT_CONVERSATION_TTL,
                now=now,
            )
            _ = self._repository.enqueue_outbox(
                effect_type=OutboxEffectType.TELEGRAM_SEND_MESSAGE,
                payload={
                    "telegram_user_id": chat_id,
                    "text": (
                        "Введите новый chat_id (например, -5151698406).\n"
                        f"{CHAT_INPUT_EXIT_HINT}"
                    ),
                },
                idempotency_key=(
                    f"chat_add_prompt:{chat_id}:{actor_id}:{now.isoformat(timespec='seconds')}"
                ),
                now=now,
            )
            return TelegramAdapterResult(
                outcome=Outcome.OK,
                reason_code=ReasonCode.UPDATED,
                message="Ожидается ввод chat_id",
            )

        if action == "clear":
            updated_count = self._repository.update_initiator_open_meetings_chat(
                initiator_telegram_user_id=actor_id,
                target_chat_id=None,
                now=now,
                states=OPEN_CHAT_SCOPE_STATES,
            )
            _ = self._repository.set_preferred_chat_id(
                telegram_user_id=actor_id,
                preferred_chat_id=None,
                now=now,
            )
            self._repository.upsert_conversation_state(
                chat_id=chat_id,
                user_id=actor_id,
                flow=CHAT_FLOW_NAME,
                state={"mode": "await_chat_target_id"},
                expires_at=now + CHAT_CONVERSATION_TTL,
                now=now,
            )
            _ = self._repository.enqueue_outbox(
                effect_type=OutboxEffectType.TELEGRAM_SEND_MESSAGE,
                payload={
                    "telegram_user_id": chat_id,
                    "text": (
                        "Чат очищен для открытых встреч.\n"
                        f"Обновлено встреч: {updated_count}\n"
                        "Сразу введите новый chat_id (например, -5151698406).\n"
                        f"{CHAT_INPUT_EXIT_HINT}"
                    ),
                },
                idempotency_key=(
                    f"chat_clear_done:{chat_id}:{actor_id}:{now.isoformat(timespec='seconds')}"
                ),
                now=now,
            )
            return TelegramAdapterResult(
                outcome=Outcome.OK,
                reason_code=ReasonCode.UPDATED,
                message="Чат очищен, ожидается новый chat_id",
            )

        return TelegramAdapterResult(
            outcome=Outcome.NOOP,
            reason_code=ReasonCode.STALE_ACTION,
            message=STALE_ACTION_MESSAGE,
        )

    def _handle_start_message(
        self,
        *,
        message: Mapping[str, object],
        chat_id: int,
        now: datetime,
    ) -> TelegramAdapterResult:
        from_obj = message.get("from")
        user_id = cast(Mapping[str, object], from_obj or {}).get("id")
        if not isinstance(user_id, int):
            return TelegramAdapterResult(
                outcome=Outcome.REJECTED,
                reason_code=ReasonCode.INVALID_CALLBACK_FORMAT,
                message="Некорректный пользователь",
            )

        mapping = self._repository.get_user_mapping(user_id)
        if mapping is not None and bool(mapping.get("is_active")):
            email_obj = mapping.get("google_email")
            email = str(email_obj) if email_obj is not None else "unknown"
            text = (
                "Аккаунт подключен ✅\n"
                f"Google: {email}\n\n"
                "Как это работает:\n"
                "1) Создавайте, переносите и отменяйте встречи в Google Calendar.\n"
                "2) Бот отправляет участникам запросы на подтверждение в личные сообщения.\n"
                "3) Статус встречи публикуется в чате встречи.\n\n"
                "Команды:\n"
                "/help — подробная инструкция\n"
                "/chat — настройка чата статусов (только менеджеры, только личный чат)\n"
                "/people — управление участниками (только менеджеры, только личный чат)"
            )
        else:
            text = (
                "Пока не вижу привязку к рабочему календарю.\n\n"
                "Что сделать:\n"
                "1) Попросите менеджера добавить вас через /people.\n"
                "2) Проверьте, что указан ваш рабочий Google email.\n"
                "3) Нажмите /start снова."
            )

        _ = self._repository.enqueue_outbox(
            effect_type=OutboxEffectType.TELEGRAM_SEND_MESSAGE,
            payload={
                "telegram_user_id": chat_id,
                "text": text,
                "keyboard": main_menu_keyboard(),
            },
            idempotency_key=f"start:{chat_id}:{now.isoformat(timespec='seconds')}",
            now=now,
        )
        return TelegramAdapterResult(
            outcome=Outcome.OK,
            reason_code=ReasonCode.UPDATED,
            message="Start message enqueued",
        )

    def _people_menu_buttons(self) -> list[list[dict[str, str]]]:
        return [
            [
                {"text": "📋 Список", "callback_data": "people_menu:list"},
                {"text": "➕ Добавить", "callback_data": "people_menu:add"},
            ],
            [
                {"text": "➖ Отключить", "callback_data": "people_menu:remove"},
                {"text": "✖️ Отмена", "callback_data": "people_menu:cancel"},
            ],
        ]

    def _people_add_template(self) -> str:
        return (
            "Отправьте данные одним сообщением (каждое поле с новой строки):\n"
            "username: @nickname\n"
            "telegram_user_id: 123456789\n"
            "google_email: user@company.com\n"
            "full_name: Имя Фамилия"
        )

    def _format_people_user_line(self, mapping: Mapping[str, object]) -> str:
        user_id_obj = mapping.get("telegram_user_id")
        username_obj = mapping.get("telegram_username")
        email_obj = mapping.get("google_email")
        full_name_obj = mapping.get("full_name")
        is_active = bool(mapping.get("is_active"))

        user_id = str(user_id_obj) if isinstance(user_id_obj, int) else "unknown"
        username = (
            username_obj.strip()
            if isinstance(username_obj, str) and username_obj.strip()
            else "-"
        )
        email = (
            email_obj.strip()
            if isinstance(email_obj, str) and email_obj.strip()
            else "-"
        )
        full_name = (
            full_name_obj.strip()
            if isinstance(full_name_obj, str) and full_name_obj.strip()
            else "-"
        )
        status = "активен" if is_active else "отключен"
        username_display = f"@{username}" if username != "-" else "-"
        return f"{full_name} | {username_display} | {email} | ID: {user_id} | {status}"

    def _people_button_label(self, mapping: Mapping[str, object]) -> str:
        full_name_obj = mapping.get("full_name")
        username_obj = mapping.get("telegram_username")
        email_obj = mapping.get("google_email")
        user_id_obj = mapping.get("telegram_user_id")

        if isinstance(full_name_obj, str) and full_name_obj.strip():
            base = full_name_obj.strip()
        elif isinstance(username_obj, str) and username_obj.strip():
            base = f"@{username_obj.strip()}"
        elif isinstance(email_obj, str) and email_obj.strip():
            base = email_obj.strip()
        elif isinstance(user_id_obj, int):
            base = str(user_id_obj)
        else:
            base = "Пользователь"

        if isinstance(user_id_obj, int):
            base = f"{base} [{user_id_obj}]"

        if len(base) > 56:
            return f"{base[:53]}..."
        return base

    def _parse_people_add_fields(self, text: str) -> dict[str, str]:
        parsed: dict[str, str] = {}
        for raw_line in text.splitlines():
            line = raw_line.strip()
            if not line:
                continue

            separator: str | None = None
            if ":" in line:
                separator = ":"
            elif "=" in line:
                separator = "="

            if separator is None:
                continue

            key_raw, value_raw = line.split(separator, 1)
            value = value_raw.strip()
            if not value:
                continue

            normalized_key = (
                key_raw.strip().casefold().replace(" ", "").replace("_", "")
            )
            if normalized_key in {
                "username",
                "telegramusername",
                "tgusername",
                "ник",
                "никнейм",
            }:
                parsed["telegram_username"] = value
                continue

            if normalized_key in {
                "telegramuserid",
                "telegramid",
                "userid",
                "id",
                "айди",
            }:
                parsed["telegram_user_id"] = value
                continue

            if normalized_key in {
                "googleemail",
                "workemail",
                "email",
                "почта",
                "рабочаяпочта",
            }:
                parsed["google_email"] = value
                continue

            if normalized_key in {
                "fullname",
                "name",
                "fio",
                "фио",
                "имя",
                "имяфамилия",
            }:
                parsed["full_name"] = value

        return parsed

    def _looks_like_email(self, token: str) -> bool:
        return bool(re.fullmatch(r"[^@\s]+@[^@\s]+\.[^@\s]+", token.strip()))

    def _handle_people_message(
        self,
        *,
        message: Mapping[str, object],
        chat_id: int,
        now: datetime,
    ) -> TelegramAdapterResult:
        from_obj = message.get("from")
        actor_id = cast(Mapping[str, object], from_obj or {}).get("id")
        if not isinstance(actor_id, int):
            return TelegramAdapterResult(
                outcome=Outcome.REJECTED,
                reason_code=ReasonCode.INVALID_CALLBACK_FORMAT,
                message="Некорректный пользователь",
            )

        private_chat_only = self._ensure_people_private_chat(
            chat_id=chat_id,
            actor_user_id=actor_id,
        )
        if private_chat_only is not None:
            return private_chat_only

        denied = self._ensure_people_manager(actor_user_id=actor_id)
        if denied is not None:
            return denied

        self._repository.clear_conversation_state(
            chat_id=chat_id,
            user_id=actor_id,
            flow=PEOPLE_FLOW_NAME,
        )

        _ = self._repository.enqueue_outbox(
            effect_type=OutboxEffectType.TELEGRAM_SEND_MESSAGE,
            payload={
                "telegram_user_id": chat_id,
                "text": (
                    "Участники:\n"
                    "- посмотреть список\n"
                    "- добавить пользователя\n"
                    "- отключить пользователя"
                ),
                "buttons": self._people_menu_buttons(),
            },
            idempotency_key=f"people_menu:{chat_id}:{actor_id}:{now.isoformat(timespec='seconds')}",
            now=now,
        )
        return TelegramAdapterResult(
            outcome=Outcome.OK,
            reason_code=ReasonCode.UPDATED,
            message="People menu enqueued",
        )

    def _handle_people_menu_callback(
        self,
        *,
        callback_query: Mapping[str, object],
        chat_id: int,
        data: str,
        now: datetime,
    ) -> TelegramAdapterResult:
        action = data.replace("people_menu:", "", 1).strip()
        from_obj = callback_query.get("from")
        actor_id = cast(Mapping[str, object], from_obj or {}).get("id")
        if not isinstance(actor_id, int):
            return TelegramAdapterResult(
                outcome=Outcome.REJECTED,
                reason_code=ReasonCode.INVALID_CALLBACK_FORMAT,
                message=STALE_ACTION_MESSAGE,
            )

        private_chat_only = self._ensure_people_private_chat(
            chat_id=chat_id,
            actor_user_id=actor_id,
        )
        if private_chat_only is not None:
            return private_chat_only

        denied = self._ensure_people_manager(actor_user_id=actor_id)
        if denied is not None:
            return denied

        if action == "list":
            users = self._repository.list_user_mappings(
                include_inactive=True, limit=200
            )
            text: str
            if not users:
                text = "База пользователей пока пустая."
            else:
                lines = ["Пользователи в базе:"]
                for mapping in users:
                    lines.append(f"- {self._format_people_user_line(mapping)}")
                text = "\n".join(lines)

            chunks: list[str] = [text]
            if len(text) > 3500:
                chunks = []
                current = ""
                for line in text.splitlines():
                    candidate = f"{current}\n{line}" if current else line
                    if len(candidate) <= 3500:
                        current = candidate
                        continue
                    if current:
                        chunks.append(current)
                    current = line
                if current:
                    chunks.append(current)
            for idx, chunk in enumerate(chunks):
                request_key = hashlib.sha1(
                    f"{chunk}:{idx}".encode("utf-8")
                ).hexdigest()[:8]
                _ = self._repository.enqueue_outbox(
                    effect_type=OutboxEffectType.TELEGRAM_SEND_MESSAGE,
                    payload={"telegram_user_id": chat_id, "text": chunk},
                    idempotency_key=(
                        f"people_list:{chat_id}:{actor_id}:{request_key}:{now.isoformat(timespec='seconds')}"
                    ),
                    now=now,
                )
            return TelegramAdapterResult(
                outcome=Outcome.OK,
                reason_code=ReasonCode.UPDATED,
                message="Список пользователей отправлен",
            )

        if action == "add":
            self._repository.upsert_conversation_state(
                chat_id=chat_id,
                user_id=actor_id,
                flow=PEOPLE_FLOW_NAME,
                state={"mode": "await_people_add_fields"},
                expires_at=now + PEOPLE_CONVERSATION_TTL,
                now=now,
            )
            _ = self._repository.enqueue_outbox(
                effect_type=OutboxEffectType.TELEGRAM_SEND_MESSAGE,
                payload={
                    "telegram_user_id": chat_id,
                    "text": self._people_add_template(),
                },
                idempotency_key=(
                    f"people_add_prompt:{chat_id}:{actor_id}:{now.isoformat(timespec='seconds')}"
                ),
                now=now,
            )
            return TelegramAdapterResult(
                outcome=Outcome.OK,
                reason_code=ReasonCode.UPDATED,
                message="Запрошены данные нового пользователя",
            )

        if action == "remove":
            self._repository.upsert_conversation_state(
                chat_id=chat_id,
                user_id=actor_id,
                flow=PEOPLE_FLOW_NAME,
                state={"mode": "await_people_remove_query"},
                expires_at=now + PEOPLE_CONVERSATION_TTL,
                now=now,
            )
            _ = self._repository.enqueue_outbox(
                effect_type=OutboxEffectType.TELEGRAM_SEND_MESSAGE,
                payload={
                    "telegram_user_id": chat_id,
                    "text": ("Кого отключить? Укажите ID, email, @ник или часть ФИО."),
                },
                idempotency_key=(
                    f"people_remove_prompt:{chat_id}:{actor_id}:{now.isoformat(timespec='seconds')}"
                ),
                now=now,
            )
            return TelegramAdapterResult(
                outcome=Outcome.OK,
                reason_code=ReasonCode.UPDATED,
                message="Запрошен пользователь для отключения",
            )

        if action == "cancel":
            self._repository.clear_conversation_state(
                chat_id=chat_id,
                user_id=actor_id,
                flow=PEOPLE_FLOW_NAME,
            )
            _ = self._repository.enqueue_outbox(
                effect_type=OutboxEffectType.TELEGRAM_SEND_MESSAGE,
                payload={"telegram_user_id": chat_id, "text": "Операция отменена."},
                idempotency_key=(
                    f"people_cancel:{chat_id}:{actor_id}:{now.isoformat(timespec='seconds')}"
                ),
                now=now,
            )
            return TelegramAdapterResult(
                outcome=Outcome.NOOP,
                reason_code=ReasonCode.INVALID_STATE,
                message="Операция с пользователями отменена",
            )

        return TelegramAdapterResult(
            outcome=Outcome.NOOP,
            reason_code=ReasonCode.STALE_ACTION,
            message=STALE_ACTION_MESSAGE,
        )

    def _handle_people_add_fields_input(
        self,
        *,
        chat_id: int,
        actor_user_id: int,
        text: str,
        now: datetime,
    ) -> TelegramAdapterResult:
        private_chat_only = self._ensure_people_private_chat(
            chat_id=chat_id,
            actor_user_id=actor_user_id,
        )
        if private_chat_only is not None:
            self._repository.clear_conversation_state(
                chat_id=chat_id,
                user_id=actor_user_id,
                flow=PEOPLE_FLOW_NAME,
            )
            return private_chat_only

        denied = self._ensure_people_manager(actor_user_id=actor_user_id)
        if denied is not None:
            self._repository.clear_conversation_state(
                chat_id=chat_id,
                user_id=actor_user_id,
                flow=PEOPLE_FLOW_NAME,
            )
            return denied

        parsed = self._parse_people_add_fields(text)

        username_raw = parsed.get("telegram_username", "").strip().lstrip("@")
        user_id_raw = parsed.get("telegram_user_id", "").strip()
        email_raw = parsed.get("google_email", "").strip().lower()
        full_name_raw = parsed.get("full_name", "").strip()

        missing: list[str] = []
        invalid: list[str] = []

        if not username_raw:
            missing.append("username")
        telegram_user_id: int | None = None
        if not user_id_raw:
            missing.append("telegram_user_id")
        else:
            try:
                telegram_user_id = int(user_id_raw)
            except ValueError:
                invalid.append("telegram_user_id должен быть числом")

        if not email_raw:
            missing.append("google_email")
        elif not self._looks_like_email(email_raw):
            invalid.append("google_email некорректный")

        if not full_name_raw:
            missing.append("full_name")

        if missing or invalid:
            lines = ["Не удалось принять данные пользователя."]
            if missing:
                lines.append(f"Не хватает полей: {', '.join(missing)}")
            if invalid:
                lines.append(f"Ошибки: {'; '.join(invalid)}")
            lines.append("")
            lines.append("Проверьте данные и отправьте снова:")
            lines.append(self._people_add_template())
            _ = self._repository.enqueue_outbox(
                effect_type=OutboxEffectType.TELEGRAM_SEND_MESSAGE,
                payload={"telegram_user_id": chat_id, "text": "\n".join(lines)},
                idempotency_key=(
                    f"people_add_retry:{chat_id}:{actor_user_id}:{now.isoformat(timespec='seconds')}"
                ),
                now=now,
            )
            return TelegramAdapterResult(
                outcome=Outcome.REJECTED,
                reason_code=ReasonCode.INVALID_CALLBACK_FORMAT,
                message="Некорректные поля пользователя",
            )

        assert telegram_user_id is not None
        existing_by_email = self._repository.get_user_mapping_by_email(email_raw)
        if existing_by_email is not None:
            existing_id_obj = existing_by_email.get("telegram_user_id")
            if isinstance(existing_id_obj, int) and existing_id_obj != telegram_user_id:
                _ = self._repository.enqueue_outbox(
                    effect_type=OutboxEffectType.TELEGRAM_SEND_MESSAGE,
                    payload={
                        "telegram_user_id": chat_id,
                        "text": (
                            f"Email {email_raw} уже привязан к Telegram ID {existing_id_obj}."
                        ),
                    },
                    idempotency_key=(
                        f"people_add_email_conflict:{chat_id}:{actor_user_id}:{now.isoformat(timespec='seconds')}"
                    ),
                    now=now,
                )
                return TelegramAdapterResult(
                    outcome=Outcome.REJECTED,
                    reason_code=ReasonCode.INVALID_STATE,
                    message="Email уже используется",
                )

        self._repository.upsert_conversation_state(
            chat_id=chat_id,
            user_id=actor_user_id,
            flow=PEOPLE_FLOW_NAME,
            state={
                "mode": "await_people_add_confirm",
                "telegram_username": username_raw,
                "telegram_user_id": telegram_user_id,
                "google_email": email_raw,
                "full_name": full_name_raw,
            },
            expires_at=now + PEOPLE_CONVERSATION_TTL,
            now=now,
        )
        _ = self._repository.enqueue_outbox(
            effect_type=OutboxEffectType.TELEGRAM_SEND_MESSAGE,
            payload={
                "telegram_user_id": chat_id,
                "text": (
                    "Проверьте данные перед добавлением:\n"
                    f"- username: @{username_raw}\n"
                    f"- telegram_user_id: {telegram_user_id}\n"
                    f"- google_email: {email_raw}\n"
                    f"- full_name: {full_name_raw}"
                ),
                "buttons": [
                    [
                        {"text": "✅ Добавить", "callback_data": "people_add:confirm"},
                        {"text": "✖️ Отмена", "callback_data": "people_add:cancel"},
                    ]
                ],
            },
            idempotency_key=(
                f"people_add_confirm_prompt:{chat_id}:{actor_user_id}:{now.isoformat(timespec='seconds')}"
            ),
            now=now,
        )
        return TelegramAdapterResult(
            outcome=Outcome.OK,
            reason_code=ReasonCode.UPDATED,
            message="Ожидается подтверждение добавления пользователя",
        )

    def _handle_people_add_callback(
        self,
        *,
        callback_query: Mapping[str, object],
        chat_id: int,
        data: str,
        now: datetime,
    ) -> TelegramAdapterResult:
        action = data.replace("people_add:", "", 1).strip()
        from_obj = callback_query.get("from")
        actor_id = cast(Mapping[str, object], from_obj or {}).get("id")
        if not isinstance(actor_id, int):
            return TelegramAdapterResult(
                outcome=Outcome.REJECTED,
                reason_code=ReasonCode.INVALID_CALLBACK_FORMAT,
                message=STALE_ACTION_MESSAGE,
            )

        private_chat_only = self._ensure_people_private_chat(
            chat_id=chat_id,
            actor_user_id=actor_id,
        )
        if private_chat_only is not None:
            self._repository.clear_conversation_state(
                chat_id=chat_id,
                user_id=actor_id,
                flow=PEOPLE_FLOW_NAME,
            )
            return private_chat_only

        denied = self._ensure_people_manager(actor_user_id=actor_id)
        if denied is not None:
            self._repository.clear_conversation_state(
                chat_id=chat_id,
                user_id=actor_id,
                flow=PEOPLE_FLOW_NAME,
            )
            return denied

        if action == "cancel":
            self._repository.clear_conversation_state(
                chat_id=chat_id,
                user_id=actor_id,
                flow=PEOPLE_FLOW_NAME,
            )
            return TelegramAdapterResult(
                outcome=Outcome.NOOP,
                reason_code=ReasonCode.INVALID_STATE,
                message="Добавление пользователя отменено",
            )

        if action != "confirm":
            return TelegramAdapterResult(
                outcome=Outcome.NOOP,
                reason_code=ReasonCode.STALE_ACTION,
                message=STALE_ACTION_MESSAGE,
            )

        conversation = self._repository.get_conversation_state(
            chat_id=chat_id,
            user_id=actor_id,
            flow=PEOPLE_FLOW_NAME,
            now=now,
        )
        if not isinstance(conversation, dict):
            return TelegramAdapterResult(
                outcome=Outcome.NOOP,
                reason_code=ReasonCode.STALE_ACTION,
                message=STALE_ACTION_MESSAGE,
            )

        mode_obj = conversation.get("mode")
        user_id_obj = conversation.get("telegram_user_id")
        username_obj = conversation.get("telegram_username")
        email_obj = conversation.get("google_email")
        full_name_obj = conversation.get("full_name")

        if (
            mode_obj != "await_people_add_confirm"
            or not isinstance(user_id_obj, int)
            or not isinstance(username_obj, str)
            or not isinstance(email_obj, str)
            or not isinstance(full_name_obj, str)
        ):
            self._repository.clear_conversation_state(
                chat_id=chat_id,
                user_id=actor_id,
                flow=PEOPLE_FLOW_NAME,
            )
            return TelegramAdapterResult(
                outcome=Outcome.NOOP,
                reason_code=ReasonCode.STALE_ACTION,
                message=STALE_ACTION_MESSAGE,
            )

        existing_by_email = self._repository.get_user_mapping_by_email(email_obj)
        if existing_by_email is not None:
            existing_id_obj = existing_by_email.get("telegram_user_id")
            if isinstance(existing_id_obj, int) and existing_id_obj != user_id_obj:
                self._repository.clear_conversation_state(
                    chat_id=chat_id,
                    user_id=actor_id,
                    flow=PEOPLE_FLOW_NAME,
                )
                return TelegramAdapterResult(
                    outcome=Outcome.REJECTED,
                    reason_code=ReasonCode.INVALID_STATE,
                    message="Email уже используется другим пользователем",
                )

        self._repository.upsert_user_mapping(
            telegram_user_id=user_id_obj,
            telegram_username=username_obj,
            google_email=email_obj,
            full_name=full_name_obj,
            is_active=True,
            now=now,
        )
        self._repository.clear_conversation_state(
            chat_id=chat_id,
            user_id=actor_id,
            flow=PEOPLE_FLOW_NAME,
        )
        _ = self._repository.enqueue_outbox(
            effect_type=OutboxEffectType.TELEGRAM_SEND_MESSAGE,
            payload={
                "telegram_user_id": chat_id,
                "text": (
                    f"Пользователь @{username_obj} (ID {user_id_obj}) сохранен и активирован."
                ),
            },
            idempotency_key=(
                f"people_add_done:{chat_id}:{actor_id}:{user_id_obj}:{now.isoformat(timespec='seconds')}"
            ),
            now=now,
        )
        return TelegramAdapterResult(
            outcome=Outcome.OK,
            reason_code=ReasonCode.UPDATED,
            message="Пользователь добавлен",
        )

    def _enqueue_people_remove_confirmation(
        self,
        *,
        chat_id: int,
        actor_user_id: int,
        target_mapping: Mapping[str, object],
        now: datetime,
    ) -> TelegramAdapterResult:
        target_id_obj = target_mapping.get("telegram_user_id")
        if not isinstance(target_id_obj, int):
            return TelegramAdapterResult(
                outcome=Outcome.REJECTED,
                reason_code=ReasonCode.INVALID_CALLBACK_FORMAT,
                message="Некорректный пользователь",
            )

        self._repository.upsert_conversation_state(
            chat_id=chat_id,
            user_id=actor_user_id,
            flow=PEOPLE_FLOW_NAME,
            state={
                "mode": "await_people_remove_confirm",
                "target_user_id": target_id_obj,
            },
            expires_at=now + PEOPLE_CONVERSATION_TTL,
            now=now,
        )
        _ = self._repository.enqueue_outbox(
            effect_type=OutboxEffectType.TELEGRAM_SEND_MESSAGE,
            payload={
                "telegram_user_id": chat_id,
                "text": (
                    "Подтвердите отключение:\n"
                    f"{self._format_people_user_line(target_mapping)}"
                ),
                "buttons": [
                    [
                        {
                            "text": "✅ Да, отключить",
                            "callback_data": f"people_remove:confirm:{target_id_obj}",
                        },
                        {
                            "text": "✖️ Отмена",
                            "callback_data": "people_remove:cancel",
                        },
                    ]
                ],
            },
            idempotency_key=(
                f"people_remove_confirm_prompt:{chat_id}:{actor_user_id}:{target_id_obj}:{now.isoformat(timespec='seconds')}"
            ),
            now=now,
        )
        return TelegramAdapterResult(
            outcome=Outcome.OK,
            reason_code=ReasonCode.UPDATED,
            message="Ожидается подтверждение отключения",
        )

    def _handle_people_remove_query_input(
        self,
        *,
        chat_id: int,
        actor_user_id: int,
        text: str,
        now: datetime,
    ) -> TelegramAdapterResult:
        private_chat_only = self._ensure_people_private_chat(
            chat_id=chat_id,
            actor_user_id=actor_user_id,
        )
        if private_chat_only is not None:
            self._repository.clear_conversation_state(
                chat_id=chat_id,
                user_id=actor_user_id,
                flow=PEOPLE_FLOW_NAME,
            )
            return private_chat_only

        denied = self._ensure_people_manager(actor_user_id=actor_user_id)
        if denied is not None:
            self._repository.clear_conversation_state(
                chat_id=chat_id,
                user_id=actor_user_id,
                flow=PEOPLE_FLOW_NAME,
            )
            return denied

        query = text.strip()
        if not query:
            _ = self._repository.enqueue_outbox(
                effect_type=OutboxEffectType.TELEGRAM_SEND_MESSAGE,
                payload={
                    "telegram_user_id": chat_id,
                    "text": "Укажите ID, email, @ник или часть ФИО.",
                },
                idempotency_key=(
                    f"people_remove_query_retry:{chat_id}:{actor_user_id}:{now.isoformat(timespec='seconds')}"
                ),
                now=now,
            )
            return TelegramAdapterResult(
                outcome=Outcome.REJECTED,
                reason_code=ReasonCode.INVALID_CALLBACK_FORMAT,
                message="Пустой запрос на отключение",
            )

        matches = self._repository.search_user_mappings(
            query=query,
            active_only=True,
            limit=8,
        )
        if not matches:
            _ = self._repository.enqueue_outbox(
                effect_type=OutboxEffectType.TELEGRAM_SEND_MESSAGE,
                payload={
                    "telegram_user_id": chat_id,
                    "text": "Ничего не нашел. Попробуйте другой запрос.",
                },
                idempotency_key=(
                    f"people_remove_not_found:{chat_id}:{actor_user_id}:{now.isoformat(timespec='seconds')}"
                ),
                now=now,
            )
            return TelegramAdapterResult(
                outcome=Outcome.NOOP,
                reason_code=ReasonCode.PARTICIPANT_NOT_FOUND,
                message="Пользователь не найден",
            )

        if len(matches) == 1:
            return self._enqueue_people_remove_confirmation(
                chat_id=chat_id,
                actor_user_id=actor_user_id,
                target_mapping=matches[0],
                now=now,
            )

        buttons: list[dict[str, str]] = []
        for mapping in matches:
            target_id_obj = mapping.get("telegram_user_id")
            if not isinstance(target_id_obj, int):
                continue
            buttons.append(
                {
                    "text": self._people_button_label(mapping),
                    "callback_data": f"people_remove:pick:{target_id_obj}",
                }
            )

        buttons.append({"text": "✖️ Отмена", "callback_data": "people_remove:cancel"})

        _ = self._repository.enqueue_outbox(
            effect_type=OutboxEffectType.TELEGRAM_SEND_MESSAGE,
            payload={
                "telegram_user_id": chat_id,
                "text": "Нашел несколько пользователей. Выберите нужного:",
                "buttons": buttons,
            },
            idempotency_key=(
                f"people_remove_pick:{chat_id}:{actor_user_id}:{now.isoformat(timespec='seconds')}"
            ),
            now=now,
        )
        return TelegramAdapterResult(
            outcome=Outcome.OK,
            reason_code=ReasonCode.UPDATED,
            message="Отправлен список кандидатов на отключение",
        )

    def _handle_people_remove_callback(
        self,
        *,
        callback_query: Mapping[str, object],
        chat_id: int,
        data: str,
        now: datetime,
    ) -> TelegramAdapterResult:
        payload = data.replace("people_remove:", "", 1).strip()
        from_obj = callback_query.get("from")
        actor_id = cast(Mapping[str, object], from_obj or {}).get("id")
        if not isinstance(actor_id, int):
            return TelegramAdapterResult(
                outcome=Outcome.REJECTED,
                reason_code=ReasonCode.INVALID_CALLBACK_FORMAT,
                message=STALE_ACTION_MESSAGE,
            )

        private_chat_only = self._ensure_people_private_chat(
            chat_id=chat_id,
            actor_user_id=actor_id,
        )
        if private_chat_only is not None:
            self._repository.clear_conversation_state(
                chat_id=chat_id,
                user_id=actor_id,
                flow=PEOPLE_FLOW_NAME,
            )
            return private_chat_only

        denied = self._ensure_people_manager(actor_user_id=actor_id)
        if denied is not None:
            self._repository.clear_conversation_state(
                chat_id=chat_id,
                user_id=actor_id,
                flow=PEOPLE_FLOW_NAME,
            )
            return denied

        if payload == "cancel":
            self._repository.clear_conversation_state(
                chat_id=chat_id,
                user_id=actor_id,
                flow=PEOPLE_FLOW_NAME,
            )
            return TelegramAdapterResult(
                outcome=Outcome.NOOP,
                reason_code=ReasonCode.INVALID_STATE,
                message="Отключение пользователя отменено",
            )

        if payload.startswith("pick:"):
            target_id_raw = payload.replace("pick:", "", 1).strip()
            try:
                target_id = int(target_id_raw)
            except ValueError:
                return TelegramAdapterResult(
                    outcome=Outcome.REJECTED,
                    reason_code=ReasonCode.INVALID_CALLBACK_FORMAT,
                    message=STALE_ACTION_MESSAGE,
                )

            target_mapping = self._repository.get_user_mapping(target_id)
            if target_mapping is None or not bool(target_mapping.get("is_active")):
                return TelegramAdapterResult(
                    outcome=Outcome.NOOP,
                    reason_code=ReasonCode.PARTICIPANT_NOT_FOUND,
                    message="Пользователь уже отключен или не найден",
                )
            return self._enqueue_people_remove_confirmation(
                chat_id=chat_id,
                actor_user_id=actor_id,
                target_mapping=target_mapping,
                now=now,
            )

        if payload.startswith("confirm:"):
            target_id_raw = payload.replace("confirm:", "", 1).strip()
            try:
                target_id = int(target_id_raw)
            except ValueError:
                return TelegramAdapterResult(
                    outcome=Outcome.REJECTED,
                    reason_code=ReasonCode.INVALID_CALLBACK_FORMAT,
                    message=STALE_ACTION_MESSAGE,
                )

            conversation = self._repository.get_conversation_state(
                chat_id=chat_id,
                user_id=actor_id,
                flow=PEOPLE_FLOW_NAME,
                now=now,
            )
            if not isinstance(conversation, dict):
                return TelegramAdapterResult(
                    outcome=Outcome.NOOP,
                    reason_code=ReasonCode.STALE_ACTION,
                    message=STALE_ACTION_MESSAGE,
                )

            mode_obj = conversation.get("mode")
            target_obj = conversation.get("target_user_id")
            if mode_obj != "await_people_remove_confirm" or target_obj != target_id:
                return TelegramAdapterResult(
                    outcome=Outcome.NOOP,
                    reason_code=ReasonCode.STALE_ACTION,
                    message=STALE_ACTION_MESSAGE,
                )

            target_mapping = self._repository.get_user_mapping(target_id)
            if target_mapping is None or not bool(target_mapping.get("is_active")):
                self._repository.clear_conversation_state(
                    chat_id=chat_id,
                    user_id=actor_id,
                    flow=PEOPLE_FLOW_NAME,
                )
                return TelegramAdapterResult(
                    outcome=Outcome.NOOP,
                    reason_code=ReasonCode.PARTICIPANT_NOT_FOUND,
                    message="Пользователь уже отключен или не найден",
                )

            updated = self._repository.set_user_mapping_active(
                telegram_user_id=target_id,
                is_active=False,
                now=now,
            )
            self._repository.clear_conversation_state(
                chat_id=chat_id,
                user_id=actor_id,
                flow=PEOPLE_FLOW_NAME,
            )
            if not updated:
                return TelegramAdapterResult(
                    outcome=Outcome.NOOP,
                    reason_code=ReasonCode.PARTICIPANT_NOT_FOUND,
                    message="Пользователь не найден",
                )

            _ = self._repository.enqueue_outbox(
                effect_type=OutboxEffectType.TELEGRAM_SEND_MESSAGE,
                payload={
                    "telegram_user_id": chat_id,
                    "text": (
                        "Пользователь отключен: "
                        f"{self._people_button_label(target_mapping)}"
                    ),
                },
                idempotency_key=(
                    f"people_remove_done:{chat_id}:{actor_id}:{target_id}:{now.isoformat(timespec='seconds')}"
                ),
                now=now,
            )
            return TelegramAdapterResult(
                outcome=Outcome.OK,
                reason_code=ReasonCode.UPDATED,
                message="Пользователь отключен",
            )

        return TelegramAdapterResult(
            outcome=Outcome.NOOP,
            reason_code=ReasonCode.STALE_ACTION,
            message=STALE_ACTION_MESSAGE,
        )

    def _handle_help_message(
        self,
        *,
        message: Mapping[str, object],
        chat_id: int,
        now: datetime,
    ) -> TelegramAdapterResult:
        from_obj = message.get("from")
        user_id = cast(Mapping[str, object], from_obj or {}).get("id")
        if not isinstance(user_id, int):
            return TelegramAdapterResult(
                outcome=Outcome.REJECTED,
                reason_code=ReasonCode.INVALID_CALLBACK_FORMAT,
                message="Некорректный пользователь",
            )

        text = (
            "Как пользоваться ботом\n\n"
            "1) Создавайте, переносите и отменяйте встречи в Google Calendar.\n"
            "2) Бот синхронизирует изменения и отправляет обязательным участникам запросы в личку.\n"
            "3) Участники отвечают кнопками в личном сообщении бота.\n"
            "4) Статус встречи публикуется в чате встречи.\n\n"
            "Команды:\n"
            "/start — проверить подключение\n"
            "/help — открыть эту инструкцию\n"
            "/chat — настройка чата статусов (только менеджеры, только личный чат)\n"
            "/people — управление участниками (только менеджеры, только личный чат)"
        )

        _ = self._repository.enqueue_outbox(
            effect_type=OutboxEffectType.TELEGRAM_SEND_MESSAGE,
            payload={
                "telegram_user_id": chat_id,
                "text": text,
                "keyboard": main_menu_keyboard(),
            },
            idempotency_key=f"help:{chat_id}:{now.isoformat(timespec='seconds')}",
            now=now,
        )
        return TelegramAdapterResult(
            outcome=Outcome.OK,
            reason_code=ReasonCode.UPDATED,
            message="Help message enqueued",
        )

    def _enqueue_callback_answer(self, *, callback_id: str, now: datetime) -> None:
        _ = self._repository.enqueue_outbox(
            effect_type=OutboxEffectType.TELEGRAM_ANSWER_CALLBACK,
            payload={"callback_query_id": callback_id},
            idempotency_key=f"cb_answer:{callback_id}",
            now=now,
        )

    def _callback_message_text(
        self,
        *,
        callback_query: Mapping[str, object],
    ) -> str | None:
        message_obj = callback_query.get("message")
        if not isinstance(message_obj, dict):
            return None

        text_obj = cast(Mapping[str, object], message_obj).get("text")
        if not isinstance(text_obj, str) or not text_obj.strip():
            return None
        return text_obj

    def _callback_cleanup_text(
        self,
        *,
        callback_data: str,
        callback_query: Mapping[str, object],
        result: TelegramAdapterResult,
    ) -> str | None:
        if callback_data.startswith("act:") and (
            result.outcome == Outcome.OK
            or result.reason_code == ReasonCode.INVALID_STATE
        ):
            result_text = result.message.strip()
            if result_text:
                return result_text

        return self._callback_message_text(callback_query=callback_query)

    def _should_clear_callback_buttons(
        self,
        *,
        callback_data: str,
        result: TelegramAdapterResult,
    ) -> bool:
        if callback_data.startswith("act:"):
            return (
                result.outcome == Outcome.OK
                or result.reason_code == ReasonCode.INVALID_STATE
            )

        if result.outcome != Outcome.OK:
            return False

        return callback_data.startswith(
            (
                "act:",
                "chat_menu:",
                "people_menu:",
                "people_add:",
                "people_remove:pick:",
                "people_remove:confirm:",
            )
        )

    def _maybe_clear_callback_buttons(
        self,
        *,
        message_text: str | None,
        chat_id: int,
        message_id: int | None,
        callback_id: str,
        now: datetime,
    ) -> None:
        if not isinstance(message_id, int):
            return

        if not isinstance(message_text, str) or not message_text.strip():
            return

        _ = self._repository.enqueue_outbox(
            effect_type=OutboxEffectType.TELEGRAM_EDIT_MESSAGE,
            payload={
                "telegram_user_id": chat_id,
                "message_id": message_id,
                "text": message_text,
                "buttons": [],
            },
            idempotency_key=f"cb_cleanup:{callback_id}",
            now=now,
        )

    def _finalize_callback_result(
        self,
        *,
        callback_data: str,
        callback_query: Mapping[str, object],
        chat_id: int,
        message_id: int | None,
        callback_id: str,
        now: datetime,
        result: TelegramAdapterResult,
    ) -> TelegramAdapterResult:
        if self._should_clear_callback_buttons(
            callback_data=callback_data,
            result=result,
        ):
            cleanup_text = self._callback_cleanup_text(
                callback_data=callback_data,
                callback_query=callback_query,
                result=result,
            )
            self._maybe_clear_callback_buttons(
                message_text=cleanup_text,
                chat_id=chat_id,
                message_id=message_id,
                callback_id=callback_id,
                now=now,
            )

        return result

    def _handle_callback_query(
        self,
        *,
        callback_query: Mapping[str, object],
        now: datetime,
    ) -> TelegramAdapterResult:
        callback_id = callback_query.get("id")
        if not isinstance(callback_id, str) or not callback_id.strip():
            return TelegramAdapterResult(
                outcome=Outcome.REJECTED,
                reason_code=ReasonCode.INVALID_CALLBACK_FORMAT,
                message=STALE_ACTION_MESSAGE,
            )

        message_obj = callback_query.get("message")
        chat_id = None
        message_id: int | None = None
        if isinstance(message_obj, dict):
            message_map = cast(Mapping[str, object], message_obj)
            chat_obj = message_map.get("chat")
            if isinstance(chat_obj, dict):
                chat_id = cast(Mapping[str, object], chat_obj).get("id")
            message_id_obj = message_map.get("message_id")
            if isinstance(message_id_obj, int):
                message_id = message_id_obj

        if not isinstance(chat_id, int):
            actor = callback_query.get("from")
            if isinstance(actor, dict):
                chat_id = cast(Mapping[str, object], actor).get("id")

        if not isinstance(chat_id, int):
            return TelegramAdapterResult(
                outcome=Outcome.REJECTED,
                reason_code=ReasonCode.INVALID_CALLBACK_FORMAT,
                message=STALE_ACTION_MESSAGE,
            )

        accepted = self._repository.register_inbound_event(
            source=InboundEventSource.TELEGRAM_CALLBACK,
            external_event_id=callback_id,
            received_at=now,
        )
        if not accepted:
            self._enqueue_callback_answer(callback_id=callback_id, now=now)
            return TelegramAdapterResult(
                outcome=Outcome.NOOP,
                reason_code=ReasonCode.DUPLICATE_INBOUND_EVENT,
                message=STALE_ACTION_MESSAGE,
            )

        try:
            return self._handle_registered_callback_query(
                callback_query=callback_query,
                chat_id=chat_id,
                message_id=message_id,
                callback_id=callback_id,
                now=now,
            )
        except Exception:
            self._repository.unregister_inbound_event(
                source=InboundEventSource.TELEGRAM_CALLBACK,
                external_event_id=callback_id,
            )
            raise

    def _handle_registered_callback_query(
        self,
        *,
        callback_query: Mapping[str, object],
        chat_id: int,
        message_id: int | None,
        callback_id: str,
        now: datetime,
    ) -> TelegramAdapterResult:
        self._enqueue_callback_answer(callback_id=callback_id, now=now)

        data = callback_query.get("data")
        if not isinstance(data, str):
            return TelegramAdapterResult(
                outcome=Outcome.REJECTED,
                reason_code=ReasonCode.INVALID_CALLBACK_FORMAT,
                message=STALE_ACTION_MESSAGE,
            )

        def finalize(result: TelegramAdapterResult) -> TelegramAdapterResult:
            return self._finalize_callback_result(
                callback_data=data,
                callback_query=callback_query,
                chat_id=chat_id,
                message_id=message_id,
                callback_id=callback_id,
                now=now,
                result=result,
            )

        if data.startswith("people_menu:"):
            return finalize(
                self._handle_people_menu_callback(
                    callback_query=callback_query,
                    chat_id=chat_id,
                    data=data,
                    now=now,
                )
            )

        if data.startswith("chat_menu:"):
            return finalize(
                self._handle_chat_menu_callback(
                    callback_query=callback_query,
                    chat_id=chat_id,
                    data=data,
                    now=now,
                )
            )

        if data.startswith("people_add:"):
            return finalize(
                self._handle_people_add_callback(
                    callback_query=callback_query,
                    chat_id=chat_id,
                    data=data,
                    now=now,
                )
            )

        if data.startswith("people_remove:"):
            return finalize(
                self._handle_people_remove_callback(
                    callback_query=callback_query,
                    chat_id=chat_id,
                    data=data,
                    now=now,
                )
            )

        if data.startswith("act:"):
            token = parse_callback_data(data)
            if token is None:
                return TelegramAdapterResult(
                    outcome=Outcome.REJECTED,
                    reason_code=ReasonCode.INVALID_CALLBACK_FORMAT,
                    message=STALE_ACTION_MESSAGE,
                )

            callback_token = self._repository.get_callback_action_token(token)
            if callback_token is None or now > callback_token.expires_at:
                return TelegramAdapterResult(
                    outcome=Outcome.NOOP,
                    reason_code=ReasonCode.STALE_ACTION,
                    message=STALE_ACTION_MESSAGE,
                )

            actor = callback_query.get("from")
            actor_id = None
            if isinstance(actor, dict):
                actor_map = cast(Mapping[str, object], actor)
                actor_id = actor_map.get("id")
            if not isinstance(actor_id, int):
                return TelegramAdapterResult(
                    outcome=Outcome.REJECTED,
                    reason_code=ReasonCode.INVALID_CALLBACK_FORMAT,
                    message=STALE_ACTION_MESSAGE,
                )

            if actor_id != callback_token.allowed_user_id:
                return TelegramAdapterResult(
                    outcome=Outcome.NOOP,
                    reason_code=ReasonCode.STALE_ACTION,
                    message=STALE_ACTION_MESSAGE,
                )

            meeting = self._workflow_service.get_meeting(callback_token.meeting_id)
            if meeting is None:
                return TelegramAdapterResult(
                    outcome=Outcome.NOOP,
                    reason_code=ReasonCode.STALE_ACTION,
                    message=STALE_ACTION_MESSAGE,
                )

            if meeting.state == MeetingState.CANCELLED:
                return finalize(
                    TelegramAdapterResult(
                        outcome=Outcome.NOOP,
                        reason_code=ReasonCode.INVALID_STATE,
                        message="Встреча уже отменена. Этот ответ больше не нужен.",
                    )
                )

            if meeting.state == MeetingState.EXPIRED:
                return finalize(
                    TelegramAdapterResult(
                        outcome=Outcome.NOOP,
                        reason_code=ReasonCode.INVALID_STATE,
                        message="Встреча уже истекла. Этот ответ больше не нужен.",
                    )
                )

            if meeting.state == MeetingState.CONFIRMED:
                return finalize(
                    TelegramAdapterResult(
                        outcome=Outcome.NOOP,
                        reason_code=ReasonCode.INVALID_STATE,
                        message="Встреча уже подтверждена. Этот ответ больше не нужен.",
                    )
                )

            if meeting.confirmation_round != callback_token.round:
                return TelegramAdapterResult(
                    outcome=Outcome.NOOP,
                    reason_code=ReasonCode.STALE_ACTION,
                    message=STALE_ACTION_MESSAGE,
                )

            if callback_token.action_type in {
                CallbackActionType.PARTICIPANT_CONFIRM,
                CallbackActionType.PARTICIPANT_CANCEL,
            }:
                active_participant_ids = {
                    participant.telegram_user_id
                    for participant in meeting.participants
                    if participant.is_required
                }
                if actor_id not in active_participant_ids:
                    return TelegramAdapterResult(
                        outcome=Outcome.NOOP,
                        reason_code=ReasonCode.STALE_ACTION,
                        message=STALE_ACTION_MESSAGE,
                    )

            if callback_token.action_type == CallbackActionType.PARTICIPANT_CONFIRM:
                execution = self._workflow_service.record_participant_decision(
                    meeting_id=meeting.meeting_id,
                    round=callback_token.round,
                    actor_user_id=actor_id,
                    decision=Decision.CONFIRM,
                    source="telegram",
                    now=now,
                )
                return finalize(
                    TelegramAdapterResult(
                        outcome=execution.result.outcome,
                        reason_code=execution.result.reason_code,
                        message="Готово, отметили ваше участие ✅",
                    )
                )

            if callback_token.action_type == CallbackActionType.PARTICIPANT_CANCEL:
                execution = self._workflow_service.record_participant_decision(
                    meeting_id=meeting.meeting_id,
                    round=callback_token.round,
                    actor_user_id=actor_id,
                    decision=Decision.CANCEL,
                    source="telegram",
                    now=now,
                )
                return finalize(
                    TelegramAdapterResult(
                        outcome=execution.result.outcome,
                        reason_code=execution.result.reason_code,
                        message="Принято, отметили отказ ❌",
                    )
                )

            if callback_token.action_type == CallbackActionType.INITIATOR_CANCEL:
                effective_actor_id = actor_id
                if (
                    actor_id != meeting.initiator_telegram_user_id
                    and self._repository.is_manager(telegram_user_id=actor_id)
                ):
                    effective_actor_id = meeting.initiator_telegram_user_id
                execution = self._workflow_service.cancel_meeting(
                    meeting_id=meeting.meeting_id,
                    actor_user_id=effective_actor_id,
                    reason="initiator_callback",
                    requested_by_user_id=actor_id,
                    now=now,
                )
                return finalize(
                    TelegramAdapterResult(
                        outcome=execution.result.outcome,
                        reason_code=execution.result.reason_code,
                        message="Встреча отменена",
                    )
                )

            if (
                callback_token.action_type
                == CallbackActionType.INITIATOR_PROCEED_WITHOUT_SUBSET
            ):
                effective_actor_id = actor_id
                if (
                    actor_id != meeting.initiator_telegram_user_id
                    and self._repository.is_manager(telegram_user_id=actor_id)
                ):
                    effective_actor_id = meeting.initiator_telegram_user_id
                execution = self._workflow_service.proceed_without_subset(
                    meeting_id=meeting.meeting_id,
                    actor_user_id=effective_actor_id,
                    requested_by_user_id=actor_id,
                    now=now,
                )
                return finalize(
                    TelegramAdapterResult(
                        outcome=execution.result.outcome,
                        reason_code=execution.result.reason_code,
                        message="Встреча подтверждена без ожидающих ответа",
                    )
                )

            if callback_token.action_type == CallbackActionType.INITIATOR_REPLAN:
                return finalize(
                    TelegramAdapterResult(
                        outcome=Outcome.OK,
                        reason_code=ReasonCode.UPDATED,
                        message=(
                            "Перенесите встречу в Google Calendar. "
                            "Бот автоматически синхронизирует изменения."
                        ),
                    )
                )

        return finalize(
            TelegramAdapterResult(
                outcome=Outcome.NOOP,
                reason_code=ReasonCode.STALE_ACTION,
                message=STALE_ACTION_MESSAGE,
            )
        )
