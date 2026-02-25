from __future__ import annotations

from dataclasses import dataclass
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone as dt_timezone
from difflib import SequenceMatcher
import logging
import uuid
from zoneinfo import ZoneInfo

from bot_vstrechi.domain.commands import (
    CancelMeeting,
    CommandExecution,
    HandleConfirmDeadline,
    HandleInitiatorTimeout,
    ProceedWithoutSubset,
    RecordParticipantDecision,
    RescheduleMeeting,
    SelectSlot,
)
from bot_vstrechi.db.repository import SQLiteRepository
from bot_vstrechi.domain.models import (
    CommandResult,
    Decision,
    Meeting,
    MeetingParticipant,
    MeetingState,
    OutboxEffectType,
    Outcome,
    RecurringConfirmationMode,
    ReasonCode,
)
from bot_vstrechi.calendar.gateway import DaySlotAvailability, GoogleCalendarGateway
from bot_vstrechi.telegram.callback_tokens import CallbackTokenService
from bot_vstrechi.telegram.presentation import (
    format_local_datetime,
    format_local_range,
    meeting_title_or_default,
    normalize_timezone_name,
)


logger = logging.getLogger(__name__)

TERMINAL_SYNC_PENDING_TEXT = (
    "⏳ Обновляем финальный статус в календаре. Скоро покажем итог."
)


@dataclass(frozen=True)
class MeetDraftSession:
    meeting_id: str
    timezone: str
    duration_minutes: int


@dataclass(frozen=True)
class DaySlotOption:
    start_at: datetime
    end_at: datetime
    is_free: bool
    busy_usernames: tuple[str, ...]


class MeetingWorkflowService:
    def __init__(
        self, repository: SQLiteRepository, calendar_gateway: GoogleCalendarGateway
    ) -> None:
        self._repository: SQLiteRepository = repository
        self._calendar_gateway: GoogleCalendarGateway = calendar_gateway
        self._callback_tokens: CallbackTokenService = CallbackTokenService(repository)

    def create_meeting(self, meeting: Meeting, *, now: datetime) -> None:
        self._repository.insert_meeting(meeting, now=now)

    def _resolve_participant_mappings(
        self,
        *,
        initiator_user_id: int,
        participant_usernames: tuple[str, ...],
    ) -> tuple[
        list[dict[str, object]],
        tuple[str, ...],
        dict[str, tuple[str, ...]],
    ]:
        normalized_terms = tuple(
            term.lstrip("@").strip() for term in participant_usernames if term.strip()
        )
        mappings: list[dict[str, object]] = []
        missing_usernames: tuple[str, ...] = ()
        ambiguous_terms: dict[str, tuple[str, ...]] = {}

        if normalized_terms:
            all_active = self._repository.get_all_active_users()
            missing_terms: list[str] = []
            ambiguous: dict[str, tuple[str, ...]] = {}

            for term in normalized_terms:
                exact_matches: list[dict[str, object]] = [
                    mapping
                    for mapping in all_active
                    if self._is_exact_participant_match(mapping=mapping, term=term)
                ]
                candidate_matches: list[dict[str, object]] = exact_matches
                if not candidate_matches:
                    candidate_matches = [
                        mapping
                        for mapping in all_active
                        if self._is_partial_participant_match(
                            mapping=mapping, term=term
                        )
                    ]
                if not candidate_matches:
                    candidate_matches = [
                        mapping
                        for mapping in all_active
                        if self._is_typo_participant_match(mapping=mapping, term=term)
                    ]

                if len(candidate_matches) == 1:
                    mappings.append(candidate_matches[0])
                    continue
                if not candidate_matches:
                    missing_terms.append(term)
                    continue

                labels = tuple(
                    self._participant_mapping_label(mapping)
                    for mapping in candidate_matches[:5]
                )
                ambiguous[term] = labels

            missing_usernames = tuple(missing_terms)
            ambiguous_terms = ambiguous
        else:
            mappings = self._repository.get_all_active_users()

        unique_mappings: dict[int, dict[str, object]] = {}
        for mapping in mappings:
            user_id_obj = mapping.get("telegram_user_id")
            if isinstance(user_id_obj, int):
                unique_mappings[user_id_obj] = mapping

        initiator_mapping = self._repository.get_user_mapping(initiator_user_id)
        if initiator_mapping is not None and bool(initiator_mapping.get("is_active")):
            initiator_user_id_obj = initiator_mapping.get("telegram_user_id")
            if isinstance(initiator_user_id_obj, int):
                unique_mappings[initiator_user_id_obj] = initiator_mapping

        return list(unique_mappings.values()), missing_usernames, ambiguous_terms

    def _participant_mapping_label(self, mapping: dict[str, object]) -> str:
        username_obj = mapping.get("telegram_username")
        if isinstance(username_obj, str) and username_obj.strip():
            return f"@{username_obj.strip()}"

        full_name_obj = mapping.get("full_name")
        if isinstance(full_name_obj, str) and full_name_obj.strip():
            return full_name_obj.strip()

        email_obj = mapping.get("google_email")
        if isinstance(email_obj, str) and email_obj.strip():
            return email_obj.strip()

        user_id_obj = mapping.get("telegram_user_id")
        if isinstance(user_id_obj, int):
            return str(user_id_obj)

        return "unknown"

    def _participant_display_label(self, mapping: dict[str, object]) -> str:
        username_obj = mapping.get("telegram_username")
        username = username_obj.strip() if isinstance(username_obj, str) else ""

        full_name_obj = mapping.get("full_name")
        full_name = full_name_obj.strip() if isinstance(full_name_obj, str) else ""

        if full_name and username:
            return f"{full_name} (@{username})"
        if full_name:
            return full_name
        if username:
            return f"@{username}"

        email_obj = mapping.get("google_email")
        if isinstance(email_obj, str) and email_obj.strip():
            return email_obj.strip()

        user_id_obj = mapping.get("telegram_user_id")
        if isinstance(user_id_obj, int):
            return str(user_id_obj)

        return "unknown"

    def _participant_match_values(self, mapping: dict[str, object]) -> tuple[str, ...]:
        values: list[str] = []

        username_obj = mapping.get("telegram_username")
        if isinstance(username_obj, str) and username_obj.strip():
            username = username_obj.strip()
            values.append(username.casefold())

        email_obj = mapping.get("google_email")
        if isinstance(email_obj, str) and email_obj.strip():
            values.append(email_obj.strip().casefold())

        full_name_obj = mapping.get("full_name")
        if isinstance(full_name_obj, str) and full_name_obj.strip():
            values.append(full_name_obj.strip().casefold())

        user_id_obj = mapping.get("telegram_user_id")
        if isinstance(user_id_obj, int):
            values.append(str(user_id_obj))

        return tuple(values)

    def _participant_fuzzy_values(self, mapping: dict[str, object]) -> tuple[str, ...]:
        values: list[str] = []

        username_obj = mapping.get("telegram_username")
        if isinstance(username_obj, str) and username_obj.strip():
            values.append(username_obj.strip().casefold())

        email_obj = mapping.get("google_email")
        if isinstance(email_obj, str) and email_obj.strip():
            email = email_obj.strip().casefold()
            values.append(email)
            local_part = email.split("@", 1)[0]
            if local_part:
                values.append(local_part)

        full_name_obj = mapping.get("full_name")
        if isinstance(full_name_obj, str) and full_name_obj.strip():
            full_name = full_name_obj.strip().casefold()
            values.append(full_name)
            values.extend(part for part in full_name.split() if part)

        user_id_obj = mapping.get("telegram_user_id")
        if isinstance(user_id_obj, int):
            values.append(str(user_id_obj))

        seen: set[str] = set()
        unique_values: list[str] = []
        for value in values:
            if value in seen:
                continue
            seen.add(value)
            unique_values.append(value)
        return tuple(unique_values)

    def _is_one_edit_apart(self, *, left: str, right: str) -> bool:
        if left == right:
            return False

        left_len = len(left)
        right_len = len(right)
        if abs(left_len - right_len) > 1:
            return False

        if left_len == right_len:
            mismatch_indices = [
                idx for idx, pair in enumerate(zip(left, right)) if pair[0] != pair[1]
            ]
            if len(mismatch_indices) == 1:
                return True
            if len(mismatch_indices) == 2:
                first = mismatch_indices[0]
                second = mismatch_indices[1]
                return (
                    second == first + 1
                    and left[first] == right[second]
                    and left[second] == right[first]
                )
            return False

        shorter, longer = (left, right) if left_len < right_len else (right, left)
        index_short = 0
        index_long = 0
        edits = 0
        while index_short < len(shorter) and index_long < len(longer):
            if shorter[index_short] == longer[index_long]:
                index_short += 1
                index_long += 1
                continue
            edits += 1
            if edits > 1:
                return False
            index_long += 1

        return True

    def _is_typo_participant_match(
        self,
        *,
        mapping: dict[str, object],
        term: str,
    ) -> bool:
        normalized_term = term.strip().casefold()
        if len(normalized_term) < 4:
            return False

        for value in self._participant_fuzzy_values(mapping):
            if len(value) < 4:
                continue
            if value[:1] != normalized_term[:1]:
                continue
            if self._is_one_edit_apart(left=normalized_term, right=value):
                return True

            similarity = SequenceMatcher(None, normalized_term, value).ratio()
            if similarity >= 0.88 and abs(len(value) - len(normalized_term)) <= 1:
                return True
        return False

    def _is_exact_participant_match(
        self, *, mapping: dict[str, object], term: str
    ) -> bool:
        normalized_term = term.strip().casefold()
        if not normalized_term:
            return False

        for value in self._participant_match_values(mapping):
            if value == normalized_term:
                return True
        return False

    def _is_partial_participant_match(
        self,
        *,
        mapping: dict[str, object],
        term: str,
    ) -> bool:
        normalized_term = term.strip().casefold()
        if not normalized_term:
            return False

        for value in self._participant_match_values(mapping):
            if normalized_term in value:
                return True
        return False

    def list_cancellable_meetings(
        self,
        *,
        initiator_user_id: int,
        now: datetime,
        limit: int = 10,
    ) -> list[Meeting]:
        return self._repository.list_initiator_meetings(
            initiator_telegram_user_id=initiator_user_id,
            now=now,
            states=(
                MeetingState.PENDING,
                MeetingState.NEEDS_INITIATOR_DECISION,
                MeetingState.CONFIRMED,
            ),
            limit=limit,
        )

    def get_schedule_for_user(
        self,
        *,
        telegram_user_id: int,
        now: datetime,
        days: int = 7,
    ) -> tuple[CommandResult, list[tuple[datetime, datetime, str]], str | None]:
        mapping = self._repository.get_user_mapping(telegram_user_id)
        if mapping is None or not bool(mapping.get("is_active")):
            return (
                CommandResult(Outcome.REJECTED, ReasonCode.PARTICIPANT_NOT_FOUND),
                [],
                None,
            )

        email_obj = mapping.get("google_email")
        if not isinstance(email_obj, str) or not email_obj.strip():
            return (
                CommandResult(Outcome.REJECTED, ReasonCode.PARTICIPANT_NOT_FOUND),
                [],
                None,
            )

        schedule_events = self._calendar_gateway.list_schedule_events(
            email=email_obj,
            now=now,
            days=days,
        )
        return (
            CommandResult(Outcome.OK, ReasonCode.UPDATED),
            schedule_events,
            email_obj,
        )

    def find_free_slots(
        self,
        *,
        initiator_user_id: int,
        participant_usernames: tuple[str, ...],
        duration_minutes: int,
        now: datetime,
    ) -> tuple[
        CommandResult,
        list[tuple[datetime, datetime]],
        tuple[str, ...],
        dict[str, tuple[str, ...]],
        str,
        tuple[str, ...],
    ]:
        mappings, missing_usernames, ambiguous_terms = (
            self._resolve_participant_mappings(
                initiator_user_id=initiator_user_id,
                participant_usernames=participant_usernames,
            )
        )

        initiator_mapping = self._repository.get_user_mapping(initiator_user_id)
        timezone = "UTC"
        if initiator_mapping:
            timezone_obj = initiator_mapping.get("timezone")
            if isinstance(timezone_obj, str):
                timezone = timezone_obj

        if missing_usernames or ambiguous_terms:
            return (
                CommandResult(Outcome.REJECTED, ReasonCode.PARTICIPANT_NOT_FOUND),
                [],
                missing_usernames,
                ambiguous_terms,
                timezone,
                (),
            )

        if not mappings:
            return (
                CommandResult(Outcome.REJECTED, ReasonCode.PARTICIPANT_NOT_FOUND),
                [],
                (),
                {},
                timezone,
                (),
            )

        emails = [
            email
            for email in (mapping.get("google_email") for mapping in mappings)
            if isinstance(email, str) and email
        ]
        if not emails:
            return (
                CommandResult(Outcome.REJECTED, ReasonCode.PARTICIPANT_NOT_FOUND),
                [],
                (),
                {},
                timezone,
                (),
            )

        resolved_labels = tuple(
            self._participant_display_label(mapping) for mapping in mappings
        )

        normalized_timezone = normalize_timezone_name(timezone)
        zone = ZoneInfo(normalized_timezone)
        now_utc = (
            now.replace(tzinfo=dt_timezone.utc)
            if now.tzinfo is None
            else now.astimezone(dt_timezone.utc)
        )
        first_day = now_utc.astimezone(zone).date()

        slots: list[tuple[datetime, datetime]] = []
        for day_offset in range(7):
            day = first_day + timedelta(days=day_offset)
            if day.weekday() >= 5:
                continue
            day_slots = self._calendar_gateway.list_day_slot_availability(
                emails=tuple(emails),
                duration_minutes=duration_minutes,
                timezone=normalized_timezone,
                day=day,
                step_minutes=15,
            )
            for option in day_slots:
                if not option.is_free:
                    continue
                if option.start_at < now_utc:
                    continue
                slots.append((option.start_at, option.end_at))

        slots.sort(key=lambda item: item[0])
        if not slots:
            return (
                CommandResult(Outcome.NOOP, ReasonCode.INVALID_STATE),
                [],
                (),
                {},
                timezone,
                resolved_labels,
            )
        return (
            CommandResult(Outcome.OK, ReasonCode.UPDATED),
            slots,
            (),
            {},
            timezone,
            resolved_labels,
        )

    def create_meeting_draft(
        self,
        *,
        initiator_user_id: int,
        chat_id: int,
        title: str,
        duration_minutes: int,
        participant_usernames: tuple[str, ...],
        now: datetime,
    ) -> tuple[
        CommandResult,
        MeetDraftSession | None,
        tuple[str, ...],
        dict[str, tuple[str, ...]],
    ]:
        mappings, missing_usernames, ambiguous_terms = (
            self._resolve_participant_mappings(
                initiator_user_id=initiator_user_id,
                participant_usernames=participant_usernames,
            )
        )
        if missing_usernames or ambiguous_terms:
            return (
                CommandResult(Outcome.REJECTED, ReasonCode.PARTICIPANT_NOT_FOUND),
                None,
                missing_usernames,
                ambiguous_terms,
            )
        if not mappings:
            return (
                CommandResult(Outcome.REJECTED, ReasonCode.PARTICIPANT_NOT_FOUND),
                None,
                (),
                {},
            )

        initiator_mapping = self._repository.get_user_mapping(initiator_user_id)
        timezone = "UTC"
        if initiator_mapping is not None:
            timezone_obj = initiator_mapping.get("timezone")
            if isinstance(timezone_obj, str):
                timezone = normalize_timezone_name(timezone_obj)

        participants = tuple(
            MeetingParticipant(
                telegram_user_id=telegram_user_id,
                is_required=telegram_user_id != initiator_user_id,
                decision=Decision.NONE,
            )
            for telegram_user_id in (
                mapping.get("telegram_user_id") for mapping in mappings
            )
            if isinstance(telegram_user_id, int)
        )
        if not participants:
            return (
                CommandResult(Outcome.REJECTED, ReasonCode.PARTICIPANT_NOT_FOUND),
                None,
                (),
                {},
            )

        meeting_id = uuid.uuid4().hex[:16]
        draft_meeting = Meeting(
            meeting_id=meeting_id,
            initiator_telegram_user_id=initiator_user_id,
            chat_id=chat_id,
            state=MeetingState.DRAFT,
            scheduled_start_at=now,
            scheduled_end_at=now,
            title=meeting_title_or_default(title),
            participants=participants,
            created_by_bot=True,
        )
        self._repository.insert_meeting(draft_meeting, now=now)
        return (
            CommandResult(Outcome.OK, ReasonCode.UPDATED),
            MeetDraftSession(
                meeting_id=meeting_id,
                timezone=timezone,
                duration_minutes=duration_minutes,
            ),
            (),
            {},
        )

    def list_meeting_day_slots(
        self,
        *,
        meeting_id: str,
        duration_minutes: int,
        timezone: str,
        day: date,
    ) -> tuple[CommandResult, tuple[DaySlotOption, ...]]:
        meeting = self._repository.get_meeting(meeting_id)
        if meeting is None:
            return (CommandResult(Outcome.NOOP, ReasonCode.STALE_ACTION), ())

        participant_user_map: dict[str, str] = {}
        emails: list[str] = []
        for participant in meeting.participants:
            mapping = self._repository.get_user_mapping(participant.telegram_user_id)
            if mapping is None:
                continue

            email_obj = mapping.get("google_email")
            if not isinstance(email_obj, str) or not email_obj:
                continue

            emails.append(email_obj)
            username_obj = mapping.get("telegram_username")
            if isinstance(username_obj, str) and username_obj.strip():
                participant_user_map[email_obj] = f"@{username_obj.strip()}"
            else:
                participant_user_map[email_obj] = str(participant.telegram_user_id)

        if not emails:
            return (
                CommandResult(Outcome.REJECTED, ReasonCode.PARTICIPANT_NOT_FOUND),
                (),
            )

        day_slots: list[DaySlotAvailability] = (
            self._calendar_gateway.list_day_slot_availability(
                emails=tuple(emails),
                duration_minutes=duration_minutes,
                timezone=timezone,
                day=day,
            )
        )

        options = tuple(
            DaySlotOption(
                start_at=slot.start_at,
                end_at=slot.end_at,
                is_free=slot.is_free,
                busy_usernames=tuple(
                    participant_user_map.get(email, email) for email in slot.busy_emails
                ),
            )
            for slot in day_slots
        )
        return (CommandResult(Outcome.OK, ReasonCode.UPDATED), options)

    def propose_slots(
        self,
        *,
        initiator_user_id: int,
        chat_id: int,
        title: str,
        duration_minutes: int,
        participant_usernames: tuple[str, ...],
        now: datetime,
    ) -> CommandResult:
        draft_result, session, missing_usernames, ambiguous_terms = (
            self.create_meeting_draft(
                initiator_user_id=initiator_user_id,
                chat_id=chat_id,
                title=title,
                duration_minutes=duration_minutes,
                participant_usernames=participant_usernames,
                now=now,
            )
        )
        if missing_usernames or ambiguous_terms:
            missing = ", ".join(missing_usernames)
            lines: list[str] = []
            if missing:
                lines.append(f"Не найден пользователь(и): {missing}.")
            if ambiguous_terms:
                lines.append("Нужно уточнить участников:")
                for term, options in ambiguous_terms.items():
                    options_text = ", ".join(options)
                    lines.append(f"- {term}: {options_text}")
            _ = self._repository.enqueue_outbox(
                effect_type=OutboxEffectType.TELEGRAM_SEND_MESSAGE,
                payload={
                    "telegram_user_id": chat_id,
                    "text": "\n".join(lines)
                    if lines
                    else "Не удалось распознать участников.",
                },
                idempotency_key=(
                    f"missing_participants:{initiator_user_id}:{now.isoformat()}"
                ),
                now=now,
            )
            return CommandResult(Outcome.REJECTED, ReasonCode.PARTICIPANT_NOT_FOUND)

        if draft_result.outcome != Outcome.OK or session is None:
            return draft_result

        meeting = self._require_meeting(session.meeting_id)
        emails: list[str] = []
        for participant in meeting.participants:
            mapping = self._repository.get_user_mapping(participant.telegram_user_id)
            if mapping is None:
                continue
            email_obj = mapping.get("google_email")
            if isinstance(email_obj, str) and email_obj:
                emails.append(email_obj)

        if not emails:
            return CommandResult(Outcome.REJECTED, ReasonCode.PARTICIPANT_NOT_FOUND)

        slots = self._calendar_gateway.search_free_slots(
            emails=tuple(emails),
            duration_minutes=duration_minutes,
            timezone=session.timezone,
            now=now,
        )

        if not slots:
            _ = self._repository.enqueue_outbox(
                effect_type=OutboxEffectType.TELEGRAM_SEND_MESSAGE,
                payload={
                    "telegram_user_id": chat_id,
                    "text": "К сожалению, не удалось найти свободные слоты на ближайшее время.",
                },
                idempotency_key=f"no_slots:{initiator_user_id}:{now.isoformat()}",
                now=now,
            )
            return CommandResult(Outcome.NOOP, ReasonCode.INVALID_STATE)

        slot_buttons: list[dict[str, str]] = []
        normalized_tz = normalize_timezone_name(session.timezone)
        for slot in slots:
            start_key = slot[0].strftime("%Y%m%dT%H%M")

            end_key = slot[1].strftime("%Y%m%dT%H%M")
            display = format_local_range(
                slot[0],
                slot[1],
                timezone_name=normalized_tz,
            )
            slot_button: dict[str, str] = {
                "text": display,
                "callback_data": f"select_slot:{session.meeting_id}:{start_key}:{end_key}",
            }
            slot_buttons.append(slot_button)

        safe_title = meeting_title_or_default(title)
        text = f"Выберите слот для встречи «{safe_title}» ({duration_minutes} мин, {normalized_tz}):"
        outbox_payload: dict[str, object] = {
            "telegram_user_id": chat_id,
            "text": text,
            "buttons": slot_buttons,
        }

        _ = self._repository.enqueue_outbox(
            effect_type=OutboxEffectType.TELEGRAM_SEND_MESSAGE,
            payload=outbox_payload,
            idempotency_key=f"propose:{session.meeting_id}",
            now=now,
        )

        return CommandResult(Outcome.OK, ReasonCode.UPDATED)

    def get_meeting(self, meeting_id: str) -> Meeting | None:
        return self._repository.get_meeting(meeting_id)

    def select_slot(
        self,
        *,
        meeting_id: str,
        actor_user_id: int,
        chat_id: int,
        scheduled_start_at: datetime,
        scheduled_end_at: datetime,
        now: datetime,
    ) -> CommandExecution:
        meeting = self._require_meeting(meeting_id)
        execution = SelectSlot(
            replace(meeting, chat_id=chat_id),
            actor_user_id=actor_user_id,
            scheduled_start_at=scheduled_start_at,
            scheduled_end_at=scheduled_end_at,
            now=now,
        )
        return self._apply_with_guard(
            before=meeting,
            execution=execution,
            action="select_slot",
            actor_user_id=actor_user_id,
            now=now,
        )

    def record_participant_decision(
        self,
        *,
        meeting_id: str,
        round: int,
        actor_user_id: int,
        decision: Decision,
        source: str,
        now: datetime,
    ) -> CommandExecution:
        meeting = self._require_meeting(meeting_id)
        execution = RecordParticipantDecision(
            meeting,
            round=round,
            actor_user_id=actor_user_id,
            decision=decision,
            source=source,
            now=now,
        )
        return self._apply_with_guard(
            before=meeting,
            execution=execution,
            action="record_participant_decision",
            actor_user_id=actor_user_id,
            now=now,
        )

    def handle_confirm_deadline(
        self,
        *,
        meeting_id: str,
        round: int,
        now: datetime,
    ) -> CommandExecution:
        meeting = self._require_meeting(meeting_id)
        execution = HandleConfirmDeadline(meeting, round=round, now=now)
        return self._apply_with_guard(
            before=meeting,
            execution=execution,
            action="handle_confirm_deadline",
            now=now,
        )

    def handle_initiator_timeout(
        self,
        *,
        meeting_id: str,
        round: int,
        now: datetime,
    ) -> CommandExecution:
        meeting = self._require_meeting(meeting_id)
        execution = HandleInitiatorTimeout(meeting, round=round, now=now)
        return self._apply_with_guard(
            before=meeting,
            execution=execution,
            action="handle_initiator_timeout",
            now=now,
        )

    def reschedule_meeting(
        self,
        *,
        meeting_id: str,
        actor_user_id: int,
        chat_id: int,
        scheduled_start_at: datetime,
        scheduled_end_at: datetime,
        now: datetime,
    ) -> CommandExecution:
        meeting = self._require_meeting(meeting_id)
        execution = RescheduleMeeting(
            replace(meeting, chat_id=chat_id),
            actor_user_id=actor_user_id,
            scheduled_start_at=scheduled_start_at,
            scheduled_end_at=scheduled_end_at,
            now=now,
        )
        return self._apply_with_guard(
            before=meeting,
            execution=execution,
            action="reschedule_meeting",
            actor_user_id=actor_user_id,
            now=now,
        )

    def select_slot_from_calendar(
        self,
        *,
        meeting_id: str,
        actor_user_id: int,
        chat_id: int,
        scheduled_start_at: datetime,
        scheduled_end_at: datetime,
        now: datetime,
        force_pending: bool = False,
    ) -> CommandExecution:
        meeting = self._require_meeting(meeting_id)
        execution = SelectSlot(
            replace(meeting, chat_id=chat_id),
            actor_user_id=actor_user_id,
            scheduled_start_at=scheduled_start_at,
            scheduled_end_at=scheduled_end_at,
            now=now,
            force_pending=force_pending,
        )
        return self._apply_with_guard(
            before=meeting,
            execution=execution,
            action="calendar_ingest_select_slot",
            actor_user_id=actor_user_id,
            now=now,
        )

    def reschedule_from_calendar(
        self,
        *,
        meeting_id: str,
        actor_user_id: int,
        chat_id: int,
        scheduled_start_at: datetime,
        scheduled_end_at: datetime,
        now: datetime,
        force_pending: bool = False,
    ) -> CommandExecution:
        meeting = self._require_meeting(meeting_id)
        execution = RescheduleMeeting(
            replace(meeting, chat_id=chat_id),
            actor_user_id=actor_user_id,
            scheduled_start_at=scheduled_start_at,
            scheduled_end_at=scheduled_end_at,
            now=now,
            force_pending=force_pending,
        )
        return self._apply_with_guard(
            before=meeting,
            execution=execution,
            action="calendar_ingest_reschedule",
            actor_user_id=actor_user_id,
            now=now,
        )

    def sync_participants_from_calendar(
        self,
        *,
        meeting_id: str,
        actor_user_id: int,
        required_participant_user_ids: tuple[int, ...],
        now: datetime,
    ) -> CommandExecution:
        meeting = self._require_meeting(meeting_id)
        if actor_user_id != meeting.initiator_telegram_user_id:
            return self._apply_with_guard(
                before=meeting,
                execution=CommandExecution(
                    result=CommandResult(
                        Outcome.REJECTED, ReasonCode.PERMISSION_DENIED
                    ),
                    meeting=meeting,
                ),
                action="calendar_ingest_sync_participants",
                actor_user_id=actor_user_id,
                now=now,
            )

        if meeting.state in (MeetingState.CANCELLED, MeetingState.EXPIRED):
            return self._apply_with_guard(
                before=meeting,
                execution=CommandExecution(
                    result=CommandResult(Outcome.NOOP, ReasonCode.INVALID_STATE),
                    meeting=meeting,
                ),
                action="calendar_ingest_sync_participants",
                actor_user_id=actor_user_id,
                now=now,
            )

        existing_by_user_id = {
            participant.telegram_user_id: participant
            for participant in meeting.participants
        }
        before_required_user_ids = {
            participant.telegram_user_id
            for participant in meeting.participants
            if participant.is_required
            and participant.telegram_user_id != meeting.initiator_telegram_user_id
        }

        normalized_required_user_ids = tuple(
            sorted(
                {
                    user_id
                    for user_id in required_participant_user_ids
                    if user_id != meeting.initiator_telegram_user_id
                }
            )
        )

        initiator_participant = existing_by_user_id.get(
            meeting.initiator_telegram_user_id
        )
        if initiator_participant is None:
            initiator_participant = MeetingParticipant(
                telegram_user_id=meeting.initiator_telegram_user_id,
                is_required=False,
                decision=Decision.NONE,
            )
        else:
            initiator_participant = replace(initiator_participant, is_required=False)

        updated_participants: list[MeetingParticipant] = [initiator_participant]
        added_required_user_ids: list[int] = []
        for user_id in normalized_required_user_ids:
            existing_participant = existing_by_user_id.get(user_id)
            if existing_participant is None:
                added_required_user_ids.append(user_id)
                updated_participants.append(
                    MeetingParticipant(
                        telegram_user_id=user_id,
                        is_required=True,
                        decision=Decision.NONE,
                    )
                )
                continue

            updated_participants.append(replace(existing_participant, is_required=True))

        updated_meeting = replace(meeting, participants=tuple(updated_participants))
        if updated_meeting.participants == meeting.participants:
            return self._apply_with_guard(
                before=meeting,
                execution=CommandExecution(
                    result=CommandResult(
                        Outcome.NOOP,
                        ReasonCode.STALE_OR_OLDER_RESPONSE,
                    ),
                    meeting=meeting,
                ),
                action="calendar_ingest_sync_participants",
                actor_user_id=actor_user_id,
                now=now,
            )

        removed_required_user_ids = tuple(
            sorted(before_required_user_ids - set(normalized_required_user_ids))
        )

        execution = self._apply_with_guard(
            before=meeting,
            execution=CommandExecution(
                result=CommandResult(Outcome.OK, ReasonCode.UPDATED),
                meeting=updated_meeting,
            ),
            action="calendar_ingest_sync_participants",
            actor_user_id=actor_user_id,
            now=now,
        )

        if (
            execution.result.outcome == Outcome.OK
            and execution.meeting.state == MeetingState.PENDING
            and (added_required_user_ids or removed_required_user_ids)
        ):
            if removed_required_user_ids:
                _ = self._repository.expire_callback_tokens_for_participants(
                    meeting_id=execution.meeting.meeting_id,
                    round=execution.meeting.confirmation_round,
                    user_ids=removed_required_user_ids,
                    now=now,
                )
                removed_pending_keys = tuple(
                    (
                        f"notify:{execution.meeting.meeting_id}:"
                        f"r{execution.meeting.confirmation_round}:"
                        f"pending:participant:{participant_user_id}"
                    )
                    for participant_user_id in removed_required_user_ids
                )
                _ = self._repository.suppress_pending_outbox_by_keys(
                    keys=removed_pending_keys,
                    reason=("suppressed: participant removed from required list"),
                    now=now,
                )
                for participant_user_id in removed_required_user_ids:
                    _ = self._repository.enqueue_outbox(
                        effect_type=OutboxEffectType.TELEGRAM_SEND_MESSAGE,
                        payload={
                            "telegram_user_id": participant_user_id,
                            "text": (
                                "Вы больше не обязательный участник этой встречи.\n"
                                "Предыдущие кнопки подтверждения больше не действуют."
                            ),
                        },
                        idempotency_key=(
                            f"notify:{execution.meeting.meeting_id}:"
                            f"r{execution.meeting.confirmation_round}:"
                            f"removed_participant:{participant_user_id}"
                        ),
                        now=now,
                    )

            for participant_user_id in added_required_user_ids:
                self._enqueue_pending_participant_decision_request(
                    meeting=execution.meeting,
                    participant_user_id=participant_user_id,
                    now=now,
                )

            self._enqueue_group_status_update(
                meeting=execution.meeting,
                text=self._pending_status_text(execution.meeting),
                status_tag="pending_participants_sync",
                status_revision=(
                    f"add{len(added_required_user_ids)}:"
                    f"rm{len(removed_required_user_ids)}:"
                    f"{now.isoformat(timespec='microseconds')}"
                ),
                now=now,
            )

        if (
            execution.result.outcome == Outcome.OK
            and execution.meeting.state == MeetingState.CONFIRMED
            and execution.meeting.recurring_confirmation_mode
            == RecurringConfirmationMode.EXCEPTIONS_ONLY
            and added_required_user_ids
        ):
            meeting_context = self._meeting_context_text(execution.meeting)
            for participant_user_id in added_required_user_ids:
                _ = self._repository.enqueue_outbox(
                    effect_type=OutboxEffectType.TELEGRAM_SEND_MESSAGE,
                    payload={
                        "telegram_user_id": participant_user_id,
                        "text": (
                            "Вас добавили в регулярную встречу.\n"
                            f"{meeting_context}\n"
                            "Подтверждение не требуется, если участие обычное."
                        ),
                    },
                    idempotency_key=(
                        f"notify:{execution.meeting.meeting_id}:"
                        f"r{execution.meeting.confirmation_round}:"
                        f"recurring_added:{participant_user_id}"
                    ),
                    now=now,
                )

        return execution

    def cancel_from_calendar(
        self,
        *,
        meeting_id: str,
        actor_user_id: int,
        now: datetime,
    ) -> CommandExecution:
        meeting = self._require_meeting(meeting_id)
        execution = CancelMeeting(
            meeting,
            actor_user_id=actor_user_id,
            reason="calendar_ingest",
        )
        return self._apply_with_guard(
            before=meeting,
            execution=execution,
            action="calendar_ingest_cancel",
            actor_user_id=actor_user_id,
            now=now,
        )

    def cancel_meeting(
        self,
        *,
        meeting_id: str,
        actor_user_id: int,
        reason: str,
        requested_by_user_id: int | None = None,
        now: datetime,
    ) -> CommandExecution:
        meeting = self._require_meeting(meeting_id)
        execution = CancelMeeting(meeting, actor_user_id=actor_user_id, reason=reason)
        return self._apply_with_guard(
            before=meeting,
            execution=execution,
            action="cancel_meeting",
            actor_user_id=actor_user_id,
            requested_by_user_id=requested_by_user_id,
            now=now,
        )

    def proceed_without_subset(
        self,
        *,
        meeting_id: str,
        actor_user_id: int,
        requested_by_user_id: int | None = None,
        now: datetime,
    ) -> CommandExecution:
        meeting = self._require_meeting(meeting_id)
        execution = ProceedWithoutSubset(meeting, actor_user_id=actor_user_id)
        return self._apply_with_guard(
            before=meeting,
            execution=execution,
            action="proceed_without_subset",
            actor_user_id=actor_user_id,
            requested_by_user_id=requested_by_user_id,
            now=now,
        )

    def _apply_with_guard(
        self,
        *,
        before: Meeting,
        execution: CommandExecution,
        action: str,
        actor_user_id: int | None = None,
        requested_by_user_id: int | None = None,
        now: datetime,
    ) -> CommandExecution:
        if execution.result.outcome != Outcome.OK:
            self._write_audit_log(
                before=before,
                execution=execution,
                action=action,
                actor_user_id=actor_user_id,
                requested_by_user_id=requested_by_user_id,
                now=now,
            )
            logger.info(
                "command executed",
                extra={
                    "meeting_id": execution.meeting.meeting_id,
                    "outcome": execution.result.outcome,
                    "reason_code": execution.result.reason_code,
                    "state_before": before.state,
                    "state_after": execution.meeting.state,
                },
            )
            return execution

        with self._repository.atomic():
            applied = self._repository.apply_execution(
                before=before, execution=execution, now=now
            )
            if applied:
                self._enqueue_transition_notifications(
                    before=before,
                    after=execution.meeting,
                    defer_terminal_status=(
                        not action.startswith("calendar_ingest_")
                        and execution.meeting.created_by_bot
                        and isinstance(execution.meeting.google_event_id, str)
                        and bool(execution.meeting.google_event_id)
                    ),
                    now=now,
                )
                if (
                    action == "record_participant_decision"
                    and actor_user_id is not None
                ):
                    self._enqueue_pending_progress_update(
                        before=before,
                        after=execution.meeting,
                        actor_user_id=actor_user_id,
                        now=now,
                    )
                self._enqueue_calendar_sync(
                    before=before,
                    after=execution.meeting,
                    action=action,
                    actor_user_id=actor_user_id,
                    now=now,
                )
                self._write_audit_log(
                    before=before,
                    execution=execution,
                    action=action,
                    actor_user_id=actor_user_id,
                    requested_by_user_id=requested_by_user_id,
                    now=now,
                )
                logger.info(
                    "command executed",
                    extra={
                        "meeting_id": execution.meeting.meeting_id,
                        "outcome": execution.result.outcome,
                        "reason_code": execution.result.reason_code,
                        "state_before": before.state,
                        "state_after": execution.meeting.state,
                    },
                )
                return execution

        conflict_execution = CommandExecution(
            result=CommandResult(Outcome.NOOP, ReasonCode.OPTIMISTIC_CONFLICT),
            meeting=replace(before),
            jobs=(),
        )
        self._write_audit_log(
            before=before,
            execution=conflict_execution,
            action=action,
            actor_user_id=actor_user_id,
            requested_by_user_id=requested_by_user_id,
            now=now,
        )
        logger.info(
            "command executed",
            extra={
                "meeting_id": conflict_execution.meeting.meeting_id,
                "outcome": conflict_execution.result.outcome,
                "reason_code": conflict_execution.result.reason_code,
                "state_before": before.state,
                "state_after": conflict_execution.meeting.state,
            },
        )
        return conflict_execution

    def _enqueue_transition_notifications(
        self,
        *,
        before: Meeting,
        after: Meeting,
        defer_terminal_status: bool,
        now: datetime,
    ) -> None:
        if before.state == after.state:
            return

        meeting_context = self._meeting_context_text(after)

        if after.state == MeetingState.PENDING:
            self._enqueue_pending_notifications(after=after, now=now)
            return

        if after.state == MeetingState.CONFIRMED:
            if before.state == MeetingState.PENDING:
                self._repository.suppress_pending_group_progress_outbox(
                    meeting_id=after.meeting_id,
                    round=after.confirmation_round,
                    now=now,
                )
            if defer_terminal_status:
                self._enqueue_group_status_update(
                    meeting=after,
                    text=TERMINAL_SYNC_PENDING_TEXT,
                    status_tag="terminal_sync_pending",
                    status_revision=after.state,
                    now=now,
                )
                return
            self._enqueue_group_status_update(
                meeting=after,
                text=f"✅ Встреча подтверждена.\n{meeting_context}",
                status_tag="confirmed",
                now=now,
            )
            return

        if after.state == MeetingState.NEEDS_INITIATOR_DECISION:
            self._enqueue_initiator_decision_notifications(after=after, now=now)
            return

        if after.state == MeetingState.CANCELLED:
            if before.state == MeetingState.PENDING:
                self._repository.suppress_pending_group_progress_outbox(
                    meeting_id=after.meeting_id,
                    round=after.confirmation_round,
                    now=now,
                )
            if defer_terminal_status:
                self._enqueue_group_status_update(
                    meeting=after,
                    text=TERMINAL_SYNC_PENDING_TEXT,
                    status_tag="terminal_sync_pending",
                    status_revision=after.state,
                    now=now,
                )
                return
            self._enqueue_group_status_update(
                meeting=after,
                text=f"❌ Встреча отменена.\n{meeting_context}",
                status_tag="cancelled",
                now=now,
            )
            return

        if after.state == MeetingState.EXPIRED:
            if before.state == MeetingState.PENDING:
                self._repository.suppress_pending_group_progress_outbox(
                    meeting_id=after.meeting_id,
                    round=after.confirmation_round,
                    now=now,
                )
            if defer_terminal_status:
                self._enqueue_group_status_update(
                    meeting=after,
                    text=TERMINAL_SYNC_PENDING_TEXT,
                    status_tag="terminal_sync_pending",
                    status_revision=after.state,
                    now=now,
                )
                return
            self._enqueue_group_status_update(
                meeting=after,
                text=f"⌛ Встреча просрочена.\n{meeting_context}",
                status_tag="expired",
                now=now,
            )

    def _enqueue_group_status_update(
        self,
        *,
        meeting: Meeting,
        text: str,
        status_tag: str,
        status_revision: str | None = None,
        now: datetime,
    ) -> None:
        payload: dict[str, object] = {
            "telegram_user_id": meeting.chat_id,
            "text": text,
            "_group_status_message": True,
            "_meeting_id": meeting.meeting_id,
            "_meeting_round": meeting.confirmation_round,
            "_group_status_tag": status_tag,
        }
        revision_suffix = (
            f":{status_revision}"
            if isinstance(status_revision, str) and status_revision
            else ""
        )

        if meeting.group_status_message_id is not None:
            payload["message_id"] = meeting.group_status_message_id
            _ = self._repository.enqueue_outbox(
                effect_type=OutboxEffectType.TELEGRAM_EDIT_MESSAGE,
                payload=payload,
                idempotency_key=(
                    f"group_status:{meeting.meeting_id}:r{meeting.confirmation_round}:"
                    f"{status_tag}:edit{revision_suffix}"
                ),
                now=now,
            )
            return

        if status_tag == "pending_progress":
            _ = self._repository.enqueue_outbox(
                effect_type=OutboxEffectType.TELEGRAM_EDIT_MESSAGE,
                payload=payload,
                idempotency_key=(
                    f"group_status:{meeting.meeting_id}:r{meeting.confirmation_round}:"
                    f"{status_tag}:edit{revision_suffix}"
                ),
                now=now,
            )
            return
        _ = self._repository.enqueue_outbox(
            effect_type=OutboxEffectType.TELEGRAM_SEND_MESSAGE,
            payload=payload,
            idempotency_key=(
                f"group_status:{meeting.meeting_id}:r{meeting.confirmation_round}:"
                f"{status_tag}:send{revision_suffix}"
            ),
            now=now,
        )

    def _enqueue_pending_notifications(self, *, after: Meeting, now: datetime) -> None:
        participants_to_notify = tuple(
            participant.telegram_user_id
            for participant in after.participants
            if participant.is_required
            and participant.decision == Decision.NONE
            and participant.telegram_user_id != after.initiator_telegram_user_id
        )
        pending_text = self._pending_status_text(after)

        self._enqueue_group_status_update(
            meeting=after,
            text=pending_text,
            status_tag="pending",
            now=now,
        )

        if after.chat_id != after.initiator_telegram_user_id:
            _ = self._repository.enqueue_outbox(
                effect_type=OutboxEffectType.TELEGRAM_SEND_MESSAGE,
                payload={
                    "telegram_user_id": after.initiator_telegram_user_id,
                    "text": self._initiator_pending_status_text(after),
                },
                idempotency_key=(
                    f"notify:{after.meeting_id}:r{after.confirmation_round}:"
                    f"pending:initiator:{after.initiator_telegram_user_id}"
                ),
                now=now,
            )

        for participant_user_id in participants_to_notify:
            self._enqueue_pending_participant_decision_request(
                meeting=after,
                participant_user_id=participant_user_id,
                now=now,
            )

    def _enqueue_pending_participant_decision_request(
        self,
        *,
        meeting: Meeting,
        participant_user_id: int,
        now: datetime,
    ) -> None:
        meeting_context = self._meeting_context_text(meeting)
        confirm_button, cancel_button = (
            self._callback_tokens.build_participant_decision_buttons(
                meeting=meeting,
                participant_user_id=participant_user_id,
                now=now,
            )
        )
        deadline_text = self._response_deadline_text(meeting)
        deadline_line = (
            f"Ответ нужен до: {deadline_text}\n"
            if isinstance(deadline_text, str)
            else ""
        )
        _ = self._repository.enqueue_outbox(
            effect_type=OutboxEffectType.TELEGRAM_SEND_MESSAGE,
            payload={
                "telegram_user_id": participant_user_id,
                "text": (
                    "Подтвердите участие, пожалуйста.\n"
                    f"{meeting_context}\n"
                    f"{deadline_line}"
                    "Выберите действие:"
                ),
                "buttons": [
                    {
                        "text": confirm_button.text,
                        "callback_data": confirm_button.callback_data,
                    },
                    {
                        "text": cancel_button.text,
                        "callback_data": cancel_button.callback_data,
                    },
                ],
                "_meeting_id": meeting.meeting_id,
                "_meeting_round": meeting.confirmation_round,
                "_participant_user_id": participant_user_id,
                "_pending_participant_request": True,
            },
            idempotency_key=(
                f"notify:{meeting.meeting_id}:r{meeting.confirmation_round}:"
                f"pending:participant:{participant_user_id}"
            ),
            now=now,
        )

    def _enqueue_pending_progress_update(
        self,
        *,
        before: Meeting,
        after: Meeting,
        actor_user_id: int,
        now: datetime,
    ) -> None:
        if (
            before.state != MeetingState.PENDING
            or after.state != MeetingState.PENDING
            or before.confirmation_round != after.confirmation_round
        ):
            return

        before_participant = next(
            (
                participant
                for participant in before.participants
                if participant.telegram_user_id == actor_user_id
            ),
            None,
        )
        after_participant = next(
            (
                participant
                for participant in after.participants
                if participant.telegram_user_id == actor_user_id
            ),
            None,
        )
        if before_participant is None or after_participant is None:
            return

        if before_participant.decision == after_participant.decision:
            return

        decision_at = after_participant.decision_received_at or now
        self._enqueue_group_status_update(
            meeting=after,
            text=self._pending_status_text(after),
            status_tag="pending_progress",
            status_revision=(
                f"u{actor_user_id}:{after_participant.decision}:"
                f"{decision_at.isoformat(timespec='microseconds')}"
            ),
            now=now,
        )

    def _enqueue_initiator_decision_notifications(
        self,
        *,
        after: Meeting,
        now: datetime,
    ) -> None:
        meeting_context = self._meeting_context_text(after)
        required_participants = tuple(
            participant for participant in after.participants if participant.is_required
        )
        confirmed_count = sum(
            1
            for participant in required_participants
            if participant.decision == Decision.CONFIRM
        )
        confirmed = tuple(
            participant.telegram_user_id
            for participant in required_participants
            if participant.decision == Decision.CONFIRM
        )
        cancelled_count = sum(
            1
            for participant in required_participants
            if participant.decision == Decision.CANCEL
        )
        cancelled = tuple(
            participant.telegram_user_id
            for participant in required_participants
            if participant.decision == Decision.CANCEL
        )
        undecided = tuple(
            participant.telegram_user_id
            for participant in required_participants
            if participant.decision == Decision.NONE
        )
        confirmed_labels = self._format_user_labels(confirmed)
        cancelled_labels = self._format_user_labels(cancelled)
        undecided_labels = self._format_user_labels(undecided)

        replan_button, cancel_button, proceed_button = (
            self._callback_tokens.build_initiator_decision_buttons(
                meeting=after,
                now=now,
            )
        )
        initiator_text = (
            "⚠️ Нужно ваше решение по встрече.\n"
            f"{meeting_context}\n"
            f"Подтвердили: {confirmed_count} ({confirmed_labels}), "
            f"отказались: {cancelled_count} ({cancelled_labels}), "
            f"без ответа: {undecided_labels}"
        )

        if after.chat_id != after.initiator_telegram_user_id:
            self._enqueue_group_status_update(
                meeting=after,
                text=(
                    "⚠️ Ждем финальное решение инициатора по встрече.\n"
                    f"{meeting_context}\n"
                    "Инициатору в личные сообщения отправлены кнопки: провести, перенести или отменить."
                ),
                status_tag="needs_initiator_decision",
                now=now,
            )

        _ = self._repository.enqueue_outbox(
            effect_type=OutboxEffectType.TELEGRAM_SEND_MESSAGE,
            payload={
                "telegram_user_id": after.initiator_telegram_user_id,
                "text": initiator_text,
                "buttons": [
                    {
                        "text": replan_button.text,
                        "callback_data": replan_button.callback_data,
                    },
                    {
                        "text": cancel_button.text,
                        "callback_data": cancel_button.callback_data,
                    },
                    {
                        "text": proceed_button.text,
                        "callback_data": proceed_button.callback_data,
                    },
                ],
            },
            idempotency_key=(
                f"notify:{after.meeting_id}:r{after.confirmation_round}:"
                f"needs_initiator_decision:initiator:{after.initiator_telegram_user_id}"
            ),
            now=now,
        )

        manager_ids = self._repository.list_active_manager_ids()
        for manager_id in manager_ids:
            if manager_id == after.initiator_telegram_user_id:
                continue

            manager_replan, manager_cancel, manager_proceed = (
                self._callback_tokens.build_initiator_decision_buttons(
                    meeting=after,
                    now=now,
                    allowed_user_id=manager_id,
                )
            )
            _ = self._repository.enqueue_outbox(
                effect_type=OutboxEffectType.TELEGRAM_SEND_MESSAGE,
                payload={
                    "telegram_user_id": manager_id,
                    "text": (
                        "⚠️ Требуется решение менеджера по встрече.\n"
                        f"{meeting_context}\n"
                        f"Подтвердили: {confirmed_count} ({confirmed_labels}), "
                        f"отказались: {cancelled_count} ({cancelled_labels}), "
                        f"без ответа: {undecided_labels}"
                    ),
                    "buttons": [
                        {
                            "text": manager_replan.text,
                            "callback_data": manager_replan.callback_data,
                        },
                        {
                            "text": manager_cancel.text,
                            "callback_data": manager_cancel.callback_data,
                        },
                        {
                            "text": manager_proceed.text,
                            "callback_data": manager_proceed.callback_data,
                        },
                    ],
                },
                idempotency_key=(
                    f"notify:{after.meeting_id}:r{after.confirmation_round}:"
                    f"needs_initiator_decision:manager:{manager_id}"
                ),
                now=now,
            )

    def _meeting_context_text(self, meeting: Meeting) -> str:
        timezone_name = self._timezone_for_user(meeting.initiator_telegram_user_id)
        slot = format_local_range(
            meeting.scheduled_start_at,
            meeting.scheduled_end_at,
            timezone_name=timezone_name,
        )
        title = meeting_title_or_default(meeting.title)
        return f"«{title}»\nКогда: {slot}"

    def _response_deadline_text(self, meeting: Meeting) -> str | None:
        deadline_at = meeting.confirmation_deadline_at
        if deadline_at is None:
            return None
        timezone_name = self._timezone_for_user(meeting.initiator_telegram_user_id)
        return format_local_datetime(
            deadline_at,
            timezone_name=timezone_name,
        )

    def _pending_status_text(self, meeting: Meeting) -> str:
        meeting_context = self._meeting_context_text(meeting)
        required_participants = tuple(
            participant
            for participant in meeting.participants
            if participant.is_required
            and participant.telegram_user_id != meeting.initiator_telegram_user_id
        )
        if not required_participants:
            return (
                "📣 Встреча создана без дополнительных подтверждений участников.\n"
                f"{meeting_context}\n"
                "Ожидаем ответы: нет (встреча только с инициатором)."
            )

        confirmed = tuple(
            participant.telegram_user_id
            for participant in required_participants
            if participant.decision == Decision.CONFIRM
        )
        not_confirmed = tuple(
            participant.telegram_user_id
            for participant in required_participants
            if participant.decision != Decision.CONFIRM
        )
        undecided = tuple(
            participant.telegram_user_id
            for participant in required_participants
            if participant.decision == Decision.NONE
        )
        confirmed_labels = self._format_user_labels(confirmed)
        not_confirmed_labels = self._format_user_labels(not_confirmed)
        undecided_labels = self._format_user_labels(undecided)
        return (
            "📣 Отправили запрос участникам в личные сообщения.\n"
            f"{meeting_context}\n"
            f"Подтвердили: {len(confirmed)} ({confirmed_labels})\n"
            f"Не подтвердили: {len(not_confirmed)} ({not_confirmed_labels})\n"
            f"Ожидаем ответы: {undecided_labels}"
        )

    def _initiator_pending_status_text(self, meeting: Meeting) -> str:
        meeting_context = self._meeting_context_text(meeting)
        deadline_text = self._response_deadline_text(meeting)
        deadline_line = (
            f"Ответы участников до: {deadline_text}\n"
            if isinstance(deadline_text, str)
            else ""
        )
        return (
            "📌 Назначена встреча.\n"
            f"{meeting_context}\n"
            f"{deadline_line}"
            "Статус по участникам отправляем в рабочий чат."
        )

    def _timezone_for_user(self, telegram_user_id: int) -> str:
        mapping = self._repository.get_user_mapping(telegram_user_id)
        if mapping is None:
            return "UTC"
        timezone_obj = mapping.get("timezone")
        if not isinstance(timezone_obj, str):
            return "UTC"
        return normalize_timezone_name(timezone_obj)

    def _format_user_labels(self, telegram_user_ids: tuple[int, ...]) -> str:
        if not telegram_user_ids:
            return "нет"

        labels: list[str] = []
        for user_id in telegram_user_ids:
            mapping = self._repository.get_user_mapping(user_id)
            if mapping is None:
                labels.append(str(user_id))
                continue

            username_obj = mapping.get("telegram_username")
            if isinstance(username_obj, str) and username_obj.strip():
                labels.append(f"@{username_obj.strip()}")
            else:
                labels.append(str(user_id))

        return ", ".join(labels)

    def _enqueue_calendar_sync(
        self,
        *,
        before: Meeting,
        after: Meeting,
        action: str,
        actor_user_id: int | None,
        now: datetime,
    ) -> None:
        if action.startswith("calendar_ingest_"):
            return

        if not after.created_by_bot:
            return

        if action == "select_slot":
            initiator_mapping = self._repository.get_user_mapping(
                after.initiator_telegram_user_id
            )
            if not initiator_mapping:
                return

            initiator_email = initiator_mapping.get("google_email")
            if not isinstance(initiator_email, str) or not initiator_email:
                return

            attendees: list[dict[str, str]] = []
            for p in after.participants:
                mapping = self._repository.get_user_mapping(p.telegram_user_id)
                if mapping:
                    email = mapping.get("google_email")
                    if isinstance(email, str) and email:
                        attendee: dict[str, str] = {
                            "email": email,
                            "responseStatus": "needsAction",
                        }
                        attendees.append(attendee)

            payload: dict[str, object] = {
                "summary": f"⏳ {after.title or 'Встреча'}",
                "description": f"Встреча ожидает подтверждения в Telegram.\nID: {after.meeting_id}",
                "start": {"dateTime": after.scheduled_start_at.isoformat()},
                "end": {"dateTime": after.scheduled_end_at.isoformat()},
                "transparency": "transparent",
                "attendees": attendees,
            }

            _ = self._repository.enqueue_outbox(
                effect_type=OutboxEffectType.CALENDAR_INSERT_EVENT,
                payload={
                    "organizer_email": initiator_email,
                    "payload": payload,
                    "meeting_id": after.meeting_id,
                },
                idempotency_key=f"cal_insert:{after.meeting_id}:r{after.confirmation_round}",
                now=now,
            )
            return

        if after.google_event_id and before.state != after.state:
            initiator_mapping = self._repository.get_user_mapping(
                after.initiator_telegram_user_id
            )
            if not initiator_mapping:
                return
            initiator_email = initiator_mapping.get("google_email")
            if not isinstance(initiator_email, str) or not initiator_email:
                return

            patch_payload: dict[str, object] = {}
            final_group_status_text: str | None = None
            if after.state == MeetingState.CONFIRMED:
                patch_payload["summary"] = f"✅ {after.title or 'Встреча'}"
                patch_payload["description"] = (
                    f"Встреча подтверждена в Telegram.\nID: {after.meeting_id}"
                )
                patch_payload["status"] = "confirmed"
                patch_payload["transparency"] = "opaque"
                final_group_status_text = (
                    f"✅ Встреча подтверждена.\n{self._meeting_context_text(after)}"
                )
            elif after.state == MeetingState.CANCELLED:
                patch_payload["summary"] = f"❌ [ОТМЕНЕНА] {after.title or 'Встреча'}"
                patch_payload["description"] = (
                    f"Встреча отменена в Telegram.\nID: {after.meeting_id}"
                )
                patch_payload["status"] = "cancelled"
                patch_payload["transparency"] = "transparent"
                final_group_status_text = (
                    f"❌ Встреча отменена.\n{self._meeting_context_text(after)}"
                )
            elif after.state == MeetingState.EXPIRED:
                patch_payload["summary"] = f"⌛ [ПРОСРОЧЕНА] {after.title or 'Встреча'}"
                patch_payload["description"] = (
                    f"Срок подтверждения в Telegram истек.\nID: {after.meeting_id}"
                )
                patch_payload["transparency"] = "transparent"
                final_group_status_text = (
                    f"⌛ Встреча просрочена.\n{self._meeting_context_text(after)}"
                )

            if patch_payload:
                patch_outbox_payload: dict[str, object] = {
                    "google_event_id": after.google_event_id,
                    "initiator_google_email": initiator_email,
                    "payload": patch_payload,
                }
                if isinstance(final_group_status_text, str):
                    patch_outbox_payload["_post_patch_group_status"] = {
                        "meeting_id": after.meeting_id,
                        "round": after.confirmation_round,
                        "target_state": after.state,
                        "chat_id": after.chat_id,
                        "initiator_user_id": after.initiator_telegram_user_id,
                        "text": final_group_status_text,
                    }
                _ = self._repository.enqueue_outbox(
                    effect_type=OutboxEffectType.CALENDAR_PATCH_EVENT,
                    payload=patch_outbox_payload,
                    idempotency_key=f"cal_patch:{after.meeting_id}:r{after.confirmation_round}:{after.state}",
                    now=now,
                )

        if (
            action != "record_participant_decision"
            or actor_user_id is None
            or not after.google_event_id
            or before.confirmation_round != after.confirmation_round
        ):
            return

        before_participant = next(
            (
                participant
                for participant in before.participants
                if participant.telegram_user_id == actor_user_id
            ),
            None,
        )
        after_participant = next(
            (
                participant
                for participant in after.participants
                if participant.telegram_user_id == actor_user_id
            ),
            None,
        )
        if before_participant is None or after_participant is None:
            return

        if before_participant.decision == after_participant.decision:
            return

        actor_mapping = self._repository.get_user_mapping(actor_user_id)
        if not actor_mapping:
            return

        actor_email_obj = actor_mapping.get("google_email")
        if not isinstance(actor_email_obj, str) or not actor_email_obj:
            return

        initiator_mapping = self._repository.get_user_mapping(
            after.initiator_telegram_user_id
        )
        if not initiator_mapping:
            return

        initiator_email_obj = initiator_mapping.get("google_email")
        if not isinstance(initiator_email_obj, str) or not initiator_email_obj:
            return

        response_status = (
            "accepted"
            if after_participant.decision == Decision.CONFIRM
            else "declined"
            if after_participant.decision == Decision.CANCEL
            else "needsAction"
        )
        has_required_confirmation = any(
            participant.is_required and participant.decision == Decision.CONFIRM
            for participant in after.participants
        )
        transparency = "opaque" if has_required_confirmation else "transparent"
        decision_at = after_participant.decision_received_at or now
        participant_patch_payload: dict[str, object] = {
            "attendeesOmitted": True,
            "attendees": [
                {"email": actor_email_obj, "responseStatus": response_status},
            ],
            "transparency": transparency,
            "_send_updates": "none",
        }

        _ = self._repository.enqueue_outbox(
            effect_type=OutboxEffectType.CALENDAR_PATCH_EVENT,
            payload={
                "google_event_id": after.google_event_id,
                "initiator_google_email": initiator_email_obj,
                "payload": participant_patch_payload,
            },
            idempotency_key=(
                f"cal_patch:{after.meeting_id}:r{after.confirmation_round}:"
                f"participant:{actor_user_id}:{response_status}:"
                f"{decision_at.isoformat(timespec='seconds')}"
            ),
            now=now,
        )

    def _write_audit_log(
        self,
        *,
        before: Meeting,
        execution: CommandExecution,
        action: str,
        actor_user_id: int | None,
        requested_by_user_id: int | None,
        now: datetime,
    ) -> None:
        actor_for_audit = requested_by_user_id or actor_user_id
        details: dict[str, object] = {
            "outcome": execution.result.outcome,
            "reason_code": execution.result.reason_code,
            "state_before": before.state,
            "state_after": execution.meeting.state,
        }
        if requested_by_user_id is not None:
            details["effective_actor_user_id"] = actor_user_id
            details["requested_by_user_id"] = requested_by_user_id
            details["delegated"] = requested_by_user_id != actor_user_id
        self._repository.insert_audit_log(
            meeting_id=execution.meeting.meeting_id,
            round=execution.meeting.confirmation_round,
            actor_telegram_user_id=actor_for_audit,
            actor_type="user" if actor_for_audit is not None else "system",
            action=action,
            details=details,
            now=now,
        )

    def _require_meeting(self, meeting_id: str) -> Meeting:
        meeting = self._repository.get_meeting(meeting_id)
        if meeting is None:
            raise LookupError(f"Meeting not found: {meeting_id}")
        return meeting
