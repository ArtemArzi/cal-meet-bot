from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
import logging
import uuid
from typing import Protocol, cast

from bot_vstrechi.application.service import MeetingWorkflowService
from bot_vstrechi.calendar.gateway import (
    CalendarOccurrenceIdentity,
    GoogleCalendarGateway,
)
from bot_vstrechi.db.repository import ClaimedCalendarSyncSignal, SQLiteRepository
from bot_vstrechi.domain.commands import CommandExecution
from bot_vstrechi.domain.models import (
    CommandResult,
    Decision,
    Meeting,
    MeetingParticipant,
    MeetingState,
    OutboxStatus,
    Outcome,
    RecurringConfirmationMode,
    ReasonCode,
)


logger = logging.getLogger(__name__)
AUTO_CREATE_LOOKAHEAD = timedelta(days=2)


@dataclass(frozen=True)
class CalendarSyncTickResult:
    processed: bool
    signal_id: int | None = None
    status: OutboxStatus | None = None


class CalendarSyncProcessor(Protocol):
    def process_signal(
        self, *, signal: ClaimedCalendarSyncSignal, now: datetime
    ) -> None: ...


class _NoopCalendarSyncProcessor:
    def process_signal(
        self, *, signal: ClaimedCalendarSyncSignal, now: datetime
    ) -> None:
        del signal, now


class DefaultCalendarSyncProcessor:
    def __init__(
        self,
        *,
        repository: SQLiteRepository,
        workflow_service: MeetingWorkflowService | None = None,
        calendar_gateway: GoogleCalendarGateway | None = None,
        calendar_client: object | None = None,
        recurring_exceptions_only_enabled: bool = False,
    ) -> None:
        self._repository: SQLiteRepository = repository
        self._workflow_service: MeetingWorkflowService | None = workflow_service
        self._calendar_gateway: GoogleCalendarGateway | None = calendar_gateway
        self._calendar_client: object | None = calendar_client
        self._recurring_exceptions_only_enabled = recurring_exceptions_only_enabled

    def process_signal(
        self, *, signal: ClaimedCalendarSyncSignal, now: datetime
    ) -> None:
        state = self._repository.get_calendar_sync_state(calendar_id=signal.calendar_id)
        current_message_number: int | None = None
        current_sync_token: str | None = None
        current_watch_channel_id: str | None = None
        current_watch_resource_id: str | None = None
        current_watch_expiration_at: datetime | None = None
        if state is not None:
            message_number_obj = state.get("last_message_number")
            if isinstance(message_number_obj, int):
                current_message_number = message_number_obj
            sync_token_obj = state.get("sync_token")
            if isinstance(sync_token_obj, str):
                current_sync_token = sync_token_obj
            watch_channel_obj = state.get("watch_channel_id")
            if isinstance(watch_channel_obj, str) and watch_channel_obj.strip():
                current_watch_channel_id = watch_channel_obj.strip()
            watch_resource_obj = state.get("watch_resource_id")
            if isinstance(watch_resource_obj, str) and watch_resource_obj.strip():
                current_watch_resource_id = watch_resource_obj.strip()
            watch_expiration_obj = state.get("watch_expiration_at")
            if isinstance(watch_expiration_obj, str) and watch_expiration_obj.strip():
                try:
                    parsed_watch_expiration = datetime.fromisoformat(
                        watch_expiration_obj.replace("Z", "+00:00")
                    )
                    current_watch_expiration_at = (
                        parsed_watch_expiration
                        if parsed_watch_expiration.tzinfo is not None
                        else parsed_watch_expiration.replace(tzinfo=timezone.utc)
                    )
                except ValueError:
                    current_watch_expiration_at = None

        if (
            signal.message_number is not None
            and current_message_number is not None
            and signal.message_number <= current_message_number
        ):
            return

        next_sync_token = current_sync_token
        if signal.resource_state == "sync_token_invalid":
            next_sync_token = None

        last_message_number = signal.message_number
        if last_message_number is None:
            last_message_number = current_message_number

        state_persisted = False

        if (
            self._workflow_service is not None
            and self._calendar_gateway is not None
            and self._calendar_client is not None
        ):
            state_persisted = self._reconcile_calendar_events(
                calendar_id=signal.calendar_id,
                current_sync_token=next_sync_token,
                resource_state=signal.resource_state,
                current_watch_channel_id=current_watch_channel_id,
                current_watch_resource_id=current_watch_resource_id,
                current_watch_expiration_at=current_watch_expiration_at,
                now=now,
                signal_message_number=last_message_number,
            )

        if not state_persisted:
            self._repository.upsert_calendar_sync_state(
                calendar_id=signal.calendar_id,
                sync_token=next_sync_token,
                watch_channel_id=current_watch_channel_id,
                watch_resource_id=current_watch_resource_id,
                watch_expiration_at=current_watch_expiration_at,
                last_message_number=last_message_number,
                now=now,
            )

        self._repository.insert_audit_log(
            meeting_id=None,
            round=None,
            actor_telegram_user_id=None,
            actor_type="system",
            action="calendar_sync_signal_processed",
            details={
                "calendar_id": signal.calendar_id,
                "external_event_id": signal.external_event_id,
                "resource_state": signal.resource_state,
                "message_number": signal.message_number,
            },
            now=now,
        )

    def _reconcile_calendar_events(
        self,
        *,
        calendar_id: str,
        current_sync_token: str | None,
        resource_state: str,
        current_watch_channel_id: str | None,
        current_watch_resource_id: str | None,
        current_watch_expiration_at: datetime | None,
        now: datetime,
        signal_message_number: int | None,
    ) -> bool:
        sync_token = current_sync_token
        calendar_email = calendar_id.strip().lower()
        if not calendar_email or calendar_email == "primary":
            return False

        list_deltas = getattr(self._calendar_client, "list_event_deltas", None)
        if not callable(list_deltas):
            return False

        window_start = now - timedelta(days=365)
        page_token: str | None = None
        should_apply_items = (
            sync_token is not None and resource_state != "sync_token_invalid"
        )

        while True:
            page_obj = list_deltas(
                email=calendar_email,
                sync_token=sync_token,
                page_token=page_token,
                time_min=window_start,
            )

            full_sync_required = bool(getattr(page_obj, "full_sync_required", False))
            if full_sync_required and sync_token is not None:
                sync_token = None
                page_token = None
                continue

            items_obj = getattr(page_obj, "items", [])
            next_page_obj = getattr(page_obj, "next_page_token", None)
            next_sync_obj = getattr(page_obj, "next_sync_token", None)

            if should_apply_items and isinstance(items_obj, list):
                for event_obj in items_obj:
                    if not isinstance(event_obj, dict):
                        continue
                    self._reconcile_single_event(
                        calendar_email=calendar_email,
                        event=cast(dict[str, object], event_obj),
                        now=now,
                    )

            page_token = next_page_obj if isinstance(next_page_obj, str) else None
            if page_token:
                continue

            if isinstance(next_sync_obj, str) and next_sync_obj.strip():
                sync_token = next_sync_obj.strip()

            self._repository.upsert_calendar_sync_state(
                calendar_id=calendar_email,
                sync_token=sync_token,
                watch_channel_id=current_watch_channel_id,
                watch_resource_id=current_watch_resource_id,
                watch_expiration_at=current_watch_expiration_at,
                last_message_number=signal_message_number,
                now=now,
            )
            return True

    def _reconcile_single_event(
        self,
        *,
        calendar_email: str,
        event: dict[str, object],
        now: datetime,
    ) -> None:
        gateway = self._calendar_gateway
        workflow_service = self._workflow_service
        if gateway is None or workflow_service is None:
            return

        identity = gateway.get_occurrence_identity(event=event)
        meeting = self._find_meeting_for_event(identity=identity)
        canonical_event_id = self._extract_google_event_id(event=event)

        status_obj = event.get("status")
        status = (
            status_obj.strip().lower() if isinstance(status_obj, str) else "confirmed"
        )

        if (
            meeting is not None
            and isinstance(canonical_event_id, str)
            and canonical_event_id != meeting.google_event_id
        ):
            _ = self._repository.apply_execution(
                before=meeting,
                execution=CommandExecution(
                    result=CommandResult(Outcome.OK, ReasonCode.UPDATED),
                    meeting=replace(
                        meeting,
                        google_event_id=canonical_event_id,
                        google_calendar_id=calendar_email,
                    ),
                ),
                now=now,
            )
            refreshed_meeting = self._repository.get_meeting(meeting.meeting_id)
            if refreshed_meeting is not None:
                meeting = refreshed_meeting

        organizer_email = self._extract_organizer_email(event=event) or calendar_email
        initiator_mapping = self._repository.get_user_mapping_by_email(organizer_email)
        if initiator_mapping is None or not bool(initiator_mapping.get("is_active")):
            return

        initiator_user_id_obj = initiator_mapping.get("telegram_user_id")
        if not isinstance(initiator_user_id_obj, int):
            return
        initiator_user_id = initiator_user_id_obj

        if status == "cancelled":
            if meeting is None:
                return
            if meeting.state in {MeetingState.CANCELLED, MeetingState.EXPIRED}:
                return
            _ = workflow_service.cancel_from_calendar(
                meeting_id=meeting.meeting_id,
                actor_user_id=initiator_user_id,
                now=now,
            )
            return

        slot = self._extract_event_slot(event=event)
        if slot is None:
            return
        start_at, end_at = slot
        if end_at <= now:
            return

        summary_obj = event.get("summary")
        title = summary_obj.strip() if isinstance(summary_obj, str) else ""

        required_participant_user_ids = (
            self._resolve_required_participant_user_ids_from_event(
                event=event,
                initiator_user_id=initiator_user_id,
            )
        )

        if meeting is None:
            if not self._repository.is_manager(telegram_user_id=initiator_user_id):
                return
            if start_at > now + AUTO_CREATE_LOOKAHEAD:
                return
            if (
                identity.series_event_id
                and self._repository.has_open_meeting_for_series(
                    series_event_id=identity.series_event_id,
                    now=now,
                )
            ):
                return

            created_meeting = self._create_meeting_from_calendar_event(
                event=event,
                initiator_user_id=initiator_user_id,
                chat_id=self._resolve_chat_id_for_initiator(
                    initiator_user_id=initiator_user_id
                ),
                title=title,
                start_at=start_at,
                end_at=end_at,
                identity=identity,
                calendar_email=calendar_email,
                now=now,
            )
            if created_meeting is None:
                return
            meeting = created_meeting
            _ = workflow_service.select_slot_from_calendar(
                meeting_id=meeting.meeting_id,
                actor_user_id=initiator_user_id,
                chat_id=meeting.chat_id,
                scheduled_start_at=start_at,
                scheduled_end_at=end_at,
                now=now,
            )
        else:
            if self._time_changed(meeting=meeting, start_at=start_at, end_at=end_at):
                _ = workflow_service.reschedule_from_calendar(
                    meeting_id=meeting.meeting_id,
                    actor_user_id=initiator_user_id,
                    chat_id=meeting.chat_id,
                    scheduled_start_at=start_at,
                    scheduled_end_at=end_at,
                    now=now,
                    force_pending=(
                        meeting.recurring_confirmation_mode
                        == RecurringConfirmationMode.EXCEPTIONS_ONLY
                    ),
                )
                refreshed = self._repository.get_meeting(meeting.meeting_id)
                if refreshed is None:
                    return
                should_sync_after_reschedule = refreshed.state in {
                    MeetingState.PENDING,
                    MeetingState.NEEDS_INITIATOR_DECISION,
                } or (
                    refreshed.state == MeetingState.CONFIRMED
                    and refreshed.recurring_confirmation_mode
                    == RecurringConfirmationMode.EXCEPTIONS_ONLY
                )
                if should_sync_after_reschedule:
                    _ = workflow_service.sync_participants_from_calendar(
                        meeting_id=refreshed.meeting_id,
                        actor_user_id=initiator_user_id,
                        required_participant_user_ids=required_participant_user_ids,
                        now=now,
                    )
                return
            elif title and title != meeting.title:
                _ = self._repository.apply_execution(
                    before=meeting,
                    execution=CommandExecution(
                        result=CommandResult(Outcome.OK, ReasonCode.UPDATED),
                        meeting=replace(meeting, title=title),
                    ),
                    now=now,
                )

        refreshed = self._repository.get_meeting(meeting.meeting_id)
        if refreshed is None:
            return

        should_sync_participants = refreshed.state in {
            MeetingState.PENDING,
            MeetingState.NEEDS_INITIATOR_DECISION,
        } or (
            refreshed.state == MeetingState.CONFIRMED
            and refreshed.recurring_confirmation_mode
            == RecurringConfirmationMode.EXCEPTIONS_ONLY
        )
        if should_sync_participants:
            _ = workflow_service.sync_participants_from_calendar(
                meeting_id=refreshed.meeting_id,
                actor_user_id=initiator_user_id,
                required_participant_user_ids=required_participant_user_ids,
                now=now,
            )
            refreshed_after_sync = self._repository.get_meeting(refreshed.meeting_id)
            if refreshed_after_sync is not None:
                refreshed = refreshed_after_sync

        self._sync_attendee_decisions(
            meeting=refreshed,
            event=event,
            now=now,
        )

    def _find_meeting_for_event(
        self,
        *,
        identity: CalendarOccurrenceIdentity,
    ) -> Meeting | None:
        event_id_obj = identity.event_id
        if isinstance(event_id_obj, str) and event_id_obj.strip():
            direct = self._repository.find_meeting_by_google_event_id(
                google_event_id=event_id_obj,
            )
            if direct is not None:
                return direct

        series_id_obj = identity.series_event_id
        occurrence_start_obj = identity.occurrence_start_at
        if (
            isinstance(series_id_obj, str)
            and series_id_obj.strip()
            and isinstance(occurrence_start_obj, datetime)
        ):
            return self._repository.find_meeting_by_occurrence_identity(
                series_event_id=series_id_obj,
                occurrence_start_at=occurrence_start_obj,
            )
        return None

    def _extract_organizer_email(self, *, event: dict[str, object]) -> str | None:
        organizer_obj = event.get("organizer")
        if not isinstance(organizer_obj, dict):
            return None
        email_obj = organizer_obj.get("email")
        if not isinstance(email_obj, str):
            return None
        normalized = email_obj.strip().lower()
        return normalized or None

    def _resolve_chat_id_for_initiator(self, *, initiator_user_id: int) -> int:
        preferred_chat_id = self._repository.get_preferred_chat_id(
            telegram_user_id=initiator_user_id
        )
        if isinstance(preferred_chat_id, int):
            return preferred_chat_id
        return initiator_user_id

    def _extract_event_slot(
        self,
        *,
        event: dict[str, object],
    ) -> tuple[datetime, datetime] | None:
        start_obj = event.get("start")
        end_obj = event.get("end")
        if not isinstance(start_obj, dict) or not isinstance(end_obj, dict):
            return None

        start_at = self._parse_google_datetime(start_obj)
        end_at = self._parse_google_datetime(end_obj)
        if start_at is None or end_at is None or end_at <= start_at:
            return None
        return start_at, end_at

    def _extract_google_event_id(self, *, event: dict[str, object]) -> str | None:
        event_id_obj = event.get("id")
        if not isinstance(event_id_obj, str):
            return None
        normalized = event_id_obj.strip()
        if not normalized:
            return None
        return normalized

    def _parse_google_datetime(self, payload: dict[str, object]) -> datetime | None:
        date_time_obj = payload.get("dateTime")
        if isinstance(date_time_obj, str) and date_time_obj.strip():
            try:
                parsed = datetime.fromisoformat(date_time_obj.replace("Z", "+00:00"))
            except ValueError:
                return None
            if parsed.tzinfo is None:
                return parsed.replace(tzinfo=timezone.utc)
            return parsed.astimezone(timezone.utc)

        date_obj = payload.get("date")
        if isinstance(date_obj, str) and date_obj.strip():
            try:
                parsed = datetime.fromisoformat(date_obj)
            except ValueError:
                return None
            if parsed.tzinfo is None:
                return parsed.replace(tzinfo=timezone.utc)
            return parsed.astimezone(timezone.utc)

        return None

    def _create_meeting_from_calendar_event(
        self,
        *,
        event: dict[str, object],
        initiator_user_id: int,
        chat_id: int,
        title: str,
        start_at: datetime,
        end_at: datetime,
        identity: CalendarOccurrenceIdentity,
        calendar_email: str,
        now: datetime,
    ) -> Meeting | None:
        participants: dict[int, MeetingParticipant] = {
            initiator_user_id: MeetingParticipant(
                telegram_user_id=initiator_user_id,
                is_required=False,
                decision=Decision.NONE,
            )
        }

        required_participant_user_ids = (
            self._resolve_required_participant_user_ids_from_event(
                event=event,
                initiator_user_id=initiator_user_id,
            )
        )
        for participant_user_id in required_participant_user_ids:
            participants[participant_user_id] = MeetingParticipant(
                telegram_user_id=participant_user_id,
                is_required=True,
                decision=Decision.NONE,
            )

        meeting_id = uuid.uuid4().hex[:16]
        series_event_id = identity.series_event_id
        normalized_series_event_id = (
            series_event_id.strip() if isinstance(series_event_id, str) else None
        )
        if (
            isinstance(normalized_series_event_id, str)
            and not normalized_series_event_id
        ):
            normalized_series_event_id = None

        recurring_confirmation_mode = RecurringConfirmationMode.STRICT
        if self._recurring_exceptions_only_enabled and normalized_series_event_id:
            recurring_confirmation_mode = RecurringConfirmationMode.EXCEPTIONS_ONLY
        meeting = Meeting(
            meeting_id=meeting_id,
            initiator_telegram_user_id=initiator_user_id,
            chat_id=chat_id,
            state=MeetingState.DRAFT,
            scheduled_start_at=start_at,
            scheduled_end_at=end_at,
            title=title,
            google_event_id=identity.event_id,
            google_calendar_id=calendar_email,
            series_event_id=normalized_series_event_id,
            occurrence_start_at=identity.occurrence_start_at,
            created_by_bot=True,
            participants=tuple(participants.values()),
            recurring_confirmation_mode=recurring_confirmation_mode,
        )
        workflow_service = self._workflow_service
        if workflow_service is None:
            return None
        workflow_service.create_meeting(meeting, now=now)
        return meeting

    def _resolve_required_participant_user_ids_from_event(
        self,
        *,
        event: dict[str, object],
        initiator_user_id: int,
    ) -> tuple[int, ...]:
        required_participant_user_ids: set[int] = set()
        attendees_obj = event.get("attendees")
        if not isinstance(attendees_obj, list):
            return ()

        for attendee_obj in attendees_obj:
            if not isinstance(attendee_obj, dict):
                continue

            email_obj = attendee_obj.get("email")
            if not isinstance(email_obj, str) or not email_obj.strip():
                continue

            mapping = self._repository.get_user_mapping_by_email(email_obj)
            if mapping is None or not bool(mapping.get("is_active")):
                continue

            user_id_obj = mapping.get("telegram_user_id")
            if not isinstance(user_id_obj, int):
                continue
            if user_id_obj == initiator_user_id:
                continue

            required_participant_user_ids.add(user_id_obj)

        return tuple(sorted(required_participant_user_ids))

    def _time_changed(
        self,
        *,
        meeting: Meeting,
        start_at: datetime,
        end_at: datetime,
    ) -> bool:
        current_start = (
            meeting.scheduled_start_at.astimezone(timezone.utc)
            if meeting.scheduled_start_at.tzinfo is not None
            else meeting.scheduled_start_at.replace(tzinfo=timezone.utc)
        )
        current_end = (
            meeting.scheduled_end_at.astimezone(timezone.utc)
            if meeting.scheduled_end_at.tzinfo is not None
            else meeting.scheduled_end_at.replace(tzinfo=timezone.utc)
        )
        return current_start != start_at or current_end != end_at

    def _sync_attendee_decisions(
        self,
        *,
        meeting: Meeting,
        event: dict[str, object],
        now: datetime,
    ) -> None:
        attendees_obj = event.get("attendees")
        if not isinstance(attendees_obj, list):
            return

        response_to_decision: dict[str, Decision] = {
            "accepted": Decision.CONFIRM,
            "declined": Decision.CANCEL,
        }
        workflow_service = self._workflow_service
        if workflow_service is None:
            return

        for attendee_obj in attendees_obj:
            if not isinstance(attendee_obj, dict):
                continue
            email_obj = attendee_obj.get("email")
            status_obj = attendee_obj.get("responseStatus")
            if not isinstance(email_obj, str) or not isinstance(status_obj, str):
                continue
            target_decision = response_to_decision.get(status_obj.strip().lower())
            if target_decision is None:
                continue

            mapping = self._repository.get_user_mapping_by_email(email_obj)
            if mapping is None:
                continue
            actor_user_id_obj = mapping.get("telegram_user_id")
            if not isinstance(actor_user_id_obj, int):
                continue

            participant = next(
                (
                    item
                    for item in meeting.participants
                    if item.telegram_user_id == actor_user_id_obj
                ),
                None,
            )
            if (
                participant is None
                or not participant.is_required
                or participant.decision == target_decision
                or meeting.state
                not in {MeetingState.PENDING, MeetingState.NEEDS_INITIATOR_DECISION}
            ):
                continue

            _ = workflow_service.record_participant_decision(
                meeting_id=meeting.meeting_id,
                round=meeting.confirmation_round,
                actor_user_id=actor_user_id_obj,
                decision=target_decision,
                source="google",
                now=now,
            )


class CalendarSyncWorker:
    def __init__(
        self,
        *,
        repository: SQLiteRepository,
        processor: CalendarSyncProcessor | None = None,
        workflow_service: MeetingWorkflowService | None = None,
        calendar_gateway: GoogleCalendarGateway | None = None,
        calendar_client: object | None = None,
        recurring_exceptions_only_enabled: bool = False,
        max_attempts: int = 5,
        retry_backoff_base: timedelta | None = None,
    ) -> None:
        self._repository: SQLiteRepository = repository
        self._processor: CalendarSyncProcessor = (
            processor
            or DefaultCalendarSyncProcessor(
                repository=repository,
                workflow_service=workflow_service,
                calendar_gateway=calendar_gateway,
                calendar_client=calendar_client,
                recurring_exceptions_only_enabled=recurring_exceptions_only_enabled,
            )
        )
        self._max_attempts: int = max_attempts
        self._retry_backoff_base: timedelta = retry_backoff_base or timedelta(seconds=5)

    def reconcile_on_startup(
        self,
        *,
        now: datetime,
        stale_running_after: timedelta | None = None,
    ) -> int:
        stale_after = stale_running_after or timedelta(minutes=5)
        return self._repository.reconcile_stale_running_calendar_sync_signals(
            stale_before=now - stale_after,
            now=now,
        )

    def run_once(self, *, now: datetime) -> CalendarSyncTickResult:
        signal = self._repository.claim_due_calendar_sync_signal(now=now)
        if signal is None:
            return CalendarSyncTickResult(processed=False)

        claim_log_extra = {
            "signal_id": signal.signal_id,
            "calendar_id": signal.calendar_id,
            "external_event_id": signal.external_event_id,
        }
        if signal.resource_state == "poll":
            logger.debug("calendar sync signal claimed", extra=claim_log_extra)
        else:
            logger.info("calendar sync signal claimed", extra=claim_log_extra)

        try:
            self._processor.process_signal(signal=signal, now=now)
            self._repository.mark_calendar_sync_signal_done(
                signal_id=signal.signal_id,
                now=now,
            )
            return CalendarSyncTickResult(
                processed=True,
                signal_id=signal.signal_id,
                status=OutboxStatus.DONE,
            )
        except Exception as error:
            if signal.attempts < self._max_attempts:
                retry_after = now + self._backoff_for_attempt(signal.attempts)
                self._repository.mark_calendar_sync_signal_retry(
                    signal_id=signal.signal_id,
                    run_after=retry_after,
                    error=str(error),
                    now=now,
                )
                return CalendarSyncTickResult(
                    processed=True,
                    signal_id=signal.signal_id,
                    status=OutboxStatus.PENDING,
                )

            self._repository.mark_calendar_sync_signal_failed(
                signal_id=signal.signal_id,
                error=str(error),
                now=now,
            )
            return CalendarSyncTickResult(
                processed=True,
                signal_id=signal.signal_id,
                status=OutboxStatus.FAILED,
            )

    def _backoff_for_attempt(self, attempt: int) -> timedelta:
        exponent = attempt - 1
        if exponent < 0:
            exponent = 0
        factor = 1 << exponent
        return self._retry_backoff_base * factor
