from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
import logging

from bot_vstrechi.db.repository import SQLiteRepository
from bot_vstrechi.domain.commands import CommandExecution
from bot_vstrechi.domain.models import (
    CommandResult,
    Decision,
    JobType,
    MeetingState,
    OutboxEffectType,
    Outcome,
    ReasonCode,
    ScheduledJobSpec,
)
from bot_vstrechi.domain.policies import DEADLINE_GRACE_WINDOW, REMINDER_INTERVAL
from bot_vstrechi.application.service import MeetingWorkflowService
from bot_vstrechi.telegram.presentation import (
    format_local_datetime,
    format_local_range,
    meeting_title_or_default,
    normalize_timezone_name,
)


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class WorkerTickResult:
    processed: bool
    job_id: int | None = None
    outcome: Outcome | None = None
    reason_code: ReasonCode | None = None


class SchedulerWorker:
    def __init__(
        self, repository: SQLiteRepository, service: MeetingWorkflowService
    ) -> None:
        self._repository: SQLiteRepository = repository
        self._service: MeetingWorkflowService = service

    def reconcile_on_startup(
        self,
        *,
        now: datetime,
        stale_running_after: timedelta | None = None,
    ) -> int:
        if stale_running_after is None:
            stale_running_after = timedelta(minutes=5)
        return self._repository.reconcile_stale_running_jobs(
            stale_before=now - stale_running_after
        )

    def run_once(self, *, now: datetime) -> WorkerTickResult:
        job = self._repository.claim_due_job(now=now)
        if job is None:
            return WorkerTickResult(processed=False)

        logger.info(
            "job claimed",
            extra={
                "job_id": job.job_id,
                "job_type": job.job_type,
                "meeting_id": job.meeting_id,
            },
        )

        try:
            execution = self._dispatch(
                job_type=job.job_type,
                meeting_id=job.meeting_id,
                round=job.round,
                run_at=job.run_at,
                now=now,
            )
            self._repository.mark_job_done(job_id=job.job_id)
            logger.info(
                "job dispatched",
                extra={
                    "job_id": job.job_id,
                    "outcome": execution.result.outcome,
                    "reason_code": execution.result.reason_code,
                },
            )
            return WorkerTickResult(
                processed=True,
                job_id=job.job_id,
                outcome=execution.result.outcome,
                reason_code=execution.result.reason_code,
            )
        except Exception as error:
            self._repository.mark_job_failed(job_id=job.job_id, error=str(error))
            logger.error(
                "job failed",
                extra={
                    "job_id": job.job_id,
                    "meeting_id": job.meeting_id,
                    "error": str(error),
                },
            )
            return WorkerTickResult(
                processed=True,
                job_id=job.job_id,
                outcome=Outcome.REJECTED,
                reason_code=ReasonCode.INVALID_STATE,
            )

    def _dispatch(
        self,
        *,
        job_type: JobType | str,
        meeting_id: str,
        round: int,
        run_at: datetime,
        now: datetime,
    ) -> CommandExecution:
        if job_type == JobType.CONFIRM_DEADLINE:
            execution = self._service.handle_confirm_deadline(
                meeting_id=meeting_id,
                round=round,
                now=now,
            )
            if (
                execution.result.outcome == Outcome.NOOP
                and execution.result.reason_code == ReasonCode.TOO_EARLY
                and execution.meeting.confirmation_deadline_at is not None
            ):
                self._repository.enqueue_jobs(
                    (
                        ScheduledJobSpec(
                            job_type=JobType.CONFIRM_DEADLINE,
                            meeting_id=meeting_id,
                            round=round,
                            run_at=(
                                execution.meeting.confirmation_deadline_at
                                + DEADLINE_GRACE_WINDOW
                            ),
                        ),
                    ),
                    now=now,
                )
            return execution

        if job_type == JobType.INITIATOR_TIMEOUT:
            return self._service.handle_initiator_timeout(
                meeting_id=meeting_id,
                round=round,
                now=now,
            )

        if job_type == JobType.REMINDER:
            meeting = self._service.get_meeting(meeting_id)
            if meeting is None:
                raise LookupError(f"Meeting not found: {meeting_id}")

            if meeting.state != MeetingState.PENDING:
                return CommandExecution(
                    result=CommandResult(Outcome.OK, ReasonCode.UPDATED),
                    meeting=meeting,
                    jobs=(),
                )

            undecided = tuple(
                participant.telegram_user_id
                for participant in meeting.participants
                if participant.is_required and participant.decision == Decision.NONE
            )

            if undecided:
                initiator_mapping = self._repository.get_user_mapping(
                    meeting.initiator_telegram_user_id
                )
                timezone_name = "UTC"
                if initiator_mapping is not None:
                    timezone_obj = initiator_mapping.get("timezone")
                    if isinstance(timezone_obj, str):
                        timezone_name = normalize_timezone_name(timezone_obj)

                slot = format_local_range(
                    meeting.scheduled_start_at,
                    meeting.scheduled_end_at,
                    timezone_name=timezone_name,
                )
                title = meeting_title_or_default(meeting.title)
                deadline_line = ""
                if meeting.confirmation_deadline_at is not None:
                    deadline = format_local_datetime(
                        meeting.confirmation_deadline_at,
                        timezone_name=timezone_name,
                    )
                    deadline_line = f"Ответы нужны до: {deadline}\n"

                for participant_user_id in undecided:
                    _ = self._repository.enqueue_outbox(
                        effect_type=OutboxEffectType.TELEGRAM_SEND_MESSAGE,
                        payload={
                            "telegram_user_id": participant_user_id,
                            "text": (
                                "⏰ Напоминание: подтвердите участие.\n"
                                f"«{title}»\n"
                                f"Когда: {slot}\n"
                                f"{deadline_line}"
                                "Ответьте в личном сообщении бота."
                            ),
                        },
                        idempotency_key=(
                            f"remind_dm:{meeting_id}:r{round}:"
                            f"run_at:{run_at.isoformat(timespec='seconds')}:"
                            f"u{participant_user_id}"
                        ),
                        now=now,
                    )

            if undecided:
                self._repository.enqueue_jobs(
                    (
                        ScheduledJobSpec(
                            job_type=JobType.REMINDER,
                            meeting_id=meeting_id,
                            round=round,
                            run_at=run_at + REMINDER_INTERVAL,
                        ),
                    ),
                    now=now,
                )

            return CommandExecution(
                result=CommandResult(Outcome.OK, ReasonCode.UPDATED),
                meeting=meeting,
                jobs=(),
            )

        raise ValueError(f"Unsupported job type: {job_type}")

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
