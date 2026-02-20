from __future__ import annotations

import asyncio
from collections.abc import Callable, Mapping
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import logging
from typing import cast, override

from fastapi import FastAPI
from bot_vstrechi.infrastructure.logging import configure_logging

from bot_vstrechi.api.webhook import create_webhook_app
from bot_vstrechi.calendar.gateway import CalendarApiClient, GoogleCalendarGateway
from bot_vstrechi.db.repository import SQLiteRepository
from bot_vstrechi.application.service import MeetingWorkflowService
from bot_vstrechi.domain.models import InboundEventSource
from bot_vstrechi.telegram.adapter import TelegramWebhookAdapter
from bot_vstrechi.workers.outbox import (
    OutboxDispatcher,
    OutboxTickResult,
    OutboxWorker,
    TelegramApiClient,
)
from bot_vstrechi.workers.calendar_sync import (
    CalendarSyncTickResult,
    CalendarSyncWorker,
)
from bot_vstrechi.workers.scheduler import SchedulerWorker, WorkerTickResult


DEFAULT_STALE_RUNNING_AFTER = timedelta(minutes=5)
DEFAULT_CALENDAR_POLL_INTERVAL_SECONDS = 60.0
DEFAULT_RETENTION_CLEANUP_INTERVAL_SECONDS = 1800.0
DEFAULT_RETENTION_CHECKPOINT_INTERVAL_SECONDS = 21600.0
DEFAULT_RETENTION_VACUUM_INTERVAL_SECONDS = 604800.0
DEFAULT_RETENTION_CALENDAR_SYNC_SIGNAL_DAYS = 5
DEFAULT_RETENTION_OUTBOX_DAYS = 14
DEFAULT_RETENTION_JOB_DAYS = 14
DEFAULT_RETENTION_AUDIT_LOG_DAYS = 30
DEFAULT_RETENTION_INBOUND_EVENT_DAYS = 7


logger = logging.getLogger(__name__)


def _default_now() -> datetime:
    return datetime.now(tz=timezone.utc)


@dataclass(frozen=True)
class RuntimeConfig:
    db_path: str
    stale_running_after: timedelta = DEFAULT_STALE_RUNNING_AFTER
    log_level: str = "INFO"
    log_format: str = "json"
    recurring_exceptions_only_enabled: bool = False


class _NullTelegramClient:
    def send_message(
        self,
        *,
        telegram_user_id: int,
        text: str,
        buttons: list[dict[str, str] | list[dict[str, str]]] | None = None,
        keyboard: list[list[str]] | None = None,
        idempotency_key: str | None = None,
    ) -> int | None:
        del telegram_user_id, text, buttons, keyboard, idempotency_key
        return None

    def edit_message(
        self,
        *,
        telegram_user_id: int,
        message_id: int,
        text: str,
        buttons: list[dict[str, str] | list[dict[str, str]]] | None = None,
        idempotency_key: str | None = None,
    ) -> None:
        del telegram_user_id, message_id, text, buttons, idempotency_key

    def answer_callback_query(
        self,
        *,
        callback_query_id: str,
        text: str | None = None,
        idempotency_key: str | None = None,
    ) -> None:
        del callback_query_id, text, idempotency_key


class _NullCalendarClient(CalendarApiClient):
    @override
    def query_free_busy(
        self,
        *,
        emails: tuple[str, ...],
        time_min: datetime,
        time_max: datetime,
    ) -> dict[str, list[tuple[datetime, datetime]]]:
        del emails, time_min, time_max
        return {}

    @override
    def insert_event(
        self,
        *,
        organizer_email: str,
        payload: dict[str, object],
        idempotency_key: str | None = None,
    ) -> str:
        del organizer_email, payload, idempotency_key
        return "null-id"

    @override
    def patch_event(
        self,
        *,
        google_event_id: str,
        initiator_google_email: str,
        payload: dict[str, object],
        idempotency_key: str | None = None,
    ) -> None:
        del google_event_id, initiator_google_email, payload, idempotency_key

    @override
    def list_events(
        self,
        *,
        email: str,
        time_min: datetime,
        time_max: datetime,
        max_results: int = 100,
    ) -> list[dict[str, object]]:
        del email, time_min, time_max, max_results
        return []


class AppRuntime:
    def __init__(
        self,
        *,
        config: RuntimeConfig,
        now_provider: Callable[[], datetime] | None = None,
        telegram_client: TelegramApiClient | None = None,
        calendar_client: CalendarApiClient | None = None,
    ) -> None:
        self._config: RuntimeConfig = config
        self._now_provider: Callable[[], datetime] = now_provider or _default_now
        self._repository: SQLiteRepository = SQLiteRepository(config.db_path)
        resolved_telegram_client: TelegramApiClient = (
            telegram_client or _NullTelegramClient()
        )
        self._telegram_client: TelegramApiClient = resolved_telegram_client
        if calendar_client is None:
            resolved_calendar_client = cast(CalendarApiClient, _NullCalendarClient())
        else:
            resolved_calendar_client = calendar_client

        self._calendar_gateway: GoogleCalendarGateway = GoogleCalendarGateway(
            resolved_calendar_client
        )
        self._workflow_service: MeetingWorkflowService = MeetingWorkflowService(
            repository=self._repository,
            calendar_gateway=self._calendar_gateway,
        )
        self._telegram_adapter: TelegramWebhookAdapter = TelegramWebhookAdapter(
            repository=self._repository,
            workflow_service=self._workflow_service,
        )
        self._worker: SchedulerWorker = SchedulerWorker(
            repository=self._repository,
            service=self._workflow_service,
        )
        self._outbox_dispatcher: OutboxDispatcher = OutboxDispatcher(
            repository=self._repository,
            telegram_client=resolved_telegram_client,
            calendar_client=resolved_calendar_client,
        )
        self._outbox_worker: OutboxWorker = OutboxWorker(
            repository=self._repository,
            dispatcher=self._outbox_dispatcher,
        )
        self._calendar_sync_worker: CalendarSyncWorker = CalendarSyncWorker(
            repository=self._repository,
            workflow_service=self._workflow_service,
            calendar_gateway=self._calendar_gateway,
            calendar_client=resolved_calendar_client,
            recurring_exceptions_only_enabled=(
                config.recurring_exceptions_only_enabled
            ),
        )

    @property
    def repository(self) -> SQLiteRepository:
        return self._repository

    @property
    def workflow_service(self) -> MeetingWorkflowService:
        return self._workflow_service

    @property
    def telegram_adapter(self) -> TelegramWebhookAdapter:
        return self._telegram_adapter

    @property
    def worker(self) -> SchedulerWorker:
        return self._worker

    @property
    def outbox_worker(self) -> OutboxWorker:
        return self._outbox_worker

    @property
    def calendar_sync_worker(self) -> CalendarSyncWorker:
        return self._calendar_sync_worker

    def now(self) -> datetime:
        return self._now_provider()

    def startup(self) -> int:
        _ = configure_logging(self._config.log_level, self._config.log_format)
        self._repository.initialize_schema()
        configure_ui = getattr(self._telegram_client, "configure_bot_ui", None)
        if callable(configure_ui):
            try:
                _ = configure_ui()
            except Exception as error:
                logger.warning(
                    "telegram ui setup failed",
                    extra={"error": str(error)},
                )
        recovered_jobs = self._worker.reconcile_on_startup(
            now=self.now(),
            stale_running_after=self._config.stale_running_after,
        )
        recovered_outbox = self._outbox_worker.reconcile_on_startup(
            now=self.now(),
            stale_running_after=self._config.stale_running_after,
        )
        recovered_calendar_sync = self._calendar_sync_worker.reconcile_on_startup(
            now=self.now(),
            stale_running_after=self._config.stale_running_after,
        )
        logger.info(
            "runtime started",
            extra={
                "db_path": self._config.db_path,
                "recovered_jobs": recovered_jobs,
                "recovered_outbox": recovered_outbox,
                "recovered_calendar_sync": recovered_calendar_sync,
            },
        )
        return recovered_jobs

    def shutdown(self) -> None:
        logger.info("runtime shutdown")
        self._repository.close()

    def run_worker_once(self) -> WorkerTickResult:
        return self._worker.run_once(now=self.now())

    def run_outbox_once(self) -> OutboxTickResult:
        return self._outbox_worker.run_once(now=self.now())

    def run_calendar_sync_once(self) -> CalendarSyncTickResult:
        return self._calendar_sync_worker.run_once(now=self.now())


def create_runtime(
    *,
    db_path: str,
    now_provider: Callable[[], datetime] | None = None,
    stale_running_after: timedelta | None = None,
    log_level: str = "INFO",
    log_format: str = "json",
    recurring_exceptions_only_enabled: bool = False,
    telegram_client: TelegramApiClient | None = None,
    calendar_client: CalendarApiClient | None = None,
) -> AppRuntime:
    stale_after = stale_running_after or DEFAULT_STALE_RUNNING_AFTER
    return AppRuntime(
        config=RuntimeConfig(
            db_path=db_path,
            stale_running_after=stale_after,
            log_level=log_level,
            log_format=log_format,
            recurring_exceptions_only_enabled=recurring_exceptions_only_enabled,
        ),
        now_provider=now_provider,
        telegram_client=telegram_client,
        calendar_client=calendar_client,
    )


def _interval_elapsed(
    *,
    last_at: datetime | None,
    interval_seconds: float,
    now_tick: datetime,
) -> bool:
    if last_at is None:
        return True
    elapsed = now_tick - last_at
    return elapsed >= timedelta(seconds=max(interval_seconds, 0.05))


async def run_background_polling_loop(
    *,
    runtime: AppRuntime,
    stop_event: asyncio.Event,
    background_poll_interval_seconds: float,
    calendar_poll_interval_seconds: float,
    retention_cleanup_interval_seconds: float,
    retention_checkpoint_interval_seconds: float,
    retention_vacuum_interval_seconds: float,
    retention_calendar_sync_signal_days: int,
    retention_outbox_days: int,
    retention_job_days: int,
    retention_audit_log_days: int,
    retention_inbound_event_days: int,
) -> None:
    last_calendar_poll_enqueued_at: datetime | None = None
    last_retention_cleanup_at: datetime | None = None
    last_checkpoint_at: datetime | None = None
    last_vacuum_at: datetime | None = None
    timeout_seconds = max(background_poll_interval_seconds, 0.05)

    while not stop_event.is_set():
        processed = False
        try:
            now_tick = runtime.now()
            worker_tick = runtime.run_worker_once()
            outbox_tick = runtime.run_outbox_once()
            sync_tick = runtime.run_calendar_sync_once()
            processed = (
                worker_tick.processed or outbox_tick.processed or sync_tick.processed
            )

            cleanup_retention = getattr(runtime.repository, "cleanup_retention", None)
            if callable(cleanup_retention) and _interval_elapsed(
                last_at=last_retention_cleanup_at,
                interval_seconds=retention_cleanup_interval_seconds,
                now_tick=now_tick,
            ):
                cleanup_result = cleanup_retention(
                    now=now_tick,
                    calendar_sync_signal_retention_days=retention_calendar_sync_signal_days,
                    outbox_retention_days=retention_outbox_days,
                    job_retention_days=retention_job_days,
                    audit_log_retention_days=retention_audit_log_days,
                    inbound_event_retention_days=retention_inbound_event_days,
                )
                last_retention_cleanup_at = now_tick
                calendar_deleted = getattr(
                    cleanup_result, "calendar_sync_signals_deleted", 0
                )
                outbox_deleted = getattr(cleanup_result, "outbox_deleted", 0)
                jobs_deleted = getattr(cleanup_result, "jobs_deleted", 0)
                audit_deleted = getattr(cleanup_result, "audit_logs_deleted", 0)
                inbound_deleted = getattr(cleanup_result, "inbound_events_deleted", 0)
                deleted_total = (
                    int(calendar_deleted)
                    + int(outbox_deleted)
                    + int(jobs_deleted)
                    + int(audit_deleted)
                    + int(inbound_deleted)
                )
                if deleted_total > 0:
                    logger.info(
                        "retention cleanup completed",
                        extra={
                            "calendar_sync_signals_deleted": int(calendar_deleted),
                            "outbox_deleted": int(outbox_deleted),
                            "jobs_deleted": int(jobs_deleted),
                            "audit_logs_deleted": int(audit_deleted),
                            "inbound_events_deleted": int(inbound_deleted),
                        },
                    )

            wal_checkpoint = getattr(runtime.repository, "wal_checkpoint", None)
            if callable(wal_checkpoint) and _interval_elapsed(
                last_at=last_checkpoint_at,
                interval_seconds=retention_checkpoint_interval_seconds,
                now_tick=now_tick,
            ):
                _ = wal_checkpoint(mode="PASSIVE")
                last_checkpoint_at = now_tick

            run_vacuum = getattr(runtime.repository, "vacuum", None)
            if callable(run_vacuum) and _interval_elapsed(
                last_at=last_vacuum_at,
                interval_seconds=retention_vacuum_interval_seconds,
                now_tick=now_tick,
            ):
                run_vacuum()
                last_vacuum_at = now_tick
                logger.info("database vacuum completed")

            if not processed:
                should_enqueue_poll = False
                if last_calendar_poll_enqueued_at is None:
                    should_enqueue_poll = True
                else:
                    elapsed = now_tick - last_calendar_poll_enqueued_at
                    should_enqueue_poll = elapsed >= timedelta(
                        seconds=max(calendar_poll_interval_seconds, 1.0)
                    )

                if should_enqueue_poll:
                    list_active_users = getattr(
                        runtime.repository, "get_all_active_users", None
                    )
                    active_users = (
                        list_active_users() if callable(list_active_users) else []
                    )
                    if not isinstance(active_users, list):
                        active_users = []
                    for mapping in active_users:
                        if not isinstance(mapping, dict):
                            continue
                        email_obj = mapping.get("google_email")
                        if not isinstance(email_obj, str):
                            continue
                        calendar_email = email_obj.strip().lower()
                        if not calendar_email:
                            continue
                        poll_id = f"poll:{calendar_email}:{int(now_tick.timestamp())}"
                        _ = runtime.repository.enqueue_calendar_sync_signal(
                            calendar_id=calendar_email,
                            external_event_id=poll_id,
                            resource_state="poll",
                            message_number=None,
                            now=now_tick,
                        )
                    last_calendar_poll_enqueued_at = now_tick
        except Exception as error:
            logger.error(
                "background polling error",
                extra={"error": str(error)},
            )
            processed = False

        if processed:
            await asyncio.sleep(0)
            continue

        await asyncio.sleep(timeout_seconds)


def create_application(
    *,
    db_path: str,
    now_provider: Callable[[], datetime] | None = None,
    stale_running_after: timedelta | None = None,
    log_level: str = "INFO",
    log_format: str = "json",
    secret_token: str | None = None,
    google_channel_token: str | None = None,
    telegram_client: TelegramApiClient | None = None,
    calendar_client: CalendarApiClient | None = None,
    run_background_workers: bool = False,
    background_poll_interval_seconds: float = 0.5,
    calendar_poll_interval_seconds: float = DEFAULT_CALENDAR_POLL_INTERVAL_SECONDS,
    retention_cleanup_interval_seconds: float = DEFAULT_RETENTION_CLEANUP_INTERVAL_SECONDS,
    retention_checkpoint_interval_seconds: float = DEFAULT_RETENTION_CHECKPOINT_INTERVAL_SECONDS,
    retention_vacuum_interval_seconds: float = DEFAULT_RETENTION_VACUUM_INTERVAL_SECONDS,
    retention_calendar_sync_signal_days: int = DEFAULT_RETENTION_CALENDAR_SYNC_SIGNAL_DAYS,
    retention_outbox_days: int = DEFAULT_RETENTION_OUTBOX_DAYS,
    retention_job_days: int = DEFAULT_RETENTION_JOB_DAYS,
    retention_audit_log_days: int = DEFAULT_RETENTION_AUDIT_LOG_DAYS,
    retention_inbound_event_days: int = DEFAULT_RETENTION_INBOUND_EVENT_DAYS,
    recurring_exceptions_only_enabled: bool = False,
) -> FastAPI:
    runtime = create_runtime(
        db_path=db_path,
        now_provider=now_provider,
        stale_running_after=stale_running_after,
        log_level=log_level,
        log_format=log_format,
        recurring_exceptions_only_enabled=recurring_exceptions_only_enabled,
        telegram_client=telegram_client,
        calendar_client=calendar_client,
    )

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        _ = runtime.startup()
        app.state.runtime = runtime
        stop_event: asyncio.Event | None = None
        polling_task: asyncio.Task[None] | None = None

        if run_background_workers:
            stop_event = asyncio.Event()
            polling_task = asyncio.create_task(
                run_background_polling_loop(
                    runtime=runtime,
                    stop_event=stop_event,
                    background_poll_interval_seconds=background_poll_interval_seconds,
                    calendar_poll_interval_seconds=calendar_poll_interval_seconds,
                    retention_cleanup_interval_seconds=retention_cleanup_interval_seconds,
                    retention_checkpoint_interval_seconds=retention_checkpoint_interval_seconds,
                    retention_vacuum_interval_seconds=retention_vacuum_interval_seconds,
                    retention_calendar_sync_signal_days=retention_calendar_sync_signal_days,
                    retention_outbox_days=retention_outbox_days,
                    retention_job_days=retention_job_days,
                    retention_audit_log_days=retention_audit_log_days,
                    retention_inbound_event_days=retention_inbound_event_days,
                )
            )

        try:
            yield
        finally:
            if stop_event is not None:
                stop_event.set()
            if polling_task is not None:
                await polling_task
            runtime.shutdown()

    def google_webhook_handler(headers: Mapping[str, str], now: datetime) -> None:
        channel_id = headers.get("x-goog-channel-id", "")
        message_number = headers.get("x-goog-message-number", "")
        resource_state = headers.get("x-goog-resource-state", "")
        resource_id = headers.get("x-goog-resource-id", "")
        resource_uri = headers.get("x-goog-resource-uri", "")

        calendar_id = "primary"
        marker = "/calendars/"
        if marker in resource_uri:
            after_marker = resource_uri.split(marker, maxsplit=1)[1]
            calendar_id = after_marker.split("/events", maxsplit=1)[0] or "primary"

        parsed_message_number: int | None = None
        if message_number.isdigit():
            parsed_message_number = int(message_number)

        external_event_id = ":".join(
            part
            for part in (channel_id, message_number, resource_state, resource_id)
            if part
        )
        if not external_event_id:
            external_event_id = f"google-webhook:{int(now.timestamp())}"

        accepted = runtime.repository.register_inbound_event(
            source=InboundEventSource.GOOGLE_WEBHOOK,
            external_event_id=external_event_id,
            received_at=now,
        )
        if not accepted:
            return

        _ = runtime.repository.enqueue_calendar_sync_signal(
            calendar_id=calendar_id,
            external_event_id=external_event_id,
            resource_state=resource_state,
            message_number=parsed_message_number,
            now=now,
        )

        runtime.repository.insert_audit_log(
            meeting_id=None,
            round=None,
            actor_telegram_user_id=None,
            actor_type="system",
            action="google_webhook_received",
            details={
                "channel_id": channel_id,
                "message_number": message_number,
                "resource_state": resource_state,
                "resource_id": resource_id,
                "calendar_id": calendar_id,
            },
            now=now,
        )

    return create_webhook_app(
        adapter=runtime.telegram_adapter,
        now_provider=runtime.now,
        lifespan=lifespan,
        secret_token=secret_token,
        google_webhook_handler=google_webhook_handler,
        google_channel_token=google_channel_token,
        readiness_probe=runtime.repository.check_connection,
    )
