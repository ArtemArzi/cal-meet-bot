from __future__ import annotations

from bot_vstrechi.infrastructure.bootstrap import load_runtime_dependencies
from bot_vstrechi.infrastructure.runtime import create_application


dependencies = load_runtime_dependencies()
settings = dependencies.settings

app = create_application(
    db_path=settings.db_path,
    log_level=settings.log_level,
    log_format=settings.log_format,
    secret_token=settings.telegram_secret_token,
    google_channel_token=settings.google_webhook_channel_token,
    telegram_client=dependencies.telegram_client,
    calendar_client=dependencies.calendar_client,
    run_background_workers=settings.run_background_workers,
    background_poll_interval_seconds=settings.background_worker_tick_seconds,
    calendar_poll_interval_seconds=settings.calendar_poll_interval_seconds,
    retention_cleanup_interval_seconds=settings.retention_cleanup_interval_seconds,
    retention_checkpoint_interval_seconds=settings.retention_checkpoint_interval_seconds,
    retention_vacuum_interval_seconds=settings.retention_vacuum_interval_seconds,
    retention_calendar_sync_signal_days=settings.retention_calendar_sync_signal_days,
    retention_outbox_days=settings.retention_outbox_days,
    retention_job_days=settings.retention_job_days,
    retention_audit_log_days=settings.retention_audit_log_days,
    retention_inbound_event_days=settings.retention_inbound_event_days,
    recurring_exceptions_only_enabled=settings.recurring_exceptions_only_enabled,
)
