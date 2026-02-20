from __future__ import annotations

import asyncio
import signal

from bot_vstrechi.infrastructure.bootstrap import load_runtime_dependencies
from bot_vstrechi.infrastructure.runtime import (
    create_runtime,
    run_background_polling_loop,
)


async def _run() -> None:
    dependencies = load_runtime_dependencies()
    settings = dependencies.settings

    runtime = create_runtime(
        db_path=settings.db_path,
        log_level=settings.log_level,
        log_format=settings.log_format,
        recurring_exceptions_only_enabled=settings.recurring_exceptions_only_enabled,
        telegram_client=dependencies.telegram_client,
        calendar_client=dependencies.calendar_client,
    )
    _ = runtime.startup()

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop_event.set)

    try:
        await run_background_polling_loop(
            runtime=runtime,
            stop_event=stop_event,
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
        )
    finally:
        runtime.shutdown()


def main() -> None:
    asyncio.run(_run())


if __name__ == "__main__":
    main()
