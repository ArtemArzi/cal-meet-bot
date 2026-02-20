from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
from typing import override

from bot_vstrechi.db.repository import ClaimedCalendarSyncSignal, SQLiteRepository
from bot_vstrechi.domain.models import OutboxStatus
from bot_vstrechi.workers.calendar_sync import (
    CalendarSyncProcessor,
    CalendarSyncWorker,
)


def _repo(tmp_path: Path) -> SQLiteRepository:
    repository = SQLiteRepository(str(tmp_path / "bot_vstrechi_calendar_sync.db"))
    repository.initialize_schema()
    return repository


class _FailOnceProcessor(CalendarSyncProcessor):
    def __init__(self) -> None:
        self._failed: bool = False

    @override
    def process_signal(
        self,
        *,
        signal: ClaimedCalendarSyncSignal,
        now: datetime,
    ) -> None:
        del signal, now
        if not self._failed:
            self._failed = True
            raise RuntimeError("temporary")


def test_calendar_sync_worker_marks_signal_done(tmp_path: Path) -> None:
    now = datetime(2026, 2, 20, 10, 0, 0)
    repository = _repo(tmp_path)
    _ = repository.enqueue_calendar_sync_signal(
        calendar_id="primary",
        external_event_id="sig-1",
        resource_state="exists",
        message_number=1,
        now=now,
    )

    worker = CalendarSyncWorker(repository=repository)
    tick = worker.run_once(now=now)

    assert tick.processed is True
    assert tick.status == OutboxStatus.DONE
    assert repository.count_calendar_sync_signals(status=OutboxStatus.DONE) == 1
    repository.close()


def test_calendar_sync_worker_retries_then_succeeds(tmp_path: Path) -> None:
    now = datetime(2026, 2, 20, 10, 0, 0)
    repository = _repo(tmp_path)
    _ = repository.enqueue_calendar_sync_signal(
        calendar_id="primary",
        external_event_id="sig-2",
        resource_state="exists",
        message_number=2,
        now=now,
    )

    worker = CalendarSyncWorker(repository=repository, processor=_FailOnceProcessor())
    first_tick = worker.run_once(now=now)
    assert first_tick.processed is True
    assert first_tick.status == OutboxStatus.PENDING
    assert repository.count_calendar_sync_signals(status=OutboxStatus.PENDING) == 1

    second_tick = worker.run_once(now=now + timedelta(seconds=6))
    assert second_tick.processed is True
    assert second_tick.status == OutboxStatus.DONE
    assert repository.count_calendar_sync_signals(status=OutboxStatus.DONE) == 1
    repository.close()


def test_webhook_and_polling_deduplicate_same_change(tmp_path: Path) -> None:
    now = datetime(2026, 2, 20, 10, 0, 0)
    repository = _repo(tmp_path)

    first = repository.enqueue_calendar_sync_signal(
        calendar_id="primary",
        external_event_id="change-1",
        resource_state="exists",
        message_number=31,
        now=now,
    )
    second = repository.enqueue_calendar_sync_signal(
        calendar_id="primary",
        external_event_id="change-1",
        resource_state="poll",
        message_number=31,
        now=now,
    )

    assert first is True
    assert second is False
    assert repository.count_calendar_sync_signals(status=OutboxStatus.PENDING) == 1
    repository.close()


def test_410_token_recovers_with_full_resync(tmp_path: Path) -> None:
    now = datetime(2026, 2, 20, 10, 0, 0)
    repository = _repo(tmp_path)
    repository.upsert_calendar_sync_state(
        calendar_id="primary",
        sync_token="token-1",
        watch_channel_id="ch-1",
        watch_resource_id="res-1",
        watch_expiration_at=now + timedelta(days=7),
        last_message_number=40,
        now=now,
    )
    _ = repository.enqueue_calendar_sync_signal(
        calendar_id="primary",
        external_event_id="change-410",
        resource_state="sync_token_invalid",
        message_number=41,
        now=now,
    )

    worker = CalendarSyncWorker(repository=repository)
    tick = worker.run_once(now=now)

    assert tick.processed is True
    assert tick.status == OutboxStatus.DONE
    state = repository.get_calendar_sync_state(calendar_id="primary")
    assert state is not None
    assert state.get("sync_token") is None
    assert state.get("last_message_number") == 41
    repository.close()


def test_sync_worker_preserves_watch_metadata_in_polling_mode(tmp_path: Path) -> None:
    now = datetime(2026, 2, 20, 10, 0, 0)
    repository = _repo(tmp_path)
    watch_expiration = now + timedelta(days=3)
    repository.upsert_calendar_sync_state(
        calendar_id="primary",
        sync_token="token-2",
        watch_channel_id="watch-channel-1",
        watch_resource_id="watch-resource-1",
        watch_expiration_at=watch_expiration,
        last_message_number=12,
        now=now,
    )
    _ = repository.enqueue_calendar_sync_signal(
        calendar_id="primary",
        external_event_id="sig-keep-watch",
        resource_state="poll",
        message_number=13,
        now=now,
    )

    worker = CalendarSyncWorker(repository=repository)
    tick = worker.run_once(now=now)

    assert tick.processed is True
    state = repository.get_calendar_sync_state(calendar_id="primary")
    assert state is not None
    assert state.get("watch_channel_id") == "watch-channel-1"
    assert state.get("watch_resource_id") == "watch-resource-1"
    watch_expiration_obj = state.get("watch_expiration_at")
    assert isinstance(watch_expiration_obj, str)
    assert watch_expiration_obj.startswith(watch_expiration.isoformat())
    repository.close()
