from __future__ import annotations

import pytest

from bot_vstrechi.infrastructure.settings import (
    DEFAULT_BACKGROUND_WORKER_TICK_SECONDS,
    DEFAULT_CALENDAR_POLL_INTERVAL_SECONDS,
    DEFAULT_DB_PATH,
    DEFAULT_GOOGLE_TOKEN_URI,
    DEFAULT_LOG_FORMAT,
    DEFAULT_LOG_LEVEL,
    DEFAULT_RECURRING_EXCEPTIONS_ONLY_ENABLED,
    DEFAULT_RUN_BACKGROUND_WORKERS,
    DEFAULT_RETENTION_AUDIT_LOG_DAYS,
    DEFAULT_RETENTION_CALENDAR_SYNC_SIGNAL_DAYS,
    DEFAULT_RETENTION_CHECKPOINT_INTERVAL_SECONDS,
    DEFAULT_RETENTION_CLEANUP_INTERVAL_SECONDS,
    DEFAULT_RETENTION_INBOUND_EVENT_DAYS,
    DEFAULT_RETENTION_JOB_DAYS,
    DEFAULT_RETENTION_OUTBOX_DAYS,
    DEFAULT_RETENTION_VACUUM_INTERVAL_SECONDS,
    load_settings,
)


def test_load_settings_uses_defaults_when_calendar_disabled() -> None:
    settings = load_settings(
        {
            "TELEGRAM_BOT_TOKEN": "token",
        }
    )

    assert settings.db_path == DEFAULT_DB_PATH
    assert settings.telegram_bot_token == "token"
    assert settings.calendar_enabled is False
    assert settings.google_sa_client_email is None
    assert settings.google_sa_private_key is None
    assert settings.google_impersonation_subject is None
    assert settings.google_sa_token_uri == DEFAULT_GOOGLE_TOKEN_URI
    assert settings.log_level == DEFAULT_LOG_LEVEL
    assert settings.log_format == DEFAULT_LOG_FORMAT
    assert (
        settings.background_worker_tick_seconds
        == DEFAULT_BACKGROUND_WORKER_TICK_SECONDS
    )
    assert (
        settings.calendar_poll_interval_seconds
        == DEFAULT_CALENDAR_POLL_INTERVAL_SECONDS
    )
    assert (
        settings.retention_cleanup_interval_seconds
        == DEFAULT_RETENTION_CLEANUP_INTERVAL_SECONDS
    )
    assert (
        settings.retention_checkpoint_interval_seconds
        == DEFAULT_RETENTION_CHECKPOINT_INTERVAL_SECONDS
    )
    assert (
        settings.retention_vacuum_interval_seconds
        == DEFAULT_RETENTION_VACUUM_INTERVAL_SECONDS
    )
    assert (
        settings.retention_calendar_sync_signal_days
        == DEFAULT_RETENTION_CALENDAR_SYNC_SIGNAL_DAYS
    )
    assert settings.retention_outbox_days == DEFAULT_RETENTION_OUTBOX_DAYS
    assert settings.retention_job_days == DEFAULT_RETENTION_JOB_DAYS
    assert settings.retention_audit_log_days == DEFAULT_RETENTION_AUDIT_LOG_DAYS
    assert settings.retention_inbound_event_days == DEFAULT_RETENTION_INBOUND_EVENT_DAYS
    assert (
        settings.recurring_exceptions_only_enabled
        == DEFAULT_RECURRING_EXCEPTIONS_ONLY_ENABLED
    )
    assert settings.run_background_workers == DEFAULT_RUN_BACKGROUND_WORKERS


def test_load_settings_requires_telegram_bot_token() -> None:
    with pytest.raises(ValueError, match="TELEGRAM_BOT_TOKEN"):
        _ = load_settings({})


def test_load_settings_requires_google_credentials_when_calendar_enabled() -> None:
    with pytest.raises(ValueError, match="GOOGLE_SA_CLIENT_EMAIL"):
        _ = load_settings(
            {
                "TELEGRAM_BOT_TOKEN": "token",
                "BOT_VSTRECHI_CALENDAR_ENABLED": "true",
            }
        )
    with pytest.raises(ValueError, match="GOOGLE_IMPERSONATION_SUBJECT"):
        _ = load_settings(
            {
                "TELEGRAM_BOT_TOKEN": "token",
                "BOT_VSTRECHI_CALENDAR_ENABLED": "true",
                "GOOGLE_SA_CLIENT_EMAIL": "bot@example.iam.gserviceaccount.com",
                "GOOGLE_SA_PRIVATE_KEY": "dummy",
            }
        )


def test_load_settings_rejects_invalid_log_format() -> None:
    with pytest.raises(ValueError, match="LOG_FORMAT"):
        _ = load_settings(
            {
                "TELEGRAM_BOT_TOKEN": "token",
                "LOG_FORMAT": "yaml",
            }
        )


def test_load_settings_accepts_pretty_log_format() -> None:
    settings = load_settings(
        {
            "TELEGRAM_BOT_TOKEN": "token",
            "LOG_FORMAT": "pretty",
        }
    )

    assert settings.log_format == "pretty"


def test_load_settings_normalizes_google_private_key() -> None:
    settings = load_settings(
        {
            "TELEGRAM_BOT_TOKEN": "token",
            "BOT_VSTRECHI_CALENDAR_ENABLED": "1",
            "GOOGLE_SA_CLIENT_EMAIL": "bot@example.iam.gserviceaccount.com",
            "GOOGLE_SA_PRIVATE_KEY": "line1\\nline2",
            "GOOGLE_IMPERSONATION_SUBJECT": "admin@example.com",
        }
    )

    assert settings.google_sa_private_key == "line1\nline2"


def test_load_settings_accepts_retention_and_poll_overrides() -> None:
    settings = load_settings(
        {
            "TELEGRAM_BOT_TOKEN": "token",
            "BOT_VSTRECHI_BACKGROUND_WORKER_TICK_SECONDS": "0.25",
            "BOT_VSTRECHI_CALENDAR_POLL_INTERVAL_SECONDS": "120",
            "BOT_VSTRECHI_RETENTION_CLEANUP_INTERVAL_SECONDS": "1800",
            "BOT_VSTRECHI_RETENTION_CHECKPOINT_INTERVAL_SECONDS": "3600",
            "BOT_VSTRECHI_RETENTION_VACUUM_INTERVAL_SECONDS": "86400",
            "BOT_VSTRECHI_RETENTION_CALENDAR_SYNC_SIGNAL_DAYS": "5",
            "BOT_VSTRECHI_RETENTION_OUTBOX_DAYS": "10",
            "BOT_VSTRECHI_RETENTION_JOB_DAYS": "11",
            "BOT_VSTRECHI_RETENTION_AUDIT_LOG_DAYS": "45",
            "BOT_VSTRECHI_RETENTION_INBOUND_EVENT_DAYS": "6",
        }
    )

    assert settings.background_worker_tick_seconds == 0.25
    assert settings.calendar_poll_interval_seconds == 120.0
    assert settings.retention_cleanup_interval_seconds == 1800.0
    assert settings.retention_checkpoint_interval_seconds == 3600.0
    assert settings.retention_vacuum_interval_seconds == 86400.0
    assert settings.retention_calendar_sync_signal_days == 5
    assert settings.retention_outbox_days == 10
    assert settings.retention_job_days == 11
    assert settings.retention_audit_log_days == 45
    assert settings.retention_inbound_event_days == 6


def test_load_settings_rejects_non_positive_retention_or_poll_values() -> None:
    with pytest.raises(ValueError, match="BOT_VSTRECHI_CALENDAR_POLL_INTERVAL_SECONDS"):
        _ = load_settings(
            {
                "TELEGRAM_BOT_TOKEN": "token",
                "BOT_VSTRECHI_CALENDAR_POLL_INTERVAL_SECONDS": "0",
            }
        )


def test_load_settings_accepts_recurring_exceptions_only_toggle() -> None:
    settings = load_settings(
        {
            "TELEGRAM_BOT_TOKEN": "token",
            "BOT_VSTRECHI_RECURRING_EXCEPTIONS_ONLY_ENABLED": "true",
            "BOT_VSTRECHI_RUN_BACKGROUND_WORKERS": "false",
        }
    )

    assert settings.recurring_exceptions_only_enabled is True
    assert settings.run_background_workers is False

    with pytest.raises(ValueError, match="BOT_VSTRECHI_RETENTION_OUTBOX_DAYS"):
        _ = load_settings(
            {
                "TELEGRAM_BOT_TOKEN": "token",
                "BOT_VSTRECHI_RETENTION_OUTBOX_DAYS": "-1",
            }
        )
