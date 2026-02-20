from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
import os


DEFAULT_DB_PATH = "./var/bot_vstrechi.db"
DEFAULT_LOG_LEVEL = "INFO"
DEFAULT_LOG_FORMAT = "json"
DEFAULT_GOOGLE_TOKEN_URI = "https://oauth2.googleapis.com/token"
DEFAULT_BACKGROUND_WORKER_TICK_SECONDS = 0.5
DEFAULT_CALENDAR_POLL_INTERVAL_SECONDS = 60.0
DEFAULT_RETENTION_CLEANUP_INTERVAL_SECONDS = 1800.0
DEFAULT_RETENTION_CHECKPOINT_INTERVAL_SECONDS = 21600.0
DEFAULT_RETENTION_VACUUM_INTERVAL_SECONDS = 604800.0
DEFAULT_RETENTION_CALENDAR_SYNC_SIGNAL_DAYS = 5
DEFAULT_RETENTION_OUTBOX_DAYS = 14
DEFAULT_RETENTION_JOB_DAYS = 14
DEFAULT_RETENTION_AUDIT_LOG_DAYS = 30
DEFAULT_RETENTION_INBOUND_EVENT_DAYS = 7
DEFAULT_RECURRING_EXCEPTIONS_ONLY_ENABLED = False
DEFAULT_RUN_BACKGROUND_WORKERS = True


@dataclass(frozen=True)
class Settings:
    db_path: str
    telegram_bot_token: str
    telegram_secret_token: str | None
    calendar_enabled: bool
    google_sa_client_email: str | None
    google_sa_private_key: str | None
    google_sa_private_key_id: str | None
    google_sa_token_uri: str
    google_impersonation_subject: str | None
    google_webhook_channel_token: str | None
    log_level: str
    log_format: str
    background_worker_tick_seconds: float
    calendar_poll_interval_seconds: float
    retention_cleanup_interval_seconds: float
    retention_checkpoint_interval_seconds: float
    retention_vacuum_interval_seconds: float
    retention_calendar_sync_signal_days: int
    retention_outbox_days: int
    retention_job_days: int
    retention_audit_log_days: int
    retention_inbound_event_days: int
    recurring_exceptions_only_enabled: bool
    run_background_workers: bool


def load_settings(env: Mapping[str, str] | None = None) -> Settings:
    values = env or os.environ

    db_path = values.get("BOT_VSTRECHI_DB_PATH", DEFAULT_DB_PATH).strip()
    if not db_path:
        raise ValueError("Environment variable BOT_VSTRECHI_DB_PATH must be non-empty")

    telegram_bot_token = _require(values, "TELEGRAM_BOT_TOKEN")
    telegram_secret_token = _optional(values, "TELEGRAM_SECRET_TOKEN")

    calendar_enabled = _parse_bool(
        values.get("BOT_VSTRECHI_CALENDAR_ENABLED", "false"),
        var_name="BOT_VSTRECHI_CALENDAR_ENABLED",
    )

    google_sa_client_email = _optional(values, "GOOGLE_SA_CLIENT_EMAIL")
    google_sa_private_key = _optional(values, "GOOGLE_SA_PRIVATE_KEY")
    google_sa_private_key_id = _optional(values, "GOOGLE_SA_PRIVATE_KEY_ID")
    google_sa_token_uri = values.get(
        "GOOGLE_SA_TOKEN_URI", DEFAULT_GOOGLE_TOKEN_URI
    ).strip()
    if not google_sa_token_uri:
        raise ValueError("Environment variable GOOGLE_SA_TOKEN_URI must be non-empty")

    google_impersonation_subject = _optional(values, "GOOGLE_IMPERSONATION_SUBJECT")
    google_webhook_channel_token = _optional(values, "GOOGLE_WEBHOOK_CHANNEL_TOKEN")

    if calendar_enabled:
        if google_sa_client_email is None:
            raise ValueError(
                "Missing required environment variable: GOOGLE_SA_CLIENT_EMAIL"
            )
        if google_sa_private_key is None:
            raise ValueError(
                "Missing required environment variable: GOOGLE_SA_PRIVATE_KEY"
            )
        if google_impersonation_subject is None:
            raise ValueError(
                "Missing required environment variable: GOOGLE_IMPERSONATION_SUBJECT"
            )

    log_level = values.get("LOG_LEVEL", DEFAULT_LOG_LEVEL).strip().upper()
    if not log_level:
        raise ValueError("Environment variable LOG_LEVEL must be non-empty")

    log_format = values.get("LOG_FORMAT", DEFAULT_LOG_FORMAT).strip().lower()
    if log_format not in {"json", "pretty", "text"}:
        raise ValueError(
            "Environment variable LOG_FORMAT must be one of: json, pretty, text"
        )

    background_worker_tick_seconds = _parse_positive_float(
        values.get(
            "BOT_VSTRECHI_BACKGROUND_WORKER_TICK_SECONDS",
            str(DEFAULT_BACKGROUND_WORKER_TICK_SECONDS),
        ),
        var_name="BOT_VSTRECHI_BACKGROUND_WORKER_TICK_SECONDS",
    )
    calendar_poll_interval_seconds = _parse_positive_float(
        values.get(
            "BOT_VSTRECHI_CALENDAR_POLL_INTERVAL_SECONDS",
            str(DEFAULT_CALENDAR_POLL_INTERVAL_SECONDS),
        ),
        var_name="BOT_VSTRECHI_CALENDAR_POLL_INTERVAL_SECONDS",
    )
    retention_cleanup_interval_seconds = _parse_positive_float(
        values.get(
            "BOT_VSTRECHI_RETENTION_CLEANUP_INTERVAL_SECONDS",
            str(DEFAULT_RETENTION_CLEANUP_INTERVAL_SECONDS),
        ),
        var_name="BOT_VSTRECHI_RETENTION_CLEANUP_INTERVAL_SECONDS",
    )
    retention_checkpoint_interval_seconds = _parse_positive_float(
        values.get(
            "BOT_VSTRECHI_RETENTION_CHECKPOINT_INTERVAL_SECONDS",
            str(DEFAULT_RETENTION_CHECKPOINT_INTERVAL_SECONDS),
        ),
        var_name="BOT_VSTRECHI_RETENTION_CHECKPOINT_INTERVAL_SECONDS",
    )
    retention_vacuum_interval_seconds = _parse_positive_float(
        values.get(
            "BOT_VSTRECHI_RETENTION_VACUUM_INTERVAL_SECONDS",
            str(DEFAULT_RETENTION_VACUUM_INTERVAL_SECONDS),
        ),
        var_name="BOT_VSTRECHI_RETENTION_VACUUM_INTERVAL_SECONDS",
    )

    retention_calendar_sync_signal_days = _parse_positive_int(
        values.get(
            "BOT_VSTRECHI_RETENTION_CALENDAR_SYNC_SIGNAL_DAYS",
            str(DEFAULT_RETENTION_CALENDAR_SYNC_SIGNAL_DAYS),
        ),
        var_name="BOT_VSTRECHI_RETENTION_CALENDAR_SYNC_SIGNAL_DAYS",
    )
    retention_outbox_days = _parse_positive_int(
        values.get(
            "BOT_VSTRECHI_RETENTION_OUTBOX_DAYS",
            str(DEFAULT_RETENTION_OUTBOX_DAYS),
        ),
        var_name="BOT_VSTRECHI_RETENTION_OUTBOX_DAYS",
    )
    retention_job_days = _parse_positive_int(
        values.get(
            "BOT_VSTRECHI_RETENTION_JOB_DAYS",
            str(DEFAULT_RETENTION_JOB_DAYS),
        ),
        var_name="BOT_VSTRECHI_RETENTION_JOB_DAYS",
    )
    retention_audit_log_days = _parse_positive_int(
        values.get(
            "BOT_VSTRECHI_RETENTION_AUDIT_LOG_DAYS",
            str(DEFAULT_RETENTION_AUDIT_LOG_DAYS),
        ),
        var_name="BOT_VSTRECHI_RETENTION_AUDIT_LOG_DAYS",
    )
    retention_inbound_event_days = _parse_positive_int(
        values.get(
            "BOT_VSTRECHI_RETENTION_INBOUND_EVENT_DAYS",
            str(DEFAULT_RETENTION_INBOUND_EVENT_DAYS),
        ),
        var_name="BOT_VSTRECHI_RETENTION_INBOUND_EVENT_DAYS",
    )

    recurring_exceptions_only_enabled = _parse_bool(
        values.get(
            "BOT_VSTRECHI_RECURRING_EXCEPTIONS_ONLY_ENABLED",
            "true" if DEFAULT_RECURRING_EXCEPTIONS_ONLY_ENABLED else "false",
        ),
        var_name="BOT_VSTRECHI_RECURRING_EXCEPTIONS_ONLY_ENABLED",
    )
    run_background_workers = _parse_bool(
        values.get(
            "BOT_VSTRECHI_RUN_BACKGROUND_WORKERS",
            "true" if DEFAULT_RUN_BACKGROUND_WORKERS else "false",
        ),
        var_name="BOT_VSTRECHI_RUN_BACKGROUND_WORKERS",
    )

    return Settings(
        db_path=db_path,
        telegram_bot_token=telegram_bot_token,
        telegram_secret_token=telegram_secret_token,
        calendar_enabled=calendar_enabled,
        google_sa_client_email=google_sa_client_email,
        google_sa_private_key=_normalize_private_key(google_sa_private_key),
        google_sa_private_key_id=google_sa_private_key_id,
        google_sa_token_uri=google_sa_token_uri,
        google_impersonation_subject=google_impersonation_subject,
        google_webhook_channel_token=google_webhook_channel_token,
        log_level=log_level,
        log_format=log_format,
        background_worker_tick_seconds=background_worker_tick_seconds,
        calendar_poll_interval_seconds=calendar_poll_interval_seconds,
        retention_cleanup_interval_seconds=retention_cleanup_interval_seconds,
        retention_checkpoint_interval_seconds=retention_checkpoint_interval_seconds,
        retention_vacuum_interval_seconds=retention_vacuum_interval_seconds,
        retention_calendar_sync_signal_days=retention_calendar_sync_signal_days,
        retention_outbox_days=retention_outbox_days,
        retention_job_days=retention_job_days,
        retention_audit_log_days=retention_audit_log_days,
        retention_inbound_event_days=retention_inbound_event_days,
        recurring_exceptions_only_enabled=recurring_exceptions_only_enabled,
        run_background_workers=run_background_workers,
    )


def _require(values: Mapping[str, str], name: str) -> str:
    value = values.get(name)
    if value is None or not value.strip():
        raise ValueError(f"Missing required environment variable: {name}")
    return value.strip()


def _optional(values: Mapping[str, str], name: str) -> str | None:
    value = values.get(name)
    if value is None:
        return None
    stripped = value.strip()
    if not stripped:
        return None
    return stripped


def _parse_bool(raw: str, *, var_name: str) -> bool:
    normalized = raw.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off", ""}:
        return False
    raise ValueError(
        f"Environment variable {var_name} must be a boolean (true/false/1/0)"
    )


def _parse_positive_float(raw: str, *, var_name: str) -> float:
    value = raw.strip()
    if not value:
        raise ValueError(f"Environment variable {var_name} must be non-empty")
    try:
        parsed = float(value)
    except ValueError as error:
        raise ValueError(
            f"Environment variable {var_name} must be a positive number"
        ) from error
    if parsed <= 0:
        raise ValueError(f"Environment variable {var_name} must be greater than zero")
    return parsed


def _parse_positive_int(raw: str, *, var_name: str) -> int:
    value = raw.strip()
    if not value:
        raise ValueError(f"Environment variable {var_name} must be non-empty")
    try:
        parsed = int(value)
    except ValueError as error:
        raise ValueError(
            f"Environment variable {var_name} must be a positive integer"
        ) from error
    if parsed <= 0:
        raise ValueError(f"Environment variable {var_name} must be greater than zero")
    return parsed


def _normalize_private_key(value: str | None) -> str | None:
    if value is None:
        return None
    return value.replace("\\n", "\n")
