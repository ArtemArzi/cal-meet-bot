from __future__ import annotations

from dataclasses import dataclass

from bot_vstrechi.calendar.client import (
    GoogleServiceAccountCalendarClient,
    GoogleServiceAccountCredentials,
)
from bot_vstrechi.infrastructure.logging import configure_logging
from bot_vstrechi.infrastructure.settings import Settings, load_settings
from bot_vstrechi.telegram.client import HttpxTelegramClient


@dataclass(frozen=True)
class RuntimeDependencies:
    settings: Settings
    telegram_client: HttpxTelegramClient
    calendar_client: GoogleServiceAccountCalendarClient | None


def load_runtime_dependencies() -> RuntimeDependencies:
    settings = load_settings()
    configure_logging(settings.log_level, settings.log_format)
    telegram_client = HttpxTelegramClient(bot_token=settings.telegram_bot_token)
    calendar_client = None
    if settings.calendar_enabled:
        if settings.google_sa_client_email is None:
            raise ValueError(
                "GOOGLE_SA_CLIENT_EMAIL is required when calendar is enabled"
            )
        if settings.google_sa_private_key is None:
            raise ValueError(
                "GOOGLE_SA_PRIVATE_KEY is required when calendar is enabled"
            )
        if settings.google_impersonation_subject is None:
            raise ValueError(
                "GOOGLE_IMPERSONATION_SUBJECT is required when calendar is enabled"
            )
        credentials = GoogleServiceAccountCredentials(
            client_email=settings.google_sa_client_email,
            private_key=settings.google_sa_private_key,
            token_uri=settings.google_sa_token_uri,
            private_key_id=settings.google_sa_private_key_id,
        )
        calendar_client = GoogleServiceAccountCalendarClient(
            credentials=credentials,
            impersonation_subject=settings.google_impersonation_subject,
        )

    return RuntimeDependencies(
        settings=settings,
        telegram_client=telegram_client,
        calendar_client=calendar_client,
    )
