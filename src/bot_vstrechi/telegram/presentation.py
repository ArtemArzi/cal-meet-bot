from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


BUTTON_HELP = "ℹ️ Как пользоваться"
BUTTON_PEOPLE = "👥 Люди"


MAIN_MENU_KEYBOARD_LAYOUT: tuple[tuple[str, ...], ...] = (
    (BUTTON_PEOPLE,),
    (BUTTON_HELP,),
)


BOT_COMMANDS: tuple[tuple[str, str], ...] = (
    ("start", "Старт и проверка подключения"),
    ("help", "Как работает calendar-first бот"),
    ("chat", "Чат статусов (менеджеры, личный чат)"),
    ("people", "Участники (менеджеры, личный чат)"),
)


_RU_WEEKDAYS_SHORT: tuple[str, ...] = ("Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс")
_RU_MONTHS_GENITIVE: tuple[str, ...] = (
    "января",
    "февраля",
    "марта",
    "апреля",
    "мая",
    "июня",
    "июля",
    "августа",
    "сентября",
    "октября",
    "ноября",
    "декабря",
)


def normalize_timezone_name(tz_name: str | None) -> str:
    raw = (tz_name or "").strip()
    if not raw:
        return "UTC"
    try:
        _ = ZoneInfo(raw)
    except ZoneInfoNotFoundError:
        return "UTC"
    return raw


def humanize_timezone_name(tz_name: str | None) -> str:
    normalized = normalize_timezone_name(tz_name)
    if "/" not in normalized:
        return normalized
    city = normalized.rsplit("/", 1)[1]
    return city.replace("_", " ")


def resolve_timezone(tz_name: str | None) -> ZoneInfo:
    return ZoneInfo(normalize_timezone_name(tz_name))


def meeting_title_or_default(title: str) -> str:
    clean = title.strip()
    return clean if clean else "Встреча"


def format_local_datetime(value: datetime, *, timezone_name: str) -> str:
    local = value.astimezone(resolve_timezone(timezone_name))
    weekday = _RU_WEEKDAYS_SHORT[local.weekday()]
    month = _RU_MONTHS_GENITIVE[local.month - 1]
    return f"{weekday}, {local.day:02d} {month} {local:%H:%M}"


def format_local_range(
    start_at: datetime,
    end_at: datetime,
    *,
    timezone_name: str,
) -> str:
    local_start = start_at.astimezone(resolve_timezone(timezone_name))
    local_end = end_at.astimezone(resolve_timezone(timezone_name))

    start_weekday = _RU_WEEKDAYS_SHORT[local_start.weekday()]
    start_month = _RU_MONTHS_GENITIVE[local_start.month - 1]

    if local_start.date() == local_end.date():
        return (
            f"{start_weekday}, {local_start.day:02d} {start_month} "
            f"{local_start:%H:%M}-{local_end:%H:%M}"
        )

    end_weekday = _RU_WEEKDAYS_SHORT[local_end.weekday()]
    end_month = _RU_MONTHS_GENITIVE[local_end.month - 1]
    return (
        f"{start_weekday}, {local_start.day:02d} {start_month} {local_start:%H:%M}"
        f" — {end_weekday}, {local_end.day:02d} {end_month} {local_end:%H:%M}"
    )


def telegram_commands_payload() -> list[dict[str, str]]:
    return [
        {"command": command, "description": description}
        for command, description in BOT_COMMANDS
    ]


def main_menu_keyboard() -> list[list[str]]:
    return [list(row) for row in MAIN_MENU_KEYBOARD_LAYOUT]
