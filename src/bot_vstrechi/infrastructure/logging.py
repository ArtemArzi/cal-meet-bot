from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import cast

from typing_extensions import override


_STANDARD_LOG_RECORD_KEYS = {
    "args",
    "asctime",
    "created",
    "exc_info",
    "exc_text",
    "filename",
    "funcName",
    "levelname",
    "levelno",
    "lineno",
    "module",
    "msecs",
    "message",
    "msg",
    "name",
    "pathname",
    "process",
    "processName",
    "relativeCreated",
    "stack_info",
    "thread",
    "threadName",
    "taskName",
}


def _extract_context(record: logging.LogRecord) -> dict[str, object]:
    record_data = cast(dict[str, object], record.__dict__)
    context: dict[str, object] = {}
    for key, value_obj in record_data.items():
        if key in _STANDARD_LOG_RECORD_KEYS or key.startswith("_"):
            continue
        context[key] = value_obj
    return context


def _format_context_value(value_obj: object) -> str:
    if isinstance(value_obj, str):
        escaped = value_obj.replace('"', '\\"')
        if any(ch.isspace() for ch in escaped):
            return f'"{escaped}"'
        return escaped

    if isinstance(value_obj, (int, float, bool)) or value_obj is None:
        return str(value_obj)

    return json.dumps(value_obj, ensure_ascii=False, separators=(",", ":"), default=str)


class PrettyLogFormatter(logging.Formatter):
    @override
    def format(self, record: logging.LogRecord) -> str:
        local_ts = datetime.fromtimestamp(record.created, tz=timezone.utc).astimezone()
        message = (
            f"{local_ts:%Y-%m-%d %H:%M:%S} "
            f"{record.levelname:<8} {record.name} {record.getMessage()}"
        )

        context = _extract_context(record)
        if context:
            context_parts = [
                f"{key}={_format_context_value(value_obj)}"
                for key, value_obj in sorted(context.items())
            ]
            message = f"{message} | {' '.join(context_parts)}"

        if record.exc_info:
            message = f"{message}\n{self.formatException(record.exc_info)}"
        if record.stack_info:
            message = f"{message}\n{self.formatStack(record.stack_info)}"

        return message


class JsonLogFormatter(logging.Formatter):
    @override
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, object] = {
            "ts": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }

        for key, value_obj in _extract_context(record).items():
            payload[key] = value_obj

        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)
        if record.stack_info:
            payload["stack_info"] = self.formatStack(record.stack_info)

        return json.dumps(
            payload, ensure_ascii=False, separators=(",", ":"), default=str
        )


def configure_logging(level: str = "INFO", format: str = "json") -> None:
    handler = logging.StreamHandler()

    if format == "json":
        handler.setFormatter(JsonLogFormatter())
    elif format == "pretty":
        handler.setFormatter(PrettyLogFormatter())
    elif format == "text":
        handler.setFormatter(
            logging.Formatter("%(asctime)s %(levelname)-8s %(name)s %(message)s")
        )
    else:
        raise ValueError("format must be one of: 'json', 'pretty', 'text'")

    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(level.upper())

    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)
