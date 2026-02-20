from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import cast
from urllib.parse import quote_plus

import httpx
import jwt


GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"
GOOGLE_CALENDAR_BASE_URL = "https://www.googleapis.com/calendar/v3"
JWT_GRANT_TYPE = "urn:ietf:params:oauth:grant-type:jwt-bearer"


def _parse_event_datetime(value: object) -> datetime | None:
    if not isinstance(value, dict):
        return None

    date_time_obj = value.get("dateTime")
    if isinstance(date_time_obj, str) and date_time_obj:
        parsed = datetime.fromisoformat(date_time_obj.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed

    date_obj = value.get("date")
    if isinstance(date_obj, str) and date_obj:
        parsed = datetime.fromisoformat(date_obj)
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed

    return None


@dataclass(frozen=True)
class GoogleServiceAccountCredentials:
    client_email: str
    private_key: str
    token_uri: str = GOOGLE_TOKEN_URL
    private_key_id: str | None = None


@dataclass(frozen=True)
class _AccessToken:
    token: str
    expires_at_epoch: int


@dataclass(frozen=True)
class CalendarDeltaPage:
    items: list[dict[str, object]]
    next_page_token: str | None
    next_sync_token: str | None
    full_sync_required: bool = False


class GoogleServiceAccountCalendarClient:
    def __init__(
        self,
        *,
        credentials: GoogleServiceAccountCredentials,
        impersonation_subject: str,
        scopes: tuple[str, ...] = (
            "https://www.googleapis.com/auth/calendar.events",
            "https://www.googleapis.com/auth/calendar.readonly",
        ),
        timeout_seconds: float = 10.0,
        max_attempts: int = 3,
        backoff_base_seconds: float = 0.3,
        http_client: httpx.Client | None = None,
    ) -> None:
        self._credentials: GoogleServiceAccountCredentials = credentials
        self._impersonation_subject: str = impersonation_subject
        self._scopes: tuple[str, ...] = scopes
        self._timeout_seconds: float = timeout_seconds
        self._max_attempts: int = max_attempts
        self._backoff_base_seconds: float = backoff_base_seconds
        self._http_client: httpx.Client = http_client or httpx.Client()
        self._token_cache: dict[str, _AccessToken] = {}
        self._applied_patch_keys: set[str] = set()

    def _is_omitted_attendees_error(self, response: httpx.Response) -> bool:
        if response.status_code != 400:
            return False
        return "omittedAttendeesSpecified" in response.text

    def query_free_busy(
        self,
        *,
        emails: tuple[str, ...],
        time_min: datetime,
        time_max: datetime,
    ) -> dict[str, list[tuple[datetime, datetime]]]:
        if not emails:
            return {}

        token = self._get_access_token(subject=self._impersonation_subject)
        url = f"{GOOGLE_CALENDAR_BASE_URL}/freeBusy"

        payload = {
            "timeMin": time_min.isoformat(),
            "timeMax": time_max.isoformat(),
            "items": [{"id": email} for email in emails],
        }

        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }

        for attempt in range(1, self._max_attempts + 1):
            try:
                response = self._http_client.post(
                    url,
                    json=payload,
                    headers=headers,
                    timeout=self._timeout_seconds,
                )
            except httpx.HTTPError as error:
                if attempt == self._max_attempts:
                    raise RuntimeError(
                        "Google Calendar FreeBusy request failed"
                    ) from error
                time.sleep(self._backoff_seconds(attempt=attempt))
                continue

            if response.status_code in {429, 500, 502, 503, 504}:
                if attempt == self._max_attempts:
                    raise RuntimeError(
                        f"Google Calendar FreeBusy failed with status {response.status_code}"
                    )
                time.sleep(self._backoff_seconds(attempt=attempt))
                continue

            if response.is_error:
                raise RuntimeError(
                    f"Google Calendar FreeBusy non-retryable status {response.status_code}: {response.text}"
                )

            data_obj: object = response.json()
            if not isinstance(data_obj, dict):
                raise RuntimeError("Google Calendar FreeBusy returned invalid payload")
            data = cast(dict[str, object], data_obj)

            calendars_obj = data.get("calendars")
            if not isinstance(calendars_obj, dict):
                return {}
            calendars = cast(dict[str, dict[str, object]], calendars_obj)

            result = {}
            for email, cal_data in calendars.items():
                busy_intervals = []
                busy_list = cal_data.get("busy")
                if isinstance(busy_list, list):
                    for interval in busy_list:
                        if (
                            isinstance(interval, dict)
                            and "start" in interval
                            and "end" in interval
                        ):
                            start = datetime.fromisoformat(
                                str(interval["start"]).replace("Z", "+00:00")
                            )
                            end = datetime.fromisoformat(
                                str(interval["end"]).replace("Z", "+00:00")
                            )
                            busy_intervals.append((start, end))
                result[email] = busy_intervals

            return result

        return {}

    def insert_event(
        self,
        *,
        organizer_email: str,
        payload: dict[str, object],
        idempotency_key: str | None = None,
    ) -> str:
        token = self._get_access_token(subject=organizer_email)
        url = (
            f"{GOOGLE_CALENDAR_BASE_URL}/calendars/{quote_plus(organizer_email)}/events"
        )
        params = {"sendUpdates": "all"}

        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }
        if idempotency_key:
            headers["X-Idempotency-Key"] = idempotency_key

        for attempt in range(1, self._max_attempts + 1):
            try:
                response = self._http_client.post(
                    url,
                    params=params,
                    json=payload,
                    headers=headers,
                    timeout=self._timeout_seconds,
                )
            except httpx.HTTPError as error:
                if attempt == self._max_attempts:
                    raise RuntimeError(
                        "Google Calendar insert request failed"
                    ) from error
                time.sleep(self._backoff_seconds(attempt=attempt))
                continue

            if response.status_code in {429, 500, 502, 503, 504}:
                if attempt == self._max_attempts:
                    raise RuntimeError(
                        f"Google Calendar insert failed with status {response.status_code}"
                    )
                time.sleep(self._backoff_seconds(attempt=attempt))
                continue

            if response.is_error:
                raise RuntimeError(
                    f"Google Calendar insert non-retryable status {response.status_code}: {response.text}"
                )

            data = cast(dict[str, object], response.json())
            return str(data["id"])

        raise RuntimeError("Google Calendar insert failed after all attempts")

    def patch_event(
        self,
        *,
        google_event_id: str,
        initiator_google_email: str,
        payload: dict[str, object],
        idempotency_key: str | None = None,
    ) -> None:
        if idempotency_key and idempotency_key in self._applied_patch_keys:
            return

        patch_payload = dict(payload)
        send_updates = "all"
        send_updates_obj = patch_payload.pop("_send_updates", None)
        if isinstance(send_updates_obj, str) and send_updates_obj in {
            "all",
            "externalOnly",
            "none",
        }:
            send_updates = send_updates_obj

        token = self._get_access_token(subject=initiator_google_email)
        event_url = (
            f"{GOOGLE_CALENDAR_BASE_URL}/calendars/"
            f"{quote_plus(initiator_google_email)}/events/{quote_plus(google_event_id)}"
        )
        params = {"sendUpdates": send_updates}

        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }
        if idempotency_key:
            headers["X-Idempotency-Key"] = idempotency_key

        for attempt in range(1, self._max_attempts + 1):
            try:
                response = self._http_client.patch(
                    event_url,
                    params=params,
                    json=patch_payload,
                    headers=headers,
                    timeout=self._timeout_seconds,
                )
            except httpx.HTTPError as error:
                if attempt == self._max_attempts:
                    raise RuntimeError(
                        "Google Calendar patch request failed"
                    ) from error
                time.sleep(self._backoff_seconds(attempt=attempt))
                continue

            if response.status_code in {429, 500, 502, 503, 504}:
                if attempt == self._max_attempts:
                    raise RuntimeError(
                        f"Google Calendar patch failed with status {response.status_code}"
                    )
                time.sleep(self._backoff_seconds(attempt=attempt))
                continue

            if (
                self._is_omitted_attendees_error(response)
                and patch_payload.get("attendeesOmitted") is True
            ):
                fallback_payload = dict(patch_payload)
                del fallback_payload["attendeesOmitted"]
                try:
                    response = self._http_client.patch(
                        event_url,
                        params=params,
                        json=fallback_payload,
                        headers=headers,
                        timeout=self._timeout_seconds,
                    )
                except httpx.HTTPError as error:
                    if attempt == self._max_attempts:
                        raise RuntimeError(
                            "Google Calendar patch request failed"
                        ) from error
                    time.sleep(self._backoff_seconds(attempt=attempt))
                    continue

                if response.status_code in {429, 500, 502, 503, 504}:
                    if attempt == self._max_attempts:
                        raise RuntimeError(
                            f"Google Calendar patch failed with status {response.status_code}"
                        )
                    time.sleep(self._backoff_seconds(attempt=attempt))
                    continue

            if response.is_error:
                raise RuntimeError(
                    f"Google Calendar patch non-retryable status {response.status_code}: {response.text}"
                )

            if idempotency_key:
                self._applied_patch_keys.add(idempotency_key)
            return

    def list_events(
        self,
        *,
        email: str,
        time_min: datetime,
        time_max: datetime,
        max_results: int = 100,
    ) -> list[dict[str, object]]:
        token = self._get_access_token(subject=email)
        events_url = f"{GOOGLE_CALENDAR_BASE_URL}/calendars/{quote_plus(email)}/events"
        headers = {"Authorization": f"Bearer {token}"}
        params = {
            "singleEvents": "true",
            "orderBy": "startTime",
            "timeMin": time_min.isoformat(),
            "timeMax": time_max.isoformat(),
            "maxResults": str(max_results),
        }

        for attempt in range(1, self._max_attempts + 1):
            try:
                response = self._http_client.get(
                    events_url,
                    params=params,
                    headers=headers,
                    timeout=self._timeout_seconds,
                )
            except httpx.HTTPError as error:
                if attempt == self._max_attempts:
                    raise RuntimeError(
                        "Google Calendar events.list request failed"
                    ) from error
                time.sleep(self._backoff_seconds(attempt=attempt))
                continue

            if response.status_code in {429, 500, 502, 503, 504}:
                if attempt == self._max_attempts:
                    raise RuntimeError(
                        f"Google Calendar events.list failed with status {response.status_code}"
                    )
                time.sleep(self._backoff_seconds(attempt=attempt))
                continue

            if response.is_error:
                raise RuntimeError(
                    f"Google Calendar events.list non-retryable status {response.status_code}: {response.text}"
                )

            payload_obj: object = response.json()
            if not isinstance(payload_obj, dict):
                return []

            items_obj = payload_obj.get("items")
            if not isinstance(items_obj, list):
                return []

            events: list[dict[str, object]] = []
            for item_obj in items_obj:
                if not isinstance(item_obj, dict):
                    continue
                start_at = _parse_event_datetime(item_obj.get("start"))
                end_at = _parse_event_datetime(item_obj.get("end"))
                if start_at is None or end_at is None:
                    continue

                summary_obj = item_obj.get("summary")
                summary = "Без названия"
                if isinstance(summary_obj, str) and summary_obj.strip():
                    summary = summary_obj.strip()

                events.append(
                    {
                        "start": start_at,
                        "end": end_at,
                        "summary": summary,
                    }
                )

            return events

        return []

    def list_event_deltas(
        self,
        *,
        email: str,
        sync_token: str | None,
        page_token: str | None,
        time_min: datetime,
        max_results: int = 250,
    ) -> CalendarDeltaPage:
        token = self._get_access_token(subject=email)
        events_url = f"{GOOGLE_CALENDAR_BASE_URL}/calendars/{quote_plus(email)}/events"
        headers = {"Authorization": f"Bearer {token}"}

        params: dict[str, str] = {
            "singleEvents": "true",
            "showDeleted": "true",
            "maxResults": str(max_results),
        }
        if isinstance(sync_token, str) and sync_token.strip():
            params["syncToken"] = sync_token
        else:
            params["orderBy"] = "updated"
            params["timeMin"] = time_min.isoformat()
        if isinstance(page_token, str) and page_token.strip():
            params["pageToken"] = page_token

        for attempt in range(1, self._max_attempts + 1):
            try:
                response = self._http_client.get(
                    events_url,
                    params=params,
                    headers=headers,
                    timeout=self._timeout_seconds,
                )
            except httpx.HTTPError as error:
                if attempt == self._max_attempts:
                    raise RuntimeError(
                        "Google Calendar delta sync request failed"
                    ) from error
                time.sleep(self._backoff_seconds(attempt=attempt))
                continue

            if response.status_code == 410:
                return CalendarDeltaPage(
                    items=[],
                    next_page_token=None,
                    next_sync_token=None,
                    full_sync_required=True,
                )

            if response.status_code in {429, 500, 502, 503, 504}:
                if attempt == self._max_attempts:
                    raise RuntimeError(
                        f"Google Calendar delta sync failed with status {response.status_code}"
                    )
                time.sleep(self._backoff_seconds(attempt=attempt))
                continue

            if response.is_error:
                raise RuntimeError(
                    f"Google Calendar delta sync non-retryable status {response.status_code}: {response.text}"
                )

            payload_obj: object = response.json()
            if not isinstance(payload_obj, dict):
                return CalendarDeltaPage(
                    items=[],
                    next_page_token=None,
                    next_sync_token=None,
                )

            items_obj = payload_obj.get("items")
            parsed_items: list[dict[str, object]] = []
            if isinstance(items_obj, list):
                for item_obj in items_obj:
                    if isinstance(item_obj, dict):
                        parsed_items.append(cast(dict[str, object], item_obj))

            next_page_token_obj = payload_obj.get("nextPageToken")
            next_sync_token_obj = payload_obj.get("nextSyncToken")

            return CalendarDeltaPage(
                items=parsed_items,
                next_page_token=(
                    next_page_token_obj.strip()
                    if isinstance(next_page_token_obj, str)
                    and next_page_token_obj.strip()
                    else None
                ),
                next_sync_token=(
                    next_sync_token_obj.strip()
                    if isinstance(next_sync_token_obj, str)
                    and next_sync_token_obj.strip()
                    else None
                ),
            )

        raise RuntimeError("Google Calendar delta sync failed after all attempts")

    def _get_access_token(self, *, subject: str) -> str:
        now_epoch = int(datetime.now(tz=timezone.utc).timestamp())
        cached = self._token_cache.get(subject)
        if cached and now_epoch < cached.expires_at_epoch - 30:
            return cached.token

        assertion = self._build_assertion(subject=subject, now_epoch=now_epoch)
        response = self._http_client.post(
            self._credentials.token_uri,
            data={
                "grant_type": JWT_GRANT_TYPE,
                "assertion": assertion,
            },
            timeout=self._timeout_seconds,
        )
        if response.is_error:
            raise RuntimeError(
                f"Token exchange failed with status {response.status_code}: {response.text}"
            )

        payload_obj: object = response.json()
        if not isinstance(payload_obj, dict):
            raise RuntimeError("Token exchange returned invalid payload")
        payload = cast(dict[str, object], payload_obj)

        token_obj = payload.get("access_token")
        expires_in_raw = payload.get("expires_in", 3600)
        if not isinstance(token_obj, str) or not token_obj:
            raise RuntimeError("Token exchange missing access_token")

        if isinstance(expires_in_raw, str):
            expires_in = int(expires_in_raw)
        elif isinstance(expires_in_raw, (int, float)):
            expires_in = int(expires_in_raw)
        else:
            raise RuntimeError("Token exchange invalid expires_in")

        self._token_cache[subject] = _AccessToken(
            token=token_obj,
            expires_at_epoch=now_epoch + expires_in,
        )
        return token_obj

    def _build_assertion(self, *, subject: str, now_epoch: int) -> str:
        claims: dict[str, object] = {
            "iss": self._credentials.client_email,
            "sub": subject,
            "scope": " ".join(self._scopes),
            "aud": self._credentials.token_uri,
            "iat": now_epoch,
            "exp": now_epoch + 3600,
        }
        headers: dict[str, str] = {"alg": "RS256", "typ": "JWT"}
        if self._credentials.private_key_id:
            headers["kid"] = self._credentials.private_key_id

        token = jwt.encode(
            claims,
            self._credentials.private_key,
            algorithm="RS256",
            headers=headers,
        )
        return str(token)

    def _backoff_seconds(self, *, attempt: int) -> float:
        return float(self._backoff_base_seconds * (2 ** (attempt - 1)))
