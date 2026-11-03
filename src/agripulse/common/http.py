"""HTTP client: requests Session + tenacity exponential-backoff retry + client-side rate limit + timeout."""

from __future__ import annotations

import threading
import time
from typing import Any

import requests
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential_jitter,
)

from agripulse.common.config import Settings, get_settings


class RetryableHTTPError(Exception):
    """Raised for 429/5xx so tenacity retries."""


class DailyQuotaExceeded(Exception):
    """Daily API quota is gone; retrying within the day is pointless, so this is NOT retried."""


class HttpClient:
    def __init__(self, settings: Settings | None = None, session: requests.Session | None = None):
        self.settings = settings or get_settings()
        self.session = session or requests.Session()
        self.session.headers.setdefault("User-Agent", "agripulse/0.1 (data-engineering project)")
        self._lock = threading.Lock()
        self._last_call = 0.0

    def _throttle(self) -> None:
        with self._lock:
            wait = self.settings.http_min_interval_s - (time.monotonic() - self._last_call)
            if wait > 0:
                time.sleep(wait)
            self._last_call = time.monotonic()

    def get_json(self, url: str, params: dict[str, Any] | None = None) -> Any:
        @retry(
            reraise=True,
            stop=stop_after_attempt(self.settings.http_max_retries),
            wait=wait_exponential_jitter(initial=1, max=60),
            retry=retry_if_exception_type((RetryableHTTPError, requests.ConnectionError, requests.Timeout)),
        )
        def _call() -> Any:
            self._throttle()
            resp = self.session.get(url, params=params, timeout=self.settings.http_timeout_s)
            if resp.status_code == 429 and "daily" in resp.text.lower():
                raise DailyQuotaExceeded(f"daily quota exhausted for {url}: {resp.text[:200]}")
            if resp.status_code == 429 or resp.status_code >= 500:
                raise RetryableHTTPError(f"{resp.status_code} from {url}")
            resp.raise_for_status()
            return resp.json()

        return _call()

    def get_text(self, url: str, params: dict[str, Any] | None = None) -> str:
        self._throttle()
        resp = self.session.get(url, params=params, timeout=self.settings.http_timeout_s)
        resp.raise_for_status()
        return resp.text
