import pytest
import requests

from agripulse.common.config import get_settings
from agripulse.common.http import DailyQuotaExceeded, HttpClient, RetryableHTTPError
from agripulse.common.runlog import RunRecord


class FakeResp:
    def __init__(self, status, payload=None, text=""):
        self.status_code = status
        self._payload = payload or {}
        self.text = text

    def json(self):
        return self._payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(str(self.status_code))


class FakeSession:
    def __init__(self, responses):
        self.responses = list(responses)
        self.headers = {}
        self.calls = 0

    def get(self, url, params=None, timeout=None):
        self.calls += 1
        return self.responses.pop(0)


def _client(responses):
    s = get_settings(http_min_interval_s=0, http_max_retries=3)
    return HttpClient(s, FakeSession(responses))


def test_table_name_and_landing():
    s = get_settings(catalog="x")
    assert s.table("bronze", "t") == "x.bronze.t"
    assert s.landing_root == "/Volumes/x/bronze/landing"


def test_retry_then_success(monkeypatch):
    monkeypatch.setattr("tenacity.nap.time.sleep", lambda *_: None)
    c = _client([FakeResp(503), FakeResp(429), FakeResp(200, {"ok": 1})])
    assert c.get_json("http://x") == {"ok": 1}
    assert c.session.calls == 3


def test_retry_exhausted(monkeypatch):
    monkeypatch.setattr("tenacity.nap.time.sleep", lambda *_: None)
    c = _client([FakeResp(500)] * 3)
    with pytest.raises(RetryableHTTPError):
        c.get_json("http://x")


def test_client_error_not_retried():
    c = _client([FakeResp(404)])
    with pytest.raises(requests.HTTPError):
        c.get_json("http://x")
    assert c.session.calls == 1


def test_run_record_row_shape():
    r = RunRecord(source="weather", layer="bronze")
    assert len(r.as_row()) == 10


def test_daily_quota_not_retried():
    c = _client([FakeResp(429, text="Daily API request limit exceeded")])
    with pytest.raises(DailyQuotaExceeded):
        c.get_json("http://x")
    assert c.session.calls == 1
