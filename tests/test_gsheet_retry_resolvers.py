from types import SimpleNamespace

import gspread
import pytest
import requests
from google.oauth2.credentials import Credentials

from megaton import errors
import megaton.gsheet as gsheet_module
from megaton.gsheet import MegatonGS, _get_status_code


def _new_gs():
    return MegatonGS.__new__(MegatonGS)


def test_get_status_code_returns_none_without_response_and_code_with_response():
    assert _get_status_code(RuntimeError("x")) is None

    exc = Exception("boom")
    exc.response = SimpleNamespace(status_code=503)
    assert _get_status_code(exc) == 503


def test_resolve_max_retries_defaults_and_floor(monkeypatch):
    gs = _new_gs()

    monkeypatch.delenv("MEGATON_GS_MAX_RETRIES", raising=False)
    assert gs._resolve_max_retries(None) == 3

    monkeypatch.setenv("MEGATON_GS_MAX_RETRIES", "invalid")
    assert gs._resolve_max_retries(None) == 3

    assert gs._resolve_max_retries(0) == 1


def test_resolve_backoff_factor_defaults_and_floor(monkeypatch):
    gs = _new_gs()

    monkeypatch.delenv("MEGATON_GS_BACKOFF_FACTOR", raising=False)
    assert gs._resolve_backoff_factor(None) == 2.0

    monkeypatch.setenv("MEGATON_GS_BACKOFF_FACTOR", "invalid")
    assert gs._resolve_backoff_factor(None) == 2.0

    assert gs._resolve_backoff_factor(-1) == 0.0


def test_resolve_max_wait_and_max_elapsed(monkeypatch):
    gs = _new_gs()

    monkeypatch.delenv("MEGATON_GS_MAX_WAIT", raising=False)
    monkeypatch.delenv("MEGATON_GS_MAX_ELAPSED", raising=False)
    assert gs._resolve_max_wait(None) is None
    assert gs._resolve_max_elapsed(None) is None

    monkeypatch.setenv("MEGATON_GS_MAX_WAIT", "invalid")
    monkeypatch.setenv("MEGATON_GS_MAX_ELAPSED", "invalid")
    assert gs._resolve_max_wait(None) is None
    assert gs._resolve_max_elapsed(None) is None

    assert gs._resolve_max_wait(0) is None
    assert gs._resolve_max_elapsed(-1) is None

    assert gs._resolve_max_wait(15) == 15.0
    assert gs._resolve_max_elapsed(20) == 20.0


def test_resolve_jitter_bounds(monkeypatch):
    gs = _new_gs()

    monkeypatch.delenv("MEGATON_GS_JITTER", raising=False)
    assert gs._resolve_jitter(None) == 0.0

    monkeypatch.setenv("MEGATON_GS_JITTER", "invalid")
    assert gs._resolve_jitter(None) == 0.0

    assert gs._resolve_jitter(-0.1) == 0.0
    assert gs._resolve_jitter(2.0) == 0.99
    assert gs._resolve_jitter(0.2) == 0.2


def test_call_with_retry_passes_retry_arguments(monkeypatch):
    gs = _new_gs()
    gs.max_retries = 4
    gs.backoff_factor = 1.7
    gs.max_wait = 30.0
    gs.max_elapsed = 90.0
    gs.jitter = 0.1

    captured = {}

    def _fake_expo_retry(func, **kwargs):
        captured.update(kwargs)
        return func()

    monkeypatch.setattr(gsheet_module.retry_utils, "expo_retry", _fake_expo_retry)

    result = gs.call_with_retry("op", lambda: "ok", retry_on_requests=True)

    assert result == "ok"
    assert captured["max_retries"] == 4
    assert captured["backoff_factor"] == 1.7
    assert captured["jitter"] == 0.1
    assert captured["max_wait"] == 30.0
    assert captured["max_elapsed"] == 90.0
    assert gspread.exceptions.APIError in captured["exceptions"]
    assert requests.exceptions.RequestException in captured["exceptions"]


def test_call_with_retry_marks_api_error_503_as_retryable(monkeypatch):
    gs = _new_gs()
    gs.max_retries = 3
    gs.backoff_factor = 2.0
    gs.max_wait = None
    gs.max_elapsed = None
    gs.jitter = 0.0

    fake_response = SimpleNamespace(status_code=503)
    api_error = Exception("boom")
    api_error.response = fake_response

    def _fake_expo_retry(_func, **kwargs):
        return kwargs["is_retryable"](api_error)

    monkeypatch.setattr(gsheet_module.retry_utils, "expo_retry", _fake_expo_retry)

    assert gs.call_with_retry("op", lambda: "ok") is True


def test_authorize_rejects_invalid_credential_type():
    gs = _new_gs()
    gs.credentials = object()
    gs.timeout = None

    with pytest.raises(errors.BadCredentialFormat):
        gs._authorize()

    assert gs.credentials is None


def test_authorize_rejects_insufficient_scope():
    gs = _new_gs()
    gs.credentials = Credentials(token="x", scopes=["https://www.googleapis.com/auth/spreadsheets"])
    gs.timeout = None

    with pytest.raises(errors.BadCredentialScope):
        gs._authorize()

    assert gs.credentials is None


def test_authorize_sets_timeout_when_client_created(monkeypatch):
    gs = _new_gs()
    gs.credentials = Credentials(token="x", scopes=list(MegatonGS.required_scopes))
    gs.timeout = 12.5

    fake_client = SimpleNamespace(http_client=SimpleNamespace(timeout=None))
    monkeypatch.setattr("megaton.gsheet.gspread.authorize", lambda _creds: fake_client)

    gs._authorize()

    assert gs._client is fake_client
    assert gs._client.http_client.timeout == 12.5


# --- quota-style 403 retry (v2.0.1, parity with megaton-app gspread_lowlevel) ---

import pytest as _pytest
from unittest.mock import MagicMock as _MagicMock

from megaton import gsheet as _gsheet


class _Fake403(Exception):
    def __init__(self, message):
        super().__init__(message)
        self.response = _MagicMock(status_code=403)


def _gs_with_fake_apierror(monkeypatch):
    import gspread

    monkeypatch.setattr(gspread.exceptions, "APIError", _Fake403)
    gs = _gsheet.MegatonGS.__new__(_gsheet.MegatonGS)
    gs.max_retries = 3
    gs.backoff_factor = 2.0
    gs.max_wait = None
    gs.max_elapsed = None
    gs.jitter = 0.0
    return gs


@_pytest.mark.parametrize(
    "message",
    ["userRateLimitExceeded", "Rate Limit Exceeded", "Quota exceeded for quota metric"],
)
def test_rate_limit_403_is_retried_with_quota_floor(monkeypatch, message):
    gs = _gs_with_fake_apierror(monkeypatch)
    sleeps = []
    calls = {"n": 0}

    def flaky():
        calls["n"] += 1
        if calls["n"] == 1:
            raise _Fake403(message)
        return "ok"

    assert gs.call_with_retry("op", flaky, sleep=sleeps.append) == "ok"
    assert calls["n"] == 2
    assert sum(sleeps) >= 30.0  # quota floor applied


def test_permission_403_fails_immediately(monkeypatch):
    gs = _gs_with_fake_apierror(monkeypatch)
    calls = {"n": 0}

    def denied():
        calls["n"] += 1
        raise _Fake403("The caller does not have permission")

    with _pytest.raises(_Fake403):
        gs.call_with_retry("op", denied, sleep=lambda *_: None)
    assert calls["n"] == 1
