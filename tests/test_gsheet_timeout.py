import pytest
import requests

from megaton import errors
from megaton.gsheet import MegatonGS


def _new_gs():
    gs = MegatonGS.__new__(MegatonGS)
    return gs


def test_resolve_timeout_default(monkeypatch):
    monkeypatch.delenv("MEGATON_GS_TIMEOUT", raising=False)
    gs = _new_gs()
    assert gs._resolve_timeout(None) == 180.0


def test_resolve_timeout_env_override(monkeypatch):
    monkeypatch.setenv("MEGATON_GS_TIMEOUT", "45")
    gs = _new_gs()
    assert gs._resolve_timeout(None) == 45.0


def test_resolve_timeout_env_disable(monkeypatch):
    monkeypatch.setenv("MEGATON_GS_TIMEOUT", "0")
    gs = _new_gs()
    assert gs._resolve_timeout(None) is None


def test_open_raises_timeout():
    gs = _new_gs()

    class DummyClient:
        def open_by_url(self, url):
            raise requests.exceptions.Timeout("boom")

    gs._client = DummyClient()

    with pytest.raises(errors.Timeout):
        gs.open("https://docs.google.com/spreadsheets/d/xxxx")


def test_open_raises_request_error():
    gs = _new_gs()

    class DummyClient:
        def open_by_url(self, url):
            raise requests.exceptions.RequestException("boom")

    gs._client = DummyClient()

    with pytest.raises(errors.RequestError):
        gs.open("https://docs.google.com/spreadsheets/d/xxxx")
