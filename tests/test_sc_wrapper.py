from datetime import datetime
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

from megaton import dates
from megaton.start import Megaton


def test_search_sites_auto_fetches_once(monkeypatch):
    app = Megaton(None, headless=True)
    calls = {"count": 0}

    def fake_list_sites():
        calls["count"] += 1
        return ["https://example.com"]

    monkeypatch.setattr(app._gsc_service, "list_sites", fake_list_sites)

    assert app.search.sites == ["https://example.com"]
    assert app.search.sites == ["https://example.com"]
    assert calls["count"] == 1


def test_search_get_sites_forces_fetch(monkeypatch):
    app = Megaton(None, headless=True)
    app.search._sites = ["https://old.example.com"]
    calls = {"count": 0}

    def fake_list_sites():
        calls["count"] += 1
        return ["https://new.example.com"]

    monkeypatch.setattr(app._gsc_service, "list_sites", fake_list_sites)

    result = app.search.get.sites()

    assert result == ["https://new.example.com"]
    assert app.search.sites == ["https://new.example.com"]
    assert calls["count"] == 1


def test_search_sites_failure_does_not_cache(monkeypatch):
    app = Megaton(None, headless=True)
    calls = {"count": 0}

    def fail_then_succeed():
        calls["count"] += 1
        if calls["count"] == 1:
            raise RuntimeError("Search Console sites fetch failed.")
        return ["https://retry.example.com"]

    monkeypatch.setattr(app._gsc_service, "list_sites", fail_then_succeed)

    with pytest.raises(RuntimeError, match="sites fetch failed"):
        _ = app.search.sites
    assert app.search._sites is None

    result = app.search.get.sites()
    assert result == ["https://retry.example.com"]
    assert app.search._sites == ["https://retry.example.com"]
    assert calls["count"] == 2


def test_search_sites_caches_empty_list(monkeypatch):
    app = Megaton(None, headless=True)
    calls = {"count": 0}

    def return_empty():
        calls["count"] += 1
        return []

    monkeypatch.setattr(app._gsc_service, "list_sites", return_empty)

    assert app.search.sites == []
    assert app.search._sites == []
    assert app.search.sites == []
    assert calls["count"] == 1


def test_search_run_uses_report_dates_and_metrics(monkeypatch):
    app = Megaton(None, headless=True)
    app.ga = {
        "4": SimpleNamespace(report=SimpleNamespace(start_date="2024-01-01", end_date="2024-01-31"))
    }
    app.search.use("https://example.com")

    called = {}

    def fake_query(**kwargs):
        called["kwargs"] = kwargs
        return "ok"

    monkeypatch.setattr(app._gsc_service, "query", fake_query)

    result = app.search.run(dimensions=["page"], metrics=["clicks", "ctr"], limit=123)

    assert result == "ok"
    assert called["kwargs"]["site_url"] == "https://example.com"
    assert called["kwargs"]["start_date"] == "2024-01-01"
    assert called["kwargs"]["end_date"] == "2024-01-31"
    assert called["kwargs"]["row_limit"] == 123
    assert called["kwargs"]["metrics"] == ["clicks", "ctr"]
    assert app.search.data == "ok"


def test_search_set_months_overrides_report_dates():
    app = Megaton(None, headless=True)
    app.ga = {
        "4": SimpleNamespace(report=SimpleNamespace(start_date="2024-02-01", end_date="2024-02-28"))
    }
    app.search.use("https://example.com")

    now = datetime(2025, 3, 15, 10, 0, 0, tzinfo=ZoneInfo("Asia/Tokyo"))
    expected = dates.get_month_window(months_ago=1, window_months=1, tz="Asia/Tokyo", now=now)

    result = app.search.set.months(ago=1, window_months=1, tz="Asia/Tokyo", now=now)

    assert result == expected
    assert app.search.start_date == expected[0]
    assert app.search.end_date == expected[1]
    assert app.search.window["ym"] == expected[2]


def test_search_run_requires_site():
    app = Megaton(None, headless=True)
    with pytest.raises(ValueError, match="site is not set"):
        app.search.run(dimensions=["page"])


def test_sc_aliases_search():
    app = Megaton(None, headless=True)
    assert app.sc is app.search
