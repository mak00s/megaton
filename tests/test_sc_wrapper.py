from datetime import datetime
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

from megaton import dates
from megaton.start import Megaton


def test_sc_sites_auto_fetches_once(monkeypatch):
    app = Megaton(None, headless=True)
    calls = {"count": 0}

    def fake_list_sites():
        calls["count"] += 1
        return ["https://example.com"]

    monkeypatch.setattr(app._gsc_service, "list_sites", fake_list_sites)

    assert app.sc.sites == ["https://example.com"]
    assert app.sc.sites == ["https://example.com"]
    assert calls["count"] == 1


def test_sc_refresh_sites_forces_fetch(monkeypatch):
    app = Megaton(None, headless=True)
    app.sc._sites = ["https://old.example.com"]
    calls = {"count": 0}

    def fake_list_sites():
        calls["count"] += 1
        return ["https://new.example.com"]

    monkeypatch.setattr(app._gsc_service, "list_sites", fake_list_sites)

    result = app.sc.refresh.sites()

    assert result == ["https://new.example.com"]
    assert app.sc.sites == ["https://new.example.com"]
    assert calls["count"] == 1


def test_sc_query_uses_report_dates_and_metrics(monkeypatch):
    app = Megaton(None, headless=True)
    app.ga = {
        "4": SimpleNamespace(report=SimpleNamespace(start_date="2024-01-01", end_date="2024-01-31"))
    }
    app.sc.use("https://example.com")

    called = {}

    def fake_query(**kwargs):
        called["kwargs"] = kwargs
        return "ok"

    monkeypatch.setattr(app._gsc_service, "query", fake_query)

    result = app.sc.query(dimensions=["page"], metrics=["clicks", "ctr"], limit=123)

    assert result == "ok"
    assert called["kwargs"]["site_url"] == "https://example.com"
    assert called["kwargs"]["start_date"] == "2024-01-01"
    assert called["kwargs"]["end_date"] == "2024-01-31"
    assert called["kwargs"]["row_limit"] == 123
    assert called["kwargs"]["metrics"] == ["clicks", "ctr"]


def test_sc_set_months_overrides_report_dates():
    app = Megaton(None, headless=True)
    app.ga = {
        "4": SimpleNamespace(report=SimpleNamespace(start_date="2024-02-01", end_date="2024-02-28"))
    }
    app.sc.use("https://example.com")

    now = datetime(2025, 3, 15, 10, 0, 0, tzinfo=ZoneInfo("Asia/Tokyo"))
    expected = dates.get_month_window(months_ago=1, window_months=1, tz="Asia/Tokyo", now=now)

    result = app.sc.set.months(ago=1, window_months=1, tz="Asia/Tokyo", now=now)

    assert result == expected
    assert app.sc.start_date == expected[0]
    assert app.sc.end_date == expected[1]
    assert app.sc.window["ym"] == expected[2]


def test_sc_query_requires_site():
    app = Megaton(None, headless=True)
    with pytest.raises(ValueError, match="site is not set"):
        app.sc.query(dimensions=["page"])
