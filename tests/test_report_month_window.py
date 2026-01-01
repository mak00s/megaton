from datetime import datetime
from types import SimpleNamespace
from zoneinfo import ZoneInfo

from megaton import dates
from megaton.start import Megaton


def test_report_set_month_window_calls_set_dates():
    report = Megaton.Report(SimpleNamespace(ga_ver=None))
    called = {}

    def fake_set_dates(date_from, date_to):
        called["args"] = (date_from, date_to)

    report.set_dates = fake_set_dates

    now = datetime(2025, 3, 15, 10, 0, 0, tzinfo=ZoneInfo("Asia/Tokyo"))
    expected = dates.get_month_window(1, 1, tz="Asia/Tokyo", now=now)

    result = report.set_month_window(1, 1, tz="Asia/Tokyo", now=now)

    assert result == expected
    assert called["args"] == expected[:2]
