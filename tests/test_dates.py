from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

from megaton import dates


def test_parse_end_date_accepts_multiple_formats():
    assert dates.parse_end_date("2024-02-29") == datetime(2024, 2, 29)
    assert dates.parse_end_date("20240229") == datetime(2024, 2, 29)
    assert dates.parse_end_date("2024-02") == datetime(2024, 2, 29)
    assert dates.parse_end_date("202402") == datetime(2024, 2, 29)


def test_get_report_range_uses_fixed_now(monkeypatch):
    class FixedDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return cls(2025, 1, 15, 12, 0, 0, tzinfo=tz)

    monkeypatch.setattr(dates, "datetime", FixedDateTime)

    start, end = dates.get_report_range(0)
    assert start == "2024-01-01"
    assert end == "2025-01-14"

    start, end = dates.get_report_range(2)
    assert start == "2023-11-01"
    assert end == "2024-11-30"


def test_get_past_date_days_and_months(monkeypatch):
    class FixedDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return cls(2025, 1, 15, 12, 0, 0, tzinfo=tz)

    monkeypatch.setattr(dates, "datetime", FixedDateTime)

    assert dates.get_past_date(n_days=1) == "2025-01-14"
    assert dates.get_past_date(n_months=1) == "2024-12-01"
    assert dates.get_past_date(return_date_obj=True) == FixedDateTime(2025, 1, 15).date()


def test_get_month_window_various_cases():
    now = datetime(2025, 3, 15, 10, 0, 0, tzinfo=ZoneInfo("Asia/Tokyo"))

    date_from, date_to, ym = dates.get_month_window(months_ago=1, window_months=1, now=now)
    assert date_from == "2025-02-01"
    assert date_to == "2025-02-28"
    assert ym == "2025-02"

    date_from, date_to, ym = dates.get_month_window(months_ago=1, window_months=13, now=now)
    assert date_from == "2024-02-01"
    assert date_to == "2025-02-28"
    assert ym == "2025-02"

    date_from, date_to, ym = dates.get_month_window(months_ago=0, window_months=1, now=now)
    assert date_from == "2025-03-01"
    assert date_to == "2025-03-14"
    assert ym == "2025-03"

    date_from, date_to, ym = dates.get_month_window(months_ago=0, window_months=13, now=now)
    assert date_from == "2024-03-01"
    assert date_to == "2025-03-14"
    assert ym == "2025-03"


def test_get_month_window_validation():
    now = datetime(2025, 3, 15, 10, 0, 0, tzinfo=ZoneInfo("Asia/Tokyo"))

    with pytest.raises(ValueError):
        dates.get_month_window(months_ago=-1, now=now)

    with pytest.raises(ValueError):
        dates.get_month_window(months_ago=1, window_months=0, now=now)
