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

    fixed_now = FixedDateTime(2025, 1, 15, 12, 0, 0, tzinfo=ZoneInfo("Asia/Tokyo"))
    for months_ago in [0, 1, 2]:
        expected = dates.get_month_window(months_ago, 13, tz="Asia/Tokyo", now=fixed_now)
        start, end = dates.get_report_range(months_ago)
        assert (start, end) == expected[:2]


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

    # Test backward compatibility with tuple unpacking
    p = dates.get_month_window(months_ago=1, window_months=1, now=now)
    date_from, date_to, ym = p[:3]
    assert date_from == "2025-02-01"
    assert date_to == "2025-02-28"
    assert ym == "202502"

    # Test all DateWindow fields
    assert p.start_iso == "2025-02-01"
    assert p.end_iso == "2025-02-28"
    assert p.start_ym == "202502"
    assert p.end_ym == "202502"
    assert p.start_ymd == "20250201"
    assert p.end_ymd == "20250228"

    p = dates.get_month_window(months_ago=1, window_months=13, now=now)
    assert p.start_iso == "2024-02-01"
    assert p.end_iso == "2025-02-28"
    assert p.start_ym == "202502"  # Target month
    assert p.end_ym == "202502"
    assert p.start_ymd == "20240201"
    assert p.end_ymd == "20250228"

    p = dates.get_month_window(months_ago=0, window_months=1, now=now)
    assert p.start_iso == "2025-03-01"
    assert p.end_iso == "2025-03-14"
    assert p.start_ym == "202503"
    assert p.end_ym == "202503"

    p = dates.get_month_window(months_ago=0, window_months=13, now=now)
    assert p.start_iso == "2024-03-01"
    assert p.end_iso == "2025-03-14"
    assert p.start_ym == "202503"
    assert p.end_ym == "202503"


def test_get_month_window_min_ymd_constraint():
    """Test that min_ymd clamps start date to minimum constraint."""
    now = datetime(2025, 3, 15, 10, 0, 0, tzinfo=ZoneInfo("Asia/Tokyo"))

    # Without constraint, window extends to 2024-03-01
    p = dates.get_month_window(months_ago=0, window_months=13, now=now)
    assert p.start_ymd == "20240301"

    # With constraint, start date is clamped to 2024-06-01
    p = dates.get_month_window(months_ago=0, window_months=13, now=now, min_ymd="20240601")
    assert p.start_ymd == "20240601"
    assert p.start_iso == "2024-06-01"
    assert p.start_ym == "202406"
    assert p.end_ymd == "20250314"

    # Constraint is later than end date (no effect)
    p = dates.get_month_window(months_ago=1, window_months=1, now=now, min_ymd="20250301")
    assert p.start_ymd == "20250301"
    assert p.end_ymd == "20250228"  # End date unchanged


def test_get_month_window_validation():
    now = datetime(2025, 3, 15, 10, 0, 0, tzinfo=ZoneInfo("Asia/Tokyo"))

    with pytest.raises(ValueError):
        dates.get_month_window(months_ago=-1, now=now)

    with pytest.raises(ValueError):
        dates.get_month_window(months_ago=1, window_months=0, now=now)


def test_resolve_relative_date_token(monkeypatch):
    class FixedDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return cls(2025, 1, 15, 12, 0, 0, tzinfo=tz)

    monkeypatch.setattr(dates, "datetime", FixedDateTime)

    assert dates.resolve_relative_date_token("today") == "2025-01-15"
    assert dates.resolve_relative_date_token("yesterday") == "2025-01-14"
    assert dates.resolve_relative_date_token("7daysAgo") == "2025-01-08"
    assert dates.resolve_relative_date_token("2025-01-01") == "2025-01-01"
