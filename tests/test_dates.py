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


# ======================================================================
# Unified vocabulary (v1.5.0)
# ======================================================================

from datetime import date as _date

import pandas as _pd
import pytest as _pytest

from megaton.dates import (
    default_tz,
    drop_current_month_rows,
    month_before_window,
    month_ranges_between,
    month_ranges_for_year,
    months_between,
    previous_month_label,
    previous_month_window,
    resolve_calendar_token,
    resolve_date,
    resolve_month,
    resolve_period_date,
    select_recent_months,
    today_in_timezone,
)

_REF = _date(2026, 3, 15)


class TestResolveDateUnified:
    def test_absolute_passthrough_and_normalize(self):
        assert resolve_date("2026-05-01", reference=_REF) == "2026-05-01"
        assert resolve_date("20260501", reference=_REF) == "2026-05-01"

    def test_invalid_absolute_raises(self):
        with _pytest.raises(ValueError):
            resolve_date("2026-13-01", reference=_REF)

    def test_ga_tokens(self):
        assert resolve_date("today", reference=_REF) == "2026-03-15"
        assert resolve_date("yesterday", reference=_REF) == "2026-03-14"
        assert resolve_date("7daysAgo", reference=_REF) == "2026-03-08"

    def test_calendar_tokens(self):
        assert resolve_date("month-start", reference=_REF) == "2026-03-01"
        assert resolve_date("month-end", reference=_REF) == "2026-03-31"
        assert resolve_date("prev-month-start", reference=_REF) == "2026-02-01"
        assert resolve_date("prev-month-end", reference=_REF) == "2026-02-28"
        assert resolve_date("prev-prev-month-start", reference=_REF) == "2026-01-01"
        assert resolve_date("prev-prev-month-end", reference=_REF) == "2026-01-31"
        assert resolve_date("year-start", reference=_REF) == "2026-01-01"
        assert resolve_date("year-end", reference=_REF) == "2026-12-31"
        assert resolve_date("today-7d", reference=_REF) == "2026-03-08"
        assert resolve_date("today+3d", reference=_REF) == "2026-03-18"
        # 2026-03-15 is a Sunday; ISO week starts Monday 03-09
        assert resolve_date("week-start", reference=_REF) == "2026-03-09"

    def test_unknown_raises_with_vocabulary(self):
        with _pytest.raises(ValueError, match="prev-month-start"):
            resolve_date("next-month", reference=_REF)


class TestResolveMonth:
    def test_tokens_and_passthrough(self):
        assert resolve_month("this-month", reference=_REF) == "202603"
        assert resolve_month("prev-month", reference=_REF) == "202602"
        assert resolve_month("prev-prev-month", reference=_REF) == "202601"
        assert resolve_month("202512", reference=_REF) == "202512"

    def test_year_boundary(self):
        jan = _date(2026, 1, 10)
        assert resolve_month("prev-month", reference=jan) == "202512"
        assert resolve_month("prev-prev-month", reference=jan) == "202511"

    def test_invalid(self):
        with _pytest.raises(ValueError):
            resolve_month("next-month", reference=_REF)


class TestLenientResolverCalendarExtension:
    """resolve_relative_date_token gains calendar tokens; passthrough intact."""

    def test_calendar_token_resolved(self):
        from megaton.dates import resolve_relative_date_token
        from datetime import datetime

        now = datetime(2026, 3, 15, 12, 0)
        assert resolve_relative_date_token("prev-month-end", now=now) == "2026-02-28"

    def test_unknown_still_passes_through(self):
        from megaton.dates import resolve_relative_date_token

        assert resolve_relative_date_token("2026-01-01") == "2026-01-01"
        assert resolve_relative_date_token("not-a-token") == "not-a-token"


class TestGa4ReportCalendarWiring:
    def test_run_resolves_calendar_tokens_but_passes_ga_tokens(self, monkeypatch):
        from types import SimpleNamespace
        from megaton import ga4

        captured = {}

        parent = SimpleNamespace(
            property=SimpleNamespace(
                id="123",
                api_metadata={
                    "dimensions": [{"api_name": "date", "display_name": "Date"}],
                    "metrics": [{"api_name": "sessions", "display_name": "Sessions"}],
                },
            ),
            data_client=SimpleNamespace(),
        )
        report = ga4.MegatonGA4.Report(parent)
        report.set_dates("prev-month-start", "yesterday")

        def fake_format_request(**kwargs):
            captured.update(kwargs)
            raise RuntimeError("stop-here")

        monkeypatch.setattr(report, "_format_request", fake_format_request)
        with _pytest.raises(RuntimeError, match="stop-here"):
            report.run(["date"], ["sessions"])

        assert captured["start_date"].count("-") == 2  # resolved to YYYY-MM-DD
        assert captured["start_date"].endswith("-01")
        assert captured["end_date"] == "yesterday"  # GA-native token passes through


class TestDateObjectApi:
    def test_windows(self):
        assert previous_month_window(_REF) == (_date(2026, 2, 1), _date(2026, 2, 28))
        assert month_before_window(_date(2026, 2, 1)) == (_date(2026, 1, 1), _date(2026, 1, 31))

    def test_resolve_period_date(self):
        assert resolve_period_date("prev-prev-month-end", today=_REF) == _date(2026, 1, 31)
        assert resolve_period_date("2026-05-09", today=_REF) == _date(2026, 5, 9)

    def test_previous_month_label(self):
        assert previous_month_label(_REF) == "2026/02/01 - 2026/02/28"

    def test_today_in_timezone_utc_boundary(self):
        from datetime import UTC, datetime

        utc_now = datetime(2026, 5, 31, 15, 30, tzinfo=UTC)
        assert today_in_timezone("UTC", now=utc_now) == _date(2026, 5, 31)
        assert today_in_timezone("Asia/Tokyo", now=utc_now) == _date(2026, 6, 1)


class TestMonthRangeHelpers:
    def test_month_ranges_for_year(self):
        r = month_ranges_for_year(2025)
        assert r[0] == ("2025-01-01", "2025-01-31")
        assert r[11] == ("2025-12-01", "2025-12-31")

    def test_month_ranges_between_clamps(self):
        assert month_ranges_between("2025-03-15", "2025-05-10") == [
            ("2025-03-15", "2025-03-31"),
            ("2025-04-01", "2025-04-30"),
            ("2025-05-01", "2025-05-10"),
        ]

    def test_months_between(self):
        assert months_between("2025-11-01", "2026-01-31") == ["202511", "202512", "202601"]

    def test_drop_current_month_rows(self, monkeypatch):
        # Patch via the function's own globals so the test is robust to
        # module re-imports performed by other tests (e.g. auto-install).
        from datetime import datetime as _dt

        monkeypatch.setitem(
            drop_current_month_rows.__globals__, "now_in_tz",
            lambda tz=None: _dt(2026, 3, 10),
        )
        df = _pd.DataFrame({"ym": ["202602", "202603"], "v": [1, 2]})
        out = drop_current_month_rows(df, month_col="ym")
        assert out["ym"].tolist() == ["202602"]

    def test_select_recent_months(self):
        df = _pd.DataFrame({"ym": ["202501", "202502", "202603"], "v": [1, 2, 3]})
        out = select_recent_months(df, month_col="ym", months=2)
        assert out["ym"].tolist() == ["202603"]


class TestTzContract:
    def test_default_tz_env(self, monkeypatch):
        monkeypatch.setenv("MEGATON_TZ", "UTC")
        assert default_tz() == "UTC"
        monkeypatch.delenv("MEGATON_TZ")
        assert default_tz() == "Asia/Tokyo"

    def test_invalid_tz_falls_back(self):
        assert resolve_calendar_token("month-start", reference=_REF, tz="Not/AZone") == "2026-03-01"
