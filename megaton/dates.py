"""Date utilities for reports."""

from __future__ import annotations

from collections import namedtuple
from datetime import datetime, timedelta
import re

from dateutil.relativedelta import relativedelta
from zoneinfo import ZoneInfo


DateWindow = namedtuple('DateWindow', [
    'start_iso',   # ISO 8601: YYYY-MM-DD
    'end_iso',     # ISO 8601: YYYY-MM-DD
    'start_ym',    # Year-Month: YYYYMM
    'end_ym',      # Year-Month: YYYYMM
    'start_ymd',   # Compact: YYYYMMDD
    'end_ymd',     # Compact: YYYYMMDD
])


def parse_end_date(raw_date_str: str) -> datetime:
    """Parse YYYY-MM-DD / YYYYMMDD / YYYY-MM / YYYYMM to datetime.

    If only year+month is provided, returns the last day of that month.
    """
    raw_date_str = raw_date_str.strip().replace("/", "-")

    try:
        if re.fullmatch(r"\d{8}", raw_date_str):  # YYYYMMDD
            return datetime.strptime(raw_date_str, "%Y%m%d")
        if re.fullmatch(r"\d{6}", raw_date_str):  # YYYYMM
            dt = datetime.strptime(raw_date_str, "%Y%m")
            return (dt.replace(day=1) + relativedelta(months=1)) - timedelta(days=1)
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", raw_date_str):  # YYYY-MM-DD
            return datetime.strptime(raw_date_str, "%Y-%m-%d")
        if re.fullmatch(r"\d{4}-\d{2}", raw_date_str):  # YYYY-MM
            dt = datetime.strptime(raw_date_str, "%Y-%m")
            return (dt.replace(day=1) + relativedelta(months=1)) - timedelta(days=1)
        raise ValueError("Invalid date format")
    except Exception as exc:
        raise ValueError(f"Invalid end_date format: {exc}") from exc


def get_report_range(target_months_ago: int, tz: str = "Asia/Tokyo") -> tuple[str, str]:
    """Compatibility wrapper for the legacy 13-month window.

    Prefer get_month_window() for configurable window sizes and timezones.
    """
    result = get_month_window(
        months_ago=target_months_ago,
        window_months=13,
        tz=tz,
    )
    return result.start_iso, result.end_iso


def get_month_window(
    months_ago: int = 1,
    window_months: int = 13,
    *,
    tz: str = "Asia/Tokyo",
    now: datetime | None = None,
    min_ymd: str | None = None,
) -> DateWindow:
    """Return date range in multiple formats for a month window.

    Args:
        months_ago: Target month offset (0 = current month).
        window_months: Window size in months.
        tz: Timezone name.
        now: Fixed datetime for testing (timezone-aware or naive).
            If timezone-aware, it will be normalized to ``tz`` via ``astimezone``.
        min_ymd: Minimum start date constraint in YYYYMMDD format.
            If start_ymd is earlier than this, it will be clamped to min_ymd.

    Returns:
        DateWindow namedtuple with 6 fields:
            - start_iso (str): Start date in YYYY-MM-DD format
            - end_iso (str): End date in YYYY-MM-DD format
            - start_ym (str): Start year-month in YYYYMM format
            - end_ym (str): End year-month in YYYYMM format
            - start_ymd (str): Start date in YYYYMMDD format
            - end_ymd (str): End date in YYYYMMDD format

    Examples:
        >>> p = get_month_window(months_ago=1, window_months=13)
        >>> p.start_iso
        '2024-01-01'
        >>> p.start_ymd
        '20240101'

        # Tuple unpacking (backward compatible with first 3 elements)
        >>> start_iso, end_iso, start_ym = p[:3]
    """
    if months_ago < 0:
        raise ValueError("months_ago must be >= 0")
    if window_months < 1:
        raise ValueError("window_months must be >= 1")

    tzinfo = ZoneInfo(tz)
    if now is None:
        now_dt = datetime.now(tzinfo)
    else:
        now_dt = now.replace(tzinfo=tzinfo) if now.tzinfo is None else now.astimezone(tzinfo)

    base_month_start = now_dt.replace(day=1).date()
    target_month_start = base_month_start - relativedelta(months=months_ago)
    target_month_end = target_month_start + relativedelta(months=1) - timedelta(days=1)

    if months_ago == 0:
        date_to = now_dt.date() - timedelta(days=1)
    else:
        date_to = target_month_end

    if window_months == 1:
        date_from = target_month_start
    else:
        date_from = target_month_start - relativedelta(months=window_months - 1)

    # Generate all format variants
    start_iso = date_from.isoformat()
    end_iso = date_to.isoformat()
    start_ym = target_month_start.strftime("%Y%m")
    end_ym = date_to.strftime("%Y%m")
    start_ymd = date_from.strftime("%Y%m%d")
    end_ymd = date_to.strftime("%Y%m%d")

    # Apply min_ymd constraint if specified
    if min_ymd and start_ymd < min_ymd:
        start_ymd = min_ymd
        # Update start_iso to match
        start_iso = f"{min_ymd[:4]}-{min_ymd[4:6]}-{min_ymd[6:]}"
        # Update start_ym to match
        start_ym = min_ymd[:6]

    return DateWindow(
        start_iso=start_iso,
        end_iso=end_iso,
        start_ym=start_ym,
        end_ym=end_ym,
        start_ymd=start_ymd,
        end_ymd=end_ymd,
    )


def get_past_date(
    n_days: int | None = None,
    n_months: int | None = None,
    tz: str = "Asia/Tokyo",
    return_date_obj: bool = False,
):
    """Return a date string (YYYY-MM-DD) N days or months ago.

    Raises:
        ValueError: When both ``n_days`` and ``n_months`` are provided.
    """
    now = datetime.now(ZoneInfo(tz))

    if n_days is None and n_months is None:
        result_date = now
    elif n_days is not None and n_months is not None:
        raise ValueError("Specify either 'n_days' or 'n_months', not both.")
    elif n_days is not None:
        result_date = now - timedelta(days=n_days)
    else:
        result_date = now.replace(day=1) - relativedelta(months=n_months)

    return result_date.date() if return_date_obj else result_date.strftime("%Y-%m-%d")
