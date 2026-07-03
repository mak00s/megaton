"""Date utilities for reports."""

from __future__ import annotations

import calendar
import os
import re
from collections import namedtuple
from datetime import date, datetime, time, timedelta

import pandas as pd
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


def resolve_relative_date_token(
    raw_date_str: str | None,
    *,
    tz: str = "Asia/Tokyo",
    now: datetime | None = None,
) -> str | None:
    """Resolve GA-style relative tokens to ISO date (YYYY-MM-DD).

    Supported tokens:
        - today
        - yesterday
        - NdaysAgo (e.g., 7daysAgo)
        - calendar tokens (v1.5+): today±Nd, month-start/end, year-start/end,
          week-start, prev-month-start/end, prev-prev-month-start/end

    Non-token inputs are returned as-is.
    """
    if raw_date_str is None:
        return None

    value = str(raw_date_str).strip()
    lowered = value.lower()

    if lowered == "today":
        days_ago = 0
    elif lowered == "yesterday":
        days_ago = 1
    else:
        matched = re.fullmatch(r"(\d+)daysago", lowered)
        if not matched:
            calendar_resolved = resolve_calendar_token(value, tz=tz, reference=now)
            return calendar_resolved if calendar_resolved is not None else value
        days_ago = int(matched.group(1))

    tzinfo = ZoneInfo(tz)
    if now is None:
        now_dt = datetime.now(tzinfo)
    else:
        now_dt = now.replace(tzinfo=tzinfo) if now.tzinfo is None else now.astimezone(tzinfo)

    return (now_dt.date() - timedelta(days=days_ago)).isoformat()


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


# ======================================================================
# Unified date-template vocabulary (v1.5.0)
#
# Merged from megaton-app's megaton_lib date stack so one resolver
# understands both GA-style tokens (today / yesterday / NdaysAgo) and
# calendar tokens (month-start / prev-month-end / today-7d / ...).
# ======================================================================

_CALENDAR_RELATIVE_RE = re.compile(r"^today([+-])(\d+)d$")


def default_tz() -> str:
    """Default timezone name: ``MEGATON_TZ`` env var, else Asia/Tokyo."""
    return (os.environ.get("MEGATON_TZ") or "").strip() or "Asia/Tokyo"


def _tzinfo(tz: str | None) -> ZoneInfo:
    """Resolve a timezone name to ZoneInfo with Asia/Tokyo fallback."""
    name = (tz or "").strip() or default_tz()
    try:
        return ZoneInfo(name)
    except Exception:
        return ZoneInfo("Asia/Tokyo")


def _reference_date(reference: date | datetime | None, tz: str | None) -> date:
    """Normalize an optional reference into a date in *tz*."""
    if reference is None:
        return datetime.now(_tzinfo(tz)).date()
    if isinstance(reference, datetime):
        if reference.tzinfo is None:
            return reference.date()
        return reference.astimezone(_tzinfo(tz)).date()
    return reference


def resolve_calendar_token(
    expr: str | None,
    *,
    reference: date | datetime | None = None,
    tz: str | None = None,
) -> str | None:
    """Resolve a calendar-style token to YYYY-MM-DD, or None if not one.

    Calendar tokens (GA-native tokens like ``today`` / ``NdaysAgo`` are NOT
    handled here so existing pass-through behavior stays intact):

        today+Nd / today-Nd
        month-start / month-end / year-start / year-end / week-start
        prev-month-start / prev-month-end
        prev-prev-month-start / prev-prev-month-end
    """
    if expr is None:
        return None
    token = str(expr).strip().lower()
    ref = _reference_date(reference, tz)

    m = _CALENDAR_RELATIVE_RE.fullmatch(token)
    if m:
        sign, days = m.group(1), int(m.group(2))
        delta = timedelta(days=days if sign == "+" else -days)
        return (ref + delta).isoformat()

    if token == "month-start":
        return ref.replace(day=1).isoformat()
    if token == "month-end":
        last = calendar.monthrange(ref.year, ref.month)[1]
        return ref.replace(day=last).isoformat()
    if token == "year-start":
        return ref.replace(month=1, day=1).isoformat()
    if token == "year-end":
        return ref.replace(month=12, day=31).isoformat()
    if token == "week-start":
        return (ref - timedelta(days=ref.weekday())).isoformat()
    if token == "prev-month-start":
        return (ref.replace(day=1) - timedelta(days=1)).replace(day=1).isoformat()
    if token == "prev-month-end":
        return (ref.replace(day=1) - timedelta(days=1)).isoformat()
    if token == "prev-prev-month-start":
        prev_first = (ref.replace(day=1) - timedelta(days=1)).replace(day=1)
        return (prev_first - timedelta(days=1)).replace(day=1).isoformat()
    if token == "prev-prev-month-end":
        prev_first = (ref.replace(day=1) - timedelta(days=1)).replace(day=1)
        return (prev_first - timedelta(days=1)).isoformat()
    return None


def resolve_date(
    expr: str,
    *,
    reference: date | datetime | None = None,
    tz: str | None = None,
) -> str:
    """Resolve a date expression to YYYY-MM-DD (strict; raises on unknown).

    Accepts absolute dates (YYYY-MM-DD / YYYYMMDD), GA-style tokens
    (today / yesterday / NdaysAgo) and calendar tokens
    (see :func:`resolve_calendar_token`).
    """
    value = str(expr).strip()

    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
        try:
            datetime.strptime(value, "%Y-%m-%d")
        except ValueError as exc:
            raise ValueError(f"Invalid absolute date: '{expr}'") from exc
        return value
    if re.fullmatch(r"\d{8}", value):
        try:
            return datetime.strptime(value, "%Y%m%d").strftime("%Y-%m-%d")
        except ValueError as exc:
            raise ValueError(f"Invalid absolute date: '{expr}'") from exc

    resolved = resolve_calendar_token(value, reference=reference, tz=tz)
    if resolved is not None:
        return resolved

    lowered = value.lower()
    ga_like = lowered in ("today", "yesterday") or re.fullmatch(r"\d+daysago", lowered)
    if ga_like:
        ref = _reference_date(reference, tz)
        if lowered == "today":
            return ref.isoformat()
        if lowered == "yesterday":
            return (ref - timedelta(days=1)).isoformat()
        days = int(re.fullmatch(r"(\d+)daysago", lowered).group(1))
        return (ref - timedelta(days=days)).isoformat()

    raise ValueError(
        f"Unknown date template: '{expr}'. "
        "Use today, yesterday, NdaysAgo, today±Nd, month-start, month-end, "
        "year-start, year-end, prev-month-start, prev-month-end, "
        "prev-prev-month-start, prev-prev-month-end, week-start, or YYYY-MM-DD."
    )


def resolve_month(
    expr: str,
    *,
    reference: date | datetime | None = None,
    tz: str | None = None,
) -> str:
    """Resolve a month expression to "YYYYMM".

    Accepts: this-month / prev-month / prev-prev-month / YYYYMM.
    """
    token = str(expr).strip()
    ref = _reference_date(reference, tz)
    lowered = token.lower()
    if lowered == "this-month":
        return f"{ref:%Y%m}"
    if lowered == "prev-month":
        return f"{ref.replace(day=1) - timedelta(days=1):%Y%m}"
    if lowered == "prev-prev-month":
        prev_first = (ref.replace(day=1) - timedelta(days=1)).replace(day=1)
        return f"{prev_first - timedelta(days=1):%Y%m}"
    if len(token) == 6 and token.isdigit():
        return token
    raise ValueError(
        f"Unknown month expression: '{expr}'. "
        "Use this-month, prev-month, prev-prev-month, or YYYYMM."
    )


# ----------------------------------------------------------------------
# Date-object API (windows as datetime.date tuples)
# ----------------------------------------------------------------------

def today_in_timezone(timezone: str | None = None, *, now: datetime | None = None) -> date:
    """Return today's date in the given timezone."""
    tzinfo = _tzinfo(timezone)
    current = now or datetime.now(tzinfo)
    if current.tzinfo is None:
        current = current.replace(tzinfo=tzinfo)
    return current.astimezone(tzinfo).date()


def previous_month_window(today: date | None = None, *, timezone: str | None = None) -> tuple[date, date]:
    """Return (first day, last day) of the previous month as date objects."""
    base = today or today_in_timezone(timezone)
    end = base.replace(day=1) - timedelta(days=1)
    return end.replace(day=1), end


def month_before_window(month_start: date) -> tuple[date, date]:
    """Return (first day, last day) of the month before *month_start*."""
    end = month_start - timedelta(days=1)
    return end.replace(day=1), end


def resolve_period_date(value: str, *, today: date | None = None, timezone: str | None = None) -> date:
    """Resolve a period token (or ISO date) to a ``datetime.date``."""
    token = str(value).strip()
    resolved = resolve_calendar_token(token, reference=today, tz=timezone)
    if resolved is not None:
        return date.fromisoformat(resolved)
    return date.fromisoformat(token)


def resolve_period_month(value: str, *, today: date | None = None, timezone: str | None = None) -> str:
    """Resolve a month token to "YYYYMM" (date-object-API counterpart of resolve_month)."""
    return resolve_month(value, reference=today, tz=timezone)


def previous_month_label(today: date | None = None, *, timezone: str | None = None) -> str:
    """Return 'YYYY/MM/DD - YYYY/MM/DD' label for the previous month."""
    start, end = previous_month_window(today, timezone=timezone)
    return f"{start:%Y/%m/%d} - {end:%Y/%m/%d}"


# ----------------------------------------------------------------------
# Month-range / DataFrame helpers (pandas)
# ----------------------------------------------------------------------

def month_ranges_for_year(year: int) -> list[tuple[str, str]]:
    """Return ``(start, end)`` ISO-date pairs for each month of *year*."""
    out: list[tuple[str, str]] = []
    for m in range(1, 13):
        last = calendar.monthrange(year, m)[1]
        out.append((date(year, m, 1).isoformat(), date(year, m, last).isoformat()))
    return out


def month_ranges_between(start: str, end: str) -> list[tuple[str, str]]:
    """Return ``(start, end)`` ISO-date pairs for each month in the range.

    Partial months are clamped to *start* / *end*.
    """
    sd = pd.to_datetime(start, errors="coerce")
    ed = pd.to_datetime(end, errors="coerce")
    if pd.isna(sd) or pd.isna(ed):
        return []
    sd_date = sd.date()
    ed_date = ed.date()
    out: list[tuple[str, str]] = []
    cur = date(sd_date.year, sd_date.month, 1)
    end_first = date(ed_date.year, ed_date.month, 1)
    while cur <= end_first:
        last = calendar.monthrange(cur.year, cur.month)[1]
        m_start = max(cur, sd_date)
        m_end = min(date(cur.year, cur.month, last), ed_date)
        out.append((m_start.isoformat(), m_end.isoformat()))
        cur = date(cur.year + 1, 1, 1) if cur.month == 12 else date(cur.year, cur.month + 1, 1)
    return out


def months_between(start, end) -> list[str]:
    """Return ``yyyymm`` strings for each month between two dates."""
    sd = pd.to_datetime(start, errors="coerce")
    ed = pd.to_datetime(end, errors="coerce")
    if pd.isna(sd) or pd.isna(ed):
        return []
    out: list[str] = []
    y, m = sd.year, sd.month
    while date(y, m, 1) <= ed.date():
        out.append(f"{y:04d}{m:02d}")
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return out


def _as_datetime(value: datetime | date | None, *, tz: str | None) -> datetime:
    """Normalize date/datetime into a timezone-aware datetime."""
    tzinfo = _tzinfo(tz)
    if value is None:
        return datetime.now(tzinfo)
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=tzinfo)
        return value.astimezone(tzinfo)
    return datetime.combine(value, time.min, tzinfo=tzinfo)


def _add_months(base: datetime, months_delta: int) -> datetime:
    """Return month-shifted datetime while keeping day clipped to month end."""
    month_index = base.year * 12 + (base.month - 1) + int(months_delta)
    year = month_index // 12
    month = (month_index % 12) + 1
    last_day = calendar.monthrange(year, month)[1]
    return base.replace(year=year, month=month, day=min(base.day, last_day))


def now_in_tz(tz: str | None = "Asia/Tokyo") -> datetime:
    """Return current timezone-aware datetime."""
    return datetime.now(_tzinfo(tz))


def previous_month_range(
    *,
    reference: datetime | date | None = None,
    tz: str = "Asia/Tokyo",
    out_fmt: str = "%Y-%m-%d",
) -> tuple[str, str]:
    """Return previous month start/end as formatted strings."""
    ref = _as_datetime(reference, tz=tz)
    prev_month_end = ref.replace(day=1) - timedelta(days=1)
    return prev_month_end.replace(day=1).strftime(out_fmt), prev_month_end.strftime(out_fmt)


def month_start_months_ago(
    months_ago: int,
    *,
    reference: datetime | date | None = None,
    tz: str = "Asia/Tokyo",
    out_fmt: str = "%Y-%m-%d",
) -> str:
    """Return month-start string for N months ago."""
    ref = _as_datetime(reference, tz=tz).replace(day=1)
    return _add_months(ref, -int(months_ago)).strftime(out_fmt)


def previous_year_start(
    *,
    reference: datetime | date | None = None,
    tz: str = "Asia/Tokyo",
    out_fmt: str = "%Y-%m-%d",
) -> str:
    """Return Jan 1 of previous year as formatted string."""
    ref = _as_datetime(reference, tz=tz)
    return ref.replace(year=ref.year - 1, month=1, day=1).strftime(out_fmt)


def month_suffix_months_ago(
    months_ago: int,
    *,
    reference: datetime | date | None = None,
    tz: str = "Asia/Tokyo",
    fmt: str = "%Y.%m",
) -> str:
    """Return month suffix (e.g. ``YYYY.MM``) for N months ago."""
    ref = _as_datetime(reference, tz=tz)
    return _add_months(ref, -int(months_ago)).strftime(fmt)


def parse_year_month_series(series: pd.Series) -> pd.Series:
    """Parse mixed month formats into month-start datetime.

    Accepted values include: ``202301``, ``202301.0``, ``2023-01``,
    ``2023/01``, ``2023年1月``.
    """
    if pd.api.types.is_datetime64_any_dtype(series):
        return pd.to_datetime(series).dt.to_period("M").dt.to_timestamp()

    s = series.astype("string").str.strip()
    s = s.str.replace(r"\.0$", "", regex=True)
    ym = s.str.extract(r"^\D*(\d{4})\D*?(\d{1,2})(?:\D|$)", expand=True)
    y = ym[0]
    m = ym[1]
    yyyymm = y.str.cat(m.str.zfill(2), na_rep="")
    yyyymm = yyyymm.where(y.notna() & m.notna(), pd.NA)
    return pd.to_datetime(yyyymm, format="%Y%m", errors="coerce")


def _to_month_start_series(series: pd.Series) -> pd.Series:
    """Normalize values to month-start ``Timestamp`` series."""
    if pd.api.types.is_datetime64_any_dtype(series):
        return pd.to_datetime(series, errors="coerce").dt.to_period("M").dt.to_timestamp()
    parsed = parse_year_month_series(series)
    if parsed.notna().any():
        return parsed
    return pd.to_datetime(series, errors="coerce").dt.to_period("M").dt.to_timestamp()


def drop_current_month_rows(
    df: pd.DataFrame,
    *,
    month_col: str,
    tz: str = "Asia/Tokyo",
) -> pd.DataFrame:
    """Drop rows that belong to the current month in *tz*."""
    month_series = _to_month_start_series(df[month_col])
    current_month = pd.Timestamp(now_in_tz(tz).date().replace(day=1))
    keep = month_series.isna() | (month_series != current_month)
    return df[keep].copy()


def select_recent_months(
    df: pd.DataFrame,
    *,
    month_col: str,
    months: int = 13,
) -> pd.DataFrame:
    """Filter DataFrame to recent N months from max(month_col)."""
    if df.empty:
        return df.copy()
    months = int(months)
    if months <= 0:
        return df.iloc[0:0].copy()
    month_series = _to_month_start_series(df[month_col])
    valid = month_series.dropna()
    if valid.empty:
        return df.iloc[0:0].copy()
    max_month = valid.max()
    start_month = (max_month - pd.DateOffset(months=months - 1)).replace(day=1)
    keep = month_series.notna() & (month_series >= start_month)
    return df[keep].copy()
