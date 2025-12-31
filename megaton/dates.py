"""Date utilities for reports."""

from __future__ import annotations

from datetime import datetime, timedelta
import re

import pytz
from dateutil.relativedelta import relativedelta


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
    """Return (date_from, date_to) based on the 13-month window logic."""
    now = datetime.now(pytz.timezone(tz))

    if target_months_ago == 0:
        base = now.replace(day=1)
        date_from = (base - relativedelta(months=12)).date().isoformat()
        date_to = (now - timedelta(days=1)).date().isoformat()
    else:
        base = now.replace(day=1) - relativedelta(months=target_months_ago)
        date_from = (base - relativedelta(months=12)).date().isoformat()
        date_to = (base + relativedelta(months=1) - timedelta(days=1)).date().isoformat()

    return date_from, date_to


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
    now = datetime.now(pytz.timezone(tz))

    if n_days is None and n_months is None:
        result_date = now
    elif n_days is not None and n_months is not None:
        raise ValueError("Specify either 'n_days' or 'n_months', not both.")
    elif n_days is not None:
        result_date = now - timedelta(days=n_days)
    else:
        result_date = now.replace(day=1) - relativedelta(months=n_months)

    return result_date.date() if return_date_obj else result_date.strftime("%Y-%m-%d")
