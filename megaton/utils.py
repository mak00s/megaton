"""
Common Functions
"""

from urllib.parse import unquote
import re
from datetime import datetime, timedelta

import pandas as pd
import pytz
from dateutil.relativedelta import relativedelta

DEFAULT_TIMEZONE = "UTC"


def is_integer(n):
    """Determines the provided string is an integer number."""
    try:
        float(n)
    except ValueError:
        return False
    else:
        return float(n).is_integer()


def extract_integer_from_string(s):
    """Extracts integer from string provided."""
    m = re.search(r'(\d+)', s)
    if m:
        return int(m.group(1))


def change_column_type(df: pd.DataFrame, to_date=None, to_datetime=None):
    """Changes column type in dataframe from str to date or datetime."""
    if not to_date:
        to_date = ['date', 'firstSessionDate']
    if not to_datetime:
        to_datetime = ['dateHour', 'dateHourMinute']

    for col in df.columns:
        if col in to_date:
            df[col] = pd.to_datetime(df[col], errors='coerce').dt.date
        if col in to_datetime:
            df[col] = pd.to_datetime(df[col], errors='coerce')

    return df


def replace_columns(df: pd.DataFrame, rules: list):
    """Converts dataframe columns using regex.

    Args
        df: dataframe to be converted
        rules: list of tuple (column name, regex, to)
    """
    for r in rules:
        col, rule, to = r
        try:
            df[col] = df[col].replace(rule, to, regex=True)
        except KeyError as e:
            print(e)
            pass


def prep_df(df, delete_columns: list = None, type_columns: dict = None, rename_columns: dict = None):
    """Processes dataframe

    Args
        delete_columns:
            list of column name to be deleted
        type_columns:
            dict of column name -> data type
            ex. {'pageviews': 'int32'}
        rename_columns:
            dict of column name -> new column name
    Returns
        processed dataframe
    """
    if len(df) > 0:
        if delete_columns:
            # delete
            df.drop(delete_columns, axis=1, inplace=True)
        if type_columns:
            # change type
            df = df.astype(type_columns)
        if rename_columns:
            # rename
            df.columns = df.columns.to_series().replace(rename_columns, regex=True)
    return df


def get_date_range(start_date: str, end_date: str, format_: str = '%Y-%m-%d'):
    """Converts date range to a list of each date in the range."""
    date_range = pd.date_range(start_date, end_date)
    return [d.strftime(format_) for d in date_range]


def get_chunked_list(original_list: list, chunk_size: int = 100):
    """Splits a list into chunks."""
    chunked_list = []
    for i in range(0, len(original_list), chunk_size):
        chunked_list.append(original_list[i:i + chunk_size])
    return chunked_list


def get_past_date(n_days=None, n_months=None, timezone=DEFAULT_TIMEZONE, return_date_obj=False):
    """Return a date string (YYYY-MM-DD) N days or months ago.

    Args:
        n_days: Number of days to subtract from today.
        n_months: Number of months to subtract (returns first day of that month).
        timezone: Olson timezone name used for "today".
        return_date_obj: When True, return ``datetime.date`` instead of string.

    Raises:
        ValueError: When both ``n_days`` and ``n_months`` are provided.
    """
    tz = pytz.timezone(timezone)
    now = datetime.now(tz)

    if n_days is None and n_months is None:
        result = now
    elif n_days is not None and n_months is not None:
        raise ValueError("Specify either 'n_days' or 'n_months', not both.")
    elif n_days is not None:
        result = now - timedelta(days=n_days)
    else:
        start_of_month = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        result = start_of_month - relativedelta(months=n_months)

    return result.date() if return_date_obj else result.strftime("%Y-%m-%d")


def get_clean_url(url: str, params_to_keep: list = None):
    """Remove parameters from URL Query String"""

    if not params_to_keep:
        params_to_keep = []

    if "?" in url:
        base_url, arglist = url.split("?", 1)
        args = arglist.split("&")
        new_args = []
        for arg in args:
            try:
                k, v = arg.split("=", 1)
            except ValueError:
                k = arg
            if k.lower() in params_to_keep:
                new_args.append(unquote(arg))
                # print(f"keeping {arg}")
            else:
                # print(f"deleting {arg}")
                pass
        if len(new_args) > 0:
            # if param remains
            return "?".join([base_url, "&".join(new_args)])
        else:
            # all params are removed
            return base_url
    else:
        return url
