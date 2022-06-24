"""
Common Functions
"""

import os
import pandas as pd
import re


def is_integer(n):
    """Determine the provided string is an integer number"""
    try:
        float(n)
    except ValueError:
        return False
    else:
        return float(n).is_integer()


def extract_integer_from_string(s):
    m = re.search('(\d+)', s)
    if m:
        return int(m.group(1))


def append_suffix_to_filename(filename, suffix):
    name, ext = os.path.splitext(filename)
    ext = ext if ext else '.csv'
    return f"{name}{suffix}{ext}"


def change_column_type(df: pd.DataFrame, to_date=None, to_datetime=None):
    """Change column type in dataframe from str to date or datetime"""
    if not to_date:
        to_date = ['date', 'firstSessionDate']
    if not to_datetime:
        to_datetime = ['dateHour', 'dateHourMinute']

    for col in df.columns:
        if col in to_date:
            df[col] = pd.to_datetime(df[col], infer_datetime_format=True, errors='coerce').dt.date
        if col in to_datetime:
            df[col] = pd.to_datetime(df[col], infer_datetime_format=True, errors='coerce')

    return df


def format_df(df: pd.DataFrame, rules: list):
    """Convert dataframe columns using regex
    Args
        df: dataframe to be converted
        rules: list of tuple (column name, regex, to)
    """
    for r in rules:
        col, rule, to = r
        try:
            df[col].replace(rule, to, inplace=True, regex=True)
        except KeyError as e:
            print(e)
            pass


def save_df(df: pd.DataFrame, filename: str, format: str = 'CSV'):
    """DataFrameを保存"""
    df.to_csv(filename, index=False)


def get_date_range(start_date: str, end_date: str, format: str = None):
    """Convert date range to a list of each date in the range"""
    date_range = pd.date_range(start_date, end_date)
    if not format:
        format = '%Y-%m-%d'
    return [d.strftime(format) for d in date_range]


def get_chunked_list(original_list: list, chunk_size: int = 100):
    """Split a list into chunks"""
    chunked_list = []
    for i in range(0, len(original_list), chunk_size):
        chunked_list.append(original_list[i:i + chunk_size])
    return chunked_list
