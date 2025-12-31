from __future__ import annotations

import numpy as np
import pandas as pd


def ensure_columns(df, columns, fill=None, drop_extra=True):
    result = df.copy()
    columns = list(columns)
    for col in columns:
        if col not in result.columns:
            result[col] = fill
    if drop_extra:
        result = result.loc[:, columns]
    return result


def normalize_key_cols(
    df,
    cols,
    to_str=True,
    strip=True,
    lower=False,
    remove_trailing_dot0=True,
):
    missing = [col for col in cols if col not in df.columns]
    if missing:
        raise ValueError(f"Missing columns: {missing}")

    result = df.copy()
    for col in cols:
        series = result[col]
        if to_str:
            series = series.astype(str)
            if remove_trailing_dot0:
                series = series.str.replace(r"\.0$", "", regex=True)
            if strip:
                series = series.str.strip()
            if lower:
                series = series.str.lower()
        result[col] = series
    return result


def dedup_by_key(df, key_cols, prefer_by=None, keep="first"):
    missing = [col for col in key_cols if col not in df.columns]
    if missing:
        raise ValueError(f"Missing key columns: {missing}")

    result = df.copy()
    if prefer_by:
        prefer_cols = [prefer_by] if isinstance(prefer_by, str) else list(prefer_by)
        missing_prefer = [col for col in prefer_cols if col not in result.columns]
        if missing_prefer:
            raise ValueError(f"Missing prefer_by columns: {missing_prefer}")
        sort_cols = list(key_cols) + prefer_cols
        ascending = [True] * len(key_cols) + [False] * len(prefer_cols)
        result = result.sort_values(by=sort_cols, ascending=ascending)

    return result.drop_duplicates(subset=key_cols, keep=keep)


def group_sum(df, group_cols, sum_cols):
    missing = [col for col in list(group_cols) + list(sum_cols) if col not in df.columns]
    if missing:
        raise ValueError(f"Missing columns: {missing}")
    return df.groupby(group_cols, as_index=False)[sum_cols].sum()


def weighted_avg(df, group_cols, value_col, weight_col, out_col=None):
    missing = [col for col in list(group_cols) + [value_col, weight_col] if col not in df.columns]
    if missing:
        raise ValueError(f"Missing columns: {missing}")

    result_col = out_col or value_col
    tmp = df.copy()
    tmp["_weighted"] = tmp[value_col] * tmp[weight_col]
    agg = tmp.groupby(group_cols, as_index=False).agg(
        weight_sum=(weight_col, "sum"),
        weighted_sum=("_weighted", "sum"),
    )
    denom = agg["weight_sum"].replace(0, np.nan)
    agg[result_col] = agg["weighted_sum"] / denom
    return agg[list(group_cols) + [result_col]]
