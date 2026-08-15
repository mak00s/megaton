"""Static contracts for the small, canonical public API surface."""

from typing import assert_type

import pandas as pd

from megaton.start import Megaton, ReportResult, SearchResult, wrap


def check_public_api(
    mg: Megaton,
    df: pd.DataFrame,
    report: ReportResult,
    search: SearchResult,
) -> None:
    assert_type(wrap(df), ReportResult)
    assert_type(report.to_int().sort("date"), ReportResult)
    assert_type(search.normalize("query", by={r".*": "other"}), SearchResult)
    assert_type(mg.sheets.read("daily"), pd.DataFrame)

    mg.sheet.save(df)
    mg.sheet.append(df)
    mg.sheet.upsert(df, keys=["date"])
