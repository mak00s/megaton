"""Utilities for loading config from Google Sheets."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import pandas as pd

from .. import errors
from typing import Any


@dataclass
class Config:
    sheet_url: str
    sites: list[dict]
    source_map: dict[str, str]
    page_map: dict[str, str]
    query_map: dict[str, str]
    # thresholds_df deprecated: thresholds are stored per-site in `sites` records
    thresholds_df: Optional[pd.DataFrame]
    group_domains: set[str]


def _to_map(df: pd.DataFrame, key_col: str, value_col: str, sheet_name: str) -> dict[str, str]:
    if df.empty:
        return {}
    missing = [col for col in [key_col, value_col] if col not in df.columns]
    if missing:
        raise ValueError(f"{sheet_name} sheet missing columns: {missing}")
    return dict(zip(df[key_col], df[value_col]))


def load_config(mg, sheet_url: str) -> Config:
    if not mg.open.sheet(sheet_url):
        raise RuntimeError(f"Failed to open config sheet: {sheet_url}")

    def read_sheet(name: str, *, required: bool = True) -> pd.DataFrame:
        try:
            mg.gs.sheet.select(name)
        except errors.SheetNotFound as exc:
            if required:
                raise ValueError(f"Required sheet not found: {name}") from exc
            return pd.DataFrame()
        data = mg.gs.sheet.data or []
        return pd.DataFrame(data)

    config_df = read_sheet("config", required=True)
    if config_df.empty:
        raise ValueError("config sheet is empty")
    if "clinic" not in config_df.columns:
        raise ValueError("config sheet missing columns: ['clinic']")

    source_map_df = read_sheet("source_map", required=False)
    page_map_df = read_sheet("page_map", required=False)
    query_map_df = read_sheet("query_map", required=False)

    source_map = _to_map(source_map_df, "pattern", "normalized", "source_map")
    page_map = _to_map(page_map_df, "pattern", "category", "page_map")
    query_map = _to_map(query_map_df, "pattern", "mapped_to", "query_map")

    # Sites rows now include threshold columns (min_impressions, max_position, min_pv, min_cv)
    # We no longer produce a separate thresholds_df; keep None for backward compatibility.
    thresholds_df = None

    sites: list[dict[str, Any]] = config_df.to_dict(orient="records")
    group_domains = set()
    for site in sites:
        domain = site.get("domain")
        if isinstance(domain, str):
            cleaned = domain.replace("www.", "").strip()
            if cleaned:
                group_domains.add(cleaned)

    return Config(
        sheet_url=sheet_url,
        sites=sites,
        source_map=source_map,
        page_map=page_map,
        query_map=query_map,
        thresholds_df=thresholds_df,
        group_domains=group_domains,
    )
