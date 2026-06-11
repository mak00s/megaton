"""Traffic-source primitives (domain/host normalization, dev-source detection).

Promoted from megaton-app ``megaton_lib/traffic.py`` so all repos share one
implementation. Business-specific channel classification (row-level rules,
company domain patterns) stays in megaton_lib.
"""

from __future__ import annotations

import ipaddress
import re
from typing import Mapping

import pandas as pd


def normalize_domain(value: str) -> str:
    """Normalize domain text for grouping/compare (strip scheme, www, path)."""
    v = str(value).strip().lower()
    v = re.sub(r"^https?://", "", v)
    v = v.split("/")[0]
    return v.replace("www.", "")


def source_host(value: object) -> str:
    """Return the host-like part of a GA source value.

    Handles scheme prefixes, paths/queries, userinfo, bracketed IPv6,
    and port suffixes. Returns "" for empty/placeholder values.
    """
    text = str(value or "").strip().lower()
    if not text or text in {"(not set)", "not set", "none", "nan"}:
        return ""
    text = re.sub(r"^[a-z][a-z0-9+.-]*://", "", text)
    text = text.split("/", 1)[0].split("?", 1)[0].strip()
    if "@" in text:
        text = text.rsplit("@", 1)[1]
    if text.startswith("["):
        bracket_end = text.find("]")
        if bracket_end != -1:
            return text[1:bracket_end]
    if text.count(":") == 1:
        host, port = text.rsplit(":", 1)
        if port.isdigit():
            text = host
    return re.sub(r"^www\.", "", text.strip("."))


def is_non_public_dev_source(value: object) -> bool:
    """True for localhost / non-public IP sources that should not be attribution."""
    host = source_host(value)
    if not host:
        return False
    if host == "localhost" or host.endswith(".localhost"):
        return True
    try:
        ip = ipaddress.ip_address(host)
    except ValueError:
        return False
    return not ip.is_global


def ensure_trailing_slash(path: str, *, preserve_suffixes: tuple[str, ...] = (".html", "/")) -> str:
    """Append ``/`` unless path already ends with known suffixes."""
    text = str(path or "")
    if text.endswith(preserve_suffixes):
        return text
    return text + "/"


def apply_source_normalization(
    df: pd.DataFrame,
    source_map: Mapping[str, str],
    *,
    source_col: str = "source",
) -> pd.DataFrame:
    """Normalize a GA source column with a regex map.

    Input source values are lowercased before matching. Unmatched values
    keep the lowercased text. Invalid patterns are warned and skipped.
    """
    if source_col not in df.columns:
        return df

    def normalize(value: object) -> str:
        src = str(value or "").lower().strip()
        for pattern, normalized in source_map.items():
            try:
                if re.search(str(pattern), src):
                    return str(normalized)
            except re.error as exc:
                print(f"[warn] invalid regex pattern in source_map: {pattern} ({exc})")
        return src

    out = df.copy()
    out[source_col] = out[source_col].apply(normalize)
    return out
