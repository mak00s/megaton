from __future__ import annotations

import re
from urllib.parse import unquote as url_unquote
from urllib.parse import urlsplit, urlunsplit

import pandas as pd


def map_by_regex(series, mapping, default=None, flags=0, lower=True, strip=True):
    if series is None or not mapping:
        return series

    def _map_value(value):
        if not isinstance(value, str):
            return value
        text = value
        if strip:
            text = text.strip()
        if lower:
            text = text.lower()
        for pattern, mapped in mapping.items():
            try:
                if re.search(pattern, text, flags=flags):
                    return mapped
            except re.error:
                continue
        return value if default is None else default

    return series.apply(_map_value)


def clean_url(series, unquote=True, drop_query=True, drop_hash=True, lower=True):
    if series is None:
        return series

    def _clean(value):
        if not isinstance(value, str):
            return value
        text = value.strip()
        if not text:
            return text
        parsed = urlsplit(text)
        path = url_unquote(parsed.path) if unquote else parsed.path
        query = "" if drop_query else parsed.query
        fragment = "" if drop_hash else parsed.fragment
        cleaned = urlunsplit((parsed.scheme, parsed.netloc, path, query, fragment))
        return cleaned.lower() if lower else cleaned

    return series.apply(_clean)


def normalize_whitespace(series, mode="remove_all"):
    if series is None:
        return series
    if mode not in {"remove_all", "collapse"}:
        raise ValueError(f"Unsupported mode: {mode}")

    def _normalize(value):
        if not isinstance(value, str):
            return value
        if mode == "remove_all":
            return re.sub(r"\s+", "", value)
        return re.sub(r"\s+", " ", value).strip()

    return series.apply(_normalize)


def force_text_if_numeric(series, prefix="'"):
    if series is None:
        return series

    def _force(value):
        if pd.isna(value):
            return value
        text = str(value)
        if re.fullmatch(r"\d+", text):
            return f"{prefix}{text}"
        return value

    return series.apply(_force)
