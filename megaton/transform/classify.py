from __future__ import annotations

import re
from urllib.parse import urlparse


def classify_by_regex(df, src_col, mapping, out_col, default="other"):
    if src_col not in df.columns:
        raise ValueError(f"Missing source column: {src_col}")

    def _classify(value):
        if not isinstance(value, str):
            return default
        for pattern, label in mapping.items():
            try:
                if re.search(pattern, value):
                    return label
            except re.error:
                continue
        return default

    result = df.copy()
    result[out_col] = result[src_col].apply(_classify)
    return result


def infer_label_by_domain(series, domain_to_label_map, default="不明"):
    if series is None:
        return series

    def _infer(value):
        if not isinstance(value, str) or not value:
            return default
        domain = urlparse(value).netloc if "://" in value else value
        domain = domain.lower().strip()
        if domain.startswith("www."):
            domain = domain[4:]
        for key, label in domain_to_label_map.items():
            if key.lower() in domain:
                return label
        return default

    return series.apply(_infer)
