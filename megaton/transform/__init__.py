from .classify import classify_by_regex, infer_label_by_domain
from .table import dedup_by_key, ensure_columns, group_sum, normalize_key_cols, weighted_avg
from .text import clean_url, force_text_if_numeric, map_by_regex, normalize_whitespace

__all__ = [
    "map_by_regex",
    "clean_url",
    "normalize_whitespace",
    "force_text_if_numeric",
    "ensure_columns",
    "normalize_key_cols",
    "dedup_by_key",
    "group_sum",
    "weighted_avg",
    "classify_by_regex",
    "infer_label_by_domain",
]
