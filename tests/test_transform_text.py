import pandas as pd

from megaton.transform import text


def test_map_by_regex_maps_and_defaults():
    series = pd.Series([" Foo", "bar", "baz"])
    mapping = {r"foo": "X", r"bar": "Y"}
    result = text.map_by_regex(series, mapping, default="Z")
    assert result.tolist() == ["X", "Y", "Z"]


def test_clean_url_drops_query_and_hash_and_unquotes():
    series = pd.Series(
        [
            "https://example.com/Path?utm=1#frag",
            "https://example.com/%7Euser",
        ]
    )
    result = text.clean_url(series)
    assert result.tolist() == ["https://example.com/path", "https://example.com/~user"]


def test_normalize_whitespace_modes():
    series = pd.Series(["a  b", "c\t d"])
    collapsed = text.normalize_whitespace(series, mode="collapse")
    removed = text.normalize_whitespace(series, mode="remove_all")
    assert collapsed.tolist() == ["a b", "c d"]
    assert removed.tolist() == ["ab", "cd"]


def test_force_text_if_numeric_prefixes_digits():
    series = pd.Series(["123", "abc", 123])
    result = text.force_text_if_numeric(series)
    assert result.tolist() == ["'123", "abc", "'123"]
