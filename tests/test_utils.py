import pandas as pd
import pytest

from megaton import utils


def test_is_integer_handles_numeric_strings():
    assert utils.is_integer("10")
    assert utils.is_integer("3.0")
    assert not utils.is_integer("3.14")
    assert not utils.is_integer("abc")


def test_extract_integer_from_string_returns_first_match():
    assert utils.extract_integer_from_string("id:123abc456") == 123
    assert utils.extract_integer_from_string("no-digits") is None


def test_get_date_range_inclusive_boundaries():
    result = utils.get_date_range("2023-01-01", "2023-01-03")
    assert result == ["2023-01-01", "2023-01-02", "2023-01-03"]


def test_get_chunked_list_respects_chunk_size():
    data = list(range(10))
    chunks = utils.get_chunked_list(data, chunk_size=4)
    assert chunks == [list(range(4)), list(range(4, 8)), list(range(8, 10))]


@pytest.mark.parametrize(
    "url,params_to_keep,expected",
    [
        ("https://example.com/path?utm_source=google&id=42", ["id"], "https://example.com/path?id=42"),
        ("https://example.com/path?utm_source=google", [], "https://example.com/path"),
        ("https://example.com/path", ["id"], "https://example.com/path"),
    ],
)
def test_get_clean_url_filters_query_parameters(url, params_to_keep, expected):
    assert utils.get_clean_url(url, params_to_keep=params_to_keep) == expected


def test_change_column_type_converts_dates_to_datetime_and_date():
    df = pd.DataFrame(
        {
            "date": ["2023-01-01"],
            "dateHour": ["2023-01-01 05"],
            "value": [1],
        }
    )

    result = utils.change_column_type(df.copy())

    assert str(result.loc[0, "date"]) == "2023-01-01"
    assert result.loc[0, "dateHour"].hour == 5
    assert result.loc[0, "value"] == 1


def test_parse_filter_conditions_accepts_ga4_custom_dimension_prefix():
    parsed = utils.parse_filter_conditions("customEvent:article_id!@not")
    assert parsed == [
        {
            "field": "customEvent:article_id",
            "operator": "!@",
            "value": "not",
        }
    ]
