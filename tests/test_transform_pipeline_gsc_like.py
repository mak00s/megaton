import pandas as pd

from megaton.transform import classify, table, text


def test_gsc_like_transform_pipeline():
    df = pd.DataFrame(
        [
            {
                "month": "202501",
                "clinic": "A",
                "query": "foo bar",
                "page": "https://example.com/a",
                "impressions": 10,
                "clicks": 1,
                "position": 2.0,
            },
            {
                "month": "202501",
                "clinic": "A",
                "query": "foo  bar",
                "page": "https://example.com/a",
                "impressions": 30,
                "clicks": 3,
                "position": 1.0,
            },
            {
                "month": "202501",
                "clinic": "A",
                "query": "baz",
                "page": "https://example.com/b",
                "impressions": 5,
                "clicks": 0,
                "position": 5.0,
            },
        ]
    )

    query_map = {r"foo": "FOO"}
    page_map = {r"/a": "A", r"/b": "B"}

    df["query"] = text.map_by_regex(df["query"], query_map)
    df["query_key"] = text.normalize_whitespace(df["query"], mode="remove_all")

    top_queries = table.dedup_by_key(
        df,
        key_cols=["month", "clinic", "page", "query_key"],
        prefer_by="impressions",
        keep="first",
    )

    df = classify.classify_by_regex(df, "page", page_map, "page_category", default="other")

    sum_df = table.group_sum(
        df,
        group_cols=["month", "clinic", "page", "query_key", "page_category"],
        sum_cols=["impressions", "clicks"],
    )
    avg_df = table.weighted_avg(
        df,
        group_cols=["month", "clinic", "page", "query_key", "page_category"],
        value_col="position",
        weight_col="impressions",
        out_col="position",
    )

    result = sum_df.merge(
        avg_df,
        on=["month", "clinic", "page", "query_key", "page_category"],
        how="left",
    ).merge(
        top_queries[["month", "clinic", "page", "query_key", "query"]],
        on=["month", "clinic", "page", "query_key"],
        how="left",
    )

    assert len(result) == 2

    row_a = result[result["page"] == "https://example.com/a"].iloc[0]
    assert row_a["query"] == "FOO"
    assert row_a["page_category"] == "A"
    assert row_a["impressions"] == 40
    assert row_a["clicks"] == 4
    assert row_a["position"] == 1.25
