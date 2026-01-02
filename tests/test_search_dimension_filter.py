import re
from unittest import mock

from megaton.services.gsc_service import GSCService
from megaton.start import Megaton


def _apply_filters(rows, dimensions, filters):
    if not filters:
        return rows

    filtered = []
    for row in rows:
        keys = row.get("keys", [])
        dim_map = {dimensions[i]: keys[i] for i in range(min(len(dimensions), len(keys)))}
        ok = True
        for flt in filters:
            value = dim_map.get(flt.get("dimension"))
            if value is None:
                ok = False
                break
            operator = flt.get("operator")
            expression = flt.get("expression", "")
            if operator == "includingRegex":
                if re.search(expression, value) is None:
                    ok = False
                    break
            elif operator == "excludingRegex":
                if re.search(expression, value) is not None:
                    ok = False
                    break
            elif operator == "contains":
                if expression not in value:
                    ok = False
                    break
            elif operator == "notContains":
                if expression in value:
                    ok = False
                    break
        if ok:
            filtered.append(row)
    return filtered


def _make_client(rows):
    client = mock.Mock()
    request = mock.Mock()

    def execute():
        body = client.searchanalytics.return_value.query.call_args.kwargs["body"]
        dimensions = body.get("dimensions", [])
        filter_groups = body.get("dimensionFilterGroups") or []
        filters = filter_groups[0].get("filters", []) if filter_groups else []
        return {"rows": _apply_filters(rows, dimensions, filters)}

    request.execute.side_effect = execute
    client.searchanalytics.return_value.query.return_value = request
    return client


def _sample_rows():
    return [
        {
            "keys": ["/blog/a", "ortho tips"],
            "clicks": 1,
            "impressions": 10,
            "position": 1.0,
        },
        {
            "keys": ["/blog/b", "news"],
            "clicks": 2,
            "impressions": 20,
            "position": 2.0,
        },
        {
            "keys": ["/service/a", "ortho pricing"],
            "clicks": 3,
            "impressions": 30,
            "position": 3.0,
        },
    ]


def _run_query(client, filters):
    service = GSCService(app=None, client=client)
    return service.query(
        site_url="https://example.com",
        start_date="2024-01-01",
        end_date="2024-01-31",
        dimensions=["page", "query"],
        metrics=["clicks", "impressions", "position"],
        dimension_filter=filters,
    )


def test_dimension_filter_none_returns_all():
    rows = _sample_rows()
    client = _make_client(rows)
    df = _run_query(client, None)

    body = client.searchanalytics.return_value.query.call_args.kwargs["body"]
    assert "dimensionFilterGroups" not in body
    assert len(df) == len(rows)


def test_dimension_filter_regex_filters_pages():
    rows = _sample_rows()
    app = Megaton(None, headless=True)
    filters = app.search._parse_dimension_filter("page=~^/blog/")

    client = _make_client(rows)
    df = _run_query(client, filters)

    body = client.searchanalytics.return_value.query.call_args.kwargs["body"]
    assert body["dimensionFilterGroups"][0]["groupType"] == "and"
    assert body["dimensionFilterGroups"][0]["filters"] == filters
    assert set(df["page"]) == {"/blog/a", "/blog/b"}


def test_dimension_filter_contains_filters_queries():
    rows = _sample_rows()
    app = Megaton(None, headless=True)
    filters = app.search._parse_dimension_filter("query=@ortho")

    client = _make_client(rows)
    df = _run_query(client, filters)

    assert all("ortho" in query for query in df["query"])


def test_dimension_filter_and_combines_conditions():
    rows = _sample_rows()
    app = Megaton(None, headless=True)
    filters = app.search._parse_dimension_filter("page=~^/blog/;query=@ortho")

    client = _make_client(rows)
    df = _run_query(client, filters)

    assert len(df) == 1
    assert df.loc[0, "page"] == "/blog/a"


def test_dimension_filter_list_matches_string():
    rows = _sample_rows()
    app = Megaton(None, headless=True)
    string_filters = app.search._parse_dimension_filter("page=~^/blog/")
    list_filters = [
        {
            "dimension": "page",
            "operator": "includingRegex",
            "expression": "^/blog/",
        }
    ]

    df_list = _run_query(_make_client(rows), list_filters)
    df_string = _run_query(_make_client(rows), string_filters)

    assert df_list.equals(df_string)
