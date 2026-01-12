import pandas as pd
import pytest
from unittest.mock import MagicMock, patch

from megaton.start import Megaton, ReportResult


def _make_app():
    app = Megaton()
    app.ga = {"4": MagicMock()}
    app.ga["4"].report = MagicMock()
    return app


def test_report_run_multi_merge_and_zero_fill():
    app = _make_app()

    def mock_run(dimensions, metrics, **kwargs):
        if kwargs.get("dimension_filter") == "eventName==page_view":
            return pd.DataFrame(
                {
                    "pagePath": ["/a", "/b"],
                    "activeUsers": [10, 5],
                    "eventCount": [100, 50],
                }
            )
        if kwargs.get("dimension_filter") == "eventName==video_start":
            return pd.DataFrame(
                {
                    "pagePath": ["/a"],
                    "eventCount": [7],
                }
            )
        return pd.DataFrame()

    app.ga["4"].report.run = mock_run

    with patch.object(app.show, "table", return_value="ok") as mock_show:
        result = app.report.run(
            d=[("pagePath", "page")],
            m=[
                ([("activeUsers", "uu"), ("eventCount", "pv")], {"filter_d": "eventName==page_view"}),
                ([("eventCount", "video_views")], {"filter_d": "eventName==video_start"}),
            ],
        )

    assert isinstance(result, ReportResult)
    mock_show.assert_called_once()
    df = app.report.data
    assert {"page", "uu", "pv", "video_views"}.issubset(df.columns)
    row_b = df[df["page"] == "/b"].iloc[0]
    assert row_b["video_views"] == 0


def test_report_run_multi_default_left_join_drops_extra_rows():
    app = _make_app()

    def mock_run(dimensions, metrics, **kwargs):
        if kwargs.get("dimension_filter") == "eventName==page_view":
            return pd.DataFrame(
                {
                    "pagePath": ["/a"],
                    "eventCount": [10],
                }
            )
        if kwargs.get("dimension_filter") == "eventName==video_start":
            return pd.DataFrame(
                {
                    "pagePath": ["/b"],
                    "eventCount": [3],
                }
            )
        return pd.DataFrame()

    app.ga["4"].report.run = mock_run

    with patch.object(app.show, "table", return_value="ok"):
        app.report.run(
            d=[("pagePath", "page")],
            m=[
                ([("eventCount", "pv")], {"filter_d": "eventName==page_view"}),
                ([("eventCount", "video_views")], {"filter_d": "eventName==video_start"}),
            ],
        )

    df = app.report.data
    assert df["page"].tolist() == ["/a"]
    assert df.iloc[0]["video_views"] == 0


def test_report_run_multi_outer_join_keeps_extra_rows():
    app = _make_app()

    def mock_run(dimensions, metrics, **kwargs):
        if kwargs.get("dimension_filter") == "eventName==page_view":
            return pd.DataFrame(
                {
                    "pagePath": ["/a"],
                    "eventCount": [10],
                }
            )
        if kwargs.get("dimension_filter") == "eventName==video_start":
            return pd.DataFrame(
                {
                    "pagePath": ["/b"],
                    "eventCount": [3],
                }
            )
        return pd.DataFrame()

    app.ga["4"].report.run = mock_run

    with patch.object(app.show, "table", return_value="ok"):
        app.report.run(
            d=[("pagePath", "page")],
            m=[
                ([("eventCount", "pv")], {"filter_d": "eventName==page_view"}),
                ([("eventCount", "video_views")], {"filter_d": "eventName==video_start"}),
            ],
            merge="outer",
        )

    df = app.report.data
    assert set(df["page"].tolist()) == {"/a", "/b"}
    row_b = df[df["page"] == "/b"].iloc[0]
    assert row_b["pv"] == 0


def test_report_run_multi_combines_filters():
    app = _make_app()
    filters = []

    def mock_run(dimensions, metrics, **kwargs):
        filters.append((kwargs.get("dimension_filter"), kwargs.get("metric_filter")))
        row = {dim: "x" for dim in dimensions}
        for metric in metrics:
            row[metric] = 1
        return pd.DataFrame([row])

    app.ga["4"].report.run = mock_run

    with patch.object(app.show, "table", return_value="ok"):
        app.report.run(
            d=[("pagePath", "page")],
            m=[
                ([("activeUsers", "uu")], {"filter_d": "eventName==page_view"}),
                ([("eventCount", "pv")], {"filter_m": "eventCount>0"}),
            ],
            filter_d="country==JP",
            filter_m="sessions>0",
        )

    assert filters == [
        ("country==JP;eventName==page_view", "sessions>0"),
        ("country==JP", "sessions>0;eventCount>0"),
    ]


def test_report_run_multi_duplicate_metric_alias_error():
    app = _make_app()
    app.ga["4"].report.run = MagicMock(return_value=pd.DataFrame())

    with pytest.raises(ValueError, match="Duplicate metric alias"):
        app.report.run(
            d=[("pagePath", "page")],
            m=[
                ([("activeUsers", "metric")], {}),
                ([("sessions", "metric")], {}),
            ],
        )


def test_report_run_multi_mixed_format_error():
    app = _make_app()
    app.ga["4"].report.run = MagicMock(return_value=pd.DataFrame())

    with pytest.raises(ValueError, match="Mixed metric format"):
        app.report.run(
            d=[("pagePath", "page")],
            m=[
                ("activeUsers", "users"),
                ([("sessions", "sessions")], {}),
            ],
        )


def test_report_run_multi_sort_after_merge():
    app = _make_app()
    order_bys = []

    def mock_run(dimensions, metrics, **kwargs):
        order_bys.append(kwargs.get("order_bys"))
        if kwargs.get("dimension_filter") == "eventName==page_view":
            return pd.DataFrame(
                {
                    "pagePath": ["/b", "/a"],
                    "eventCount": [5, 10],
                }
            )
        return pd.DataFrame(
            {
                "pagePath": ["/a", "/b"],
                "eventCount": [1, 2],
            }
        )

    app.ga["4"].report.run = mock_run

    with patch.object(app.show, "table", return_value="ok"):
        app.report.run(
            d=[("pagePath", "page")],
            m=[
                ([("eventCount", "pv")], {"filter_d": "eventName==page_view"}),
                ([("eventCount", "video_views")], {"filter_d": "eventName==video_start"}),
            ],
            sort="-pv",
        )

    assert order_bys == [None, None]
    df = app.report.data
    assert df.iloc[0]["page"] == "/a"
