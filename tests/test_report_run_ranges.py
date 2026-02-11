import pandas as pd
import pytest
from unittest.mock import MagicMock, patch, call

from megaton.start import Megaton


def _make_app():
    app = Megaton()
    app.ga = {"4": MagicMock()}
    app.ga["4"].report = MagicMock()
    return app


class TestRunRanges:
    """Tests for Report.Run.ranges() method."""

    def test_ranges_concatenates_results_from_multiple_periods(self):
        app = _make_app()
        call_count = 0

        def mock_run(dimensions, metrics, **kwargs):
            nonlocal call_count
            call_count += 1
            return pd.DataFrame({
                "yearMonth": [f"2024-0{call_count}"],
                "pagePath": ["/a"],
                "screenPageViews": [call_count * 10],
            })

        app.ga["4"].report.run = mock_run

        with patch.object(app.show, "table", return_value="ok"):
            result = app.report.run.ranges(
                [("2024-01-01", "2024-01-31"), ("2024-02-01", "2024-02-29")],
                d=["yearMonth", "pagePath"],
                m=["screenPageViews"],
            )

        assert len(result) == 2
        assert result["screenPageViews"].tolist() == [10, 20]

    def test_ranges_restores_dates_after_execution(self):
        app = _make_app()
        app.ga["4"].report.run = MagicMock(return_value=pd.DataFrame({
            "pagePath": ["/a"], "screenPageViews": [1],
        }))

        app.report.set.dates("2023-01-01", "2023-12-31")
        original_start = app.report.start_date
        original_end = app.report.end_date

        with patch.object(app.show, "table", return_value="ok"):
            app.report.run.ranges(
                [("2024-06-01", "2024-06-30")],
                d=["pagePath"],
                m=["screenPageViews"],
            )

        assert app.report.start_date == original_start
        assert app.report.end_date == original_end

    def test_ranges_restores_dates_on_error(self):
        app = _make_app()
        app.ga["4"].report.run = MagicMock(side_effect=RuntimeError("API error"))

        app.report.set.dates("2023-01-01", "2023-12-31")
        original_start = app.report.start_date
        original_end = app.report.end_date

        with pytest.raises(RuntimeError, match="API error"):
            app.report.run.ranges(
                [("2024-06-01", "2024-06-30")],
                d=["pagePath"],
                m=["screenPageViews"],
            )

        assert app.report.start_date == original_start
        assert app.report.end_date == original_end

    def test_ranges_returns_empty_dataframe_when_no_data(self):
        app = _make_app()
        app.ga["4"].report.run = MagicMock(return_value=pd.DataFrame())

        with patch.object(app.show, "table", return_value="ok"):
            result = app.report.run.ranges(
                [("2024-01-01", "2024-01-31")],
                d=["pagePath"],
                m=["screenPageViews"],
            )

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0

    def test_ranges_skips_empty_periods(self):
        app = _make_app()
        call_count = 0

        def mock_run(dimensions, metrics, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 2:
                return pd.DataFrame()  # second period returns no data
            return pd.DataFrame({
                "pagePath": ["/a"], "screenPageViews": [10],
            })

        app.ga["4"].report.run = mock_run

        with patch.object(app.show, "table", return_value="ok"):
            result = app.report.run.ranges(
                [
                    ("2024-01-01", "2024-01-31"),
                    ("2024-02-01", "2024-02-29"),
                    ("2024-03-01", "2024-03-31"),
                ],
                d=["pagePath"],
                m=["screenPageViews"],
            )

        # should have 2 rows (period 1 and 3), skipping empty period 2
        assert len(result) == 2

    def test_ranges_passes_filters_through(self):
        app = _make_app()
        captured_kwargs = []

        def mock_run(dimensions, metrics, **kwargs):
            captured_kwargs.append(kwargs)
            return pd.DataFrame({
                "pagePath": ["/a"], "screenPageViews": [1],
            })

        app.ga["4"].report.run = mock_run

        with patch.object(app.show, "table", return_value="ok"):
            app.report.run.ranges(
                [("2024-01-01", "2024-01-31")],
                d=["pagePath"],
                m=["screenPageViews"],
                filter_d="hostName==example.com",
                filter_m="screenPageViews>0",
            )

        assert len(captured_kwargs) == 1
        assert captured_kwargs[0]["dimension_filter"] == "hostName==example.com"
        assert captured_kwargs[0]["metric_filter"] == "screenPageViews>0"

    def test_ranges_stores_result_in_report_data(self):
        app = _make_app()
        app.ga["4"].report.run = MagicMock(return_value=pd.DataFrame({
            "pagePath": ["/a"], "screenPageViews": [10],
        }))

        with patch.object(app.show, "table", return_value="ok"):
            result = app.report.run.ranges(
                [("2024-01-01", "2024-01-31")],
                d=["pagePath"],
                m=["screenPageViews"],
            )

        assert app.report.data is result

    def test_ranges_single_period_equivalent_to_single_run(self):
        app = _make_app()
        app.ga["4"].report.run = MagicMock(return_value=pd.DataFrame({
            "yearMonth": ["202401"],
            "pagePath": ["/page"],
            "screenPageViews": [42],
        }))

        with patch.object(app.show, "table", return_value="ok"):
            result = app.report.run.ranges(
                [("2024-01-01", "2024-01-31")],
                d=["yearMonth", "pagePath"],
                m=["screenPageViews"],
            )

        assert len(result) == 1
        assert result.iloc[0]["screenPageViews"] == 42
