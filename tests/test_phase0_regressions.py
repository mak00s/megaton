import inspect

import pandas as pd
import pytest
from googleapiclient import errors

from megaton import bq, ga4, google_api, utils
from megaton.ui import widgets


class DummyResp:
    def __init__(self, status=500, reason="Internal Server Error"):
        self.status = status
        self.reason = reason

    def get(self, key, default=None):
        if key == "code":
            return self.status
        return default


class FailingMethod:
    def execute(self, num_retries=0):
        raise errors.HttpError(DummyResp(), b"not json")


def test_dropdown_menu_without_width_does_not_error():
    # Expected: width omitted should be accepted and return a Dropdown instance.
    dropdown = widgets.dropdown_menu("label", "default", option_list=[("A", "a")])
    from ipywidgets import Dropdown

    assert isinstance(dropdown, Dropdown)


def test_prep_df_empty_returns_dataframe_not_none():
    # Expected: empty DataFrame should return a DataFrame (not None).
    df = pd.DataFrame(columns=["a", "b"])
    result = utils.prep_df(df)

    assert isinstance(result, pd.DataFrame)
    assert result.empty


def test_retry_non_json_http_error_raises_http_error_not_unboundlocal():
    # Expected: non-JSON HttpError should propagate HttpError without UnboundLocalError.
    api = google_api.GoogleApi()
    with pytest.raises(errors.HttpError) as excinfo:
        api.retry(FailingMethod(), retry_count=0)

    assert "not json" in str(excinfo.value)


class DummyProperty:
    api_metadata = {
        "dimensions": [
            {"api_name": "date", "display_name": "Date"},
        ],
        "metrics": [
            {"api_name": "eventCount", "display_name": "Event count"},
        ],
    }


class DummyParent:
    property = DummyProperty()


def test_ga4_order_bys_metric_uses_metric_order_by_and_api_name():
    # Expected: metric sort should generate OrderBy.metric with api_name.
    report = ga4.MegatonGA4.Report(DummyParent())
    order_bys = report._format_order_bys("-Event count")

    assert order_bys[0].desc is True
    assert order_bys[0].metric.metric_name == "eventCount"
    assert order_bys[0].dimension.dimension_name == ""


def test_ga4_order_bys_dimension_uses_dimension_order_by():
    # Expected: dimension sort should generate OrderBy.dimension with api_name.
    report = ga4.MegatonGA4.Report(DummyParent())
    order_bys = report._format_order_bys("date")

    assert order_bys[0].desc is False
    assert order_bys[0].dimension.dimension_name == "date"
    assert order_bys[0].metric.metric_name == ""


@pytest.mark.parametrize(
    "method_name",
    [
        "flatten_events",
        "get_query_to_flatten_events",
        "schedule_query_to_flatten_events",
    ],
)
@pytest.mark.parametrize("param_name", ["event_parameters", "user_properties"])
def test_bq_ga4_defaults_are_none_to_avoid_shared_state(method_name, param_name):
    # Expected: default list params should be None to avoid shared state.
    sig = inspect.signature(getattr(bq.MegatonBQ.GA4, method_name))
    assert sig.parameters[param_name].default is None
