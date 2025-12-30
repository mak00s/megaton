import pandas as pd
import pytest
from googleapiclient import errors

from megaton import google_api, utils, widgets


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
