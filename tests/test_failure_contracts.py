from types import SimpleNamespace
from unittest.mock import Mock

import pandas as pd
import pytest
import requests
from google.api_core.exceptions import InvalidArgument, PermissionDenied

from megaton import errors
from megaton.auth import google_auth
from megaton.ga4 import MegatonGA4
from megaton.gsheet import MegatonGS
from megaton.services.sheets_service import SheetsService


def test_upsert_read_failure_never_writes():
    sheet = SimpleNamespace(
        get_records=Mock(side_effect=TimeoutError("read failed")),
        overwrite_data=Mock(),
    )
    service = SheetsService(SimpleNamespace(gs=SimpleNamespace(sheet=sheet)))
    service.open_or_create_sheet = Mock(return_value=True)
    with pytest.raises(errors.RequestError) as caught:
        service.upsert_df("book", "history", pd.DataFrame([{"id": "new"}]), keys=["id"])
    assert isinstance(caught.value.__cause__, TimeoutError)
    sheet.overwrite_data.assert_not_called()


@pytest.mark.parametrize("failure", [InvalidArgument("bad metric"), PermissionDenied("denied")])
def test_ga4_permanent_errors_propagate(failure):
    client = SimpleNamespace(run_report=Mock(side_effect=failure))
    report = MegatonGA4.Report(SimpleNamespace(data_client=client))
    with pytest.raises(type(failure)):
        report._request_report_api(0, SimpleNamespace())
    assert client.run_report.call_count == 1


def test_ga4_partial_result_is_never_returned():
    report = MegatonGA4.Report(SimpleNamespace(property=SimpleNamespace(id="123")))
    report._format_request = Mock(return_value=SimpleNamespace())
    report._request_report_api = Mock(side_effect=[
        ([["a", "1"]], 2, ["page", "sessions"], []), ([], 0, [], []),
    ])
    with pytest.raises(errors.PartialDataReturned):
        report.run(["page"], ["sessions"], limit=1, on_exhausted="empty")


def test_atomic_overwrite_failure_does_not_clear_separately():
    driver = Mock()
    driver.batch_update.side_effect = requests.exceptions.ConnectionError("lost response")
    parent = SimpleNamespace(_driver=driver)
    sheet = MegatonGS.Sheet(parent)
    sheet._driver = SimpleNamespace(id=1, row_count=10, col_count=3, clear=Mock())
    with pytest.raises(requests.exceptions.ConnectionError):
        sheet.overwrite_data(pd.DataFrame([{"id": "new"}]))
    sheet._driver.clear.assert_not_called()
    assert driver.batch_update.call_count == 1
    assert len(driver.batch_update.call_args.args[0]["requests"]) == 3


@pytest.mark.parametrize("operation", ["create", "delete", "duplicate"])
def test_structural_mutations_are_submitted_once(operation):
    driver = Mock()
    target = {"create": "add_worksheet", "delete": "del_worksheet", "duplicate": "duplicate_sheet"}[operation]
    getattr(driver, target).side_effect = requests.exceptions.ConnectionError("lost response")
    parent = SimpleNamespace(_driver=driver, _client=True, call_with_retry=Mock())
    parent.call_with_retry.side_effect = lambda op, call, **kw: call()
    sheet = MegatonGS.Sheet(parent)
    with pytest.raises(requests.exceptions.ConnectionError):
        getattr(sheet, operation)(*( ["old", "new"] if operation == "duplicate" else ["new"] ))
    assert getattr(driver, target).call_count == 1
    assert all("select" in call.args[0] for call in parent.call_with_retry.call_args_list)


def test_same_filename_credentials_do_not_share_cache(tmp_path, monkeypatch):
    original = google_auth.os.path.expanduser
    monkeypatch.setattr(google_auth.os.path, "expanduser", lambda p: str(tmp_path) if p == "~" else original(p))
    first = google_auth.get_cache_path(str(tmp_path / "a" / "client.json"))
    second = google_auth.get_cache_path(str(tmp_path / "b" / "client.json"))
    assert first != second
    credential = SimpleNamespace(to_json=lambda: "{}")
    google_auth.save_credentials(str(tmp_path / "a" / "client.json"), credential)
    from pathlib import Path
    assert Path(first).stat().st_mode & 0o777 == 0o600


def test_diagnostics_preserve_cause_and_status():
    from googleapiclient.errors import HttpError
    cause = HttpError(SimpleNamespace(status=503, reason="unavailable"), b"{}")
    try:
        raise errors.RequestError("Cannot read history") from cause
    except errors.RequestError as exc:
        result = errors.describe_error(exc, operation="upsert.read", target="book/history")
    assert result["http_status"] == 503
    assert result["transient"] is True
    assert result["cause_type"] == "HttpError"
    assert result["message"] == "Cannot read history"
    assert str(errors.SheetNotFound()) == "The sheet requested is not found."


def _overwrite_requests(frame, start_row=1):
    driver = Mock()
    sheet = MegatonGS.Sheet(SimpleNamespace(_driver=driver))
    sheet._driver = SimpleNamespace(id=1, row_count=100, col_count=10)
    sheet.overwrite_data_from_row(frame, row=start_row)
    return driver.batch_update.call_args.args[0]["requests"]


def test_overwrite_datetime_preserves_fractional_seconds():
    value = pd.Timestamp("2024-01-01 12:00:00.123456")
    requests = _overwrite_requests(pd.DataFrame({"date": [value, pd.NaT]}))
    cells = requests[2]["updateCells"]["rows"]
    serial = cells[1]["values"][0]["userEnteredValue"]["numberValue"]
    expected = (value - pd.Timestamp("1899-12-30")).total_seconds() / 86400
    assert serial == pytest.approx(expected, rel=0, abs=2e-11)
    assert serial != 45292.5
    assert cells[2]["values"][0]["userEnteredValue"] == {"stringValue": ""}


@pytest.mark.parametrize("stamp", [
    "2024-01-01T12:00:00.123456+09:00", "2024-01-01T00:00:00+09:00",
])
def test_overwrite_timezone_datetime_stays_iso_text(stamp):
    requests = _overwrite_requests(pd.DataFrame({"date": pd.to_datetime([stamp])}))
    cell = requests[2]["updateCells"]["rows"][1]["values"][0]
    assert cell["userEnteredValue"] == {"stringValue": stamp}
    assert not any("repeatCell" in request for request in requests)


@pytest.mark.parametrize("start_row", [1, 5])
def test_overwrite_mixed_column_formats_only_date_cells(start_row):
    requests = _overwrite_requests(pd.DataFrame({
        "date": ["2024-01-01", "TBD", "2024-01-02T12:00:00", None],
    }), start_row)
    formats = [request["repeatCell"] for request in requests if "repeatCell" in request]
    assert [(r["range"]["startRowIndex"], r["range"]["endRowIndex"]) for r in formats] == [
        (start_row, start_row + 1), (start_row + 2, start_row + 3),
    ]
    assert [r["cell"]["userEnteredFormat"]["numberFormat"]["type"] for r in formats] == ["DATE", "DATE_TIME"]
    assert all(r["fields"] == "userEnteredFormat.numberFormat" for r in formats)
