from types import SimpleNamespace
from unittest.mock import MagicMock

import pandas as pd

from megaton.start import Megaton


def _app():
    return Megaton(None, headless=True)


def _ga_app():
    app = _app()
    app.ga = {"4": MagicMock()}
    app.ga["4"].report = MagicMock()
    app.ga["4"].report.run = MagicMock(return_value=pd.DataFrame())
    return app


def test_set_retry_stores_and_returns_config():
    app = _app()
    cfg = app.set.retry(max_retries=7, backoff_factor=1.5, timeout=250)
    assert cfg == {"max_retries": 7, "backoff_factor": 1.5, "timeout": 250.0}
    assert app._retry == cfg
    # partial update keeps previously set values
    cfg2 = app.set.retry(backoff_factor=2.0)
    assert cfg2["max_retries"] == 7
    assert cfg2["backoff_factor"] == 2.0


def test_set_retry_applies_to_open_sheets_client():
    app = _app()
    app.gs = SimpleNamespace(max_retries=3, backoff_factor=2.0)
    app.set.retry(max_retries=9, backoff_factor=1.0)
    assert app.gs.max_retries == 9
    assert app.gs.backoff_factor == 1.0


def test_report_run_forwards_limit_and_timeout_to_ga4():
    # Regression: kwargs used to be dropped before reaching GA4 run.
    app = _ga_app()
    app.report.run(d=["date"], m=["sessions"], limit=5, timeout=300, show=False)
    _, kw = app.ga["4"].report.run.call_args
    assert kw["limit"] == 5
    assert kw["timeout"] == 300


def test_report_run_applies_session_retry_and_per_call_override():
    app = _ga_app()
    app.set.retry(max_retries=7, timeout=250)

    app.report.run(d=["date"], m=["sessions"], show=False)
    _, kw = app.ga["4"].report.run.call_args
    assert kw["max_retries"] == 7
    assert kw["timeout"] == 250

    # per-call wins over the session default
    app.report.run(d=["date"], m=["sessions"], max_retries=99, show=False)
    _, kw2 = app.ga["4"].report.run.call_args
    assert kw2["max_retries"] == 99


def test_gsc_resolver_reads_session_retry():
    app = _app()
    app.set.retry(max_retries=8, backoff_factor=1.25)
    # Resolution: explicit -> session -> env -> default
    assert app._gsc_service._resolve_max_retries(None) == 8
    assert app._gsc_service._resolve_backoff_factor(None) == 1.25
    assert app._gsc_service._resolve_max_retries(2) == 2  # explicit still wins
