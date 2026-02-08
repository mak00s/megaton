import builtins
import importlib
import importlib.util
import os
import sys
import types

import pytest


def _install_colab_stub():
    colab = types.ModuleType("google.colab")
    colab.data_table = types.SimpleNamespace(
        enable_dataframe_formatter=lambda: None,
        _DEFAULT_FORMATTERS={},
    )
    sys.modules["google.colab"] = colab


def _remove_colab_stub():
    sys.modules.pop("google.colab", None)


def _clear_megaton_modules():
    for name in list(sys.modules.keys()):
        if name == "megaton" or name.startswith("megaton."):
            del sys.modules[name]


def _block_ga_specs(monkeypatch):
    # start.py uses importlib.util.find_spec to decide whether to auto-install.
    real_find_spec = importlib.util.find_spec
    missing = {
        "google.analytics.data",
        "google.analytics.admin",
        "google.cloud.bigquery_datatransfer",
    }

    def fake_find_spec(name, package=None):
        if name in missing:
            return None
        return real_find_spec(name, package)

    monkeypatch.setattr(importlib.util, "find_spec", fake_find_spec)


def _mock_os_system(monkeypatch):
    calls = []

    def fake_system(cmd):
        calls.append(cmd)
        return 0

    monkeypatch.setattr(os, "system", fake_system)
    return calls


def _reload_megaton():
    _clear_megaton_modules()
    return importlib.import_module("megaton.start")


def test_auto_install_non_colab_default_disabled(monkeypatch, capsys):
    # Expected: non-colab + no env -> no auto-install, ModuleNotFoundError raised on Megaton() init.
    _remove_colab_stub()
    monkeypatch.delenv("MEGATON_AUTO_INSTALL", raising=False)
    _block_ga_specs(monkeypatch)
    calls = _mock_os_system(monkeypatch)

    with pytest.raises(ModuleNotFoundError):
        start = _reload_megaton()
        monkeypatch.setattr(start, "mount_google_drive", lambda: None)
        monkeypatch.setattr(start.Megaton, "auth", lambda *args, **kwargs: None)
        start.Megaton(headless=True)

    out = capsys.readouterr().out
    assert "pip install" in out
    assert calls == []


def test_auto_install_colab_default_enabled(monkeypatch):
    # Expected: colab + no env -> auto-install called on Megaton() init.
    _install_colab_stub()
    monkeypatch.delenv("MEGATON_AUTO_INSTALL", raising=False)
    _block_ga_specs(monkeypatch)
    calls = _mock_os_system(monkeypatch)

    start = _reload_megaton()
    monkeypatch.setattr(start, "mount_google_drive", lambda: None)
    monkeypatch.setattr(start.Megaton, "auth", lambda *args, **kwargs: None)
    start.Megaton(headless=True)

    assert calls
    _remove_colab_stub()


def test_auto_install_colab_env_zero_disabled(monkeypatch, capsys):
    # Expected: colab + MEGATON_AUTO_INSTALL=0 -> no auto-install, ModuleNotFoundError raised on Megaton() init.
    _install_colab_stub()
    monkeypatch.setenv("MEGATON_AUTO_INSTALL", "0")
    _block_ga_specs(monkeypatch)
    calls = _mock_os_system(monkeypatch)

    with pytest.raises(ModuleNotFoundError):
        start = _reload_megaton()
        monkeypatch.setattr(start, "mount_google_drive", lambda: None)
        monkeypatch.setattr(start.Megaton, "auth", lambda *args, **kwargs: None)
        start.Megaton(headless=True)

    out = capsys.readouterr().out
    assert "pip install" in out
    assert calls == []
    _remove_colab_stub()
