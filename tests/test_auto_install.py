import builtins
import importlib
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


def _block_ga_imports(monkeypatch):
    real_import = builtins.__import__

    def fake_import(name, globals=None, locals=None, fromlist=(), level=0):
        if name in ("google.analytics.data", "google.analytics.admin"):
            raise ModuleNotFoundError(f"No module named '{name}'")
        return real_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", fake_import)


def _mock_os_system(monkeypatch):
    calls = []

    def fake_system(cmd):
        calls.append(cmd)
        return 0

    monkeypatch.setattr(os, "system", fake_system)
    return calls


def _reload_megaton():
    _clear_megaton_modules()
    return importlib.import_module("megaton")


def test_auto_install_non_colab_default_disabled(monkeypatch, capsys):
    # Expected: non-colab + no env -> no auto-install, ModuleNotFoundError raised.
    _remove_colab_stub()
    monkeypatch.delenv("MEGATON_AUTO_INSTALL", raising=False)
    _block_ga_imports(monkeypatch)
    calls = _mock_os_system(monkeypatch)

    with pytest.raises(ModuleNotFoundError):
        _reload_megaton()

    out = capsys.readouterr().out
    assert "pip install" in out
    assert calls == []


def test_auto_install_colab_default_enabled(monkeypatch):
    # Expected: colab + no env -> auto-install called.
    _install_colab_stub()
    monkeypatch.delenv("MEGATON_AUTO_INSTALL", raising=False)
    _block_ga_imports(monkeypatch)
    calls = _mock_os_system(monkeypatch)

    _reload_megaton()

    assert calls
    _remove_colab_stub()


def test_auto_install_colab_env_zero_disabled(monkeypatch, capsys):
    # Expected: colab + MEGATON_AUTO_INSTALL=0 -> no auto-install.
    _install_colab_stub()
    monkeypatch.setenv("MEGATON_AUTO_INSTALL", "0")
    _block_ga_imports(monkeypatch)
    calls = _mock_os_system(monkeypatch)

    with pytest.raises(ModuleNotFoundError):
        _reload_megaton()

    out = capsys.readouterr().out
    assert "pip install" in out
    assert calls == []
    _remove_colab_stub()
