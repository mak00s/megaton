import builtins
import importlib
import sys
import types

import pytest


def ensure_module(name: str):
    parts = name.split('.')
    module_name = ''
    parent = None
    for part in parts:
        module_name = f"{module_name}.{part}" if module_name else part
        module = sys.modules.get(module_name)
        if module is None:
            module = types.ModuleType(module_name)
            sys.modules[module_name] = module
            if parent is not None:
                setattr(parent, part, module)
        parent = module
    return module


def get_module(name: str):
    try:
        return importlib.import_module(name)
    except Exception:
        return ensure_module(name)


def _ensure_ga_stubs():
    data_mod = get_module("google.analytics.data")
    if not hasattr(data_mod, "BetaAnalyticsDataClient"):
        class _StubDataClient:
            pass

        data_mod.BetaAnalyticsDataClient = _StubDataClient

    admin_mod = get_module("google.analytics.admin")
    if not hasattr(admin_mod, "AnalyticsAdminServiceClient"):
        class _StubAdminClient:
            pass

        admin_mod.AnalyticsAdminServiceClient = _StubAdminClient


def _clear_modules(prefix: str):
    for name in list(sys.modules.keys()):
        if name == prefix or name.startswith(prefix + "."):
            del sys.modules[name]


def _block_ipywidgets(monkeypatch):
    real_import = builtins.__import__

    def fake_import(name, globals=None, locals=None, fromlist=(), level=0):
        if name == "ipywidgets" or name.startswith("ipywidgets."):
            raise ModuleNotFoundError("No module named 'ipywidgets'")
        return real_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", fake_import)


def test_import_start_without_ipywidgets(monkeypatch):
    _ensure_ga_stubs()
    _clear_modules("megaton")
    _clear_modules("ipywidgets")
    _block_ipywidgets(monkeypatch)

    start = importlib.import_module("megaton.start")
    assert start.Megaton


def test_widgets_raise_without_ipywidgets(monkeypatch):
    _ensure_ga_stubs()
    _clear_modules("megaton")
    _clear_modules("ipywidgets")
    _block_ipywidgets(monkeypatch)

    widgets = importlib.import_module("megaton.widgets")
    with pytest.raises(widgets.WidgetsUnavailableError) as excinfo:
        widgets.dropdown_menu("Label", "Default")

    message = str(excinfo.value)
    assert "ipywidgets" in message
    assert "headless=True" in message
