import megaton
from megaton import errors
import sys
from types import ModuleType


def test_mount_google_drive_non_colab_returns_none_and_prints(capsys, monkeypatch):
    monkeypatch.setattr(megaton, "IS_COLAB", False)

    result = megaton.mount_google_drive()

    assert result is None
    assert "only available in Google Colab" in capsys.readouterr().out


def test_mount_google_drive_colab_calls_gdrive(monkeypatch):
    monkeypatch.setattr(megaton, "IS_COLAB", True)

    called = {"v": 0}

    def _fake_link_nbs():
        called["v"] += 1
        return "mounted"

    fake_gdrive = ModuleType("megaton.gdrive")
    fake_gdrive.link_nbs = _fake_link_nbs
    monkeypatch.setitem(sys.modules, "megaton.gdrive", fake_gdrive)

    assert megaton.mount_google_drive() == "mounted"
    assert called["v"] == 1


def test_bad_credential_scope_str_contains_required_scopes():
    err = errors.BadCredentialScope(scopes=["scope:a", "scope:b"])
    message = str(err)

    assert "Required scopes are" in message
    assert "scope:a" in message


def test_partial_data_returned_sets_message_and_exception_text():
    err = errors.PartialDataReturned("partial")

    assert err.message == "partial"
    assert "partial" in str(err)


def test_invalid_service_account_uses_default_message_and_str():
    err = errors.InvalidServiceAccount()

    assert "サービスアカウント" in err.message
    assert str(err) == err.message
