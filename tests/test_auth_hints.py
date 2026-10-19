"""Credential-store tips for legacy credential paths."""

import io
import json

import pytest

from megaton.auth import hints, store as store_mod
from megaton.auth.store import CredentialStore


def sa_key(name="sheets-api", project="proj"):
    return {
        "type": "service_account", "project_id": project,
        "client_email": f"{name}@{project}.iam.gserviceaccount.com",
        "private_key_id": "k", "private_key": "-----BEGIN PRIVATE KEY-----\nx\n-----END PRIVATE KEY-----\n",
        "client_id": "1", "token_uri": "https://oauth2.googleapis.com/token",
    }


@pytest.fixture
def home(tmp_path, monkeypatch):
    monkeypatch.setenv(store_mod.HOME_ENV, str(tmp_path / "store"))
    monkeypatch.delenv(hints.HINTS_ENV, raising=False)
    monkeypatch.delenv("CI", raising=False)
    monkeypatch.delenv("GITHUB_ACTIONS", raising=False)
    st = CredentialStore()
    st.init()
    return st


def key_file(tmp_path, name="sheets-api", file="key.json"):
    path = tmp_path / file
    path.write_text(json.dumps(sa_key(name)), encoding="utf-8")
    return path


def test_known_service_account_suggests_its_store_name(home, tmp_path):
    (home.registry_path).write_text('[sa]\ncorp-ga = "corp.json"\n', encoding="utf-8")
    (home.sa_dir / "corp.json").write_text(json.dumps(sa_key()), encoding="utf-8")
    out = io.StringIO()
    assert hints.suggest_store(key_file(tmp_path), stream=out)
    assert 'pass sa="corp-ga" (CLI: --sa corp-ga)' in out.getvalue()
    assert "MEGATON_HINTS=0" in out.getvalue()


def test_unknown_service_account_suggests_import(home, tmp_path):
    out = io.StringIO()
    path = key_file(tmp_path, name="other")
    assert hints.suggest_store(path, stream=out)
    assert f"megaton auth import --sa {path} --name <name>" in out.getvalue()


def test_oauth_client_file_suggests_login(home, tmp_path):
    path = tmp_path / "client.json"
    path.write_text(json.dumps({"installed": {"client_id": "c"}}), encoding="utf-8")
    out = io.StringIO()
    assert hints.suggest_store(path, stream=out)
    assert "megaton auth login --account <email>" in out.getvalue()


def test_tip_is_shown_once_per_credential(home, tmp_path):
    out = io.StringIO()
    path = key_file(tmp_path)
    assert hints.suggest_store(path, stream=out)
    assert not hints.suggest_store(path, stream=out)
    assert out.getvalue().count("[megaton] hint:") == 1


@pytest.mark.parametrize("env", [{"MEGATON_HINTS": "0"}, {"CI": "true"}, {"GITHUB_ACTIONS": "true"}])
def test_tip_is_silenced_by_opt_out_and_ci(home, tmp_path, monkeypatch, env):
    for name, value in env.items():
        monkeypatch.setenv(name, value)
    out = io.StringIO()
    assert not hints.suggest_store(key_file(tmp_path), stream=out)
    assert out.getvalue() == ""


def test_tip_never_breaks_on_bad_input(home, tmp_path):
    bad = tmp_path / "bad.json"
    bad.write_text("not json", encoding="utf-8")
    assert not hints.suggest_store(bad, stream=io.StringIO())
    assert not hints.suggest_store(tmp_path / "missing.json", stream=io.StringIO())


def test_service_account_login_emits_tip_on_stderr_only(home, tmp_path, monkeypatch, capsys):
    from megaton import start
    from megaton.auth import google_auth

    path = key_file(tmp_path)
    monkeypatch.setattr(google_auth, "load_service_account_credentials_from_file",
                        lambda *a, **k: object())
    monkeypatch.setattr(start.Megaton, "_build_ga_clients", lambda self: None)
    start.Megaton(str(path), headless=True)
    captured = capsys.readouterr()
    assert "[megaton] hint:" in captured.err
    assert "[megaton] hint:" not in captured.out
