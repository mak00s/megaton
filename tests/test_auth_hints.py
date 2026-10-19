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


def test_suppressed_context_silences_tips_and_restores(home, tmp_path):
    out = io.StringIO()
    path = key_file(tmp_path)
    with hints.suppressed():
        assert not hints.hints_enabled()
        assert not hints.suggest_store(path, stream=out)
    assert hints.hints_enabled()
    assert hints.suggest_store(path, stream=out)
    assert out.getvalue().count("[megaton] hint:") == 1


def test_cli_sa_option_does_not_suggest_sa(home, tmp_path, monkeypatch, capsys):
    from megaton import cli

    (home.sa_dir / "corp.json").write_text(json.dumps(sa_key()), encoding="utf-8")
    seen = []

    def fake_list(source, **kwargs):
        seen.append(hints.suggest_store(kwargs["credential"]))
        return []

    monkeypatch.setattr(cli, "list_resources", fake_list)
    assert cli.main(["resources", "gsc", "--sa", "corp"]) == 0
    assert seen == [False]
    assert "[megaton] hint:" not in capsys.readouterr().err
    assert hints.hints_enabled()  # suppression ends with the command


def test_cli_credential_path_still_gets_tip(home, tmp_path, monkeypatch, capsys):
    from megaton import cli

    (home.sa_dir / "corp.json").write_text(json.dumps(sa_key()), encoding="utf-8")
    path = key_file(tmp_path)
    monkeypatch.setattr(cli, "list_resources",
                        lambda source, **kwargs: hints.suggest_store(kwargs["credential"]) and [])
    assert cli.main(["resources", "gsc", "--credential", str(path)]) == 0
    assert 'pass sa="corp"' in capsys.readouterr().err


# ---- 2.6.2 review fixes: login scopes, quoted paths, tip only after OAuth success ----

def oauth_client(tmp_path, file="client.json"):
    path = tmp_path / file
    path.write_text(json.dumps({"installed": {
        "client_id": "cid.apps.googleusercontent.com", "client_secret": "s",
        "auth_uri": "https://accounts.google.com/o/oauth2/auth", "token_uri": "https://oauth2.googleapis.com/token",
        "redirect_uris": ["http://localhost"],
    }}), encoding="utf-8")
    return path


def test_oauth_login_hint_requests_the_legacy_session_scopes(home, tmp_path):
    import shlex

    from megaton.auth.store import expand_scopes

    message = hints.store_hint(oauth_client(tmp_path))
    command = message.split("`megaton auth login", 1)[1].split("`", 1)[0]
    scopes = shlex.split(command)[shlex.split(command).index("--scopes") + 1]
    assert set(scopes.split(",")) == {"analytics", "analytics.edit", "sheets", "drive", "webmasters"}
    assert "cloud-platform" not in scopes and "use sa= for BigQuery/Cloud" in message
    assert len(expand_scopes(scopes)) == 5


@pytest.mark.parametrize("kind", ["sa", "oauth"])
def test_paths_with_spaces_are_quoted(home, tmp_path, kind):
    import shlex

    folder = tmp_path / "my keys"
    folder.mkdir()
    path = key_file(folder, file="client key.json") if kind == "sa" else oauth_client(folder, "client key.json")
    message = hints.store_hint(path)
    command = message.split("`megaton auth import", 1)[1].split("`", 1)[0]
    argv = shlex.split(command)
    assert str(path) in argv


def _headless_oauth(monkeypatch, tmp_path, cached):
    from megaton import start
    from megaton.auth import google_auth

    monkeypatch.setattr(google_auth, "load_credentials", lambda *a, **k: cached)
    monkeypatch.setattr(start.Megaton, "_notify_invalid_oauth_config", lambda self, message: None)
    monkeypatch.setattr(start.Megaton, "_build_ga_clients", lambda self: None)
    return start.Megaton(str(oauth_client(tmp_path)), headless=True)


def test_failed_headless_oauth_prints_no_tip(home, tmp_path, monkeypatch, capsys):
    mg = _headless_oauth(monkeypatch, tmp_path, cached=None)
    assert mg.creds is None
    assert "[megaton] hint:" not in capsys.readouterr().err


def test_successful_headless_oauth_prints_tip(home, tmp_path, monkeypatch, capsys):
    mg = _headless_oauth(monkeypatch, tmp_path, cached=object())
    assert mg.creds is not None
    assert "megaton auth login --account <email>" in capsys.readouterr().err
