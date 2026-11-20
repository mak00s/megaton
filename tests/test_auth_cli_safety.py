"""Fail-safe auth CLI behavior without real credentials or external writes."""
import os
import secrets
import subprocess
import sys
from types import SimpleNamespace

import pytest

from megaton import cli
from megaton.auth.store import CredentialStore, CredentialStoreError, expand_scopes


@pytest.fixture
def isolated(monkeypatch, tmp_path):
    monkeypatch.setenv("MEGATON_HOME", str(tmp_path))


@pytest.mark.parametrize("name,suggestion", [("documents", "docs"), ("spreadsheets", "sheets"),
                                            ("documents.readonly", "docs.readonly"), ("dcos", "docs")])
def test_alias_suggestion(name, suggestion):
    with pytest.raises(CredentialStoreError) as error:
        expand_scopes(name)
    assert error.value.exit_code == 2
    assert f"did you mean: {suggestion}?" in str(error.value)
    assert "use one of:" in str(error.value)


@pytest.mark.parametrize("mode", ["token", "check", "exec"])
@pytest.mark.parametrize("scope,account,code", [("documents", "missing", 2), ("docs", "missing", 4),
                                              ("docs", "missing@example.com", 4)])
def test_validation_codes(isolated, monkeypatch, capsys, mode, scope, account, code):
    def no_child(*args, **kwargs):
        pytest.fail("child started after authentication failure")
    monkeypatch.setattr(subprocess, "run", no_child)
    args = ["auth", "token" if mode == "check" else mode, "--account", account, "--scopes", scope]
    args += ["--check"] if mode == "check" else (["--", "unused"] if mode == "exec" else [])
    assert cli.main(args) == code
    output = capsys.readouterr()
    assert not output.out
    assert output.err.startswith("error:")


@pytest.mark.parametrize("mode", ["token", "check", "exec"])
@pytest.mark.parametrize("code", [3, 5])
def test_store_failure_stops_child(isolated, monkeypatch, capsys, mode, code):
    def fail(*args):
        raise CredentialStoreError("scope or refresh failure", hint="megaton auth token --help", exit_code=code)
    monkeypatch.setattr(CredentialStore, "user_access_token", fail)
    monkeypatch.setattr(subprocess, "run", lambda *a, **k: pytest.fail("child started"))
    args = ["auth", "token" if mode == "check" else mode, "--account", "sim"]
    args += ["--check"] if mode == "check" else (["--", "unused"] if mode == "exec" else [])
    assert cli.main(args) == code
    output = capsys.readouterr()
    assert not output.out
    assert "--help" in output.err


@pytest.mark.parametrize("env_name", ["GOOGLE_ACCESS_TOKEN", "CUSTOM_TOKEN"])
def test_exec_environment_and_child_status(isolated, monkeypatch, capsys, env_name):
    value = secrets.token_urlsafe()
    monkeypatch.setattr(CredentialStore, "user_access_token", lambda *a: value)
    monkeypatch.setenv(env_name, "parent")
    def child(args, *, env):
        assert env[env_name] == value
        assert args == ["program", "--flag"]
        assert os.environ[env_name] == "parent"
        return SimpleNamespace(returncode=23)
    monkeypatch.setattr(subprocess, "run", child)
    args = ["auth", "exec", "--account", "sim"]
    if env_name != "GOOGLE_ACCESS_TOKEN":
        args += ["--env", env_name]
    assert cli.main(args + ["--", "program", "--flag"]) == 23
    output = capsys.readouterr()
    assert not output.out and not output.err


def test_exec_real_child(isolated, monkeypatch, capfd):
    monkeypatch.setattr(CredentialStore, "user_access_token", lambda *a: secrets.token_urlsafe())
    assert cli.main(["auth", "exec", "--account", "sim", "--", sys.executable, "-c",
                     'import os; print("set" if os.environ.get("GOOGLE_ACCESS_TOKEN") else "missing")']) == 0
    assert capfd.readouterr().out.strip() == "set"


def test_check_and_legacy_output(isolated, monkeypatch, capsys):
    value = secrets.token_urlsafe()
    monkeypatch.setattr(CredentialStore, "user_access_token", lambda *a: value)
    assert cli.main(["auth", "token", "--account", "sim", "--check"]) == 0
    assert capsys.readouterr().out == "OK\n"
    assert cli.main(["auth", "token", "--account", "sim"]) == 0
    output = capsys.readouterr()
    assert output.out.count("\n") == 1 and len(output.out.strip()) == len(value)
    assert not output.err


@pytest.mark.parametrize("args", [[], ["--env", "bad-name", "--", "unused"]])
def test_exec_invalid_arguments(isolated, args, capsys):
    assert cli.main(["auth", "exec", "--account", "sim"] + args) == 2
    assert "--help" in capsys.readouterr().err


@pytest.mark.parametrize("error,code", [(FileNotFoundError, 127), (PermissionError, 126)])
def test_exec_launch_failure(isolated, monkeypatch, capsys, error, code):
    monkeypatch.setattr(CredentialStore, "user_access_token", lambda *a: secrets.token_urlsafe())
    def fail(*a, **k):
        raise error()
    monkeypatch.setattr(subprocess, "run", fail)
    assert cli.main(["auth", "exec", "--account", "sim", "--", "unused"]) == code
    assert "could not be launched" in capsys.readouterr().err


@pytest.mark.parametrize("command", ["token", "exec"])
def test_help_self_contained(command, capsys):
    with pytest.raises(SystemExit) as error:
        cli.main(["auth", command, "--help"])
    assert error.value.code == 0
    text = capsys.readouterr().out
    for item in ("docs,drive", "auth scopes", "2>/dev/null", "stderr", "stdout", "3 insufficient", "4 account", "5 credential"):
        assert item in text


def test_version(capsys):
    import megaton
    from importlib.metadata import version
    with pytest.raises(SystemExit) as error:
        cli.main(["--version"])
    assert error.value.code == 0
    text = capsys.readouterr().out
    assert version("megaton") in text and megaton.__file__ in text


def test_unexpected_store_error_is_sanitized(isolated, monkeypatch, capsys):
    def fail(*a):
        raise ValueError(secrets.token_urlsafe())
    monkeypatch.setattr(CredentialStore, "user_access_token", fail)
    assert cli.main(["auth", "token", "--account", "sim", "--check"]) == 5
    assert "credential operation failed" in capsys.readouterr().err


@pytest.mark.parametrize("status,expected", [(0, 0), (7, 7), (-15, 143)])
def test_child_status(isolated, monkeypatch, status, expected):
    monkeypatch.setattr(CredentialStore, "user_access_token", lambda *a: secrets.token_urlsafe())
    monkeypatch.setattr(subprocess, "run", lambda *a, **k: SimpleNamespace(returncode=status))
    assert cli.main(["auth", "exec", "--account", "sim", "--", "unused"]) == expected


def test_missing_access_token_stops_child(isolated, monkeypatch, capsys):
    monkeypatch.setattr(CredentialStore, "user_access_token", lambda *a: "")
    monkeypatch.setattr(subprocess, "run", lambda *a, **k: pytest.fail("child started"))
    assert cli.main(["auth", "exec", "--account", "sim", "--", "unused"]) == 5
    assert "no access token" in capsys.readouterr().err
