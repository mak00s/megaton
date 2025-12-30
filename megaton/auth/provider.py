"""Credential source resolution helpers."""

from dataclasses import dataclass
import base64
import json
import os
from typing import Any, Optional, Tuple


@dataclass(frozen=True)
class CredentialSource:
    raw: Any
    origin: str
    kind: str
    value: Optional[str] = None
    info: Optional[dict] = None
    credential_type: Optional[str] = None
    error: Optional[Exception] = None


def get_credential_type_from_info(info: Optional[dict]) -> str:
    if isinstance(info, dict):
        if info.get("type") == "service_account":
            return "service_account"
        if "installed" in info:
            return "installed"
        if "web" in info:
            return "web"
    return "unknown"


def parse_json_input(value: Any) -> Optional[dict]:
    """Return dict if value looks like JSON (or base64 JSON); else None."""
    if isinstance(value, dict):
        return value
    if not isinstance(value, str):
        return None
    s = value.strip()
    if s.startswith("{") and s.endswith("}"):
        try:
            return json.loads(s)
        except Exception:
            return None
    try:
        decoded = base64.b64decode(s).decode("utf-8", errors="ignore")
        ds = decoded.strip()
        if ds.startswith("{") and ds.endswith("}"):
            return json.loads(ds)
    except Exception:
        pass
    return None


def load_json_file(path: str) -> Tuple[Optional[dict], Optional[Exception]]:
    try:
        with open(path) as fp:
            data = json.load(fp)
    except Exception as exc:
        return None, exc
    if not isinstance(data, dict):
        return None, ValueError("JSON object required")
    return data, None


def extract_email_from_file(path: str) -> Optional[str]:
    info, _ = load_json_file(path)
    if not isinstance(info, dict):
        return None
    return info.get("client_email")


def resolve_credential_source(
    credential: Any,
    *,
    env_var: str = "MEGATON_CREDS_JSON",
    in_colab: bool = False,
    colab_default: str = "/nbs",
) -> CredentialSource:
    origin = "explicit"
    raw = credential
    value = credential
    if credential is None:
        env_val = os.environ.get(env_var)
        if env_val:
            origin = "env"
            value = env_val
        elif in_colab:
            origin = "colab_default"
            value = colab_default
        else:
            return CredentialSource(raw=None, origin="none", kind="none")

    if isinstance(value, dict):
        info = value
        ctype = get_credential_type_from_info(info)
        return CredentialSource(raw=raw, origin=origin, kind="inline", info=info, credential_type=ctype)

    if not isinstance(value, str):
        return CredentialSource(raw=raw, origin=origin, kind="unknown")

    info = parse_json_input(value)
    if info is not None:
        ctype = get_credential_type_from_info(info)
        return CredentialSource(raw=raw, origin=origin, kind="inline", value=value, info=info, credential_type=ctype)

    if os.path.isdir(value):
        return CredentialSource(raw=raw, origin=origin, kind="directory", value=value)

    if os.path.isfile(value):
        info, error = load_json_file(value)
        ctype = get_credential_type_from_info(info) if info else None
        return CredentialSource(
            raw=raw,
            origin=origin,
            kind="file",
            value=value,
            info=info,
            credential_type=ctype,
            error=error,
        )

    return CredentialSource(raw=raw, origin=origin, kind="unknown", value=value)
