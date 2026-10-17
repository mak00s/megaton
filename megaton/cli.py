"""Installed, checkout-independent single-query interface."""

from __future__ import annotations

import argparse
import contextlib
import json
import sys
from pathlib import Path

from .auth import cli as auth_cli
from .auth.store import CredentialStore, CredentialStoreError
from .query import (
    QueryValidationError,
    _validate_auth,
    list_resources,
    run_query,
    validate_query,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="megaton")
    commands = parser.add_subparsers(dest="command", required=True)
    query = commands.add_parser("query", help="Execute an explicit GA4/GSC/BQ request")
    query.add_argument("--params", required=True, help="JSON request file; - for stdin")
    query.add_argument("--output", help="CSV or JSON file; default is JSON on stdout")
    query.add_argument("--validate-only", action="store_true")
    resources = commands.add_parser("resources", help="List accessible resources")
    resources.add_argument("source", choices=["ga4", "gsc", "bigquery"])
    resources.add_argument("--project-id")
    for command in (query, resources):
        command.add_argument("--credential", help="Explicit Google credential JSON file")
        command.add_argument("--sa", help="Service account name in the credential store (see `megaton auth`)")
        command.add_argument("--cache-key", help="Existing Megaton OAuth cache identifier")
        command.add_argument("--adc", action="store_true", help="Explicitly use ADC for BigQuery")
    auth_cli.add_parser(commands)
    args = parser.parse_args(argv)
    if args.command == "auth":
        return auth_cli.run(args)
    try:
        if args.sa:
            if args.credential:
                raise QueryValidationError("pass --credential or --sa, not both")
            try:
                args.credential = str(CredentialStore().service_account_path(args.sa))
            except CredentialStoreError as exc:
                raise QueryValidationError(str(exc)) from exc
        # Legacy Python APIs may print; stdout must remain one JSON document.
        with contextlib.redirect_stdout(sys.stderr):
            if args.command == "query":
                raw = sys.stdin.read() if args.params == "-" else Path(args.params).read_text(encoding="utf-8")
                params = validate_query(json.loads(raw))
                _validate_auth(params["source"], args.credential, args.cache_key, args.adc)
                if args.output and Path(args.output).suffix.lower() not in {".csv", ".json"}:
                    raise QueryValidationError("output must end in .csv or .json")
                if args.validate_only:
                    payload = {"status": "ok", "validated": True, "params": params}
                else:
                    result = run_query(params, credential=args.credential, cache_key=args.cache_key, adc=args.adc)
                    if args.output:
                        result.save(args.output)
                        payload = {"status": "ok", "output": args.output, "row_count": len(result.df)}
                        if result.completeness is not None:
                            payload["completeness"] = result.completeness.to_dict()
                    else:
                        payload = result.to_dict()
            else:
                data = list_resources(args.source, credential=args.credential, project_id=args.project_id,
                                      cache_key=args.cache_key, adc=args.adc)
                payload = {"status": "ok", "source": args.source, "data": data}
        print(json.dumps(payload, ensure_ascii=False))
        return 0
    except (QueryValidationError, json.JSONDecodeError) as exc:
        print(json.dumps({"status": "error", "error_code": "INVALID_REQUEST", "message": str(exc)}))
        return 2
    except Exception as exc:  # noqa: BLE001 - CLI boundary reports provider failures.
        # Do not copy raw provider errors/SQL/credentials into machine output.
        print(json.dumps({"status": "error", "error_code": "EXECUTION_FAILED", "error_type": type(exc).__name__}))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
