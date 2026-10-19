"""Single Google query contract shared by Python, CLI and transport adapters."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd

from .constants import GSC_DIMENSIONS
from .query_metadata import COMPLETENESS_ATTR, QueryCompleteness

QUERY_COMPLETENESS_CONTRACT = "raw_rows/v1"


class QueryValidationError(ValueError):
    """The request is invalid; no API call has been made."""


class QueryAuthenticationError(RuntimeError):
    """Authentication failed during execution, rather than request validation."""


def _validate_auth(source, credential, cache_key, adc, client=None):
    if credential is not None and adc:
        raise QueryValidationError("pass credential or adc=True, not both")
    if client is not None and (credential is not None or cache_key is not None or adc):
        raise QueryValidationError("pass client or authentication options, not both")
    if source == "bigquery" and cache_key is not None:
        raise QueryValidationError("cache_key is only supported for GA4/GSC")
    if source != "bigquery" and adc:
        raise QueryValidationError("adc is only supported for BigQuery")


def _ga_filter(value):
    from .utils import parse_filter_conditions

    if value is None:
        return
    if isinstance(value, str):
        if not parse_filter_conditions(value, error_class=QueryValidationError):
            raise QueryValidationError("filter strings must contain a condition")
    elif isinstance(value, dict) and len(value) == 1:
        key, nodes = next(iter(value.items()))
        if key == "not":
            if nodes is None or nodes == "":
                raise QueryValidationError("not requires a filter node")
            _ga_filter(nodes)
        elif key in {"and", "or"} and isinstance(nodes, (list, tuple)) and nodes:
            for node in nodes:
                if node is None or node == "":
                    raise QueryValidationError("composite filters require non-empty nodes")
                _ga_filter(node)
        else:
            raise QueryValidationError("filter tree requires and/or lists or a not node")
    else:
        raise QueryValidationError("GA4 filters must be strings or and/or/not trees")


def _text(value: Any, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise QueryValidationError(f"{name} must be a non-empty string")
    return value.strip()


def _fields(value: Any, name: str, *, empty: bool = False) -> list:
    if not isinstance(value, (list, tuple)) or (not value and not empty):
        raise QueryValidationError(f"{name} must be a field list")
    result = []
    for field in value:
        if isinstance(field, str):
            result.append(_text(field, name))
        elif isinstance(field, (list, tuple)) and len(field) == 2:
            result.append(tuple(_text(part, name) for part in field))
        else:
            raise QueryValidationError(f"{name} fields must be strings or [name, alias]")
    return result


def validate_query(raw: Mapping[str, Any]) -> dict[str, Any]:
    """Validate the versioned single-query contract before authentication."""
    if not isinstance(raw, Mapping):
        raise QueryValidationError("request must be an object")
    params = dict(raw)
    if params.get("schema_version") != "1.0":
        raise QueryValidationError('schema_version must be "1.0"')
    source = params.get("source")
    if source == "bq":
        source = "bigquery"
    if not isinstance(source, str) or source not in {"ga4", "gsc", "bigquery"}:
        raise QueryValidationError("source must be ga4, gsc or bigquery")
    params["source"] = source
    required = {
        "ga4": {"property_id", "date_range", "dimensions", "metrics"},
        "gsc": {"site_url", "date_range", "dimensions"},
        "bigquery": {"project_id", "sql"},
    }[source]
    optional = {
        "ga4": {"limit", "filter_d", "filter_m", "sort"},
        "gsc": {"limit", "filter_d", "page_to_path"},
        "bigquery": {"location"},
    }[source]
    missing = required - params.keys()
    extra = params.keys() - required - optional - {"schema_version", "source"}
    if missing or extra:
        raise QueryValidationError(f"missing fields: {sorted(missing)}; unknown fields: {sorted(extra)}")
    for name in required - {"date_range", "dimensions", "metrics"}:
        params[name] = _text(params[name], name)
    if source == "bigquery":
        if "location" in params:
            params["location"] = _text(params["location"], "location")
        return params
    period = params["date_range"]
    if not isinstance(period, dict) or set(period) != {"start", "end"}:
        raise QueryValidationError("date_range requires only start and end")
    try:
        start, end = (date.fromisoformat(_text(period[key], key)) for key in ("start", "end"))
    except ValueError as exc:
        raise QueryValidationError("dates must be YYYY-MM-DD") from exc
    if period["start"] != start.isoformat() or period["end"] != end.isoformat() or start > end:
        raise QueryValidationError("dates must be YYYY-MM-DD and start <= end")
    params["date_range"] = {"start": start.isoformat(), "end": end.isoformat()}
    params["dimensions"] = _fields(params["dimensions"], "dimensions", empty=source == "ga4")
    allowed_dims = GSC_DIMENSIONS
    if source == "ga4":
        params["metrics"] = _fields(params["metrics"], "metrics")
        if len(params["dimensions"]) > 9 or len(params["metrics"]) > 10:
            raise QueryValidationError("GA4 allows up to 9 dimensions and 10 metrics")
    else:
        if any(not isinstance(dim, str) or dim not in allowed_dims for dim in params["dimensions"]):
            raise QueryValidationError("GSC requires supported dimension names; aliases are not supported")
        if "searchAppearance" in params["dimensions"] and len(params["dimensions"]) != 1:
            raise QueryValidationError("searchAppearance must be the only grouping dimension; use a filter for breakdowns")
        if "date" in params["dimensions"] and "month" in params["dimensions"]:
            raise QueryValidationError("GSC requires either date or month, not both")
    limit = params.get("limit", 1000)
    if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 100000:
        raise QueryValidationError("limit must be an integer between 1 and 100000")
    params["limit"] = limit
    if "page_to_path" in params and not isinstance(params["page_to_path"], bool):
        raise QueryValidationError("page_to_path must be boolean")
    for name in ("filter_d", "filter_m"):
        value = params.get(name)
        if source == "ga4":
            _ga_filter(value)
        elif name == "filter_d" and value is not None:
            from .start import Megaton

            try:
                filters = Megaton.Search._parse_dimension_filter(value)
                for item in filters or []:
                    if (set(item) != {"dimension", "operator", "expression"}
                            or item["dimension"] not in allowed_dims - {"month"}
                            or item["operator"] not in {"contains", "notContains", "equals", "notEquals", "includingRegex", "excludingRegex"}
                            or not isinstance(item["expression"], str)):
                        raise ValueError("invalid GSC filter")
            except (ValueError, TypeError) as exc:
                raise QueryValidationError("invalid GSC filter") from exc
    if "sort" in params:
        sort = params["sort"]
        tokens = sort.split(",") if isinstance(sort, str) else sort
        if not isinstance(tokens, (list, tuple)) or not tokens:
            raise QueryValidationError("sort must be a comma-separated string or list of field names")
        fields = {part if isinstance(part, str) else part[0] for part in params["dimensions"] + params["metrics"]}
        aliases = {part[1]: part[0] for part in params["dimensions"] + params["metrics"] if isinstance(part, tuple)}
        normalized = []
        for token in tokens:
            token = _text(token, "sort")
            descending = token.startswith("-")
            field = token[1:] if token.startswith(("+", "-")) else token
            field = aliases.get(field, field)
            if field not in fields:
                raise QueryValidationError("sort fields must appear in dimensions or metrics")
            normalized.append(("-" if descending else "") + field)
        params["sort"] = ",".join(normalized)
    return params


@dataclass(frozen=True)
class QueryResult:
    """Data plus normalized request; authentication never enters metadata."""

    df: pd.DataFrame
    params: dict[str, Any]
    completeness: QueryCompleteness | None = None

    def to_dict(self) -> dict[str, Any]:
        records = self.df.to_json(orient="records", date_format="iso")
        if records is None:
            raise RuntimeError("DataFrame serialization returned no JSON")
        return {
            "status": "ok", "schema_version": "1.0", "source": self.params["source"],
            "row_count": len(self.df), "columns": list(self.df.columns),
            "params": self.params,
            "completeness": self.completeness.to_dict() if self.completeness is not None else None,
            "data": json.loads(records),
        }

    def save(self, path: str | Path) -> None:
        target = Path(path)
        if target.suffix.lower() == ".csv":
            self.df.to_csv(target, index=False)
        elif target.suffix.lower() == ".json":
            target.write_text(json.dumps(self.to_dict(), ensure_ascii=False) + "\n", encoding="utf-8")
        else:
            raise QueryValidationError("output must end in .csv or .json")


def _bq_client(project_id: str, credential: str | None, adc: bool):
    import google.auth
    from google.cloud import bigquery

    if credential:
        credentials, _ = google.auth.load_credentials_from_file(
            credential, scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )
    elif adc:
        credentials, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
    else:
        raise QueryValidationError("BigQuery requires credential or adc=True")
    return bigquery.Client(project=project_id, credentials=credentials)


def _google_session(source: str, credential: str, cache_key: str | None):
    """Headless Megaton session; standalone GSC skips GA4 account discovery."""
    from .start import Megaton

    if source == "gsc":
        return Megaton(credential, headless=True, cache_key=cache_key, initialize_ga=False)
    return Megaton(credential, headless=True, cache_key=cache_key)


def run_query(
    params: Mapping[str, Any], *, credential: str | None = None,
    cache_key: str | None = None, adc: bool = False, client: Any = None,
) -> QueryResult:
    """Run one query with explicit auth or a caller-selected client.

    GA4/GSC client is a Megaton session; BigQuery client is a native BQ client.
    The caller owns lifecycle and routing of supplied clients.
    """
    request = validate_query(params)
    source = request["source"]
    completeness = None
    _validate_auth(source, credential, cache_key, adc, client)
    if source == "bigquery":
        bq = client if client is not None else _bq_client(request["project_id"], credential, adc)
        rows = bq.query(request["sql"], location=request.get("location")).result()
        df = rows.to_dataframe(create_bqstorage_client=False)
    else:
        if client is None:
            if not credential:
                raise QueryValidationError("GA4/GSC requires an explicit credential")
            client = _google_session(source, credential, cache_key)
            if client.creds is None:
                raise QueryAuthenticationError("authentication did not produce credentials")
        period = request["date_range"]
        if source == "ga4":
            client.use_property(request["property_id"])
            client.report.set.dates(period["start"], period["end"])
            client.report.data = None
            result = client.report.run(
                d=request["dimensions"], m=request["metrics"], show=False,
                limit=request["limit"], max_rows=request["limit"], filter_d=request.get("filter_d"),
                filter_m=request.get("filter_m"), sort=request.get("sort"),
            )
        else:
            client.search.use(request["site_url"])
            client.search.set.dates(period["start"], period["end"])
            result = client.search.run(
                dimensions=request["dimensions"], limit=min(request["limit"], 25000),
                max_rows=request["limit"],
                filter_d=request.get("filter_d"),
            )
        if result is None or not isinstance(result.df, pd.DataFrame):
            raise RuntimeError("query did not return a DataFrame result")
        df = result.df.copy()
        completeness = QueryCompleteness.from_frame(df, row_limit=request["limit"])
        if source == "gsc" and request.get("page_to_path") and "page" in df.columns:
            from urllib.parse import urlparse

            df["page"] = df["page"].map(lambda value: urlparse(value).path)
            if not df.empty:
                from .services.gsc_service import GSCService

                columns = list(df.columns)
                df = GSCService._aggregate(df, request["dimensions"])
                if "ctr" in columns:
                    df["ctr"] = (df["clicks"] / df["impressions"].replace(0, float("nan"))).fillna(0)
                df = df.reindex(columns=columns)
        df.attrs[COMPLETENESS_ATTR] = completeness.to_dict()
    return QueryResult(df=df, params=request, completeness=completeness)


def list_resources(
    source: str, *, credential: str | None = None, project_id: str | None = None,
    cache_key: str | None = None, adc: bool = False,
) -> list:
    """List resources using only caller-selected authentication."""
    _validate_auth(source, credential, cache_key, adc)
    if source == "bigquery":
        project = _text(project_id, "project_id")
        return [{"id": item.dataset_id} for item in _bq_client(project, credential, adc).list_datasets()]
    if source not in {"ga4", "gsc"} or not credential or adc:
        raise QueryValidationError("GA4/GSC resources requires source and explicit credential")
    mg = _google_session(source, credential, cache_key)
    if mg.creds is None:
        raise QueryAuthenticationError("authentication did not produce credentials")
    if source == "ga4" and getattr(mg, "_ga_initialization_error", None) is not None:
        raise RuntimeError("GA4 discovery failed") from mg._ga_initialization_error
    return mg.properties() if source == "ga4" else mg.sites()
