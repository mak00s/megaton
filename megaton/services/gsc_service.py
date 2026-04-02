"""Google Search Console service wrapper."""

import logging
import os
import time
from typing import Optional
from urllib.parse import unquote

import pandas as pd
from googleapiclient.errors import HttpError

from .. import retry_utils, searchconsole

logger = logging.getLogger(__name__)


class GSCService:
    def __init__(self, app, client=None):
        self.app = app
        self._client = client

    @staticmethod
    def _resolve_env(value, env_key: str, default, cast):
        if value is None:
            env = os.getenv(env_key)
            if env is not None:
                try:
                    value = cast(env)
                except (ValueError, TypeError):
                    value = default
            else:
                value = default
        try:
            value = cast(value)
        except Exception:  # noqa: BLE001
            value = default
        return value

    @classmethod
    def _resolve_max_retries(cls, value: Optional[int]) -> int:
        return max(1, cls._resolve_env(value, "MEGATON_GSC_MAX_RETRIES", 3, int))

    @classmethod
    def _resolve_backoff_factor(cls, value: Optional[float]) -> float:
        return max(0.0, cls._resolve_env(value, "MEGATON_GSC_BACKOFF_FACTOR", 2.0, float))

    @staticmethod
    def _is_retryable_http_error(exc: BaseException) -> bool:
        if not isinstance(exc, HttpError):
            return False
        resp = getattr(exc, "resp", None)
        status = getattr(resp, "status", None)
        return status in {429, 500, 502, 503, 504}

    @staticmethod
    def _retryable_exceptions() -> tuple[type[BaseException], ...]:
        return (HttpError, TimeoutError, ConnectionError, BrokenPipeError)

    def _get_client(self):
        if self._client is not None:
            return self._client
        if self.app is None:
            raise RuntimeError("Search Console client is not initialized.")

        sc = getattr(self.app, "_sc_client", None)
        if sc is not None:
            client = getattr(sc, "client", None) or getattr(sc, "_client", None)
            if client is not None:
                self._client = client
                return client

        creds = getattr(self.app, "creds", None)
        if creds is None:
            raise RuntimeError("Search Console credentials are not available.")

        sc = searchconsole.MegatonSC(creds)
        self.app._sc_client = sc
        self._client = sc.client
        return self._client

    @staticmethod
    def _site_url_candidates(site_url: str) -> list[str]:
        """Return normalized site_url candidates (preserving order).

        Search Console URL-prefix properties can differ only by trailing slash.
        This helper keeps the original first, then tries slash/no-slash variants.
        """
        raw = str(site_url or "").strip()
        if not raw:
            return []

        if raw.startswith("sc-domain:"):
            return [raw]

        variants = [raw]
        if raw.startswith("http://") or raw.startswith("https://"):
            base = raw.rstrip("/")
            if base:
                variants.extend([base, f"{base}/"])

        candidates: list[str] = []
        for value in variants:
            if value and value not in candidates:
                candidates.append(value)
        return candidates

    @staticmethod
    def _clean_page(value: str) -> str:
        """
        URL 正規化
        1. URL デコード（%xx → 文字）
        2. ? 以降（クエリパラメータ）を削除
        3. # 以降（フラグメント）を削除
        4. 小文字化
        
        Args:
            value: 正規化する URL
        
        Returns:
            正規化された URL
        """
        try:
            # 1. デコード（既存の unquote を活用）
            # 2-3. ? と # を削除（既存ロジック）
            # 4. 小文字化（既存ロジック）
            return unquote(str(value)).split("?", 1)[0].split("#", 1)[0].strip().lower()
        except Exception:
            return str(value)

    @staticmethod
    def _aggregate(df: pd.DataFrame, dimensions: list) -> pd.DataFrame:
        if df.empty:
            return df

        dims = [dim for dim in dimensions if dim in df.columns]
        if not dims:
            return df

        df = df.copy()
        if "impressions" not in df.columns or "clicks" not in df.columns:
            return df

        if "position" in df.columns:
            df["weighted_position"] = df["position"] * df["impressions"]

        agg_dict = {
            "clicks": "sum",
            "impressions": "sum",
        }
        if "weighted_position" in df.columns:
            agg_dict["weighted_position"] = "sum"

        grouped = df.groupby(dims, as_index=False).agg(agg_dict)
        if "weighted_position" in grouped.columns:
            grouped["position"] = (grouped["weighted_position"] / grouped["impressions"]).round(6)
            grouped.drop(columns=["weighted_position"], inplace=True)

        metric_cols = [col for col in ["clicks", "impressions", "position"] if col in grouped.columns]
        return grouped[dims + metric_cols]

    def query(
        self,
        site_url: str,
        start_date: str,
        end_date: str,
        dimensions: list,
        metrics: Optional[list] = None,
        dimension_filter: Optional[list] = None,
        country: Optional[str] = None,
        row_limit: int = 25000,
        start_row: int = 0,
        max_rows: int = 100000,
        max_retries: Optional[int] = None,
        backoff_factor: Optional[float] = None,
        clean: bool = False,
        verbose: bool = False,
    ) -> pd.DataFrame:
        client = self._get_client()
        max_retries = self._resolve_max_retries(max_retries)
        backoff_factor = self._resolve_backoff_factor(backoff_factor)
        site_candidates = self._site_url_candidates(site_url)
        if not site_candidates:
            return pd.DataFrame()

        if metrics is None:
            metrics = ["clicks", "impressions", "ctr", "position"]

        allowed_dims = {"date", "hour", "country", "device", "page", "query", "month"}
        filter_dims = allowed_dims - {"month"}
        allowed_ops = {
            "contains",
            "notContains",
            "equals",
            "notEquals",
            "includingRegex",
            "excludingRegex",
        }
        invalid_dims = [dim for dim in dimensions if dim not in allowed_dims]
        if invalid_dims:
            raise ValueError(f"Invalid dimensions: {invalid_dims}. Allowed: {sorted(allowed_dims)}")

        has_month = "month" in dimensions
        if has_month and "date" in dimensions:
            raise ValueError("Use either 'month' or 'date' in dimensions, not both.")

        api_dimensions = ["date" if dim == "month" else dim for dim in dimensions]

        dimension_filters = []
        if dimension_filter is not None:
            if not isinstance(dimension_filter, (list, tuple)):
                raise ValueError(
                    "dimension_filter must be a list or tuple of dicts "
                    "with keys: dimension, operator, expression"
                )
            for item in dimension_filter:
                if not isinstance(item, dict):
                    raise ValueError(
                        "dimension_filter items must be dicts "
                        "with keys: dimension, operator, expression"
                    )
                dimension = item.get("dimension")
                operator = item.get("operator")
                expression = item.get("expression")
                if not dimension or operator is None or expression is None:
                    raise ValueError(
                        "dimension_filter items must have dimension/operator/expression"
                    )
                if dimension not in filter_dims:
                    raise ValueError(
                        f"Invalid filter dimension: {dimension}. "
                        f"Allowed: {sorted(filter_dims)}"
                    )
                if operator not in allowed_ops:
                    raise ValueError(
                        f"Invalid filter operator: {operator}. "
                        f"Allowed: {sorted(allowed_ops)}"
                    )
                dimension_filters.append(
                    {
                        "dimension": dimension,
                        "operator": operator,
                        "expression": expression,
                    }
                )

        all_rows = []
        for idx, site_candidate in enumerate(site_candidates):
            all_rows = []
            current_start = start_row
            total_rows = 0
            fallback_next = False

            while True:
                request_body = {
                    "startDate": start_date,
                    "endDate": end_date,
                    "dimensions": api_dimensions,
                    "rowLimit": row_limit,
                    "startRow": current_start,
                }

                filters = list(dimension_filters)
                if country:
                    filters.append(
                        {
                            "dimension": "country",
                            "operator": "equals",
                            "expression": country,
                        }
                    )
                if filters:
                    request_body["dimensionFilterGroups"] = [
                        {
                            "groupType": "and",
                            "filters": filters,
                        }
                    ]

                def _on_retry(attempt_no, max_attempts, wait, exc):
                    if verbose:
                        logger.warning(
                            "Search Console API error on startRow=%s; retrying in %.1fs (%s/%s)",
                            current_start,
                            wait,
                            attempt_no,
                            max_attempts,
                        )

                def _is_retryable(exc: BaseException) -> bool:
                    if isinstance(exc, (TimeoutError, ConnectionError, BrokenPipeError)):
                        return True
                    return self._is_retryable_http_error(exc)

                try:
                    response = retry_utils.expo_retry(
                        lambda: (
                            client.searchanalytics()
                            .query(siteUrl=site_candidate, body=request_body)
                            .execute()
                        ),
                        max_retries=max_retries,
                        backoff_factor=backoff_factor,
                        exceptions=self._retryable_exceptions(),
                        is_retryable=_is_retryable,
                        on_retry=_on_retry,
                        sleep=time.sleep,
                    )
                except HttpError as exc:
                    status = getattr(getattr(exc, "resp", None), "status", None)
                    fallbackable = status in {400, 403, 404}
                    first_page_failed = current_start == start_row and total_rows == 0
                    has_next_candidate = idx < (len(site_candidates) - 1)
                    if fallbackable and first_page_failed and has_next_candidate:
                        fallback_next = True
                        if verbose:
                            next_site = site_candidates[idx + 1]
                            logger.info(
                                "Search Console site_url '%s' failed (%s). Retrying with '%s'.",
                                site_candidate,
                                status,
                                next_site,
                            )
                        break
                    if verbose:
                        logger.warning(
                            "Search Console API error on startRow=%s: %s",
                            current_start,
                            exc,
                        )
                    return pd.DataFrame()
                except Exception as exc:
                    if verbose:
                        logger.error(
                            "Search Console request failed on startRow=%s: %s",
                            current_start,
                            exc,
                        )
                    return pd.DataFrame()

                rows = response.get("rows", [])
                if not rows:
                    break

                all_rows.extend(rows)
                total_rows += len(rows)

                if verbose:
                    logger.info("Fetched %s rows (startRow=%s)", len(rows), current_start)

                if len(rows) < row_limit:
                    break
                if total_rows >= max_rows:
                    if verbose:
                        logger.warning("Reached max_rows limit (%s), stopping.", max_rows)
                    break
                current_start += len(rows)

            if fallback_next:
                continue
            break

        if not all_rows:
            return pd.DataFrame()

        data = []
        for idx, row in enumerate(all_rows):
            keys = row.get("keys", [])
            if len(keys) < len(api_dimensions):
                if verbose:
                    logger.warning("Skipping row %s due to missing keys: %s", idx, keys)
                continue
            row_data = {dim: keys[i] for i, dim in enumerate(api_dimensions)}
            row_data.update(
                {
                    "clicks": row.get("clicks", 0),
                    "impressions": row.get("impressions", 0),
                    "position": row.get("position", 0.0),
                }
            )
            data.append(row_data)

        df = pd.DataFrame(data)
        if df.empty:
            return df

        if clean and "page" in df.columns:
            df = df.copy()
            df["page"] = df["page"].apply(self._clean_page)

        if has_month and "date" in df.columns:
            df = df.copy()
            df["month"] = pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y%m")

        if clean or has_month:
            df = self._aggregate(df, dimensions)

        if "ctr" in metrics:
            if "ctr" not in df.columns:
                denom = df.get("impressions", pd.Series(index=df.index, dtype="float"))
                denom = denom.replace(0, pd.NA)
                df["ctr"] = (df.get("clicks", 0) / denom).fillna(0)

        for metric in metrics:
            if metric not in df.columns:
                df[metric] = 0

        ordered = [col for col in dimensions if col in df.columns] + metrics
        return df[ordered]

    def fetch_sites(
        self,
        sites: list,
        clinic_filter: Optional[list],
        start_date: str,
        end_date: str,
        dimensions: list,
        country: str = "jpn",
        include_clinic: bool = True,
        include_month: bool = True,
        clean: bool = True,
        verbose: bool = False,
    ) -> pd.DataFrame:
        dfs = []
        selected_sites = [
            site
            for site in sites
            if not clinic_filter or site.get("clinic") in clinic_filter
        ]

        for site in selected_sites:
            clinic = site.get("clinic")
            site_url = site.get("url", "").strip()
            if not site_url:
                continue

            if verbose:
                logger.info("Fetching GSC data: %s (%s)", clinic, site_url)

            df = self.query(
                site_url=site_url,
                start_date=start_date,
                end_date=end_date,
                dimensions=dimensions,
                country=country,
                clean=clean,
                verbose=verbose,
            )

            if df.empty:
                continue

            if include_clinic:
                df = df.copy()
                df["clinic"] = clinic
            if include_month:
                df = df.copy()
                df["month"] = pd.to_datetime(start_date).strftime("%Y%m")
            dfs.append(df)

        if not dfs:
            return pd.DataFrame()

        return pd.concat(dfs, ignore_index=True)

    def list_sites(self) -> list[str]:
        client = self._get_client()
        try:
            response = retry_utils.expo_retry(
                lambda: client.sites().list().execute(),
                max_retries=self._resolve_max_retries(None),
                backoff_factor=self._resolve_backoff_factor(None),
                exceptions=self._retryable_exceptions(),
                is_retryable=lambda exc: (
                    isinstance(exc, (TimeoutError, ConnectionError, BrokenPipeError))
                    or self._is_retryable_http_error(exc)
                ),
                on_retry=lambda attempt_no, max_attempts, wait, exc: logger.warning(
                    "Search Console sites fetch failed; retrying in %.1fs (%s/%s): %s",
                    wait,
                    attempt_no,
                    max_attempts,
                    exc,
                ),
                sleep=time.sleep,
            )
        except (HttpError, TimeoutError, ConnectionError, BrokenPipeError) as exc:
            logger.warning("Search Console API error while listing sites: %s", exc)
            raise RuntimeError("Search Console sites fetch failed.") from exc
        except Exception as exc:
            logger.error("Search Console sites list failed: %s", exc)
            raise RuntimeError("Search Console sites fetch failed.") from exc

        entries = response.get("siteEntry") if isinstance(response, dict) else None
        if not entries:
            return []
        return [
            entry.get("siteUrl")
            for entry in entries
            if isinstance(entry, dict) and entry.get("siteUrl")
        ]
