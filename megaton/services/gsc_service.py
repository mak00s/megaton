"""Google Search Console service wrapper."""

import logging
import time
from typing import Optional
from urllib.parse import unquote

import pandas as pd
from googleapiclient.errors import HttpError

from .. import searchconsole

logger = logging.getLogger(__name__)


class GSCService:
    def __init__(self, app, client=None):
        self.app = app
        self._client = client

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
    def _clean_page(value: str) -> str:
        try:
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
        max_retries: int = 3,
        backoff_factor: float = 2.0,
        clean: bool = False,
        verbose: bool = False,
    ) -> pd.DataFrame:
        client = self._get_client()

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
        current_start = start_row
        total_rows = 0

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

            response = None
            for attempt in range(max_retries):
                try:
                    response = (
                        client.searchanalytics()
                        .query(siteUrl=site_url, body=request_body)
                        .execute()
                    )
                    break
                except HttpError as exc:
                    wait = backoff_factor * (2**attempt)
                    if attempt + 1 >= max_retries:
                        if verbose:
                            logger.warning(
                                "Search Console API error on startRow=%s: %s",
                                current_start,
                                exc,
                            )
                        return pd.DataFrame()
                    if verbose:
                        logger.warning(
                            "Search Console API error on startRow=%s; retrying in %.1fs (%s/%s)",
                            current_start,
                            wait,
                            attempt + 1,
                            max_retries,
                        )
                    time.sleep(wait)
                except Exception as exc:
                    if verbose:
                        logger.error("Search Console request failed on startRow=%s: %s", current_start, exc)
                    return pd.DataFrame()

            if response is None:
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
            response = client.sites().list().execute()
        except HttpError as exc:
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
