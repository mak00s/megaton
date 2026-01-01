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
        country: Optional[str] = None,
        row_limit: int = 25000,
        start_row: int = 0,
        max_rows: int = 100000,
        max_retries: int = 3,
        backoff_factor: float = 2.0,
        clean: bool = False,
        aggregate: bool = False,
        verbose: bool = False,
    ) -> pd.DataFrame:
        client = self._get_client()

        all_rows = []
        current_start = start_row
        total_rows = 0

        while True:
            request_body = {
                "startDate": start_date,
                "endDate": end_date,
                "dimensions": dimensions,
                "rowLimit": row_limit,
                "startRow": current_start,
            }
            if country:
                request_body["dimensionFilterGroups"] = [
                    {
                        "filters": [
                            {
                                "dimension": "country",
                                "operator": "equals",
                                "expression": country,
                            }
                        ]
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
            if len(keys) < len(dimensions):
                if verbose:
                    logger.warning("Skipping row %s due to missing keys: %s", idx, keys)
                continue
            row_data = {dim: keys[i] for i, dim in enumerate(dimensions)}
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

        if aggregate:
            df = self._aggregate(df, dimensions)

        return df

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
        aggregate: bool = True,
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
                aggregate=aggregate,
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
