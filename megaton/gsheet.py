"""
Functions for Google Sheets
"""

from typing import Optional, Union
import logging
import os
import time

import pandas as pd
import requests

from google.oauth2.credentials import Credentials
from google.oauth2 import service_account
from google.auth.exceptions import RefreshError
from gspread_dataframe import set_with_dataframe
import gspread

from . import errors, retry_utils

LOGGER = logging.getLogger(__name__)

_RETRYABLE_STATUS_CODES = frozenset({429, 500, 502, 503, 504})


def _get_status_code(exc) -> Optional[int]:
    resp = getattr(exc, "response", None)
    if resp is None:
        return None
    return getattr(resp, "status_code", None)


class MegatonGS(object):
    """Google Sheets client
    """
    required_scopes = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive',
    ]

    def __init__(
        self,
        credentials: Credentials,
        url: Optional[str] = None,
        timeout: Optional[float] = None,
        *,
        max_retries: Optional[int] = None,
        backoff_factor: Optional[float] = None,
        max_wait: Optional[float] = None,
        max_elapsed: Optional[float] = None,
        jitter: Optional[float] = None,
    ):
        """constructor"""
        self.credentials = credentials
        self._client: gspread.client.Client = None
        self._driver: gspread.spreadsheet.Spreadsheet = None
        self.sheet = self.Sheet(self)
        self.timeout = self._resolve_timeout(timeout)
        self.max_retries = self._resolve_max_retries(max_retries)
        self.backoff_factor = self._resolve_backoff_factor(backoff_factor)
        self.max_wait = self._resolve_max_wait(max_wait)
        self.max_elapsed = self._resolve_max_elapsed(max_elapsed)
        self.jitter = self._resolve_jitter(jitter)

        self._authorize()
        if url:
            self.open(url)

    def _authorize(self):
        """Validate credentials given and build client"""
        if not isinstance(self.credentials, (Credentials, service_account.Credentials)):
            self.credentials = None
            raise errors.BadCredentialFormat
        elif self.credentials.scopes:
            if not set(self.required_scopes) <= set(self.credentials.scopes):
                self.credentials = None
                raise errors.BadCredentialScope(self.required_scopes)
        self._client = gspread.authorize(self.credentials)
        if self.timeout is not None:
            self._client.http_client.timeout = self.timeout

    def _resolve_timeout(self, timeout: Optional[float]) -> Optional[float]:
        if timeout is None:
            env = os.getenv("MEGATON_GS_TIMEOUT")
            if env is not None:
                try:
                    timeout = float(env)
                except ValueError:
                    timeout = 180.0
            else:
                timeout = 180.0
        if timeout is not None and timeout <= 0:
            return None
        return timeout

    def _resolve_max_retries(self, value: Optional[int]) -> int:
        if value is None:
            env = os.getenv("MEGATON_GS_MAX_RETRIES")
            if env is not None:
                try:
                    value = int(env)
                except ValueError:
                    value = 3
            else:
                value = 3
        try:
            value = int(value)
        except Exception:  # noqa: BLE001
            value = 3
        return max(1, value)

    def _resolve_backoff_factor(self, value: Optional[float]) -> float:
        if value is None:
            env = os.getenv("MEGATON_GS_BACKOFF_FACTOR")
            if env is not None:
                try:
                    value = float(env)
                except ValueError:
                    value = 2.0
            else:
                value = 2.0
        try:
            value = float(value)
        except Exception:  # noqa: BLE001
            value = 2.0
        return max(0.0, value)

    def _resolve_max_wait(self, value: Optional[float]) -> Optional[float]:
        if value is None:
            env = os.getenv("MEGATON_GS_MAX_WAIT")
            if env is None:
                return None
            try:
                value = float(env)
            except ValueError:
                return None
        try:
            value = float(value)
        except Exception:  # noqa: BLE001
            return None
        if value <= 0:
            return None
        return value

    def _resolve_max_elapsed(self, value: Optional[float]) -> Optional[float]:
        if value is None:
            env = os.getenv("MEGATON_GS_MAX_ELAPSED")
            if env is None:
                return None
            try:
                value = float(env)
            except ValueError:
                return None
        try:
            value = float(value)
        except Exception:  # noqa: BLE001
            return None
        if value <= 0:
            return None
        return value

    def _resolve_jitter(self, value: Optional[float]) -> float:
        if value is None:
            env = os.getenv("MEGATON_GS_JITTER")
            if env is None:
                return 0.0
            try:
                value = float(env)
            except ValueError:
                value = 0.0
        try:
            value = float(value)
        except Exception:  # noqa: BLE001
            value = 0.0
        if value < 0:
            return 0.0
        if value >= 1:
            return 0.99
        return value

    def _call_with_retry(
        self,
        op: str,
        func,
        *,
        max_retries: Optional[int] = None,
        backoff_factor: Optional[float] = None,
        retry_on_requests: bool = False,
        sleep=time.sleep,
    ):
        default_max = getattr(self, "max_retries", 3)
        default_backoff = getattr(self, "backoff_factor", 2.0)
        default_max_wait = getattr(self, "max_wait", None)
        default_max_elapsed = getattr(self, "max_elapsed", None)
        default_jitter = getattr(self, "jitter", 0.0)
        max_retries = default_max if max_retries is None else max(1, int(max_retries))
        backoff_factor = default_backoff if backoff_factor is None else float(backoff_factor)

        exception_types = (gspread.exceptions.APIError,)
        if retry_on_requests:
            exception_types = exception_types + (requests.exceptions.RequestException,)

        def _is_retryable(exc: BaseException) -> bool:
            if retry_on_requests and isinstance(exc, requests.exceptions.RequestException):
                return True
            status = _get_status_code(exc)
            return status in _RETRYABLE_STATUS_CODES

        def _on_retry(attempt_no: int, max_attempts: int, wait: float, exc: BaseException) -> None:
            LOGGER.warning(
                "%s failed; retrying in %.1fs (%s/%s): %s",
                op,
                wait,
                attempt_no,
                max_attempts,
                exc,
            )

        return retry_utils.expo_retry(
            func,
            max_retries=max_retries,
            backoff_factor=backoff_factor,
            exceptions=exception_types,
            is_retryable=_is_retryable,
            on_retry=_on_retry,
            sleep=sleep,
            jitter=default_jitter,
            max_wait=default_max_wait,
            max_elapsed=default_max_elapsed,
        )

    @property
    def sheets(self):
        return [s.title for s in self._driver.worksheets()] if self._driver else []

    @property
    def title(self):
        return self._driver.title if self._driver else None

    @property
    def url(self):
        return self._driver.url if self._driver else None

    def open(
        self,
        url: str,
        sheet: str = None,
        *,
        max_retries: Optional[int] = None,
        backoff_factor: Optional[float] = None,
    ):
        """Get or create an api service

        Args:
            url (str): URL of the Google Sheets to open.
            sheet (str): Sheet name to open. optional

        Returns:
            title (str): the title of the Google Sheets opened
        """

        try:
            self._driver = self._call_with_retry(
                "Google Sheets open",
                lambda: self._client.open_by_url(url),
                max_retries=max_retries,
                backoff_factor=backoff_factor,
                retry_on_requests=True,
            )
        except gspread.exceptions.NoValidUrlKeyFound:
            raise errors.BadUrlFormat
        except PermissionError as exc:
            raise errors.BadPermission from exc
        except requests.exceptions.Timeout as exc:
            raise errors.Timeout from exc
        except requests.exceptions.RequestException as exc:
            raise errors.RequestError(str(exc)) from exc

        try:
            title = self._driver.title
        except RefreshError:
            raise errors.BadCredentialScope
        except gspread.exceptions.APIError as e:
            if 'disabled' in str(e):
                raise errors.ApiDisabled
            elif 'PERMISSION_DENIED' in str(e):
                raise errors.BadPermission
            elif 'NOT_FOUND' in str(e):
                raise errors.UrlNotFound
            else:
                raise

        if sheet:
            self.sheet.select(sheet)

        return title

    class Sheet(object):
        def __init__(self, parent):
            """constructor"""
            self.parent = parent
            self._driver: gspread.worksheet.Worksheet = None
            self.cell = self.Cell(self)

        def _maybe_retry(
            self,
            op: str,
            func,
            *,
            max_retries: Optional[int] = None,
            backoff_factor: Optional[float] = None,
            retry_on_requests: bool = False,
        ):
            call = getattr(self.parent, "_call_with_retry", None)
            if callable(call):
                return call(
                    op,
                    func,
                    max_retries=max_retries,
                    backoff_factor=backoff_factor,
                    retry_on_requests=retry_on_requests,
                )
            return func()

        def _refresh(self):
            """Rebuild the Gspread client"""
            # self._driver = self.parent._driver.worksheet(self.name)
            self.select(self.name)

        def clear(self, *, max_retries: Optional[int] = None, backoff_factor: Optional[float] = None):
            """Blank all the cells on the sheet"""
            return self._maybe_retry(
                "Google Sheets clear",
                lambda: self._driver.clear(),
                max_retries=max_retries,
                backoff_factor=backoff_factor,
                retry_on_requests=True,
            )

        def create(self, name: str, *, max_retries: Optional[int] = None, backoff_factor: Optional[float] = None):
            if not self.parent._client:
                LOGGER.warn("Open URL first.")
                return
            self._maybe_retry(
                "Google Sheets create worksheet",
                lambda: self.parent._driver.add_worksheet(title=name, rows=100, cols=20),
                max_retries=max_retries,
                backoff_factor=backoff_factor,
                retry_on_requests=True,
            )
            self.select(name)

        def delete(self, name: str, *, max_retries: Optional[int] = None, backoff_factor: Optional[float] = None):
            if not self.parent._client:
                LOGGER.error("Open URL first.")
                return
            try:
                ws = self.parent._driver.worksheet(name)
            except gspread.exceptions.WorksheetNotFound:
                raise errors.SheetNotFound
            self._maybe_retry(
                "Google Sheets delete worksheet",
                lambda: self.parent._driver.del_worksheet(ws),
                max_retries=max_retries,
                backoff_factor=backoff_factor,
                retry_on_requests=True,
            )
            if self._driver and self._driver.title == name:
                self._driver = None

        def select(self, name: str, *, max_retries: Optional[int] = None, backoff_factor: Optional[float] = None):
            if not self.parent._client:
                LOGGER.error("Open URL first.")
                return
            try:
                self._driver = self._maybe_retry(
                    "Google Sheets select worksheet",
                    lambda: self.parent._driver.worksheet(name),
                    max_retries=max_retries,
                    backoff_factor=backoff_factor,
                    retry_on_requests=True,
                )
            except gspread.exceptions.WorksheetNotFound:
                raise errors.SheetNotFound
            except gspread.exceptions.APIError as e:
                if 'disabled' in str(e):
                    raise errors.ApiDisabled
                elif 'PERMISSION_DENIED' in str(e):
                    raise errors.BadPermission
            return self.name

        @property
        def id(self):
            """Sheet ID"""
            return self._driver.id if self._driver else None

        @property
        def name(self):
            """Sheet Name"""
            return self._driver.title if self._driver else None

        @property
        def last_row(self):
            """looks for the last row based on values appearing in all columns
            """
            cols = self._driver.range(1, 1, self._driver.row_count, self._driver.col_count)
            last = [cell.row for cell in cols if cell.value]
            return max(last) if last else 0

        @property
        def next_available_row(self):
            """looks for the first empty row based on values appearing in all columns
            """
            return self.last_row + 1

        def get_records(self, *, max_retries: Optional[int] = None, backoff_factor: Optional[float] = None):
            """Read all records with header row as keys."""
            if not self._driver:
                LOGGER.error("Please select a sheet first.")
                return
            return self._maybe_retry(
                "Google Sheets read records",
                lambda: self._driver.get_all_records(),
                max_retries=max_retries,
                backoff_factor=backoff_factor,
                retry_on_requests=True,
            )

        @property
        def data(self):
            """Returns a list of dictionaries (header row as keys)."""
            return self.get_records()

        def auto_resize(self, cols: list, *, max_retries: Optional[int] = None, backoff_factor: Optional[float] = None):
            """Auto resize columns to fit text"""
            sheet_id = self.id
            _requests = []
            for i in cols:
                dim = {
                    "autoResizeDimensions": {
                        "dimensions": {
                            "sheetId": sheet_id,
                            "dimension": "COLUMNS",
                            "startIndex": i - 1,
                            "endIndex": i
                        }
                    }
                }
                _requests.append(dim)
            return self._maybe_retry(
                "Google Sheets auto resize columns",
                lambda: self.parent._driver.batch_update({'requests': _requests}),
                max_retries=max_retries,
                backoff_factor=backoff_factor,
                retry_on_requests=True,
            )

        def resize(self, col: int, width: int, *, max_retries: Optional[int] = None, backoff_factor: Optional[float] = None):
            """Resize columns"""
            sheet_id = self.id
            _requests = []
            for i in [col]:
                dim = {
                    "updateDimensionProperties": {
                        "range": {
                            "sheetId": sheet_id,
                            "dimension": "COLUMNS",
                            "startIndex": i - 1,
                            "endIndex": i
                        },
                        "properties": {
                            "pixelSize": width
                        },
                        "fields": "pixelSize"
                    }
                }
                _requests.append(dim)
            return self._maybe_retry(
                "Google Sheets resize column",
                lambda: self.parent._driver.batch_update({'requests': _requests}),
                max_retries=max_retries,
                backoff_factor=backoff_factor,
                retry_on_requests=True,
            )

        def freeze(
            self,
            rows: Optional[int] = None,
            cols: Optional[int] = None,
            *,
            max_retries: Optional[int] = None,
            backoff_factor: Optional[float] = None,
        ):
            """Freeze rows and/or columns on the worksheet"""
            return self._maybe_retry(
                "Google Sheets freeze",
                lambda: self._driver.freeze(rows=rows, cols=cols),
                max_retries=max_retries,
                backoff_factor=backoff_factor,
                retry_on_requests=True,
            )

        def save_data(
            self,
            df: pd.DataFrame,
            mode: str = 'a',
            row: int = 1,
            include_index: bool = False,
            *,
            max_retries: Optional[int] = None,
            backoff_factor: Optional[float] = None,
        ):
            """Save the dataframe to the sheet"""
            if not len(df):
                LOGGER.info("no data to write.")
                return
            elif not self._driver:
                LOGGER.warn("Please select a sheet first.")
                return
            elif mode == 'w':
                try:
                    self.clear()
                except gspread.exceptions.APIError as e:
                    if 'disabled' in str(e):
                        raise errors.ApiDisabled
                    elif 'PERMISSION_DENIED' in str(e):
                        raise errors.BadPermission

                self._maybe_retry(
                    "Google Sheets overwrite (set_with_dataframe)",
                    lambda: set_with_dataframe(
                        self._driver,
                        df,
                        include_index=include_index,
                        include_column_header=True,
                        row=row,
                        resize=True,
                    ),
                    max_retries=max_retries,
                    backoff_factor=backoff_factor,
                    retry_on_requests=True,
                )
                return True
            elif mode == 'a':
                next_row = self.next_available_row
                current_row = self._driver.row_count
                new_rows = df.shape[0]
                if current_row < next_row + new_rows - 1:
                    LOGGER.debug(f"adding {next_row + new_rows - current_row - 1} rows")
                    self._maybe_retry(
                        "Google Sheets add rows",
                        lambda: self._driver.add_rows(next_row + new_rows - current_row),
                        max_retries=max_retries,
                        backoff_factor=backoff_factor,
                        retry_on_requests=True,
                    )
                    self._refresh()

                # Append is not strictly idempotent; avoid retrying on generic network errors by default.
                self._maybe_retry(
                    "Google Sheets append (set_with_dataframe)",
                    lambda: set_with_dataframe(
                        self._driver,
                        df,
                        include_index=include_index,
                        include_column_header=False,
                        row=next_row,
                        resize=False,
                    ),
                    max_retries=max_retries,
                    backoff_factor=backoff_factor,
                    retry_on_requests=False,
                )
                return True

        def overwrite_data(
            self,
            df: pd.DataFrame,
            include_index: bool = False,
            *,
            max_retries: Optional[int] = None,
            backoff_factor: Optional[float] = None,
        ):
            """Clear the sheet and save the dataframe"""
            return self.save_data(
                df,
                mode='w',
                include_index=include_index,
                max_retries=max_retries,
                backoff_factor=backoff_factor,
            )

        def overwrite_data_from_row(
            self,
            df: pd.DataFrame,
            row: int,
            include_index: bool = False,
            *,
            max_retries: Optional[int] = None,
            backoff_factor: Optional[float] = None,
        ):
            """Preserve rows above ``row`` and overwrite data from ``row`` onward."""
            if row <= 1:
                kwargs = {}
                if max_retries is not None:
                    kwargs["max_retries"] = max_retries
                if backoff_factor is not None:
                    kwargs["backoff_factor"] = backoff_factor
                return self.overwrite_data(df, include_index=include_index, **kwargs)

            if not len(df):
                LOGGER.info("no data to write.")
                return
            if not self._driver:
                LOGGER.warn("Please select a sheet first.")
                return

            try:
                # Keep rows above `row` and clear old payload from target row onward.
                self._maybe_retry(
                    "Google Sheets batch clear",
                    lambda: self._driver.batch_clear([f"{row}:{self._driver.row_count}"]),
                    max_retries=max_retries,
                    backoff_factor=backoff_factor,
                    retry_on_requests=True,
                )
            except gspread.exceptions.APIError as e:
                if 'disabled' in str(e):
                    raise errors.ApiDisabled
                elif 'PERMISSION_DENIED' in str(e):
                    raise errors.BadPermission
                raise

            header_rows = 1
            required_last_row = row + len(df) + header_rows - 1
            if self._driver.row_count < required_last_row:
                self._maybe_retry(
                    "Google Sheets add rows",
                    lambda: self._driver.add_rows(required_last_row - self._driver.row_count),
                    max_retries=max_retries,
                    backoff_factor=backoff_factor,
                    retry_on_requests=True,
                )
                self._refresh()

            required_cols = len(df.columns) + (1 if include_index else 0)
            if self._driver.col_count < required_cols:
                self._maybe_retry(
                    "Google Sheets add cols",
                    lambda: self._driver.add_cols(required_cols - self._driver.col_count),
                    max_retries=max_retries,
                    backoff_factor=backoff_factor,
                    retry_on_requests=True,
                )
                self._refresh()

            self._maybe_retry(
                "Google Sheets overwrite from row (set_with_dataframe)",
                lambda: set_with_dataframe(
                    self._driver,
                    df,
                    include_index=include_index,
                    include_column_header=True,
                    row=row,
                    resize=False,
                ),
                max_retries=max_retries,
                backoff_factor=backoff_factor,
                retry_on_requests=True,
            )
            return True

        class Cell(object):
            def __init__(self, parent):
                self.parent = parent
                self.address: str = ''
                # self.data = None

            @property
            def data(self):
                if self.address:
                    return self.parent._driver.acell(self.address).value

            @data.setter
            def data(self, value):
                if self.address:
                    self.parent._driver.update(self.address, value)

            def select(self, row: Union[int, str], col: Optional[int] = None):
                if not self.parent._driver:
                    LOGGER.error("Please select a sheet first.")
                    return

                self.address = gspread.utils.rowcol_to_a1(row, col) if col else row
                return self.data
