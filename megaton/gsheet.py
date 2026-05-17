"""
Functions for Google Sheets
"""

from typing import NoReturn, Optional, Union
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


def _raise_for_api_error(exc: gspread.exceptions.APIError) -> NoReturn:
    """gspread APIError をプロジェクトの errors.* に分類して送出する。

    既知パターン (disabled / PERMISSION_DENIED / NOT_FOUND) は対応する
    errors.* へ変換し、未知の APIError はそのまま再送出する
    (silent swallow しない)。必ず ``except`` 節の中から呼ぶこと —
    末尾の bare ``raise`` は処理中の例外を再送出する。
    """
    text = str(exc)
    if 'disabled' in text:
        raise errors.ApiDisabled
    if 'PERMISSION_DENIED' in text:
        raise errors.BadPermission
    if 'NOT_FOUND' in text:
        raise errors.UrlNotFound
    raise


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

    def call_with_retry(
        self,
        op: str,
        func,
        *,
        max_retries: Optional[int] = None,
        backoff_factor: Optional[float] = None,
        retry_on_requests: bool = False,
        sleep=time.sleep,
    ):
        """Run ``func`` with exponential-backoff retry for transient Google API errors.

        ``op`` is used as the log label. Set ``retry_on_requests=True`` to also
        retry ``requests`` exceptions. HTTP 429 quota retries add enough extra
        sleep to wait at least 30 seconds before the next attempt.
        """
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

        _QUOTA_FLOOR_WAIT = 30.0  # 429 needs at least this many seconds

        def _on_retry(attempt_no: int, max_attempts: int, wait: float, exc: BaseException) -> None:
            LOGGER.warning(
                "%s failed; retrying in %.1fs (%s/%s): %s",
                op,
                wait,
                attempt_no,
                max_attempts,
                exc,
            )
            # For quota errors, ensure we wait long enough for the quota
            # window to reset (~60s) even when the calculated backoff is
            # shorter.
            if _get_status_code(exc) == 429 and wait < _QUOTA_FLOOR_WAIT:
                extra = _QUOTA_FLOOR_WAIT - wait
                LOGGER.info("Quota error: adding %.1fs extra wait", extra)
                sleep(extra)

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
    def workbook(self):
        """開いている gspread Spreadsheet を返す read-only アクセサ。

        未 open のときは None。外部コンシューマが gspread の Spreadsheet API
        を直接使いたいとき用の公開 API（内部フィールド _driver の公開窓口）。
        """
        return self._driver

    @property
    def sheets(self):
        if not self._driver:
            return []
        worksheets = self.call_with_retry(
            "Google Sheets list worksheets",
            lambda: self._driver.worksheets(),
            retry_on_requests=True,
        )
        return [s.title for s in worksheets]

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
            self._driver = self.call_with_retry(
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
            _raise_for_api_error(e)

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
            call = getattr(self.parent, "call_with_retry", None)
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
                LOGGER.warning("Open URL first.")
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

        def duplicate(
            self,
            source_name: str,
            new_sheet_name: str,
            *,
            max_retries: Optional[int] = None,
            backoff_factor: Optional[float] = None,
        ):
            if not self.parent._client:
                LOGGER.error("Open URL first.")
                return
            try:
                source_ws = self._maybe_retry(
                    "Google Sheets select worksheet for duplicate",
                    lambda: self.parent._driver.worksheet(source_name),
                    max_retries=max_retries,
                    backoff_factor=backoff_factor,
                    retry_on_requests=True,
                )
            except gspread.exceptions.WorksheetNotFound:
                raise errors.SheetNotFound

            self._maybe_retry(
                "Google Sheets duplicate worksheet",
                lambda: self.parent._driver.duplicate_sheet(
                    source_ws.id,
                    new_sheet_name=new_sheet_name,
                ),
                max_retries=max_retries,
                backoff_factor=backoff_factor,
                retry_on_requests=True,
            )
            return self.select(
                new_sheet_name,
                max_retries=max_retries,
                backoff_factor=backoff_factor,
            )

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
                _raise_for_api_error(e)
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
            if not self._driver:
                return 0
            cols = self._maybe_retry(
                "Google Sheets read range",
                lambda: self._driver.range(
                    1, 1, self._driver.row_count, self._driver.col_count
                ),
                retry_on_requests=True,
            )
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

        def resize_dimensions(
            self,
            *,
            rows: Optional[int] = None,
            cols: Optional[int] = None,
            shrink: bool = False,
            max_retries: Optional[int] = None,
            backoff_factor: Optional[float] = None,
        ):
            """Resize worksheet grid dimensions.

            By default this is expand-only: values smaller than the current
            row/column count are ignored. Pass ``shrink=True`` to allow reducing
            grid size.
            """
            if rows is None and cols is None:
                return None
            if not self._driver:
                LOGGER.warning("Please select a sheet first.")
                return

            grid = {}
            fields = []
            if rows is not None:
                rows = int(rows)
                if rows < 1:
                    raise ValueError("rows must be >= 1")
                if shrink or rows > self._driver.row_count:
                    grid["rowCount"] = rows
                    fields.append("gridProperties.rowCount")
            if cols is not None:
                cols = int(cols)
                if cols < 1:
                    raise ValueError("cols must be >= 1")
                if shrink or cols > self._driver.col_count:
                    grid["columnCount"] = cols
                    fields.append("gridProperties.columnCount")

            if not fields:
                return None

            request = {
                "updateSheetProperties": {
                    "properties": {
                        "sheetId": self.id,
                        "gridProperties": grid,
                    },
                    "fields": ",".join(fields),
                }
            }
            return self._maybe_retry(
                "Google Sheets resize grid",
                lambda: self.parent._driver.batch_update({"requests": [request]}),
                max_retries=max_retries,
                backoff_factor=backoff_factor,
                retry_on_requests=True,
            )

        def set_gridlines(
            self,
            visible: bool,
            *,
            max_retries: Optional[int] = None,
            backoff_factor: Optional[float] = None,
        ):
            """Show or hide worksheet gridlines."""
            if not self._driver:
                LOGGER.warning("Please select a sheet first.")
                return
            request = {
                "updateSheetProperties": {
                    "properties": {
                        "sheetId": self.id,
                        "gridProperties": {"hideGridlines": not bool(visible)},
                    },
                    "fields": "gridProperties.hideGridlines",
                }
            }
            return self._maybe_retry(
                "Google Sheets set gridlines",
                lambda: self.parent._driver.batch_update({"requests": [request]}),
                max_retries=max_retries,
                backoff_factor=backoff_factor,
                retry_on_requests=True,
            )

        @staticmethod
        def _normalize_tab_color(color):
            if isinstance(color, str):
                value = color.strip()
                if value.startswith("#"):
                    value = value[1:]
                if len(value) != 6:
                    raise ValueError("tab color must be '#RRGGBB' or 'RRGGBB'")
                try:
                    red = int(value[0:2], 16) / 255
                    green = int(value[2:4], 16) / 255
                    blue = int(value[4:6], 16) / 255
                except ValueError as exc:
                    raise ValueError("tab color must be '#RRGGBB' or 'RRGGBB'") from exc
                return {"red": red, "green": green, "blue": blue}
            if isinstance(color, dict):
                allowed = {"red", "green", "blue", "alpha"}
                unknown = set(color) - allowed
                if unknown:
                    raise ValueError(f"unknown tab color keys: {sorted(unknown)}")
                normalized = {}
                for key, value in color.items():
                    value = float(value)
                    if not 0.0 <= value <= 1.0:
                        raise ValueError(
                            "tab color dict values must be between 0.0 and 1.0"
                        )
                    normalized[key] = value
                return normalized
            raise TypeError("tab color must be a hex string or Google Sheets color dict")

        def set_tab_color(
            self,
            color,
            *,
            max_retries: Optional[int] = None,
            backoff_factor: Optional[float] = None,
        ):
            """Set worksheet tab color from '#RRGGBB' or a Sheets color dict."""
            if not self._driver:
                LOGGER.warning("Please select a sheet first.")
                return
            request = {
                "updateSheetProperties": {
                    "properties": {
                        "sheetId": self.id,
                        "tabColor": self._normalize_tab_color(color),
                    },
                    "fields": "tabColor",
                }
            }
            return self._maybe_retry(
                "Google Sheets set tab color",
                lambda: self.parent._driver.batch_update({"requests": [request]}),
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
            if not self._driver:
                LOGGER.warning("Please select a sheet first.")
                return
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
                LOGGER.warning("Please select a sheet first.")
                return
            elif mode == 'w':
                try:
                    self.clear()
                except gspread.exceptions.APIError as e:
                    _raise_for_api_error(e)

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
                LOGGER.warning("Please select a sheet first.")
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
                _raise_for_api_error(e)

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
                    return self.parent._maybe_retry(
                        "Google Sheets read cell",
                        lambda: self.parent._driver.acell(self.address).value,
                        retry_on_requests=True,
                    )

            @data.setter
            def data(self, value):
                if self.address:
                    self.parent._maybe_retry(
                        "Google Sheets update cell",
                        lambda: self.parent._driver.update(self.address, value),
                        retry_on_requests=True,
                    )

            def select(self, row: Union[int, str], col: Optional[int] = None):
                if not self.parent._driver:
                    LOGGER.error("Please select a sheet first.")
                    return

                self.address = gspread.utils.rowcol_to_a1(row, col) if col else row
                return self.data
