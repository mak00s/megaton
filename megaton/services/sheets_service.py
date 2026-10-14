"""Google Sheets service wrapper."""

import logging
from datetime import datetime, timedelta
from numbers import Real
from typing import Optional

import pandas as pd

from .. import errors, gsheet, gsheet_lowlevel, mount_google_drive

logger = logging.getLogger(__name__)


def _public_sheet_frame(frame):
    """Keep native serial/format carriers out of public results and sizing."""
    def public_value(value):
        if not isinstance(value, gsheet_lowlevel._NativeDate):
            return value
        instant = datetime(1899, 12, 30) + timedelta(days=value.serial)
        kind = value.number_format.get("type")
        if kind == "TIME":
            return instant.time()
        if kind == "DATE" and instant == instant.replace(hour=0, minute=0, second=0, microsecond=0):
            return instant.date()
        return instant
    result = frame.copy()
    for column in frame.columns:
        if any(isinstance(value, gsheet_lowlevel._NativeDate) for value in frame[column]):
            result[column] = pd.Series(
                [public_value(value) for value in frame[column]], index=frame.index, dtype=object,
            )
    return result


class SheetsService:
    def __init__(self, app):
        self.app = app

    def _open_sheet(self, url: str, *, reset_gs: bool, mount_drive: bool, no_creds_message: str):
        if not self.app.creds:
            logger.warning(no_creds_message)
            return None
        if reset_gs:
            self.app.gs = None
        if mount_drive and self.app.in_colab:
            mount_google_drive()
        try:
            _retry = getattr(self.app, "_retry", {}) or {}
            self.app.gs = gsheet.MegatonGS(
                self.app.creds,
                url,
                max_retries=_retry.get("max_retries"),
                backoff_factor=_retry.get("backoff_factor"),
            )
        except errors.BadCredentialFormat:
            print("認証情報のフォーマットが正しくないため、Google Sheets APIを利用できません。")
        except errors.BadCredentialScope:
            print("認証情報のスコープ不足のため、Google Sheets APIを利用できません。")
        except errors.BadUrlFormat:
            print("URLのフォーマットが正しくありません")
        except errors.ApiDisabled:
            print("Google SheetsのAPIが有効化されていません。")
        except errors.UrlNotFound:
            print("URLが見つかりません。")
        except errors.BadPermission:
            print("該当スプレッドシートを読み込む権限がありません。")
            if hasattr(self.app, 'creds') and hasattr(self.app.creds, 'service_account_email'):
                print(f"使用中のアカウント: {self.app.creds.service_account_email}")
                print(f"対象のスプレッドシートにこのアカウントへの閲覧権限を付与してください。")
            print(f"URL: {url}")
        except errors.Timeout:
            print("Google Sheetsの接続がタイムアウトしました。")
        except errors.RequestError as exc:
            print(f"Google Sheetsへの接続でエラーが発生しました: {exc}")
        except Exception as exc:
            raise exc
        else:
            if self.app.gs.title:
                print(f"Googleスプレッドシート「{self.app.gs.title}」を開きました。")
                self.app.state.gs_url = url
                self.app.state.gs_title = self.app.gs.title
                return True

    def launch_gs(self, url: str):
        """APIでGoogle Sheetsにアクセスする準備"""
        return self._open_sheet(
            url,
            reset_gs=False,
            mount_drive=True,
            no_creds_message='認証が完了していないため、Google Sheets API を初期化できません。',
        )

    def open_sheet(self, url: str):
        """Google Sheets APIの準備"""
        return self._open_sheet(
            url,
            reset_gs=True,
            mount_drive=False,
            no_creds_message='認証が完了していないため、Google Sheets を開けません。',
        )

    def select_sheet(self, sheet_name: str) -> Optional[bool]:
        """開いたGoogle Sheetsのシートを選択"""
        try:
            name = self.app.gs.sheet.select(sheet_name)
            if name:
                print(f"「{sheet_name}」シートを選択しました。")
                self.app.state.gs_sheet_name = sheet_name
                return True
        except errors.SheetNotFound:
            print(f"{sheet_name} シートが存在しません。")

    def duplicate_sheet(
        self,
        sheet_url: str,
        source_sheet_name: str,
        new_sheet_name: str,
        *,
        cell_update: Optional[dict] = None,
        max_retries: Optional[int] = None,
        backoff_factor: Optional[float] = None,
    ) -> Optional[bool]:
        if not self.open_sheet(sheet_url):
            return None

        try:
            duplicated = self.app.gs.sheet.duplicate(
                source_sheet_name,
                new_sheet_name,
                max_retries=max_retries,
                backoff_factor=backoff_factor,
            )
            if not duplicated:
                return None
            self.app.state.gs_sheet_name = new_sheet_name
        except errors.SheetNotFound:
            print(f"{source_sheet_name} シートが存在しません。")
            return None
        except Exception as exc:
            print(
                f"'{source_sheet_name}' シートの複製に失敗しました: "
                f"{exc}"
            )
            return None

        if cell_update:
            cell = cell_update.get("cell")
            value = cell_update.get("value")
            if cell is not None:
                try:
                    call = getattr(self.app.gs, "call_with_retry", None)
                    if callable(call):
                        call(
                            "Google Sheets update cell",
                            lambda: self.app.gs.sheet._driver.update_acell(cell, value),
                            max_retries=max_retries,
                            backoff_factor=backoff_factor,
                            retry_on_requests=True,
                        )
                    else:
                        self.app.gs.sheet._driver.update_acell(cell, value)
                except Exception as exc:
                    logger.warning(
                        "Sheet duplicated but optional cell update failed for '%s' (%s): %s",
                        new_sheet_name,
                        cell,
                        exc,
                    )
                    print(
                        f"'{source_sheet_name}' シートを '{new_sheet_name}' として複製しました。"
                    )
                    print(
                        f"ただし、セル {cell} の更新には失敗しました: {exc}"
                    )
                    return True

        print(f"'{source_sheet_name}' シートを '{new_sheet_name}' として複製しました。")
        return True

    def _calc_pixel_size(self, value, single_byte_multiplier: int, multi_byte_multiplier: int) -> int:
        total_width = 0
        for char in str(value):
            total_width += single_byte_multiplier if ord(char) < 128 else multi_byte_multiplier
        return total_width

    def _sort_df(self, df: pd.DataFrame, sort_by, sort_desc: bool) -> pd.DataFrame:
        if not sort_by:
            return df
        if isinstance(sort_by, str):
            sort_by = [sort_by]
        return df.sort_values(by=sort_by, ascending=not sort_desc)

    def _apply_column_widths(
        self,
        df: pd.DataFrame,
        *,
        width_min: int,
        width_max: int,
        single_byte_multiplier: int,
        multi_byte_multiplier: int,
        max_retries: Optional[int] = None,
        backoff_factor: Optional[float] = None,
    ) -> None:
        if df is None or len(df.columns) == 0:
            return
        sheet = self.app.gs.sheet
        sheet_id = sheet.id
        if not sheet_id:
            return

        requests = []
        for idx, col_name in enumerate(df.columns):
            series = df[col_name].astype(str)
            try:
                max_len = series.map(
                    lambda x: self._calc_pixel_size(x, single_byte_multiplier, multi_byte_multiplier)
                ).max()
            except ValueError:
                max_len = 0
            if pd.isna(max_len):
                max_len = 0
            header_len = self._calc_pixel_size(col_name, single_byte_multiplier, multi_byte_multiplier)
            pixel_size = max(min(max(max_len, header_len), width_max), width_min)
            requests.append(
                {
                    "updateDimensionProperties": {
                        "range": {
                            "sheetId": sheet_id,
                            "dimension": "COLUMNS",
                            "startIndex": idx,
                            "endIndex": idx + 1,
                        },
                        "properties": {"pixelSize": int(pixel_size)},
                        "fields": "pixelSize",
                    }
                }
            )
        if requests:
            call = getattr(self.app.gs, "call_with_retry", None)
            if callable(call):
                call(
                    "Google Sheets set column widths",
                    lambda: self.app.gs._driver.batch_update({"requests": requests}),
                    max_retries=max_retries,
                    backoff_factor=backoff_factor,
                    retry_on_requests=True,
                )
            else:
                self.app.gs._driver.batch_update({"requests": requests})

    def _sheet_to_df(self) -> pd.DataFrame:
        try:
            data = self.app.gs.sheet.data or []
        except Exception:
            return pd.DataFrame()
        return pd.DataFrame(data)

    def _apply_write_options(
        self,
        df_for_width: pd.DataFrame | None,
        *,
        auto_width: bool,
        freeze_header: bool,
        width_min: int,
        width_max: int,
        single_byte_multiplier: int,
        multi_byte_multiplier: int,
        max_retries: Optional[int] = None,
        backoff_factor: Optional[float] = None,
    ) -> None:
        if auto_width:
            self._apply_column_widths(
                df_for_width,
                width_min=width_min,
                width_max=width_max,
                single_byte_multiplier=single_byte_multiplier,
                multi_byte_multiplier=multi_byte_multiplier,
                max_retries=max_retries,
                backoff_factor=backoff_factor,
            )
        if freeze_header:
            self.app.gs.sheet.freeze(rows=1, max_retries=max_retries, backoff_factor=backoff_factor)

    def _select_or_create_sheet(self, sheet_name: str, *, create_if_missing: bool) -> bool:
        if not self.app.gs:
            raise ValueError("Google Sheetsが開かれていません。先に mg.open.sheet(url) を実行してください。")
        if sheet_name in self.app.gs.sheets:
            return bool(self.select_sheet(sheet_name))
        if not create_if_missing:
            print(f"{sheet_name} シートが存在しません。")
            return False
        try:
            self.app.gs.sheet.create(sheet_name)
        except Exception as exc:
            print(f"'{sheet_name}' シートの作成に失敗しました: {exc}")
            raise
        self.app.state.gs_sheet_name = sheet_name
        print(f"'{sheet_name}' シートを作成しました。")
        return True

    def save_sheet(
        self,
        sheet_name: str,
        df,
        *,
        sort_by=None,
        sort_desc: bool = True,
        start_row: int = 1,
        create_if_missing: bool = False,
        auto_width: bool = False,
        freeze_header: bool = False,
        max_retries: Optional[int] = None,
        backoff_factor: Optional[float] = None,
        width_min: int = 50,
        width_max: int = 500,
        single_byte_multiplier: int = 7,
        multi_byte_multiplier: int = 14,
    ):
        if start_row < 1:
            raise ValueError("start_row must be >= 1")

        if self._select_or_create_sheet(sheet_name, create_if_missing=create_if_missing):
            df = self._sort_df(df, sort_by, sort_desc)
            if start_row == 1:
                wrote = self.app.gs.sheet.overwrite_data(
                    df,
                    include_index=False,
                    max_retries=max_retries,
                    backoff_factor=backoff_factor,
                )
            else:
                wrote = self.app.gs.sheet.overwrite_data_from_row(
                    df,
                    row=start_row,
                    include_index=False,
                    max_retries=max_retries,
                    backoff_factor=backoff_factor,
                )

            if wrote:
                self._apply_write_options(
                    df,
                    auto_width=auto_width,
                    freeze_header=freeze_header,
                    width_min=width_min,
                    width_max=width_max,
                    single_byte_multiplier=single_byte_multiplier,
                    multi_byte_multiplier=multi_byte_multiplier,
                    max_retries=max_retries,
                    backoff_factor=backoff_factor,
                )
                print(f"データを「{sheet_name}」シートへ反映しました。")
            return bool(wrote)
        raise errors.SheetNotFound(f"Cannot save worksheet: {sheet_name}")

    def append_sheet(
        self,
        sheet_name: str,
        df,
        *,
        create_if_missing: bool = False,
        auto_width: bool = False,
        freeze_header: bool = False,
        max_retries: Optional[int] = None,
        backoff_factor: Optional[float] = None,
        width_min: int = 50,
        width_max: int = 500,
        single_byte_multiplier: int = 7,
        multi_byte_multiplier: int = 14,
    ):
        if self._select_or_create_sheet(sheet_name, create_if_missing=create_if_missing):
            if self.app.gs.sheet.save_data(
                df,
                include_index=False,
                max_retries=max_retries,
                backoff_factor=backoff_factor,
            ):
                width_df = self._sheet_to_df() if auto_width else None
                self._apply_write_options(
                    width_df,
                    auto_width=auto_width,
                    freeze_header=freeze_header,
                    width_min=width_min,
                    width_max=width_max,
                    single_byte_multiplier=single_byte_multiplier,
                    multi_byte_multiplier=multi_byte_multiplier,
                    max_retries=max_retries,
                    backoff_factor=backoff_factor,
                )
                print(f"データを「{sheet_name}」シートに追記しました。")
                return True
            return False
        raise errors.SheetNotFound(f"Cannot append worksheet: {sheet_name}")

    def open_or_create_sheet(self, sheet_url: str, sheet_name: str) -> Optional[bool]:
        if not self.app.gs or (self.app.gs.url and self.app.gs.url != sheet_url):
            if not self.open_sheet(sheet_url):
                return None

        if sheet_name in self.app.gs.sheets:
            try:
                self.app.gs.sheet.select(sheet_name)
                self.app.state.gs_sheet_name = sheet_name
                return True
            except errors.SheetNotFound:
                pass

        try:
            self.app.gs.sheet.create(sheet_name)
            self.app.state.gs_sheet_name = sheet_name
            print(f"'{sheet_name}' シートを作成しました。")
            return True
        except Exception as exc:
            print(f"'{sheet_name}' シートの作成に失敗しました: {exc}")
            return None

    def read_df(self, sheet_url: str, sheet_name: str) -> pd.DataFrame:
        if not self.open_sheet(sheet_url):
            raise errors.RequestError(f"Cannot open spreadsheet: {sheet_url}")
        try:
            self.app.gs.sheet.select(sheet_name)
            self.app.state.gs_sheet_name = sheet_name
        except errors.SheetNotFound:
            raise
        except Exception as exc:
            raise errors.RequestError(f"Cannot read {sheet_url} / {sheet_name}") from exc

        data = self.app.gs.sheet.data or []
        return pd.DataFrame(data)

    def upsert_df(
        self,
        sheet_url: str,
        sheet_name: str,
        df_new: pd.DataFrame,
        keys,
        columns=None,
        sort_by=None,
        create_if_missing: bool = True,
        auto_width: bool = False,
        freeze_header: bool = False,
        max_retries: Optional[int] = None,
        backoff_factor: Optional[float] = None,
        width_min: int = 50,
        width_max: int = 500,
        single_byte_multiplier: int = 7,
        multi_byte_multiplier: int = 14,
    ) -> pd.DataFrame:
        if not isinstance(df_new, pd.DataFrame):
            raise TypeError("df_new must be a pandas.DataFrame")
        keys = [keys] if isinstance(keys, str) else list(keys)
        if not keys or (not df_new.empty and any(key not in df_new.columns for key in keys)):
            raise ValueError("keys must name existing DataFrame columns")

        if create_if_missing:
            if not self.open_or_create_sheet(sheet_url, sheet_name):
                raise errors.RequestError(f"Cannot open {sheet_url} / {sheet_name}")
        else:
            if not self.open_sheet(sheet_url):
                raise errors.RequestError(f"Cannot open spreadsheet: {sheet_url}")
            try:
                self.app.gs.sheet.select(sheet_name)
                self.app.state.gs_sheet_name = sheet_name
            except errors.SheetNotFound:
                raise

        try:
            df_existing = pd.DataFrame(
                self.app.gs.sheet.get_records(
                    max_retries=max_retries,
                    backoff_factor=backoff_factor,
                    value_render_option="UNFORMATTED_VALUE",
                    numericise_ignore=["all"],
                    preserve_dates=True,
                )
            )
        except Exception as exc:
            logger.exception("Failed to read sheet '%s' for upsert", sheet_name)
            raise errors.RequestError(
                f"Cannot upsert {sheet_url} / {sheet_name}: existing data could not be read; no write performed."
            ) from exc

        df_new = df_new.copy()
        if df_new.empty:
            return _public_sheet_frame(df_existing)
        if df_existing.empty:
            try:
                public_frame = _public_sheet_frame(df_new)
                self.app.gs.sheet.overwrite_data(
                    df_new,
                    include_index=False,
                    max_retries=max_retries,
                    backoff_factor=backoff_factor,
                )
                self._apply_write_options(
                    public_frame,
                    auto_width=auto_width,
                    freeze_header=freeze_header,
                    width_min=width_min,
                    width_max=width_max,
                    single_byte_multiplier=single_byte_multiplier,
                    multi_byte_multiplier=multi_byte_multiplier,
                    max_retries=max_retries,
                    backoff_factor=backoff_factor,
                )
                print(f"'{sheet_name}' シートへ {len(df_new)} 行を書き込みました。")
                return public_frame
            except Exception as exc:
                logger.exception("Failed to overwrite sheet '%s' (empty upsert)", sheet_name)
                print(f"'{sheet_name}' シートへの書き込みに失敗しました: {exc}")
                raise

        df_existing = df_existing.copy()
        # Legacy formatted dates are parsed only when the incoming column has
        # an explicit datetime dtype. Native cells retain their serial/format.
        date_keys = [key for key in keys if pd.api.types.is_datetime64_any_dtype(df_new[key])]
        for key in date_keys:
            if key in df_existing:
                def date_value(value):
                    if isinstance(value, gsheet_lowlevel._NativeDate):
                        return value
                    if isinstance(value, Real) and not pd.isna(value):
                        return gsheet_lowlevel._NativeDate(float(value), {
                            "type": "DATE_TIME", "pattern": "yyyy-mm-dd hh:mm:ss.000",
                        })
                    return pd.Timestamp(value) if not pd.isna(value) else value
                df_existing[key] = df_existing[key].map(date_value)
        try:
            def normalize(value):
                cell = gsheet_lowlevel.cell_data(value)["userEnteredValue"]
                if "numberValue" in cell:
                    number = cell["numberValue"]
                    return str(int(number)) if float(number).is_integer() else repr(float(number))
                return str(next(iter(cell.values()), "")).strip()
            def key_value(value):
                cell = gsheet_lowlevel.cell_data(value)["userEnteredValue"]
                kind, content = next(iter(cell.items()), ("stringValue", ""))
                if kind == "stringValue" and content == "":
                    return ""
                return (kind, content)
            new_keys = df_new[keys].apply(lambda col: col.map(key_value))
            existing_keys = df_existing[keys].apply(lambda col: col.map(key_value))
            new_tuples = new_keys.apply(tuple, axis=1)
            existing_tuples = existing_keys.apply(tuple, axis=1)
            incomplete = new_keys.eq("").any(axis=1)
            keys_to_remove = set(new_tuples[~incomplete])
            mask = existing_tuples.isin(keys_to_remove)
        except KeyError as exc:
            raise ValueError(f"Existing sheet is missing upsert keys: {keys}") from exc

        # Incomplete keys can have historical duplicates. Keep those rows when
        # the incoming row is already present; never collapse ambiguous history.
        skip_new = pd.Series(False, index=df_new.index)
        history_indexes = {}
        for index, key_tuple in existing_tuples.items():
            history_indexes.setdefault(key_tuple, []).append(index)
        for index in df_new.index[incomplete]:
            matches = history_indexes.get(new_tuples.loc[index], [])
            history = df_existing.loc[matches].reindex(columns=df_new.columns)
            row = df_new.loc[index].astype(object)
            row = row.where(pd.notna(row), "")
            history = history.astype(object).where(pd.notna(history), "")
            row = row.map(lambda value: gsheet_lowlevel.cell_data(value)["userEnteredValue"])
            history = history.apply(lambda col: col.map(
                lambda value: gsheet_lowlevel.cell_data(value)["userEnteredValue"]
            ))
            if not history.empty and history.eq(row, axis="columns").all(axis=1).any():
                skip_new.loc[index] = True
            elif len(history) > 1:
                raise ValueError("Cannot update an incomplete upsert key with ambiguous historical rows; no write performed")
            else:
                mask.loc[matches] = True

        df_cleaned = df_existing[~mask]
        df_combined = pd.concat([df_cleaned, df_new[~skip_new]], ignore_index=True)

        sort_cols = sort_by or list(keys)
        if sort_cols:
            missing_sort = [col for col in sort_cols if col not in df_combined.columns]
            if missing_sort:
                print(f"ソート対象の列が見つかりません: {missing_sort}")
            else:
                def sort_value(value):
                    cell = gsheet_lowlevel.cell_data(value)
                    fmt = cell.get("userEnteredFormat", {}).get("numberFormat", {})
                    if fmt.get("type") in ("DATE", "DATE_TIME", "TIME"):
                        return cell["userEnteredValue"]["numberValue"]
                    return value
                sort_frame = df_combined[sort_cols].apply(
                    lambda col: col.map(sort_value)
                )
                for col in sort_cols:
                    try:
                        sort_frame[col].sort_values()
                    except TypeError:
                        sort_frame[col] = sort_frame[col].map(normalize)
                order = sort_frame.sort_values(by=sort_cols, kind="stable").index
                df_combined = df_combined.loc[order]

        if columns:
            for col in columns:
                if col not in df_combined.columns:
                    df_combined[col] = None
            df_combined = df_combined[columns]

        try:
            public_frame = _public_sheet_frame(df_combined)
            self.app.gs.sheet.overwrite_data(
                df_combined,
                include_index=False,
                max_retries=max_retries,
                backoff_factor=backoff_factor,
            )
            self._apply_write_options(
                public_frame,
                auto_width=auto_width,
                freeze_header=freeze_header,
                width_min=width_min,
                width_max=width_max,
                single_byte_multiplier=single_byte_multiplier,
                multi_byte_multiplier=multi_byte_multiplier,
                max_retries=max_retries,
                backoff_factor=backoff_factor,
            )
            print(f"'{sheet_name}' シートを更新しました（新規 {len(df_new)} 行、削除 {mask.sum()} 行）。")
            return public_frame
        except Exception as exc:
            logger.exception("Failed to overwrite sheet '%s' (upsert)", sheet_name)
            print(f"'{sheet_name}' シートへの書き込みに失敗しました: {exc}")
            raise

    def update_cells(self, sheet_url: str, sheet_name: str, updates: dict) -> Optional[bool]:
        if not updates:
            return None
        if not self.open_sheet(sheet_url):
            return None
        try:
            self.app.gs.sheet.select(sheet_name)
            self.app.state.gs_sheet_name = sheet_name
        except errors.SheetNotFound:
            print(f"{sheet_name} シートが存在しません。")
            return None
        except Exception as exc:
            print(f"'{sheet_name}' シートの読み込みに失敗しました: {exc}")
            return None

        try:
            call = getattr(self.app.gs, "call_with_retry", None)
            for cell, value in updates.items():
                if callable(call):
                    call(
                        "Google Sheets update cell",
                        lambda c=cell, v=value: self.app.gs.sheet._driver.update_acell(c, v),
                        retry_on_requests=True,
                    )
                else:
                    self.app.gs.sheet._driver.update_acell(cell, value)
            print(f"'{sheet_name}' シートのセルを更新しました: {', '.join(updates.keys())}")
            return True
        except Exception as exc:
            print(f"'{sheet_name}' シートのセル更新に失敗しました: {exc}")
            return None

    def update_range(self, sheet_url: str, sheet_name: str, a1_range: str, values) -> Optional[bool]:
        if values is None:
            return None
        if not self.open_sheet(sheet_url):
            return None
        try:
            self.app.gs.sheet.select(sheet_name)
            self.app.state.gs_sheet_name = sheet_name
        except errors.SheetNotFound:
            print(f"{sheet_name} シートが存在しません。")
            return None
        except Exception as exc:
            print(f"'{sheet_name}' シートの読み込みに失敗しました: {exc}")
            return None

        try:
            call = getattr(self.app.gs, "call_with_retry", None)
            if callable(call):
                call(
                    "Google Sheets update range",
                    lambda: self.app.gs.sheet._driver.update(a1_range, values),
                    retry_on_requests=True,
                )
            else:
                self.app.gs.sheet._driver.update(a1_range, values)
            print(f"'{sheet_name}' シートの範囲を更新しました: {a1_range}")
            return True
        except Exception as exc:
            print(f"'{sheet_name}' シートの範囲更新に失敗しました: {exc}")
            return None
