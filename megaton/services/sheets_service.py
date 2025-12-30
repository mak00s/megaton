"""Google Sheets service wrapper."""

import logging
from typing import Optional

import pandas as pd

from .. import errors, gsheet, mount_google_drive

logger = logging.getLogger(__name__)


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
            self.app.gs = gsheet.MegatonGS(self.app.creds, url)
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

    def save_sheet(self, sheet_name: str, df):
        if self.select_sheet(sheet_name):
            if self.app.gs.sheet.overwrite_data(df, include_index=False):
                print(f"データを「{sheet_name}」シートへ反映しました。")

    def append_sheet(self, sheet_name: str, df):
        if self.select_sheet(sheet_name):
            if self.app.gs.sheet.save_data(df, include_index=False):
                print(f"データを「{sheet_name}」シートに追記しました。")

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
            return pd.DataFrame()
        try:
            self.app.gs.sheet.select(sheet_name)
            self.app.state.gs_sheet_name = sheet_name
        except errors.SheetNotFound:
            print(f"{sheet_name} シートが存在しません。")
            return pd.DataFrame()
        except Exception as exc:
            print(f"⚠️ Failed to read sheet '{sheet_name}': {exc}")
            return pd.DataFrame()

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
    ) -> Optional[pd.DataFrame]:
        if not isinstance(df_new, pd.DataFrame):
            raise TypeError("df_new must be a pandas.DataFrame")

        if create_if_missing:
            if not self.open_or_create_sheet(sheet_url, sheet_name):
                return None
        else:
            if not self.open_sheet(sheet_url):
                return None
            try:
                self.app.gs.sheet.select(sheet_name)
                self.app.state.gs_sheet_name = sheet_name
            except errors.SheetNotFound:
                print(f"{sheet_name} シートが存在しません。")
                return None

        try:
            df_existing = pd.DataFrame(self.app.gs.sheet.data)
        except Exception as exc:
            print(f"'{sheet_name}' シートの読み込みに失敗しました: {exc}")
            df_existing = pd.DataFrame()

        df_new = df_new.copy()
        if df_existing.empty:
            try:
                self.app.gs.sheet.overwrite_data(df_new, include_index=False)
                print(f"'{sheet_name}' シートへ {len(df_new)} 行を書き込みました。")
                return df_new
            except Exception as exc:
                print(f"'{sheet_name}' シートへの書き込みに失敗しました: {exc}")
                return None

        df_existing = df_existing.copy()
        for key in keys:
            if key in df_existing.columns:
                df_existing[key] = df_existing[key].astype(str).str.strip()
            if key in df_new.columns:
                df_new[key] = df_new[key].astype(str).str.strip()

        try:
            keys_to_remove = set(tuple(row) for row in df_new[keys].drop_duplicates().values)
            mask = df_existing[keys].apply(tuple, axis=1).isin(keys_to_remove)
        except KeyError as exc:
            print(f"重複判定に必要な列が見つかりません: {exc}")
            return None

        df_cleaned = df_existing[~mask]
        df_combined = pd.concat([df_cleaned, df_new], ignore_index=True)

        sort_cols = sort_by or list(keys)
        if sort_cols:
            missing_sort = [col for col in sort_cols if col not in df_combined.columns]
            if missing_sort:
                print(f"ソート対象の列が見つかりません: {missing_sort}")
            else:
                df_combined.sort_values(by=sort_cols, inplace=True)

        if columns:
            for col in columns:
                if col not in df_combined.columns:
                    df_combined[col] = None
            df_combined = df_combined[columns]

        try:
            self.app.gs.sheet.overwrite_data(df_combined, include_index=False)
            print(f"'{sheet_name}' シートを更新しました（新規 {len(df_new)} 行、削除 {mask.sum()} 行）。")
            return df_combined
        except Exception as exc:
            print(f"'{sheet_name}' シートへの書き込みに失敗しました: {exc}")
            return None

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
            for cell, value in updates.items():
                self.app.gs.sheet._driver.update_acell(cell, value)
            print(f"'{sheet_name}' シートのセルを更新しました: {', '.join(updates.keys())}")
            return True
        except Exception as exc:
            print(f"'{sheet_name}' シートのセル更新に失敗しました: {exc}")
            return None
