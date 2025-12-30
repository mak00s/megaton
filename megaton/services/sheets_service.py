"""Google Sheets service wrapper."""

import logging
from typing import Optional

from .. import errors, gsheet, mount_google_drive

logger = logging.getLogger(__name__)


class SheetsService:
    def __init__(self, app):
        self.app = app

    def launch_gs(self, url: str):
        """APIでGoogle Sheetsにアクセスする準備"""
        if not self.app.creds:
            logger.warning('認証が完了していないため、Google Sheets API を初期化できません。')
            return None
        if self.app.in_colab:
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

    def open_sheet(self, url: str):
        """Google Sheets APIの準備"""
        if not self.app.creds:
            logger.warning('認証が完了していないため、Google Sheets を開けません。')
            return None
        self.app.gs = None
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
