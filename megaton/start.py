"""An app for Jupyter Notebook/Google Colaboratory to get data from Google Analytics
"""

import hashlib
import logging
import pandas as pd
import sys
from types import SimpleNamespace
from typing import Optional
from datetime import datetime
from IPython.display import clear_output
from oauthlib.oauth2.rfc6749.errors import InvalidGrantError

from . import bq, constants, dates, errors, files, ga3, ga4, recipes, searchconsole, utils, mount_google_drive
from .auth import google_auth as auth_google, provider as auth_provider
from .services.bq_service import BQService
from .services.gsc_service import GSCService
from .services.sheets_service import SheetsService
from .state import MegatonState
from .ui import widgets

logger = logging.getLogger(__name__)  # .setLevel(logging.ERROR)


class Megaton:
    """メガトンはGAを使うアナリストの味方
    """

    def __init__(self,
                 credential: Optional[str] = None,
                 use_ga3: bool = False,
                 cache_key: Optional[str] = None,
                 headless: bool = False):
        self.json = None
        self.required_scopes = constants.DEFAULT_SCOPES
        self.creds = None
        self.auth_menu = None
        self.use_ga3 = use_ga3
        self.headless = headless
        self.ga = {}  # GA clients
        self.gs = None  # Google Sheets client
        self.sc = None  # Google Search Console client
        self.bq = None  # BigQuery
        self.state = MegatonState()
        self.state.headless = headless
        self.recipes = SimpleNamespace(load_config=lambda sheet_url: recipes.load_config(self, sheet_url))
        self.bq_service = BQService(self)
        self.gsc_service = GSCService(self)
        self.sheets_service = SheetsService(self)
        self.open = self.Open(self)
        self.save = self.Save(self)
        self.append = self.Append(self)
        self.upsert = self.Upsert(self)
        self.load = self.Load(self)
        self.select = self.Select(self)
        self.show = self.Show(self)
        self.report = self.Report(self)
        self._pending_flow = None
        self._pending_cache_identifier = None
        self._pending_json_marker = None
        self._credential = credential
        if self.in_colab:
            mount_google_drive()
        self.auth(credential=self._credential, cache_key=cache_key)

    def _notify_invalid_oauth_config(self, message: str):
        clear_output(wait=True)
        logger.error(message)
        print(message)
        self._reset_pending_oauth()

    def _validate_oauth_client(self, info: dict) -> bool:
        ctype = None
        if 'installed' in info:
            ctype = 'installed'
        elif 'web' in info:
            ctype = 'web'
        if not ctype:
            self._notify_invalid_oauth_config('OAuth クライアント設定を認識できません。')
            return False
        config = info.get(ctype, {})
        redirects = config.get('redirect_uris') or []
        if not isinstance(redirects, list):
            redirects = []
        if self.in_colab:
            if 'urn:ietf:wg:oauth:2.0:oob' not in redirects:
                self._notify_invalid_oauth_config('Colab で使用するには redirect_uris に urn:ietf:wg:oauth:2.0:oob を追加してください。')
                return False
            return True
        if not any(uri.startswith('http://127.0.0.1') or uri.startswith('http://localhost') for uri in redirects):
            self._notify_invalid_oauth_config('ローカル環境では redirect_uris に http://127.0.0.1 などのループバック URI を登録してください。')
            return False
        return True

    def _notify_invalid_service_account(self, email: Optional[str] = None):
        clear_output(wait=True)
        if email:
            message = f"指定の {email} のサービスアカウントは存在しない、または無効です。"
        else:
            message = "指定したサービスアカウントは存在しない、または無効です。"
        logger.error(message)
        print(message)

        self._reset_pending_oauth()

    def _reset_pending_oauth(self):
        self._pending_flow = None
        self._pending_cache_identifier = None
        self._pending_json_marker = None

    def _handle_oauth_credentials(self, info: dict, cache_identifier: str, json_marker: str, menu: Optional["Megaton.AuthMenu"] = None):
        cache = auth_google.load_credentials(cache_identifier, self.required_scopes)
        if cache:
            self.creds = cache
            self.json = json_marker
            if not self.headless:
                self.select.ga()
            else:
                logger.debug('Headless mode enabled; skipping GA menu display.')
            self._reset_pending_oauth()
            return True

        flow, auth_url = auth_google.get_oauth_redirect_from_info(info, self.required_scopes)
        self._pending_flow = flow
        self._pending_cache_identifier = cache_identifier
        self._pending_json_marker = json_marker

        if self.headless:
            logger.debug('Headless mode: OAuth prompt suppressed. Please authorize via provided URL manually.')
            return False

        if menu is None:
            placeholder = {'OAuth': {}, 'Service Account': {}}
            self.auth_menu = self.AuthMenu(self, json_files=placeholder)
            menu = self.auth_menu
            menu.show()
        else:
            self.auth_menu = menu

        menu.flow = flow
        menu.show_oauth_prompt(auth_url)
        return False

    def _handle_headless_oauth(self, info: dict, cache_identifier: str, json_marker: str) -> bool:
        logger.debug('Headless mode: attempting to reuse cached OAuth credentials only.')
        cache = auth_google.load_credentials(cache_identifier, self.required_scopes)
        if cache:
            self.creds = cache
            self.json = json_marker
            self._reset_pending_oauth()
            return True
        self._notify_invalid_oauth_config('headless=True では既存の認証キャッシュが必要です。cache_key を指定するか先にブラウザ環境で認証してください。')
        return False

    def _handle_oauth_local_server(self, info: dict, cache_identifier: str, json_marker: str) -> bool:
        cache = auth_google.load_credentials(cache_identifier, self.required_scopes)
        if cache:
            self.creds = cache
            self.json = json_marker
            if not self.headless:
                self.select.ga()
            else:
                logger.debug('Headless mode: using cached credentials without UI.')
            return True

        try:
            flow = auth_google.InstalledAppFlow.from_client_config(info, scopes=self.required_scopes)
            prompt = 'ブラウザが開かない場合は、表示された URL を手動で開いて認証を完了してください。\nPlease visit this URL to authorize this application: {url}'
            credentials = flow.run_local_server(
                port=0,
                authorization_prompt_message=prompt,
                success_message='認証が完了しました。ブラウザを閉じてください。'
            )
        except Exception as exc:
            logger.error('OAuth ローカルサーバーフローに失敗しました: %s', exc)
            return False

        self.creds = credentials
        self.json = json_marker
        if cache_identifier:
            auth_google.save_credentials(cache_identifier, self.creds)
        if not self.headless:
            self.select.ga()
        else:
            logger.debug('Headless mode: OAuth completed without UI.')
        self._reset_pending_oauth()
        return True

    @property
    def in_colab(self):
        """Check if the code is running in Google Colaboratory"""
        return 'google.colab' in sys.modules

    @property
    def ga_ver(self) -> str:
        """現在選択されたGAのバージョン"""
        if len(self.ga.keys()) == 1:
            # タブが一つなら確定
            return list(self.ga.keys())[0]
        else:
            # タブの状態でGAのバージョンを切り替える
            ver = list(self.select.ga_menu.keys())
            if ver:
                return ver[self.select.ga_tab.selected_index]

    @property
    def enabled(self):
        """有効化されたサービス"""
        services = [
            ('ga3', self.ga.get('3')),
            ('ga4', self.ga.get('4')),
            ('gs', getattr(self, 'gs', None)),
            ('sc', getattr(self, 'sc', None)),
        ]
        return [name for name, active in services if active]

    def auth(self, credential: Optional[str] = None, cache_key: Optional[str] = None):
        """認証
        ・ディレクトリ→選択メニュー
        ・ファイル→そのまま認証
        ・JSON文字列→メモリから認証（SA or OAuth）
        """
        source = auth_provider.resolve_credential_source(
            credential,
            in_colab=self.in_colab,
        )
        if source.kind == "inline" and source.info:
            ctype = source.credential_type or auth_google.get_credential_type_from_info(source.info)
            if ctype == "service_account":
                self.creds = auth_google.load_service_account_credentials_from_info(source.info, self.required_scopes)
                if not self.creds:
                    self._notify_invalid_service_account(source.info.get('client_email'))
                    return
                self.json = "<inline:service_account>"
                if not self.headless:
                    self.select.ga()
                else:
                    logger.debug('Headless mode: service account credentials loaded without UI.')
                    self._build_ga_clients()
                self._reset_pending_oauth()
                return

            if ctype in ("installed", "web"):
                if not self._validate_oauth_client(source.info):
                    return
                if not cache_key:
                    client_id = source.info.get(ctype, {}).get("client_id", "")
                    scope_sig = hashlib.sha256(" ".join(sorted(self.required_scopes)).encode()).hexdigest()[:10]
                    cache_key = f"inline_oauth_{client_id}_{scope_sig}"
                json_marker = f"<inline:oauth:{cache_key}>"
                if self.headless:
                    if self._handle_headless_oauth(source.info, cache_key, json_marker):
                        return
                    return
                if self.in_colab:
                    if self._handle_oauth_credentials(source.info, cache_key, json_marker):
                        return
                    return
                if self._handle_oauth_local_server(source.info, cache_key, json_marker):
                    return
                return

        if source.kind == "directory" and source.value:
            if self.headless and self.in_colab:
                logger.debug('headless mode on Colab: mounting Google Drive for credential selection.')
                mount_google_drive()
            if self.headless and not self.in_colab:
                logger.error('headless=True ではディレクトリ選択メニューを表示できません。個別の JSON ファイルを指定してください。')
                return
            json_files = auth_google.get_json_files_from_dir(source.value)
            self.auth_menu = self.AuthMenu(self, json_files)
            self.auth_menu.show()
            return

        if source.kind == "file" and source.value:
            logger.debug(f"auth with a file {source.value}")
            creds_type = source.credential_type
            if creds_type in ['installed', 'web']:
                if not source.info:
                    logger.error('OAuth クライアント設定の読み込みに失敗しました: %s', source.error or 'unknown error')
                    return
                if not self._validate_oauth_client(source.info):
                    return
                if self.headless:
                    if self._handle_headless_oauth(source.info, source.value, source.value):
                        return
                    return
                if self.in_colab:
                    if self._handle_oauth_credentials(source.info, source.value, source.value):
                        return
                    return
                if self._handle_oauth_local_server(source.info, source.value, source.value):
                    return
                return
            if creds_type == 'service_account':
                self.creds = auth_google.load_service_account_credentials_from_file(source.value, constants.DEFAULT_SCOPES)
                if not self.creds:
                    email = auth_provider.extract_email_from_file(source.value)
                    self._notify_invalid_service_account(email)
                    return
                self.json = source.value
                if not self.headless:
                    self.select.ga()
                else:
                    logger.debug('Headless mode: service account credentials loaded without UI.')
                    self._build_ga_clients()
                self._reset_pending_oauth()
                return

        warning_value = source.value if source.value is not None else credential
        if warning_value:
            logger.warning('指定した認証情報を解釈できません: %s', warning_value)

    def _build_ga_clients(self):
        """GA APIの準備"""
        self.ga = {}
        if not self.creds:
            logger.warning('認証が完了していないため、GA クライアントを初期化できません。')
            return
        # GA4
        try:
            client = ga4.MegatonGA4(self.creds)
            client._state = self.state
            client._ga_version = '4'
            if client.accounts:
                self.ga['4'] = client
            else:
                logger.warning("GA4はアカウントが無いのでスキップします。")
        except errors.ApiDisabled as e:
            logger.warning(f"GCPプロジェクトで{e.api}を有効化してください。")
        # UA
        if self.use_ga3:
            try:
                client = ga3.MegatonUA(self.creds)
                client._state = self.state
                client._ga_version = '3'
                if client.accounts:
                    self.ga['3'] = client
                else:
                    logger.warning("UAのアカウントがありません")
            except errors.ApiDisabled as e:
                logger.warning(f"GCPプロジェクトで{e.api}を有効化してください。")
            except errors.NoDataReturned:
                logger.warning("UAはアカウントが無いのでスキップします。")

    def save_df(self, df: pd.DataFrame, filename: str, mode: str = 'w', include_dates: bool = True, quiet: bool = False):
        """データフレームをCSV保存：ファイル名に期間を付与。拡張子がなければ付与
        """
        if include_dates:
            new_filename = files.append_suffix_to_filename(filename, f"_{self.report.dates}")
        else:
            new_filename = files.append_suffix_to_filename(filename, "")
        files.save_df(df, new_filename, mode=mode)
        if quiet:
            return new_filename
        if mode == 'w':
            print(f"CSVファイル{new_filename}を保存しました。")
        elif mode == 'a':
            print(f"CSVファイル{new_filename}に追記しました。")

    def download(self, df: pd.DataFrame, filename: str = None):
        """データフレームをCSV保存し、Google Colaboratoryからダウンロード
        """
        if not filename:
            filename = 'report'
        new_filename = self.save_df(df, filename, quiet=True)
        files.download_file(new_filename)

    def launch_sc(self, site_url: Optional[str] = None):
        if not self.creds:
            logger.warning('Search Console を利用するには先に認証を完了してください。')
            return None
        try:
            self.sc = searchconsole.MegatonSC(self.creds, site_url=site_url)
        except errors.BadCredentialFormat:
            logger.error('Search Console の資格情報形式が正しくありません。')
            return None
        except errors.BadCredentialScope:
            logger.error('Search Console に必要なスコープが不足しています。')
            return None
        except Exception as exc:
            logger.error('Search Console client の初期化に失敗しました: %s', exc)
            return None
        logger.debug('Search Console client initialized%s', f" for {site_url}" if site_url else "")
        return self.sc

    def launch_bigquery(self, gcp_project: str):
        return self.bq_service.launch_bigquery(gcp_project)

    def launch_gs(self, url: str):
        """APIでGoogle Sheetsにアクセスする準備"""
        return self.sheets_service.launch_gs(url)

    class AuthMenu:
        """認証用のメニュー生成と選択時の処理"""
        def __init__(self, parent, json_files: dict):
            self.parent = parent
            # build menu of json files
            self.creds_selector = widgets.menu_for_credentials(json_files)
            self.log_text = widgets.Output()
            self.message_text = widgets.html_text(value='', placeholder='', description='')
            self.code_selector = widgets.input_text(
                value='',
                placeholder='コードをここへ',
                description='Auth Code:',
                width=('210px', '700px')
            )
            self.flow = None

        # when json is selected
        def _json_selected(self, change):
            with self.log_text:
                clear_output()
                if change.new:
                    self.parent.select.reset()
                    source = auth_provider.resolve_credential_source(change.new)
                    if source.kind != "file":
                        return
                    creds_type = source.credential_type
                    # OAuth
                    if creds_type in ['installed', 'web']:
                        if not source.info:
                            logger.error('OAuth クライアント設定の読み込みに失敗しました: %s', source.error or 'unknown error')
                            return
                        if not self.parent._validate_oauth_client(source.info):
                            return
                        if self.parent.in_colab:
                            if self.parent._handle_oauth_credentials(source.info, change.new, change.new, menu=self):
                                return
                            self.parent.json = change.new
                            return
                        if self.parent._handle_oauth_local_server(source.info, change.new, change.new):
                            self.parent.json = change.new
                            return
                        return

                    # Service Account
                    if creds_type in ['service_account']:
                        self.parent.creds = auth_google.load_service_account_credentials_from_file(change.new,
                                                                                            self.parent.required_scopes)
                        if not self.parent.creds:
                            email = auth_provider.extract_email_from_file(change.new)
                            self.parent._notify_invalid_service_account(email)
                            return
                        self.parent.json = change.new
                        self.parent.select.ga()
                        self.parent._reset_pending_oauth()

        def show_oauth_prompt(self, auth_url: str):
            with self.log_text:
                clear_output()
            self.message_text.value = f'<a href="{auth_url}" target="_blank">ここをクリックし、認証後に表示されるcodeを以下に貼り付けてエンターを押してください</a> '
            self.code_selector.layout.display = "block"
            self.code_selector.value = ''


        # when code is entered
        def _code_entered(self, change):
            with self.log_text:
                if change.new:
                    try:
                        flow = self.parent._pending_flow or getattr(self, 'flow', None)
                        if not flow:
                            logger.error('OAuth フローが初期化されていません。')
                            return
                        # get token from auth code
                        self.parent.creds = auth_google.get_token(flow, change.new)
                        cache_identifier = self.parent._pending_cache_identifier or self.parent.json
                        if cache_identifier:
                            auth_google.save_credentials(cache_identifier, self.parent.creds)
                        self.code_selector.value = ''
                        self.code_selector.layout.display = "none"
                        self.parent.json = self.parent._pending_json_marker or cache_identifier
                        self.reset()
                        self.parent.select.ga()
                        self.parent._reset_pending_oauth()
                    except InvalidGrantError:
                        logging.error("正しいフォーマットのauthorization codeを貼り付けてください")

        def show(self):
            self.reset()
            self.creds_selector.observe(self._json_selected, names='value')
            self.code_selector.observe(self._code_entered, names='value')
            display(self.creds_selector, self.message_text, self.code_selector, self.log_text)

        def reset(self):
            self.message_text.value = ''
            self.code_selector.layout.display = "none"
            self.code_selector.value = ''
            self.log_text.value = ''

    class GaMenu:
        """GAのアカウント・プロパティ（・ビュー）を選択するUI
        """

        def __init__(self, parent, ver: str, accounts: list):
            self.parent = parent
            self.ver = ver
            self.accounts = accounts
            logging.debug(f"building menu for GA{ver}")
            # build blank menus
            self.account_menu = self._get_account_menu()
            self.property_menu = self._get_property_menu()
            if self.ver == '3':
                self.view_menu = self._get_view_menu()
                self.view_menu.observe(self._view_menu_selected, names='value')
            self.property_menu.observe(self._property_menu_selected, names='value')
            self.account_menu.observe(self._account_menu_selected, names='value')
            # update menu options
            self._update_account_menu()

        def _get_account_menu(self):
            if self.ver == '3':
                return widgets.create_blank_menu('GAアカウント', width=('110px', 'max-content'))
            elif self.ver == '4':
                return widgets.create_blank_menu('GA4アカウント', width=('110px', 'max-content'))

        def _get_property_menu(self):
            if self.ver == '3':
                return widgets.create_blank_menu('GAプロパティ', width=('110px', 'max-content'))
            elif self.ver == '4':
                return widgets.create_blank_menu('GA4プロパティ', width=('110px', 'max-content'))

        def _get_view_menu(self):
            if self.ver == '3':
                return widgets.create_blank_menu('GAビュー', width=('110px', 'max-content'))
            elif self.ver == '4':
                return None  # widget.create_blank_menu('')

        def _update_account_menu(self):
            # 取得済みのアカウント一覧を得る
            accounts = [(d['name'], d['id']) for d in self.accounts]
            options = accounts
            # メニューの選択肢を更新
            self.account_menu.options = options
            if accounts:
                self.account_menu.value = accounts[0][1]

        def _account_menu_selected(self, change):
            account_id = change.new
            self.parent.ga[self.ver].account.select(account_id)
            self.parent.state.ga_version = self.ver
            self.parent.state.ga_account_id = account_id or None
            if not account_id:
                self.parent.state.ga_property_id = None
                self.parent.state.ga_view_id = None
            # 選択肢が変更されたら
            if account_id:
                # 選択されたGAアカウントに紐付くGAプロパティを得る
                properties = [d for d in self.parent.ga[self.ver].accounts if d['id'] == account_id][0]['properties']
                option_values = [(d['name'], d['id']) for d in properties]
                # メニューの選択肢を更新
                self.property_menu.options = option_values
                if option_values:
                    self.property_menu.value = option_values[0][1]
            else:
                self.reset()

        def _property_menu_selected(self, change):
            property_id = change.new
            self.parent.ga[self.ver].property.select(property_id)
            self.parent.state.ga_version = self.ver
            self.parent.state.ga_property_id = property_id or None
            if not property_id:
                self.parent.state.ga_view_id = None
            # 選択肢が変更されたら
            if self.ver == '3':
                if property_id:
                    # 選択されたGAプロパティに紐付くGAビューを得る
                    views = [d for d in self.parent.ga[self.ver].property.views if d['property_id'] == property_id]
                    option_values = [(d['name'], d['id']) for d in views]
                    # メニューの選択肢を更新
                    self.view_menu.options = option_values
                    if option_values:
                        self.view_menu.value = option_values[0][1]
                else:
                    self.view_menu.options = []

        def _view_menu_selected(self, change):
            view_id = change.new
            self.parent.ga[self.ver].view.select(view_id)
            self.parent.state.ga_version = self.ver
            self.parent.state.ga_view_id = view_id or None

        def list(self):
            if self.ver == '3':
                return [self.account_menu, self.property_menu, self.view_menu]
            else:
                return [self.account_menu, self.property_menu]

        def reset(self):
            if self.ver in self.parent.ga.keys():
                self.parent.ga[self.ver].account.select('')
            self.accounts = []
            self.account_menu.options = []
            self.property_menu.options = []
            if self.ver == '3':
                self.view_menu.options = []

    class Append:
        """DaraFrameをCSVやGoogle Sheetsに追記
        """
        def __init__(self, parent):
            self.parent = parent
            self.to = self.To(self)

        class To:
            def __init__(self, parent):
                self.parent = parent

            def csv(self, df: pd.DataFrame = None, filename: str = 'report', include_dates: bool = True, quiet: bool = False):
                """DataFrameをCSVに追記：ファイル名に期間を付与。拡張子がなければ付与

                Args:
                    df: DataFrame
                    filename: path to a file
                    include_dates: when True, start_date and end_date is added to the filename
                    quiet: when True, message won't be displayed
                """
                if df is None:
                    df = self.parent.parent.report.data
                if not isinstance(df, pd.DataFrame):
                    raise TypeError(
                        "df must be a pandas DataFrame (or omit df to use mg.report.data)."
                    )

                self.parent.parent.save_df(df, filename, mode='a', include_dates=include_dates, quiet=quiet)

            def sheet(self, sheet_name: str, df: pd.DataFrame = None):
                """DataFrameをGoogle Sheetsへ反映する
                """
                if not isinstance(df, pd.DataFrame):
                    df = self.parent.parent.report.data

                self.parent.parent.sheets_service.append_sheet(sheet_name, df)

    class Save:
        """DaraFrameをCSVやGoogle Sheetsとして保存
        """
        def __init__(self, parent):
            self.parent = parent
            self.to = self.To(self)

        class To:
            def __init__(self, parent):
                self.parent = parent

            def csv(self, df: pd.DataFrame = None, filename: str = 'report', mode: str = 'w', include_dates: bool = True, quiet: bool = False):
                """DataFrameをCSV保存：ファイル名に期間を付与。拡張子がなければ付与

                Args:
                    df: DataFrame
                    filename: path to a file
                    mode: w for overwrite, a for append
                    include_dates: if True, "_" + start date + "_" + end date is added to the filename
                    quiet: when True, message won't be displayed
                """
                if not isinstance(df, pd.DataFrame):
                    df = self.parent.parent.report.data

                self.parent.parent.save_df(df, filename, mode=mode, include_dates=include_dates, quiet=quiet)

            def sheet(self, sheet_name: str, df: pd.DataFrame = None):
                """DataFrameをGoogle Sheetsへ反映する

                Args:
                    sheet_name: path to a file
                    df: DataFrame. If omitted, mg.report.data will be saved.
                """
                if not isinstance(df, pd.DataFrame):
                    df = self.parent.parent.report.data

                self.parent.parent.sheets_service.save_sheet(sheet_name, df)

    class Upsert:
        """DataFrameをGoogle Sheetsへupsert（dedup + overwrite）"""
        def __init__(self, parent):
            self.parent = parent
            self.to = self.To(self)

        class To:
            def __init__(self, parent):
                self.parent = parent

            def sheet(self, sheet_name: str, df: pd.DataFrame = None, *, keys, columns=None, sort_by=None):
                """DataFrameをGoogle Sheetsへupsertする

                Args:
                    sheet_name: sheet name
                    df: DataFrame. If omitted, mg.report.data will be used.
                    keys: columns used for dedup
                    columns: optional output column order
                    sort_by: optional sort columns
                """
                if df is None:
                    df = self.parent.parent.report.data
                if not isinstance(df, pd.DataFrame):
                    raise TypeError(
                        "df must be a pandas DataFrame (or omit df to use mg.report.data)."
                    )

                sheet_url = self.parent.parent.state.gs_url
                if not sheet_url:
                    raise ValueError("No active spreadsheet. Call mg.open.sheet(url) first.")

                return self.parent.parent.sheets_service.upsert_df(
                    sheet_url,
                    sheet_name,
                    df,
                    keys=keys,
                    columns=columns,
                    sort_by=sort_by,
                    create_if_missing=True,
                )

    class Select:
        """選択するUIの構築と処理
        """

        def __init__(self, parent):
            self.parent = parent
            self.ga_menu = {}
            self.ga_tab = None

        def reset(self):
            if '3' in self.ga_menu.keys():
                self.ga_menu['3'].reset()
            if '4' in self.ga_menu.keys():
                self.ga_menu['4'].reset()

        def ga(self):
            """GAアカウントを選択するパネルを表示
            """
            if not self.parent.creds:
                self.reset()
                logger.warning('認証が完了していません。先に認証を行ってください。')
                return
            # 選択された認証情報でGAクライアントを生成
            self.parent._build_ga_clients()
            # メニューをリセット
            self.reset()

            # GA選択メニューのタブを構築
            tab_children, titles = [], []
            for ver in ['3', '4']:
                if ver in self.parent.ga.keys():
                    try:
                        self.ga_menu[ver] = self.parent.GaMenu(self.parent,
                                                               ver,
                                                               self.parent.ga[ver].accounts)
                        tab_children.append(widgets.tab(self.ga_menu[ver].list()))
                        titles.append(f"GA{ver}")
                    except errors.NoDataReturned:
                        logger.warning("選択された認証情報でアクセスできるアカウントがありません")
                        del self.ga[ver]
                    except errors.ApiDisabled as e:
                        logger.warning(f"GCPプロジェクトで{e.api}を有効化してください")
                        del self.parent.ga[ver]
            self.ga_tab = widgets.tab_set(tab_children, titles)

            # GA選択メニュのタブを表示
            if tab_children:
                display(self.ga_tab)

        def sheet(self, sheet_name: str):
            """開いたGoogle Sheetsのシートを選択"""
            return self.parent.sheets_service.select_sheet(sheet_name)

    class Open:
        def __init__(self, parent):
            self.parent = parent

        def sheet(self, url):
            """Google Sheets APIの準備"""
            return self.parent.sheets_service.open_sheet(url)

    class Load:
        """DaraFrameをCSVやGoogle Sheetsから読み込む
        """
        def __init__(self, parent):
            self.parent = parent

        def csv(self, filename: str):
            """指定ディレクトリ中のCSVファイルをロードして結合しDataFrame化"""
            df = files.load_df(filename)
            return df

        def cell(self, row, col, what: str = None):
            self.parent.gs.sheet.cell.select(row, col)
            value = self.parent.gs.sheet.cell.data
            if what:
                print(f"{what}は{value}")
            return value

    class Show:
        def __init__(self, parent):
            self.parent = parent

        @property
        def ga(self):
            return self.Ga(self)

        class Ga:
            def __init__(self, parent):
                self.parent = parent

            @property
            def dimensions(self):
                print("dimensions are:")
                df = pd.DataFrame(self.parent.parent.ga['4'].property.dimensions)
                return self.parent.table(df)

            @property
            def metrics(self):
                print("metrics are:")
                df = pd.DataFrame(self.parent.parent.ga['4'].property.metrics)
                return self.parent.table(df)

            @property
            def properties(self):
                print("properties are:")
                df = self.parent.parent.ga['4'].property.show('info')
                return self.parent.table(df)

        def table(self, df: pd.core.frame.DataFrame, rows: int = 10, include_index: bool = False):
            if isinstance(df, pd.DataFrame):
                if self.parent.in_colab:
                    from google.colab import data_table
                    return data_table.DataTable(
                        df,
                        include_index=include_index,
                        num_rows_per_page=rows
                    )
                try:
                    itables.show(df)
                except NameError:
                    display(df)
            else:
                print("該当するデータがありません。")

    class Report:
        """GA/GA4からデータを抽出
        """
        def __init__(self, parent):
            self.parent = parent
            self.data = None
            self.to = self.To(self)
            self.dates = self.Dates(self)

        @property
        def start_date(self):
            if self.parent.ga_ver:
                return self.parent.ga[self.parent.ga_ver].report.start_date

        @start_date.setter
        def start_date(self, date):
            """Sets start date for later reporting.

            Args:
                date: date in 'YYYY-MM-DD' / 'NdaysAgo' format or 'yesterday' or 'today'
            """
            if self.parent.ga_ver:
                self.parent.ga[self.parent.ga_ver].report.start_date = date

        @property
        def end_date(self):
            if self.parent.ga_ver:
                return self.parent.ga[self.parent.ga_ver].report.end_date

        @end_date.setter
        def end_date(self, date):
            """Sets end date for later reporting.

            Args:
                date: date in 'YYYY-MM-DD' / 'NdaysAgo' format or 'yesterday' or 'today'
            """
            if self.parent.ga_ver:
                self.parent.ga[self.parent.ga_ver].report.end_date = date

        def set_dates(self, date1, date2):
            """開始日と終了日を同時に指定

            Args:
                date1: start date
                date2: end date
            """
            self.start_date = date1
            self.end_date = date2

        def set_month_window(
            self,
            months_ago: int = 1,
            window_months: int = 13,
            *,
            tz: str = "Asia/Tokyo",
            now: datetime | None = None,
        ) -> tuple[str, str, str]:
            """Set dates from a month window and return (date_from, date_to, ym)."""
            date_from, date_to, ym = dates.get_month_window(
                months_ago,
                window_months,
                tz=tz,
                now=now,
            )
            self.set_dates(date_from, date_to)
            self.last_month_window = {
                "date_from": date_from,
                "date_to": date_to,
                "ym": ym,
                "months_ago": months_ago,
                "window_months": window_months,
                "tz": tz,
            }
            return date_from, date_to, ym

        def run(self, d: list, m: list, filter_d=None, filter_m=None, sort=None, **kwargs):
            """レポートを実行

            Args:
                d: list of dimensions. Item can be an api_name or a display_name
                    or a tuple of api_name and a new column name.
                m: list of metrics. Item can be an api_name or a display_name
                    or a tuple of api_name and a new column name.
                filter_d: dimension filter
                filter_m: metric filter
                sort: dimension or metric to order by
                segments: segment (only for GA3)
            """
            rename_columns = {}
            dimensions = []
            for i in d:
                if isinstance(i, tuple):
                    dimensions.append(i[0])
                    rename_columns[i[0]] = i[1]
                else:
                    dimensions.append(i)

            metrics = []
            for i in m:
                if isinstance(i, tuple):
                    metrics.append(i[0])
                    rename_columns[i[0]] = i[1]
                else:
                    metrics.append(i)

            ver = self.parent.ga_ver
            try:
                if ver:
                    self.data = self.parent.ga[ver].report.run(
                        dimensions,
                        metrics,
                        dimension_filter=filter_d,
                        metric_filter=filter_m,
                        order_bys=sort,
                        segments=kwargs.get('segments'),
                    )
                    if isinstance(self.data, pd.DataFrame):
                        self.data = utils.prep_df(self.data, rename_columns=rename_columns)
                        return self.show()
                else:
                    logger.warning("GAのアカウントを選択してください。")
            except (errors.BadRequest, ValueError) as e:
                print("抽出条件に問題があります。", e.message)
            except errors.ApiDisabled as e:
                logger.warning(f"GCPプロジェクトで{e.api}を有効化してください")

        def show(self):
            """Displays dataframe"""
            return self.parent.show.table(self.data)

        def download(self, filename: str):
            self.parent.download(self.data, filename)

        def prep(self, conf: dict, df: pd.DataFrame = None):
            """dataframeを前処理

            Args:
                conf: dict
                df: dataframe to be processed. If omitted, self.data is processed.

            Returns:
                processed dataframe
            """
            if not isinstance(df, pd.DataFrame):
                df = self.data

            rename_columns = {}
            delete_columns = []
            type_columns = {}

            # loop dimensions and metrics
            for col, d in conf.items():
                for act, v in d.items():
                    if act == "cut":
                        # make it a list if it is a single item
                        if not isinstance(v, list):
                            v = [v]
                        for before in v:
                            utils.replace_columns(df, [(col, before, '')])
                    if act == "delete":
                        if v:
                            delete_columns.append(col)
                    elif act == "name":
                        rename_columns[col] = v
                    elif act == "replace":
                        if isinstance(v, tuple):
                            before, after = v
                            utils.replace_columns(df, [(col, before, after)])
                    elif act == "type":
                        if v:
                            type_columns[col] = v

            df = utils.prep_df(df, delete_columns, type_columns, rename_columns)
            self.data = df
            # return df
            return self.show()

        class Dates:
            def __init__(self, parent):
                self.parent = parent
                self.to = self.To(self)

            @property
            def value(self):
                """セットされているレポート対象期間を文字列に変換"""
                if self.parent.parent.ga_ver:
                    start = self.parent.start_date
                    end = self.parent.end_date
                    if start and end:
                        return f"{start.replace('-', '')}-{end.replace('-', '')}"
                return None

            def __str__(self):
                return self.value or ""

            def __repr__(self):
                return self.value or ""

            def __format__(self, spec):
                return format(str(self), spec)

            def __bool__(self):
                return bool(self.value)

            def __eq__(self, other):
                return self.value == other

            def __getattr__(self, name):
                value = self.value
                if value is None:
                    raise AttributeError(name)
                return getattr(value, name)

            class To:
                def __init__(self, parent):
                    self.parent = parent

                def sheet(self, sheet: str, start_cell: str, end_cell: str):
                    report = self.parent.parent
                    app = report.parent
                    if not report.start_date or not report.end_date:
                        raise ValueError("report.start_date and report.end_date must be set before writing.")

                    sheet_url = app.state.gs_url
                    if not sheet_url:
                        raise ValueError("No active spreadsheet. Call mg.open.sheet(url) first.")

                    updates = {
                        start_cell: report.start_date,
                        end_cell: report.end_date,
                    }
                    return app.sheets_service.update_cells(sheet_url, sheet, updates)

        class To:
            def __init__(self, parent):
                self.parent = parent

            def csv(self, filename: str = 'report', quiet: bool = False):
                """レポート結果をCSV保存：ファイル名に期間を付与。拡張子がなければ付与

                Args:
                    filename: path to a file
                    quiet: when True, message won't be displayed
                """
                self.parent.parent.save_df(self.parent.data, filename, quiet=quiet)

            def sheet(self, sheet_name: str):
                """レポートをGoogle Sheetsへ反映する
                """
                if self.parent.parent.select.sheet(sheet_name):
                    if self.parent.parent.gs.sheet.overwrite_data(self.parent.data, include_index=False):
                        print(f"レポートのデータを上書き保存しました。")
