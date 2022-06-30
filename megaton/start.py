"""run megaton"""

from IPython.display import clear_output
import logging
import os
import sys
from time import sleep

from oauthlib.oauth2.rfc6749.errors import InvalidGrantError

try:
    # check if packages for GA4 are installed
    from google.analytics.data import BetaAnalyticsDataClient
    from google.analytics.admin import AnalyticsAdminServiceClient
except ModuleNotFoundError:
    clear_output()
    print("Installing packages for GA4...")
    from .install import ga4

    clear_output()
    # print("Runtime is now restarting...")
    # print("You can ignore the error message [Your session crashed for an unknown reason.]")
    print("もう一度このセルを実行してください。")
    sleep(0.5)
    os._exit(0)  # restart

from . import auth, constants, errors, ga3, ga4, widgets

_in_colab = "google.colab" in sys.modules
if _in_colab:
    from google.colab import data_table
    # mount google drive
    from . import gdrive
    json_path = gdrive.link_nbs()

logger = logging.getLogger(__name__)  #.setLevel(logging.ERROR)

logger.debug("Notebookの準備ができました。")


class Megaton:
    """ メガトンはGAを使うアナリストの味方
    """
    def __init__(self, path: str = None, use_ga3: bool = True):
        if not path and _in_colab:
            path = '/nbs'
        self.json = None
        self.creds = None
        self.auth_menu = None
        self.use_ga3 = use_ga3
        self.ga = {}  # GA clients
        self.ga_ver = None
        self.select = self.Select(self)
        self.show = self.Show(self)

        self.auth(path)

    def auth(self, path: str):
        """ JSONファイルへのパスが指定されたら認証情報を生成、ディレクトリの場合は選択メニューを表示
        """
        if os.path.isdir(path):
            # if directory, show menu
            json_files = auth.get_credentials_files_from(path)
            self.auth_menu = self.AuthMenu(self, json_files)
            self.auth_menu.show()

        elif os.path.isfile(path):
            # if file, auth with it
            logger.debug(f"auth with a file {path}")
            self.creds = auth.load_service_account_credentials_from_file(path, constants.DEFAULT_SCOPES)
            self.json = path
            self.select.ga()

    def build_ga_clients(self):
        self.ga = {}
        # GA4
        try:
            client = ga4.MegatonGA4(self.creds)
            if client.accounts:
                self.ga['4'] = client
            else:
                logger.warning("GA4はアカウントが無いのでスキップします。")
        except errors.ApiDisabled as e:
            logger.warning(f"GCPプロジェクトで{e.api}を有効化してください。")
        # UA
        if self.use_ga3:
            try:
                client = ga3.MegatonUA(
                    self.creds,
                    credential_cache_file=auth.get_cache_filename_from_json(self.json)
                )
                if client.accounts:
                    self.ga['3'] = client
                else:
                    logger.warning("UAのアカウントがありません")
            except errors.ApiDisabled as e:
                logger.warning(f"GCPプロジェクトで{e.api}を有効化してください。")
            except errors.NoDataReturned:
                logger.warning("UAはアカウントが無いのでスキップします。")

    def reset_menu(self):
        self.auth_menu.reset()
        self.select.reset()

    class AuthMenu:
        def __init__(self, parent, json_files: dict):
            self.parent = parent
            # build menu of json files
            self.creds_selector = widgets.menu_for_credentials(json_files)
            self.log_text = widgets.Output()
            self.message_text = widgets.html_text(value='', placeholder='', description='')
            self.code_selector = widgets.input_text(
                value='',
                placeholder='コードをここへ',
                description='CODE:',
            )
            self.flow = None

        # when json is selected
        def _json_selected(self, change):
            with self.log_text:
                self.parent.reset_menu()
                if change.new:
                    creds_type = auth.get_client_secrets_type_from_file(change.new)
                    if creds_type in ['installed', 'web']:
                        self.flow, auth_url = auth.get_oauth_redirect(change.new, constants.DEFAULT_SCOPES)
                        self.message_text.value = f'<a href="{auth_url}" target="_blank">ここをクリックし、認証後に表示されるcodeを以下に貼り付けてエンターを押してください</a>'
                        self.code_selector.layout.display = "block"

                    if creds_type in ['service_account']:
                        self.parent.creds = auth.load_service_account_credentials_from_file(change.new,
                                                                                            constants.DEFAULT_SCOPES)
                        self.parent.json = change.new
                        self.parent.select.ga()

        # when code is entered
        def _code_entered(self, change):
            with self.log_text:
                if change.new:
                    try:
                        self.parent.creds = auth.get_token(self.flow, change.new)
                        self.code_selector.value = ''
                        self.code_selector.layout.display = "none"
                        self.parent.json = change.new
                        self.parent.select.ga()
                        self.reset()
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
        """ GAのアカウント・プロパティ（・ビュー）を選択するUI
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
                return widgets.create_blank_menu('GAアカウント')
            elif self.ver == '4':
                return widgets.create_blank_menu('GA4アカウント')

        def _get_property_menu(self):
            if self.ver == '3':
                return widgets.create_blank_menu('GAプロパティ')
            elif self.ver == '4':
                return widgets.create_blank_menu('GA4プロパティ')

        def _get_view_menu(self):
            if self.ver == '3':
                return widgets.create_blank_menu('GAビュー')
            elif self.ver == '4':
                return None  # widget.create_blank_menu('')

        def _update_account_menu(self):
            # 取得済みのアカウント一覧を得る
            accounts = [(d['name'], d['id']) for d in self.accounts]
            options = accounts
            # メニューの選択肢を更新
            self.account_menu.options = options

        def _account_menu_selected(self, change):
            account_id = change.new
            self.parent.ga[self.ver].account.select(account_id)
            # 選択肢が変更されたら
            if account_id:
                # 選択されたGAアカウントに紐付くGAプロパティを得る
                properties = [d for d in self.parent.ga[self.ver].accounts if d['id'] == account_id][0]['properties']
                # メニューの選択肢を更新
                self.property_menu.options = [(d['name'], d['id']) for d in properties]
            else:
                self.reset()

        def _property_menu_selected(self, change):
            property_id = change.new
            self.parent.ga[self.ver].property.select(property_id)
            # 選択肢が変更されたら
            if self.ver == '3':
                if property_id:
                    # 選択されたGAプロパティに紐付くGAビューを得る
                    views = [d for d in self.parent.ga[self.ver].property.views if d['property_id'] == property_id]
                    # メニューの選択肢を更新
                    self.view_menu.options = [(d['name'], d['id']) for d in views]
                else:
                    self.view_menu.options = []

        def _view_menu_selected(self, change):
            view_id = change.new
            self.parent.ga[self.ver].view.select(view_id)

        def show(self):
            clear_output()
            if self.ver == '3':
                display(self.account_menu, self.property_menu, self.view_menu)
            else:
                display(self.account_menu, self.property_menu)

        def reset(self):
            if self.ver in self.parent.ga.keys():
                self.parent.ga[self.ver].account.select('')
            self.accounts = []
            self.account_menu.options = []
            self.property_menu.options = []
            if self.ver == '3':
                self.view_menu.options = []

    class Select:
        """ 選択するUIの構築と処理
        """
        def __init__(self, parent):
            self.parent = parent
            self.menu_ga = {}

        def reset(self):
            if '3' in self.menu_ga.keys():
                self.menu_ga['3'].reset()
            if '4' in self.menu_ga.keys():
                self.menu_ga['4'].reset()

        def ga(self):
            """ GAアカウントを選択するパネルを表示
            """
            # 選択された認証情報でGAクライアントを生成
            self.parent.build_ga_clients()
            # メニューをリセット
            self.reset()

            # GA選択メニューを表示
            for ver in ['3', '4']:
                if ver in self.parent.ga.keys():
                    try:
                        self.menu_ga[ver] = self.parent.GaMenu(self.parent, ver, self.parent.ga[ver].accounts)
                        self.menu_ga[ver].show()
                    except errors.NoDataReturned:
                        logger.warning("選択された認証情報でアクセスできるアカウントがありません")
                        del self.ga[ver]
                    except errors.ApiDisabled as e:
                        logger.warning(f"GCPプロジェクトで{e.api}を有効化してください")
                        del self.parent.ga[ver]

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
                df = self.parent.parent.ga['4'].property.show('dimensions')
                self.parent.table(df)

            @property
            def metrics(self):
                print("metrics are:")
                df = self.parent.parent.ga['4'].property.show('metrics')
                self.parent.table(df)

            @property
            def properties(self):
                print("properties are:")
                df = self.parent.parent.ga['4'].property.show('info')
                self.parent.table(df)

        def table(self, df, rows: int = 10, include_index: bool = False):
            if _in_colab:
                return data_table.DataTable(
                    df,
                    include_index=include_index,
                    num_rows_per_page=rows
                )
            try:
                itables.show(df)
            except NameError:
                display(df)
