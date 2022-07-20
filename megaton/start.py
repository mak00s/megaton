"""An app for Jupyter Notebook/Google Colaboratory to get data from Google Analytics
"""

import logging
import os
import pandas as pd
import sys

from IPython.display import clear_output
from oauthlib.oauth2.rfc6749.errors import InvalidGrantError

from . import auth, constants, errors, files, ga3, ga4, utils, widgets

logger = logging.getLogger(__name__)  # .setLevel(logging.ERROR)


class Megaton:
    """メガトンはGAを使うアナリストの味方
    """

    def __init__(self, path: str = None, use_ga3: bool = True):
        if not path and self.in_colab:
            path = '/nbs'
        self.json = None
        self.required_scopes = constants.DEFAULT_SCOPES
        self.creds = None
        self.auth_menu = None
        self.use_ga3 = use_ga3
        self.ga = {}  # GA clients
        self.select = self.Select(self)
        self.show = self.Show(self)
        self.report = self.Report(self)

        self.auth(path)

    @property
    def in_colab(self):
        """Check if the code is running in Google Colaboratory"""
        return 'google.colab' in sys.modules

    def auth(self, path: str):
        """認証
        ・JSONファイルへのパスが指定された場合は認証情報を生成
        ・ディレクトリへのパスが指定された場合は選択メニューを表示
        """
        if os.path.isdir(path):
            # if directory, show menu
            json_files = auth.get_json_files_from_dir(path)
            self.auth_menu = self.AuthMenu(self, json_files)
            self.auth_menu.show()

        elif os.path.isfile(path):
            # if file, auth with it
            logger.debug(f"auth with a file {path}")
            self.creds = auth.load_service_account_credentials_from_file(path, constants.DEFAULT_SCOPES)
            self.json = path
            self.select.ga()

    def build_ga_clients(self):
        """APIの準備"""
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
                client = ga3.MegatonUA(self.creds)
                if client.accounts:
                    self.ga['3'] = client
                else:
                    logger.warning("UAのアカウントがありません")
            except errors.ApiDisabled as e:
                logger.warning(f"GCPプロジェクトで{e.api}を有効化してください。")
            except errors.NoDataReturned:
                logger.warning("UAはアカウントが無いのでスキップします。")

    def reset_menu(self):
        """メニューの表示と内容をリセット"""
        self.auth_menu.reset()
        self.select.reset()

    def save(self, df: pd.core.frame.DataFrame, filename: str = None, quiet: bool = False):
        """データフレームをCSV保存：ファイル名に期間を付与。拡張子がなければ付与
        """
        if not filename:
            filename = 'report'
        new_filename = files.append_suffix_to_filename(filename, f"_{self.report.dates}")
        files.save_df(df, new_filename)
        if quiet:
            return new_filename
        else:
            print(f"CSVファイル{new_filename}を保存しました。")

    def download(self, df: pd.core.frame.DataFrame, filename: str = None):
        """データフレームをCSV保存し、Google Colaboratoryからダウンロード
        """
        if not filename:
            filename = 'report'
        new_filename = self.save(df, filename, quiet=True)
        files.download_file(new_filename)

    @property
    def ga_ver(self):
        """タブの状態でGAのバージョンを切り替える"""
        ver = list(self.select.ga_menu.keys())
        if ver:
            return ver[self.select.ga_tab.selected_index]

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
                description='CODE:',
            )
            self.flow = None

        # when json is selected
        def _json_selected(self, change):
            with self.log_text:
                clear_output()
                if change.new:
                    self.parent.select.reset()
                    creds_type = auth.get_credential_type_from_file(change.new)
                    # OAuth
                    if creds_type in ['installed', 'web']:
                        # load from cache
                        cache = auth.load_credentials(change.new, self.parent.required_scopes)
                        if cache:
                            self.parent.creds = cache
                            self.parent.select.ga()
                        else:
                            # run flow
                            self.flow, auth_url = auth.get_oauth_redirect(change.new, self.parent.required_scopes)
                            self.message_text.value = f'<a href="{auth_url}" target="_blank">ここをクリックし、認証後に表示されるcode' \
                                                      f'を以下に貼り付けてエンターを押してください</a> '
                            self.code_selector.layout.display = "block"
                        self.parent.json = change.new

                    # Service Account
                    if creds_type in ['service_account']:
                        self.parent.creds = auth.load_service_account_credentials_from_file(change.new,
                                                                                            self.parent.required_scopes)
                        self.parent.json = change.new
                        self.parent.select.ga()

        # when code is entered
        def _code_entered(self, change):
            with self.log_text:
                if change.new:
                    try:
                        # get token from auth code
                        self.parent.creds = auth.get_token(self.flow, change.new)
                        # save cache
                        auth.save_credentials(self.parent.json, self.parent.creds)
                        # reset menu
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
            # 選択された認証情報でGAクライアントを生成
            self.parent.build_ga_clients()
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
                return self.parent.table(df)

            @property
            def metrics(self):
                print("metrics are:")
                df = self.parent.parent.ga['4'].property.show('metrics')
                return self.parent.table(df)

            @property
            def properties(self):
                print("properties are:")
                df = self.parent.parent.ga['4'].property.show('info')
                return self.parent.table(df)

        def table(self, df: pd.core.frame.DataFrame, rows: int = 10, include_index: bool = False):
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

    class Report:
        """GA/GA4からデータを抽出"""

        def __init__(self, parent):
            self.parent = parent
            self.data = None

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

        @property
        def dates(self):
            """セットされているレポート対象期間を文字列に変換"""
            if self.parent.ga_ver:
                return f"{self.start_date.replace('-', '')}-{self.end_date.replace('-', '')}"

        def set_dates(self, date1, date2):
            """開始日と終了日を同時に指定
            Args:
                date1: start date
                date2: end date
            """
            self.start_date = date1
            self.end_date = date2

        def run(self, d: list, m: list, filter_d=None, filter_m=None, sort=None, **kwargs):
            """レポートを実行

            Args:
                d: list of dimensions
                m: list of metrics
                filter_d: dimension filter
                filter_m: metric filter
                sort:
                segments: segment (only for GA3)
            """
            dimensions = [i for i in d if i]
            metrics = [i for i in m if i]
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
                    # return self.data
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

        def save(self, filename: str = 'report', quiet: bool = False):
            """データフレームをCSV保存：ファイル名に期間を付与。拡張子がなければ付与

            Args:
                filename: path to a file
                quiet: when True, message won't be displayed
            """
            self.parent.save(filename, quiet)

        def download(self, filename: str):
            self.parent.download(self.data, filename)

        def prep(self, conf: dict, df: pd.core.frame.DataFrame = None):
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

            for col in conf.keys():
                # filter
                if 'replace' in conf[col].keys():
                    filters = conf[col]['replace']
                    # make it a list if it is a single item
                    if not isinstance(filters, list):
                        filters = [filters]
                    for f in filters:
                        utils.replace_columns(df, [(col, f, '')])
                # delete
                if 'delete' in conf[col].keys():
                    if conf[col]['delete']:
                        delete_columns.append(col)
                # rename
                if 'name' in conf[col].keys():
                    rename_columns[col] = conf[col]['name']
                # change type
                if 'type' in conf[col].keys():
                    type_columns[col] = conf[col]['type']

            df = utils.prep_df(df, delete_columns, type_columns, rename_columns)
            self.data = df
            # return df
            return self.show()
