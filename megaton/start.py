"""run megaton"""

from IPython.display import clear_output
import os
import sys
from time import sleep

from oauthlib.oauth2.rfc6749.errors import InvalidGrantError

from . import auth, widgets

_in_colab = "google.colab" in sys.modules

try:
    # check if packages for GA4 are installed
    from google.analytics.data import BetaAnalyticsDataClient
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

if _in_colab:
    # mount google drive
    from . import gdrive
    json_path = gdrive.link_nbs()

print("Notebookの準備ができました。")

required_scopes = [
            # 'https://www.googleapis.com/auth/analytics.readonly',
            'https://www.googleapis.com/auth/analytics.edit',
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive',
            'https://www.googleapis.com/auth/cloud-platform',
        ]


class Megaton:

    def __init__(self, path: str = None):
        self.creds = None
        self.auth_menu = None
        self.ga_menu = None
        if not path and _in_colab:
            path = '/nbs'
        self.auth(path)

    def auth(self, path: str):
        # get files in a directory and display select menu
        if os.path.isdir(path):
            # if directory
            json_files = auth.get_credentials_files_from(path)
            # self.auth_with_menu(json_files)
            self.auth_menu = AuthMenu(self, json_files)
            self.auth_menu.show()

        # auth with a file
        elif os.path.isfile(path):
            print(f"auth with a file {path}")
            self.creds = auth.load_service_account_credentials_from_file(path, required_scopes)
            self.select_ga()

    def select_ga(self):
        self.ga_menu = GaMenu(self)


class GaMenu:
    def __init__(self, parent):
        self.parent = parent
        # build menu of GA accounts
        print("Select GA")


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
    def load_json(self, change):
        with self.log_text:
            if change.new:
                creds_type = auth.get_client_secrets_type_from_file(change.new)
                if creds_type in ['installed', 'web']:
                    # print("running flow")
                    self.flow, auth_url = auth._get_oauth_redirect(change.new, required_scopes)
                    self.message_text.value = f'<a href="{auth_url}" target="_blank">ここをクリックし、認証後に表示されるcodeを以下に貼り付けてエンターを押してください</a>'
                    self.code_selector.layout.display = "block"
                    self.log_text.value = ''

                if creds_type in ['service_account']:
                    self.message_text.value = ''
                    self.code_selector.value = ''
                    self.code_selector.layout.display = "none"
                    self.parent.creds = auth.load_service_account_credentials_from_file(change.new,
                                                                                 required_scopes)
                    self.parent.select_ga()

    # when code is entered
    def get_code(self, change):
        with self.log_text:
            if change.new:
                # print(f"code is entered: {change.new}")
                try:
                    self.parent.creds = auth._get_token(self.flow, change.new)
                    self.code_selector.value = ''
                    self.code_selector.layout.display = "none"
                    self.parent.select_ga()
                except InvalidGrantError:
                    print("malformed")

    def show(self):
        display(self.creds_selector, self.message_text, self.code_selector, self.log_text)
        self.code_selector.layout.display = "none"
        self.creds_selector.observe(self.load_json, names='value')
        self.code_selector.observe(self.get_code, names='value')
