"""run megaton"""

from IPython.display import clear_output
import os
import sys
from time import sleep

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
    from . import gdrive
    # mount google drive
    json_path = gdrive.link_nbs()

print("Notebookの準備ができました。")


class Megaton:

    def __init__(self, path: str):
        self.creds = None
        self.required_scopes = [
            # 'https://www.googleapis.com/auth/analytics.readonly',
            'https://www.googleapis.com/auth/analytics.edit',
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive',
            'https://www.googleapis.com/auth/cloud-platform',
        ]
        if path:
            self.auth(path)

    def auth(self, path: str):
        # get files in a directory and display select menu
        if os.path.isdir(path):
            # if directory
            json_files = auth.get_credentials_files_from(path)

            # show menu of json files
            menu_creds = widgets.menu_for_credentials(json_files)
            out1 = widgets.Output()
            display(menu_creds, out1)

            # when selected
            def on_change(change):
                with out1:
                    if change.new:
                        creds_type = auth.get_client_secrets_type_from_file(change.new)
                        # print(f"{change.new} is {creds_type}")
                        if creds_type in ['installed', 'web']:
                            print("running flow")

                        if creds_type in ['service_account']:
                            print("sa")
                            self.creds = auth.load_service_account_credentials_from_file(change.new,
                                                                                         self.required_scopes)
                            print(self.creds)
            menu_creds.observe(on_change, names='value')

        # auth with a file
        elif os.path.isfile(path):
            print(f"auth with a file {path}")
