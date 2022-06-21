from time import sleep
from IPython.display import clear_output

from kora import os

# install packages for GA4
try:
    from google.analytics.data import BetaAnalyticsDataClient
except ModuleNotFoundError:
    print("Installing packages for GA4")
    os.system("pip install -U -q google-analytics-admin")
    os.system("pip install -U -q google-analytics-data")
    # update packages
    os.system("pip install -U -q google-api-core")
    clear_output()

    print("Runtime is now restarting...")
    print("You can ignore the error message [Your session crashed for an unknown reason.]")
    sleep(0.5)
    os._exit(0)  # restart
