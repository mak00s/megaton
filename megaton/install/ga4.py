import os

try:
    # check if packages for GA4 are installed
    from google.analytics.admin import AnalyticsAdminServiceClient
    from google.analytics.data import BetaAnalyticsDataClient
except ModuleNotFoundError:
    # install packages for GA4
    os.system("pip install -U -q google-analytics-admin")
    os.system("pip install -U -q google-analytics-data")
    # update packages
    os.system("pip install -U -q google-api-core")
