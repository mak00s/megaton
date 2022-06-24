"""Configurations
"""

# Scopes necessary to use Megaton
DEFAULT_SCOPES = [
    'https://www.googleapis.com/auth/analytics.readonly',
    'https://www.googleapis.com/auth/analytics.edit',
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive',
    'https://www.googleapis.com/auth/cloud-platform',
]

# URL of Google Sheets template for GA4
GOOGLE_SHEET_GA4_TEMPLATE_URL = 'https://docs.google.com/spreadsheets/d/1Jr4au61Xy7gfGBf5yvNKdT43awsSv98zOs4FTfM6Nms'

GOOGLE_SHEET_GA4_TEMPLATE_SHEET = '推奨CustomDimension'
