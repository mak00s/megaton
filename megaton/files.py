"""Functions for saving and downloading files
"""

import os
import sys

import pandas as pd


def in_colab():
    """Check if the code is running in Google Colaboratory"""
    return 'google.colab' in sys.modules


IN_COLAB = in_colab()

if IN_COLAB:
    from google.colab import files


def append_suffix_to_filename(filename: str, suffix: str):
    name, ext = os.path.splitext(filename)
    ext = ext if ext else '.csv'
    return f"{name}{suffix}{ext}"


def save_df(df: pd.DataFrame, filename: str, mode: str = 'w', include_header: bool = True):
    """Save a DataFrame as CSV
    """
    if mode == 'a':
        include_header = False

    df.to_csv(filename, mode=mode, index=False, header=include_header)


def download_file(filename: str):
    """Download a file from Google Colaboratory
    """
    if IN_COLAB:
        files.download(filename)
