import os

# install packages for BigQuery
# os.system("pip install -U -q google-cloud-bigquery")
os.system("pip install -U -q google-cloud-bigquery-datatransfer")
# update packages
os.system("pip install -q protobuf==3.20.*")
