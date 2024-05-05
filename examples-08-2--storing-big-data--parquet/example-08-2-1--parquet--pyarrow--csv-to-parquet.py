import pyarrow.parquet as pq
from pyarrow import csv

# Big big_data file is available under
# https://tiago.org/yellow_tripdata_2020-01.csv.gz

# Add big files to .gitignore with:
"""
# big big_data for GitHub
# 109.9 MB
big_data/yellow_tripdata_2020-01.csv.gz
# 113.1 MB
big_data/yellow_tripdata_2020-01.parquet
"""

table = csv.read_csv("../data/yellow_tripdata_2020-01.csv.gz")
pq.write_table(table, "../big_data/yellow_tripdata_2020-01.parquet")
