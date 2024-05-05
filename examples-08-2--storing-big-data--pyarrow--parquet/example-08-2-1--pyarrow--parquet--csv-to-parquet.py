import pyarrow.parquet as pq
from pyarrow import csv

# Big data file is available under
# https://tiago.org/yellow_tripdata_2020-01.csv.gz

# Add big files to .gitignore with:
"""
# big data for GitHub
# 109.9 MB
big_data/yellow_tripdata_2020-01.csv.gz
# 113.1 MB
big_data/yellow_tripdata_2020-01.parquet
"""

table = csv.read_csv("../big_data/yellow_tripdata_2020-01.csv.gz")
pq.write_table(table, "../big_data/yellow_tripdata_2020-01.parquet")

import pandas as pd

df = pd.read_parquet("../big_data/yellow_tripdata_2020-01.parquet")
df.head()
#    VendorID tpep_pickup_datetime tpep_dropoff_datetime  passenger_count  trip_distance  RatecodeID store_and_fwd_flag  ...  extra  mta_tax  tip_amount  tolls_amount  improvement_surcharge  total_amount  congestion_surcharge
# 0       1.0  2020-01-01 00:28:15   2020-01-01 00:33:03              1.0            1.2         1.0                  N  ...    3.0      0.5        1.47           0.0                    0.3         11.27                   2.5
# 1       1.0  2020-01-01 00:35:39   2020-01-01 00:43:04              1.0            1.2         1.0                  N  ...    3.0      0.5        1.50           0.0                    0.3         12.30                   2.5
# 2       1.0  2020-01-01 00:47:41   2020-01-01 00:53:52              1.0            0.6         1.0                  N  ...    3.0      0.5        1.00           0.0                    0.3         10.80                   2.5
# 3       1.0  2020-01-01 00:55:23   2020-01-01 01:00:14              1.0            0.8         1.0                  N  ...    0.5      0.5        1.36           0.0                    0.3          8.16                   0.0
# 4       2.0  2020-01-01 00:01:58   2020-01-01 00:04:16              1.0            0.0         1.0                  N  ...    0.5      0.5        0.00           0.0                    0.3          4.80                   0.0
#
# [5 rows x 18 columns]
