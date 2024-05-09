"""
New York City taxi data
"""
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from pyarrow import csv

# Large data file is available under
# https://tiago.org/yellow_tripdata_2020-01.csv.gz

# Add files to .gitignore with:
"""
# large datasets for posting on GitHub
# 109.9 MB
large_datasets/yellow_tripdata_2020-01.csv.gz
# 113.1 MB
large_datasets/yellow_tripdata_2020-01.parquet
# 86,4 MB
large_datasets/yellow_tripdata_2020-01-ZSTD.parquet
"""

convert_options = csv.ConvertOptions(
    column_types={
        "VendorID": pa.uint8(),
        "passenger_count": pa.uint8(),
    }
)

table = csv.read_csv(
    "../large_datasets/yellow_tripdata_2020-01.csv.gz",
    convert_options=convert_options
)

pq.write_table(table, "../large_datasets/yellow_tripdata_2020-01.parquet")

# Use the ZSTD compression on all columns
pq.write_table(table, "../large_datasets/yellow_tripdata_2020-01-ZSTD.parquet", compression="ZSTD")

# Read file into Pandas dataframe

df = pd.read_parquet("../large_datasets/yellow_tripdata_2020-01.parquet")
df.head()
#    VendorID tpep_pickup_datetime tpep_dropoff_datetime  passenger_count  trip_distance  RatecodeID store_and_fwd_flag  ...  extra  mta_tax  tip_amount  tolls_amount  improvement_surcharge  total_amount  congestion_surcharge
# 0       1.0  2020-01-01 00:28:15   2020-01-01 00:33:03              1.0            1.2         1.0                  N  ...    3.0      0.5        1.47           0.0                    0.3         11.27                   2.5
# 1       1.0  2020-01-01 00:35:39   2020-01-01 00:43:04              1.0            1.2         1.0                  N  ...    3.0      0.5        1.50           0.0                    0.3         12.30                   2.5
# 2       1.0  2020-01-01 00:47:41   2020-01-01 00:53:52              1.0            0.6         1.0                  N  ...    3.0      0.5        1.00           0.0                    0.3         10.80                   2.5
# 3       1.0  2020-01-01 00:55:23   2020-01-01 01:00:14              1.0            0.8         1.0                  N  ...    0.5      0.5        1.36           0.0                    0.3          8.16                   0.0
# 4       2.0  2020-01-01 00:01:58   2020-01-01 00:04:16              1.0            0.0         1.0                  N  ...    0.5      0.5        0.00           0.0                    0.3          4.80                   0.0
#
# [5 rows x 18 columns]

# Create unordered and ordered datasets for demonstrating the efficiency of
# the Run Length Encoding (RLE) encoding in the next example
silly_table_unordered = table.from_arrays(
    [table["VendorID"]],
    ["unordered"],
)

silly_table_ordered = table.from_arrays(
    [table["VendorID"].take(pc.sort_indices(table["VendorID"]))],
    ["ordered"],
)

pq.write_table(silly_table_unordered, "../datasets/silly_unordered.parquet")  # 953.266 Bytes
pq.write_table(silly_table_ordered, "../datasets/silly_ordered.parquet")  # 1.976 Bytes
