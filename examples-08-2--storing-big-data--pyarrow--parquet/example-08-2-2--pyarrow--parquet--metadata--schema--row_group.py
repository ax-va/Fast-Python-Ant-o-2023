import pyarrow.parquet as pq
parquet_file = pq.ParquetFile("../large_data/yellow_tripdata_2020-01.parquet")

print("parquet_file.metadata:\n", parquet_file.metadata)
# parquet_file.metadata:
#  <pyarrow._parquet.FileMetaData object at 0x78001ed43420>
#   created_by: parquet-cpp-arrow version 16.0.0
#   num_columns: 18
#   num_rows: 6405008
#   num_row_groups: 7
#   format_version: 2.6
#   serialized_size: 14846

# Notice:
# There are 18 columns.
# There are 6,405,008 rows.
# There is 7 row groups in the file that is a partition of the total of rows.

print("parquet_file.schema:\n", parquet_file.schema)
# parquet_file.schema:
#  <pyarrow._parquet.ParquetSchema object at 0x7800185dbe00>
# required group field_id=-1 schema {
#   optional int32 field_id=-1 VendorID (Int(bitWidth=8, isSigned=false));
#   optional int64 field_id=-1 tpep_pickup_datetime (Timestamp(isAdjustedToUTC=false, timeUnit=milliseconds, is_from_converted_type=false, force_set_converted_type=false));
#   optional int64 field_id=-1 tpep_dropoff_datetime (Timestamp(isAdjustedToUTC=false, timeUnit=milliseconds, is_from_converted_type=false, force_set_converted_type=false));
#   optional int32 field_id=-1 passenger_count (Int(bitWidth=8, isSigned=false));
#   optional double field_id=-1 trip_distance;
#   optional int64 field_id=-1 RatecodeID;
#   optional binary field_id=-1 store_and_fwd_flag (String);
#   optional int64 field_id=-1 PULocationID;
#   optional int64 field_id=-1 DOLocationID;
#   optional int64 field_id=-1 payment_type;
#   optional double field_id=-1 fare_amount;
#   optional double field_id=-1 extra;
#   optional double field_id=-1 mta_tax;
#   optional double field_id=-1 tip_amount;
#   optional double field_id=-1 tolls_amount;
#   optional double field_id=-1 improvement_surcharge;
#   optional double field_id=-1 total_amount;
#   optional double field_id=-1 congestion_surcharge;
# }

print("parquet_file.metadata.row_group(0):\n", parquet_file.metadata.row_group(0))
# parquet_file.metadata.row_group(0):
#  <pyarrow._parquet.RowGroupMetaData object at 0x77fffffd76f0>
#   num_columns: 18
#   num_rows: 1048576
#   total_byte_size: 139470304
#   sorting_columns: ()
