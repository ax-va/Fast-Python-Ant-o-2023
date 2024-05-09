import pyarrow.parquet as pq

parquet_file = pq.ParquetFile("../large_datasets/yellow_tripdata_2020-01.parquet")

print("parquet_file.metadata:\n", parquet_file.metadata)
# parquet_file.metadata:
#  <pyarrow._parquet.FileMetaData object at 0x78001ed43420>
#   created_by: parquet-cpp-arrow version 16.0.0
#   num_columns: 18  # <-- There are 18 columns.
#   num_rows: 6405008  # <-- There are 6,405,008 rows.
#   num_row_groups: 7  # <-- There is 7 row groups in the file that is a partition of the total of rows.
#   format_version: 2.6
#   serialized_size: 14846

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
#   optional binary field_id=-1 store_and_fwd_flag (String);  # <-- Text is stored as general binary data
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

# Look at the existing metadata for columns

tip_amount_col_meta = parquet_file.metadata.row_group(0).column(13)
print("tip_amount_col_meta:\n", tip_amount_col_meta)
# tip_amount_col_meta:
#  <pyarrow._parquet.ColumnChunkMetaData object at 0x725c5ffcf9c0>
#   file_offset: 16754437
#   file_path:
#   physical_type: DOUBLE
#   num_values: 1048576
#   path_in_schema: tip_amount
#   is_stats_set: True
#   statistics:  # <-- The statistical information started here
#     <pyarrow._parquet.Statistics object at 0x725c5ffcfa60>
#       has_min_max: True
#       min: -7.0
#       max: 450.0
#       null_count: 0
#       distinct_count: None
#       num_values: 1048576
#       physical_type: DOUBLE
#       logical_type: None
#       converted_type (legacy): NONE
#   compression: SNAPPY  # <-- The compression type (algorithm) can be different for different columns or cannot be used at all.
#   encodings: ('PLAIN', 'RLE', 'RLE_DICTIONARY')
#   has_dictionary_page: True
#   dictionary_page_offset: 15351114
#   data_page_offset: 15360558
#   total_compressed_size: 1403323
#   total_uncompressed_size: 1504833

# Compression algorithms:
# https://facebook.github.io/zstd/#benchmarks

table = parquet_file.read()

len(table["tip_amount"].unique())
# 3626

# encoding a column with a dictionary
# -> long values are converted to an indirect reference
# -> only 3626 different values with the 64-bits double type
# -> encoding can reduce from 64 bits to 12 bits per value
# -> enough to encode up to 4096 values

# Demonstrate Run Length Encoding (RLE) encoding

silly_unordered = pq.ParquetFile("../datasets/silly_unordered.parquet")
# <pyarrow.parquet.core.ParquetFile at 0x72768c7cf350>

silly_unordered_group = silly_unordered.metadata.row_group(0)
# <pyarrow._parquet.RowGroupMetaData object at 0x72768e4e1d50>
#   num_columns: 1
#   num_rows: 1048576
#   total_byte_size: 157418
#   sorting_columns: ()

print("silly_unordered_group.column(0):\n", silly_unordered_group.column(0))
# silly_unordered_group.column(0):
#  <pyarrow._parquet.ColumnChunkMetaData object at 0x72768db4d030>
#   file_offset: 156686
#   file_path:
#   physical_type: INT32
#   num_values: 1048576
#   path_in_schema: unordered
#   is_stats_set: True
#   statistics:
#     <pyarrow._parquet.Statistics object at 0x72768db4ddf0>
#       has_min_max: True
#       min: 1
#       max: 2
#       null_count: 0
#       distinct_count: None
#       num_values: 1048576
#       physical_type: INT32
#       logical_type: Int(bitWidth=8, isSigned=false)
#       converted_type (legacy): UINT_8
#   compression: SNAPPY
#   encodings: ('PLAIN', 'RLE', 'RLE_DICTIONARY')
#   has_dictionary_page: True
#   dictionary_page_offset: 4
#   data_page_offset: 28
#   total_compressed_size: 156682
#   total_uncompressed_size: 157418

silly_ordered = pq.ParquetFile("../datasets/silly_ordered.parquet")
# <pyarrow.parquet.core.ParquetFile at 0x72766bb3ae50>

silly_ordered_group = silly_ordered.metadata.row_group(0)
# <pyarrow._parquet.RowGroupMetaData object at 0x72767dd2a4d0>
#   num_columns: 1
#   num_rows: 1048576
#   total_byte_size: 69
#   sorting_columns: ()

print("silly_ordered_group.column(0):\n", silly_ordered_group.column(0))
# silly_ordered_group.column(0):
#  <pyarrow._parquet.ColumnChunkMetaData object at 0x72766bba7b00>
#   file_offset: 77
#   file_path:
#   physical_type: INT32
#   num_values: 1048576
#   path_in_schema: ordered
#   is_stats_set: True
#   statistics:
#     <pyarrow._parquet.Statistics object at 0x72768db284f0>
#       has_min_max: True
#       min: 1
#       max: 1
#       null_count: 0
#       distinct_count: None
#       num_values: 1048576
#       physical_type: INT32
#       logical_type: Int(bitWidth=8, isSigned=false)
#       converted_type (legacy): UINT_8
#   compression: SNAPPY
#   encodings: ('PLAIN', 'RLE', 'RLE_DICTIONARY')
#   has_dictionary_page: True

# -> RLE typically works well for ordered fields or fields with few values
