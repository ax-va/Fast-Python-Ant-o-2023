from fsspec.implementations.zip import ZipFileSystem
from pyarrow import csv
from pyarrow.fs import PyFileSystem, FSSpecHandler

zfs = ZipFileSystem("dummy.zip")
# Map the fsscep filesystem to the Arrow filesystem
arrow_fs = PyFileSystem(FSSpecHandler(zfs))
dummy1_csv = csv.read_csv(arrow_fs.open_input_stream("dummy/dummy1.csv"))
print(dummy1_csv)
# pyarrow.Table
# col1: int64
# col2: int64
# ----
# col1: [[1,3,5]]
# col2: [[2,4,6]]
