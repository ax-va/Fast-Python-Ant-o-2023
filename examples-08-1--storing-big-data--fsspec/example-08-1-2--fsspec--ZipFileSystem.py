"""
Compare zipfile and fsspec on the inspection of zips contained csvs.

Motivation:
- zipfile -> more mess
- fsspec -> less mess and no need to learn the zipfile API
"""
import zipfile
import pandas as pd
from typing import Iterable
from fsspec.implementations.zip import ZipFileSystem


def describe_csvs_in_zips_with_zipfile(zip_files: Iterable[str]) -> None:
    """ Describe csvs in zips with zipfile and Pandas """
    for zip_file in zip_files:
        print("zip_file:", zip_file)
        # Open the zip file using zipfile
        zf = zipfile.ZipFile(zip_file)
        for zf_info in zf.infolist():
            print("\nzf_info.filename:", zf_info.filename)
            if not zf_info.filename.endswith(".csv"):
                continue
            df = pd.read_csv(zipfile.Path(zf, zf_info.filename).open())
            print("df.describe():\n", df.describe())


def describe_csvs_in_zips_with_fsspec(zip_files: Iterable[str]) -> None:
    """ Describe csvs in zips with fsspec and Pandas """
    for zip_file in zip_files:
        print("zip_file:", zip_file)
        zfs = ZipFileSystem(zip_file)
        # Use the find method that is common for fsspec
        for zipped_file in zfs.find(""):  # Find from the root in the zip file
            print("\nzipped_file:", zipped_file)
            if not zipped_file.endswith(".csv"):
                continue
            df = pd.read_csv(zfs.open(zipped_file))
            print("df.describe():\n", df.describe())


describe_csvs_in_zips_with_zipfile(['dummy.zip'])
# zip_file: dummy.zip
#
# zf_info.filename: dummy/
#
# zf_info.filename: dummy/file1.txt
#
# zf_info.filename: dummy/dummy1.csv
# df.describe():
#         col1  col2
# count   3.0   3.0
# mean    3.0   4.0
# std     2.0   2.0
# min     1.0   2.0
# 25%     2.0   3.0
# 50%     3.0   4.0
# 75%     4.0   5.0
# max     5.0   6.0
#
# zf_info.filename: dummy/dummy2.csv
# df.describe():
#          name     address
# count      3           3
# unique     3           3
# top     john  manchester
# freq       1           1
#
# zf_info.filename: dummy/dir1/
#
# zf_info.filename: dummy/dir1/file2.txt
#
# zf_info.filename: dummy/dir2/

describe_csvs_in_zips_with_fsspec(['dummy.zip'])
# zip_file: dummy.zip
#
# zipped_file: dummy/dir1/file2.txt
#
# zipped_file: dummy/dummy1.csv
# df.describe():
#         col1  col2
# count   3.0   3.0
# mean    3.0   4.0
# std     2.0   2.0
# min     1.0   2.0
# 25%     2.0   3.0
# 50%     3.0   4.0
# 75%     4.0   5.0
# max     5.0   6.0
#
# zipped_file: dummy/dummy2.csv
# df.describe():
#          name     address
# count      3           3
# unique     3           3
# top     john  manchester
# freq       1           1
#
# zipped_file: dummy/file1.txt
