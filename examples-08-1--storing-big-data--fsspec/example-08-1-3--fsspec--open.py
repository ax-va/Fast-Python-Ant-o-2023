import fsspec
import pandas as pd

with fsspec.open("dummy.txt", "r") as f:
    for line in f:
        print(repr(line))
# 'Dummy text 1\n'
# 'Dummy text 2'

# Use URL chaining to open a csv file in a zip file
with fsspec.open("zip://dummy/dummy1.csv::dummy.zip", "r") as f:  # "zip://" is a protocol
    print(f.read())
# col1,col2
# 1,2
# 3,4
# 5,6

# URL chain to GitHub
url_chain = (
    "zip://dummy/dummy1.csv::github://ax-va:Fast-Python-Antao-2023@"
    "/examples-08-1--storing-big-data--fsspec/dummy.zip"
)

with fsspec.open(url_chain) as f:
    print(pd.read_csv(f))
#    col1  col2
# 0     1     2
# 1     3     4
# 2     5     6
