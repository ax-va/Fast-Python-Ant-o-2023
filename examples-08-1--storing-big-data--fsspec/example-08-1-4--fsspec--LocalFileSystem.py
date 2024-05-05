import importlib
from fsspec.implementations.local import LocalFileSystem
gfs = importlib.import_module("example-08-1-1--fsspec--GithubFileSystem")

fs = LocalFileSystem()

local_zip_files = list(gfs.get_zip_files(fs))
# root: /home/delorian/PycharmProjects/Fast-Python-Antao-2023/examples-08-1--storing-big-data--fsspec/
# ...
print(local_zip_files)
# [..., '/home/delorian/PycharmProjects/Fast-Python-Antao-2023/examples-08-1--storing-big-data--fsspec/dummy.zip', ...]
