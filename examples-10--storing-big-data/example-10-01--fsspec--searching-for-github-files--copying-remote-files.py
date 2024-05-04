"""
Demonstrate functionalities of fsspec's GithubFileSystem.

Extra functionality:
- Navigate repository at any point in time, not just at the current time point of the master branch.
- Specify a branch or a tag to inspect the repository at that precise point.

Limitations:
- If you query the server many times, it will rate-limit you.
"""
import os
from typing import Generator
from fsspec.implementations.github import GithubFileSystem  # requests installed

github_user = "ax-va"
github_repo = "Fast-Python-Antao-2023"

fs = GithubFileSystem(github_user, github_repo)
print(fs.ls(""))  # The root directory is represented by the empty string, not by the typical /
# [..., 'README.md', ..., 'examples-10--storing-big-data', ...]


def get_remote_zip_files(fs: "FileSystem", root_path: str = "") -> Generator[str, None, None]:
    """ Get remote zip files """
    # similar to os.walk with the root_path /
    for root, dirs, fnames in fs.walk(root_path):
        for fname in fnames:
            if fname.endswith(".zip"):
                yield f"{root}/{fname}"


remote_zip_files = list(get_remote_zip_files(fs))
# [..., 'examples-10--storing-big-data/dummy.zip', ...]


def copy_zip_files(fs):
    """ Copy single remote file to local file """
    for remote_zip_file in get_remote_zip_files(fs):
        basename = os.path.basename(remote_zip_file)
        new_local_file = basename.split(".")[0] + "_copied.zip"
        # Copy
        fs.get_file(remote_zip_file, new_local_file)
        yield new_local_file


copied_zip_files = list(copy_zip_files(fs))
# ['dummy_copied.zip']
