"""
Demonstrate fsspec's GithubFileSystem.

Extra functionality:
- Navigate repository at any point in time, not just at the current time point of the master branch.
- Specify a branch or a tag to inspect the repository at that precise point.

Limitations:
- If you query the server many times, it will rate-limit you.
"""
import os
from typing import Generator, Iterable


def get_zip_files(fs: "FileSystem", root_path: str = "") -> Generator[str, None, None]:
    """ Get remote zip files """
    # similar to os.walk with the root_path /
    for root, dirs, fnames in fs.walk(root_path):
        print("root:", root)
        for fname in fnames:
            if fname.endswith(".zip"):
                yield os.path.join(root, fname)


def copy_files_to_local(files: Iterable[str]) -> Generator[str, None, None]:
    """ Copy files to local """
    for file in files:
        file_basename = os.path.basename(file)
        new_local_file = file_basename.split(".")[0] + "_copied.zip"
        # Copy
        fs.get_file(file, new_local_file)
        yield new_local_file


if __name__ == "__main__":
    from fsspec.implementations.github import GithubFileSystem  # requests installed

    github_user = "ax-va"
    github_repo = "Fast-Python-Antao-2023"

    fs = GithubFileSystem(github_user, github_repo)
    print(fs.ls(""))  # The root directory is represented by the empty string, not by the typical /
    # [..., 'README.md', ..., 'examples-08-1--storing-big-data--fsspec', ...]

    remote_zip_files = list(get_zip_files(fs))
    # root:
    # ...
    # root: examples-08-1--storing-big-data--fsspec
    # ...
    print(remote_zip_files)
    # [..., 'examples-08-1--storing-big-data--fsspec/dummy.zip', ...]

    copied_zip_files = list(copy_files_to_local(remote_zip_files))
    print(copied_zip_files)
    # [..., 'dummy_copied.zip', ...]
