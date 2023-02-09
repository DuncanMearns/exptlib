from __future__ import annotations
from pathlib import Path
import typing
from collections.abc import Mapping


class Directory(Mapping):
    """More user-friendly wrapper for pathlib.Path directories. Allows for easier and more explicit navigation through
    subdirectories, and file and subdirectory creation.
    
    Parameters
    ----------
    directory: str or Path
    """

    def __init__(self, directory: typing.Union[str, Path]):
        super().__init__()
        self.directory = Path(directory)

    def __repr__(self):
        return f"Directory('{str(self.directory)}')"

    def __str__(self):
        return str(self.directory)

    @property
    def subdirs(self) -> typing.List[str]:
        """Returns a list of subdirectory names in the directory."""
        return [subdir.name for subdir in self.directory.glob("*") if subdir.is_dir()]

    @property
    def files(self) -> typing.List[str]:
        """Returns a list of file names in the directory."""
        return [path.name for path in self.directory.glob('*') if path.is_file()]

    @property
    def children(self) -> typing.List[str]:
        """Returns a list of file and subdirectory names in the directory."""
        return [path.name for path in self.directory.glob("*")]

    def new_subdir(self, *names: str) -> Directory:
        """Create a new subdirectory with the given name."""
        new = self.directory.joinpath(*names)
        new.mkdir(exist_ok=True, parents=True)
        return Directory(new)

    def new_file(self, name: str, ext: str=None) -> Path:
        """Return a filepath in the directory with the given name and extension."""
        if ext:
            name = ".".join([name, ext])
        return self.directory.joinpath(name)

    def __getattr__(self, item):
        val = getattr(self.directory, item)
        if isinstance(val, Path) and val.is_dir():
            return Directory(val)
        return val

    def _get_child(self, item):
        child = self.directory.joinpath(item)
        if not child.exists():
            raise ValueError(f"{item} not in {self}")
        if child.is_dir():
            return Directory(child)
        return child

    def __getitem__(self, keys) -> typing.Union[Directory, Path]:
        if isinstance(keys, str):
            keys = (keys,)
        result = self
        for item in keys:
            try:
                result = result._get_child(item)
            except AttributeError:
                raise ValueError(f"{item} not in {result}")
        return result

    def __iter__(self):
        return iter(self.children)

    def __len__(self):
        return len(self.children)

    def to_dict(self, full=True):
        """Return all subdirectory and files as a nested dictionary.

        Parameters
        ----------
        full : bool, default=True
            If True, walk the complete directory tree including all subdirectories.
        """
        tree = {}
        # tree = dict
        for name, path in self.items():
            stem, *_ = name.split(".")
            if full and isinstance(path, Directory):
                node = path.to_dict()
                tree[name] = node
                continue
            try:
                tree[stem].append(path)
            except KeyError:
                tree[stem] = [path]
        return tree
