from __future__ import annotations
from pathlib import Path
import typing
from collections.abc import Mapping


class Directory(Mapping):

    def __init__(self, directory: typing.Union[str, Path]):
        super().__init__()
        self.directory = Path(directory)

    def __repr__(self):
        return f"Directory({str(self.directory)})"

    @property
    def subdirs(self) -> list:
        return [subdir.name for subdir in self.directory.glob("*") if subdir.is_dir()]

    @property
    def files(self) -> list:
        return [path.name for path in self.directory.glob('*') if path.is_file()]

    @property
    def children(self) -> list:
        return [path.name for path in self.directory.glob("*")]

    def new_subdir(self, name) -> Directory:
        new = self.directory.joinpath(name)
        new.mkdir(exist_ok=True)
        return Directory(new)

    def new_file(self, name, ext=None) -> Path:
        if ext:
            name = ".".join([name, ext])
        return self.directory.joinpath(name)

    def __getattr__(self, item):
        val = getattr(self.directory, item)
        if isinstance(val, Path) and val.is_dir():
            return Directory(val)
        return val

    def __getitem__(self, item) -> typing.Union[Directory, Path]:
        child = self.directory.joinpath(item)
        if not child.exists():
            raise ValueError(f"{item} not in {self}")
        if child.is_dir():
            return Directory(child)
        return child

    def __iter__(self):
        return iter(self.children)

    def __len__(self):
        return len(self.children)
