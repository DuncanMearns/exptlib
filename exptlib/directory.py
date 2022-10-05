from pathlib import Path
from anytree import Node
import os


class TreeLevel(Node):

    def keys(self):
        return [k for k in self.__dict__.keys() if not k.startswith("_")]

    def child(self, name, make=False):
        try:
            return list(filter(lambda x: x.name == name, self.children)).pop()
        except IndexError as e:
            if make:
                child = TreeLevel(name, parent=self)
                return child
            raise e

    def __setitem__(self, key, value):
        self.__setattr__(key, value)

    def __getitem__(self, item):
        try:
            return self.__getattribute__(item)
        except AttributeError:
            raise KeyError(f"{self} has no key '{item}'")

    def __str__(self):
        return f"TreeLevel({os.path.join(*[parent.name for parent in self.path])})"


class NamedTree(TreeLevel):

    def __init__(self, *level_names, name="root"):
        super().__init__(name)
        self.level_names = level_names

    @property
    def level_names(self):
        return self._level_names

    @level_names.setter
    def level_names(self, names):
        self._level_names = names

    @property
    def n_levels(self):
        return len(self.level_names)

    def __setitem__(self, levels, value):
        assert len(levels) == self.n_levels
        current = self
        for level in levels:
            current = current.child(level, make=True)
        current.data = value

    def __getitem__(self, levels):
        assert len(levels) == self.n_levels
        result = self
        for level in levels:
            result = result.child(level)
        return result

    def __iter__(self):
        for leaf in self.leaves:
            yield leaf.path


class DirectoryTree(NamedTree):

    def __init__(self, root, *, levels, map_data):
        super().__init__(*levels, name=root)
        self.directory = Path(self.name)
        assert self.directory.exists(), f"Directory: {self.name} does not exist!"
        self.map_data = map_data
        match = "/".join(["*"] * len(self.level_names))
        for subdir in self.directory.glob(match):
            levels = reversed([part for level, part in zip(reversed(self.level_names), reversed(subdir.parts))])
            self[list(levels)] = self.map_data(subdir)
