import json
from abc import abstractmethod
from pathlib import Path
import pandas as pd


class Metadata:
    """Base Metadata class."""

    def __init__(self, path):
        self.path = path
        if self.path.exists():
            self.data = self.read()
        else:
            self.data = self.new()

    @property
    def path(self):
        return self._path

    @path.setter
    def path(self, value):
        self._path = Path(value)

    @property
    def data(self):
        return self._data

    @data.setter
    def data(self, val):
        self._data = val

    def __str__(self):
        return self.data.__str__()

    @abstractmethod
    def read(self):
        return

    @abstractmethod
    def write(self):
        return

    @abstractmethod
    def new(self):
        return


class MappedMetadata(Metadata):
    """Class for handling metadata stored as key, value pairs."""

    def __init__(self, path):
        super().__init__(path)

    def read(self):
        with open(self.path, "r") as json_file:
            return json.load(json_file)

    def write(self):
        with open(self.path, "w") as json_file:
            json.dump(self.data, json_file)

    def new(self):
        return {}

    def __str__(self):
        return '\n'.join([f'{key}: {val}' for key, val in self.data.items()]) + '\n'

    def keys(self):
        return self.data.keys()

    def __setitem__(self, key, value):
        self.data[key] = value
        self.write()

    def __getitem__(self, item):
        return self.data[item]


class TabulatedMetadata(Metadata):
    """Class for handling tabulated metadata."""

    def __init__(self, path, columns, dtypes=None):
        self.columns = tuple(columns)
        self.dtypes = dtypes or {}
        super().__init__(path)
        for col in self.columns:
            if col not in self.data.columns:
                self.data[col] = None

    def read(self):
        return pd.read_csv(self.path, dtype=self.dtypes)

    def write(self):
        self.data.to_csv(self.path, index=False)

    def new(self):
        return pd.DataFrame(columns=self.columns)

    def get(self, col, val):
        df = self.data
        return df[df[col] == val]

    def set(self, index, column, val):
        self.data.loc[index, column] = val

    def append(self, a):
        self.data = self.data.append(a, ignore_index=True)

    def iterrows(self, missing_data=None):
        if missing_data:
            df = self.data[pd.isnull(self.data[missing_data])]
        else:
            df = self.data
        for i, vals in df.iterrows():
            yield i, vals
