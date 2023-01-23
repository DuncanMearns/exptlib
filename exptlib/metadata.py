import json
import typing
import warnings
from abc import abstractmethod
from pathlib import Path
import pandas as pd

from .descriptors import SetOnce


__all__ = ["Metadata", "JSONMetadata", "CSVMetadata"]


class Metadata:
    """Base metadata class. Mixes file management into any data type.

    Attributes
    ----------
    metadata_type: Any or callable
        A data type, or callable that returns a data type, that can be saved by the write method.
    metadata_extension: str
        Extension appended to path (if not already present).
    path: Path
        Where metadata is saved.
    """

    metadata_type = object
    metadata_extension = ""

    path = SetOnce()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def set_path(self, path: typing.Union[str, Path]):
        if not str(path).endswith(self.metadata_extension):
            path = ".".join([path, self.metadata_extension])
        self.path = Path(path)

    @abstractmethod
    def read(self, **kwargs):
        """Returns a data structure that can be passed to metadata_type from the path."""
        pass

    @abstractmethod
    def write(self, **kwargs):
        """Writes an instance returned by metadata_type to the path."""
        pass

    @classmethod
    def create(cls, path: typing.Union[str, Path], *args, **kwargs):
        """Create a new instance of the metadata_type mixed with metadata file management.

        Parameters
        ----------
        path: str or Path
            Where metadata is saved.
        """
        metadata = type(cls.__name__, (cls, cls.metadata_type), {})(*args, **kwargs)
        metadata.set_path(path)
        return metadata

    @classmethod
    def open(cls, path: typing.Union[str, Path], **kwargs):
        """Import metadata.

        Parameters
        ----------
        path: str or Path
            Where metadata is saved.
        """
        metadata = cls.create(path)
        if not metadata.path.exists():
            warnings.warn(f"Metadata path {path} does not exist. Creating new file.")
            metadata.write()
            return metadata
        data = metadata.read(**kwargs)
        return cls.create(path, data)


class JSONMetadata(Metadata):
    """Metadata format that saves dict data as json."""

    metadata_type = dict
    metadata_extension = "json"

    def read(self):
        with open(self.path, "r") as json_file:
            return json.load(json_file)

    def write(self):
        with open(self.path, "w") as json_file:
            json.dump(self, json_file)


class CSVMetadata(Metadata):
    """Metadata format that saves dataframe data as csv."""

    metadata_type = pd.DataFrame
    metadata_extension = "csv"

    def read(self, **kwargs):
        return pd.read_csv(self.path, **kwargs)

    def write(self, **kwargs):
        self.to_csv(self.path, **kwargs)


if __name__ == "__main__":

    import numpy as np

    md = JSONMetadata.open("hello.json")
    md["hello"] = "world"
    md.write()
    print(md)

    data = np.arange(6).reshape((3, 2))
    md = CSVMetadata.create("spam", data=data, columns=["col1", "col2"])
    print(md)
    md.write(index=False)

    md = CSVMetadata.open("spam")
    print(md)
