from __future__ import annotations
import typing
import warnings
from abc import abstractmethod, ABCMeta
from pathlib import Path
import json
import pandas as pd
import yaml

from .attributes import SetOnce


__all__ = ["MetadataAttribute", "Metadata",
           "JSONMetadata", "CSVMetadata", "MultiCSVMetadata", "YamlMetadata"]


class MetadataAttribute:
    """Metadata descriptor for Experiment classes.

    Parameters
    ----------
    metadata : Metadata type
        A subclass of Metadata.
    filename : str
        The name of the file where metadata should be saved in the experiment directory.
    read_kw : dict
        Keyword arguments for reading metadata file.
    write_kw : dict
        Keyword arguments for writing metadata file.
    safe_overwrite : bool, default = True
        If True (default), always prompt the user before overwriting metadata.
    read_only : bool, default = False
        If True, makes metadata a read-only attribute.
    """

    def __init__(self,
                 metadata: typing.Type[Metadata], filename: str, read_kw: dict = None, write_kw: dict = None,
                 safe_overwrite=True, read_only=False):
        self.metadata = metadata
        self.filename = filename
        self.read_kw = read_kw or {}
        self.write_kw = write_kw or {}
        self.safe_overwrite = safe_overwrite
        self.read_only = read_only

    def __set_name__(self, owner, name):
        self.name = name

    def __set__(self, instance, data):
        if self.read_only:
            warnings.warn(f"Cannot set read-only metadata: {self.name} in {instance}.")
            return
        path = self.metadata_path(instance)
        metadata = self.metadata.create(path, data)
        setattr(instance, self.private_name, metadata)
        if self.safe_overwrite and path.exists():
            write = instance.yes_no_question(f"Attempting to overwrite metadata file: {self.filename}. Overwrite?")
        else:
            write = True
        if write:
            metadata.write(**self.write_kw)

    def __get__(self, instance, owner):
        if instance:
            path = self.metadata_path(instance)
            metadata = self.metadata.open(path)
            setattr(instance, self.private_name, metadata)
            return metadata
        return self

    @property
    def private_name(self):
        return self.name + "_cached"

    def metadata_path(self, instance):
        return instance.directory.new_file(self.filename)


class Metadata(metaclass=ABCMeta):
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
            path = ".".join([str(path), self.metadata_extension])
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

        Raises
        ------
        FileNotFoundError
            If the metadata path does not exist.
        """
        metadata = cls.create(path)
        if not metadata.path.exists():
            raise FileNotFoundError(f"Metadata path {path} does not exist.")
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


class MultiCSVMetadata(Metadata):

    metadata_type = pd.DataFrame

    def read(self, **kwargs):
        dfs = []
        for f in self.path.glob("*.csv"):
            df = pd.read_csv(f, **kwargs)
            dfs.append(df)
        return pd.concat(dfs, axis=0, ignore_index=True)

    def write(self, **kwargs):
        raise NotImplementedError


class YamlMetadata(Metadata):
    """Metadata format for yaml files."""

    metadata_type = dict
    metadata_extension = "yaml"

    def read(self, **kwargs):
        """Returns a data structure that can be passed to metadata_type from the path."""
        stream = open(self.path, "r")
        return yaml.load(stream, yaml.Loader)

    def write(self, **kwargs):
        """Writes an instance returned by metadata_type to the path."""
        stream = open(self.path, "w")
        yaml.dump(self, stream)
