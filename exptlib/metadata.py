from typing import Type, Any
from pathlib import Path
import warnings
import pandas as pd
import json
import yaml


class _IO:

    def __init__(self, dtype: Type[Any], extension: str, reader: callable, writer: callable = None):
        self.dtype = dtype
        self.extension = "." + extension.strip(".")
        self.reader = reader
        self.writer = writer

    @property
    def read_only(self):
        return self.writer is None

    def validate_type(self, data):
        if not isinstance(data, self.dtype):
            data = self.dtype(data)
        return data

    def read(self, path, **kwargs):
        data = self.reader(path, **kwargs)
        return self.validate_type(data)

    def write(self, data, path, **kwargs):
        if self.read_only:
            return False
        self.writer(data, path, **kwargs)
        return True

    def get_path(self, directory: Path, filename: str):
        return Path(directory.joinpath(filename)).with_suffix(self.extension)


class Metadata:

    dtype = object
    extension = ""
    reader = None
    writer = None

    def __init__(self,
                 filename: str,
                 set_read_only=False,
                 read_kw: dict = None,
                 write_kw: dict = None,
                 safe_overwrite=True):
        self.filename = filename
        self._io = _IO(self.dtype, self.extension, self.reader, self.writer)
        if set_read_only:
            self._io.writer = None
        self.read_kw = read_kw or {}
        self.write_kw = write_kw or {}
        self.safe_overwrite = safe_overwrite

    @property
    def read_only(self):
        return self._io.read_only

    def __set_name__(self, owner, name):
        self.name = name

    @property
    def private_name(self):
        return self.name + "_cached"

    def __get__(self, instance, owner):
        if instance:
            path = self._io.get_path(instance.directory, self.filename)
            if not path.exists():
                raise FileNotFoundError(f"Metadata path {path} does not exist.")
            data = self._io.read(path, **self.read_kw)
            setattr(instance, self.private_name, data)
            return data
        return self

    def __set__(self, instance, data):
        if self.read_only:
            warnings.warn(f"Cannot set read-only metadata: {self.name} in {instance}.")
            return
        path = self._io.get_path(instance.directory, self.filename)
        data = self._io.validate_type(data)
        setattr(instance, self.private_name, data)
        if self.safe_overwrite and path.exists():
            write = instance.yes_no_question(f"Attempting to overwrite metadata file: {self.filename}. Overwrite?")
        else:
            write = True
        if write:
            self._io.write(data, path, **self.write_kw)


class TXTMetadata(Metadata):

    @staticmethod
    def read_txt(path, **kwargs):
        with open(path, "r") as txt_file:
            lines = txt_file.readlines()
        return "".join(lines)

    @staticmethod
    def write_txt(data, path, **kwargs):
        with open(path, "w") as txt_file:
            txt_file.write(data)

    dtype = str
    extension = "txt"
    reader = read_txt
    writer = write_txt


class JSONMetadata(Metadata):
    """Metadata format that saves dict data as json."""

    @staticmethod
    def read_json(path, **kwargs):
        with open(path, "r") as json_file:
            return json.load(json_file, **kwargs)

    @staticmethod
    def write_json(data, path, **kwargs):
        with open(path, "w") as json_file:
            json.dump(data, json_file, **kwargs)

    dtype = dict
    extension = "json"
    reader = read_json
    writer = write_json


class CSVMetadata(Metadata):

    @staticmethod
    def read_csv(path, **kwargs):
        return pd.read_csv(path, **kwargs)

    @staticmethod
    def write_csv(data, path, **kwargs):
        data.to_csv(path, **kwargs)

    dtype = pd.DataFrame
    extension = "csv"
    reader = read_csv
    writer = write_csv


class YAMLMetadata(Metadata):
    """Metadata format for yaml files."""

    @staticmethod
    def read_yaml(path, **kwargs):
        """Returns a data structure that can be passed to metadata_type from the path."""
        stream = open(path, "r")
        return yaml.load(stream, yaml.Loader)

    @staticmethod
    def write_yaml(data, path, **kwargs):
        """Writes an instance returned by metadata_type to the path."""
        stream = open(path, "w")
        yaml.dump(data, stream, **kwargs)

    dtype = dict
    extension = "yaml"
    reader = read_yaml
    writer = write_yaml
