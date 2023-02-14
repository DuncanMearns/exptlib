import sys
from pathlib import Path
import typing

from .directory import Directory


class Experiment:
    """Base Experiment class.

    Attributes
    ----------
    directory : str or Path
        Experiment directory.
    data_directory : str or Path
        Directory containing raw data.
    """

    def __init__(self, directory: Path, data_directory: Path = None, *args, **kwargs):
        self.directory = Directory(directory)
        self.data_directory = data_directory

    @property
    def data_directory(self):
        return self._data_directory

    @data_directory.setter
    def data_directory(self, path):
        self._data_directory = Directory(path) if path else path

    @classmethod
    def open(cls,
             directory: typing.Union[str, Path],
             data_directory: typing.Union[str, Path] = None,
             mkdir: bool = False, *args, **kwargs):
        """
        Open an experiment. Prompts user to create a new experiment if one does not already exist in the directory
        provided.

        Parameters
        ----------
        directory : str or Path
            Experiment directory.
        data_directory : str or Path
            Directory containing raw data. Can be path-like (str or Path object), or folder name (str) within the main
            experiment directory.
        mkdir : bool
            Automatically create experiment directory if it does not exist already.
        """
        # Set the experiment directory
        directory = Path(directory)
        if not directory.exists():
            if mkdir:
                ret = True
            else:
                ret = cls.yes_no_question(f"Directory {directory} does not exist. Create a new experiment in this directory?")
            if not ret:
                print("Exiting.")
                sys.exit()
            directory.mkdir(parents=True, exist_ok=True)
        # Set the data directory
        if data_directory:
            data_directory = Path(data_directory)
            if not data_directory.exists():  # check if data directory is subdirectory of experiment directory
                try:
                    data_directory = directory.joinpath(data_directory)
                    assert data_directory.exists()
                except AssertionError:
                    raise ValueError(f"Data directory {data_directory} does not exist!")
        return cls(directory, data_directory, *args, **kwargs)

    @staticmethod
    def yes_no_question(q: str,
                        affirmative_answers=('y', 'yes', '1', 't', 'true'),
                        negative_answers=('n', 'no', '0', 'f', 'false')):
        """Asks the user a yes/no question in the command line.

        Parameters
        ----------
        q : str
            A question that the user can answer in the command line
        affirmative_answers : tuple
            Valid affirmative answers (case insensitive). Default: ('y', 'yes', '1', 't', 'true')
        negative_answers : tuple
            Valid negative answers (case insensitive). Default: ('n', 'no', '0', 'f', 'false')

        Returns
        -------
        bool : True for affirmative answers, False for negative answers

        Raises
        ------
        ValueError
            If unrecognised answer given
        """
        answer = input(q + ' [y/n] ')
        if answer.lower() in affirmative_answers:
            return True
        elif answer.lower() in negative_answers:
            return False
        else:
            raise ValueError(f'Invalid answer! Recognised responses: {affirmative_answers + negative_answers}')
