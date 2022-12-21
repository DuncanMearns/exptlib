from .metadata import MappedMetadata
from pathlib import Path


class Experiment:
    """Base Experiment class.

    Attributes
    ----------
    directory : str or Path
        Main experiment directory containing metadata.
    data_directory : str or Path
        Directory containing raw data. Can be path-like (str or Path object), or folder name (str) within the main
        experiment directory.
    """

    def __init__(self, directory, data_directory):
        # Main experiment directory
        self.directory = Path(directory)
        try:
            assert self.directory.exists()
        except AssertionError:
            self._invalid_directory(f"Experiment directory {directory}")
        # Data directory (may or may not be within main experiment directory)
        if isinstance(data_directory, str):
            try:
                self.data_directory = Path(data_directory)
                assert self.data_directory.exists()
            except AssertionError:
                self.data_directory = self.directory.joinpath(data_directory)
                if not self.data_directory.exists():
                    self._invalid_directory(f"Data directory {data_directory})")
        elif isinstance(data_directory, Path):
            self.data_directory = data_directory
            if not self.data_directory.exists():
                self._invalid_directory(f"Data directory {data_directory}")
        else:
            raise TypeError("`data_directory` must be str or Path")
        # Create metadata file
        md_path = self.directory.joinpath("metadata.json")
        self.metadata = MappedMetadata(md_path)

    @property
    def subdirs(self):
        return dict([(path.stem, path) for path in self.directory.glob('*') if path.is_dir()])

    @property
    def files(self):
        return [path for path in self.directory.glob('*') if path.is_file()]

    @classmethod
    def create(cls):
        return

    @classmethod
    def open(cls):
        return

    def new_subdir(self, *folders) -> Path:
        new_dir = self.directory.joinpath(*folders)
        new_dir.mkdir(parents=True, exist_ok=True)
        return new_dir

    def new_file(self, name, ext=None) -> Path:
        if ext:
            name = ".".join([name, ext])
        return self.directory.joinpath(name)

    @staticmethod
    def _invalid_directory(directory):
        raise ValueError(f"{directory} does not exist!")

    @staticmethod
    def yes_no_question(q,
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
