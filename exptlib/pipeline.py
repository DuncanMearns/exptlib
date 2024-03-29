import warnings
from multiprocessing import Pool
import typing
import inspect
from pathlib import Path
import pandas as pd

from .experiment import Experiment


__all__ = ["Pipeline", "run_pipeline", "pipeline_params", "create_analysis_pipeline", "PathGenerator"]


class Params:
    """Allows shared parameters to be set for all Pipeline instances."""

    n_cores = 4
    overwrite = False

    @classmethod
    def parallelize(cls):
        return not cls.n_cores == 1


pipeline_params = Params


def eat_kwargs(func):
    """Decorator for eating unused keyword arguments passed to a function."""
    sig = inspect.signature(func)
    kw = []  # keep track of keywords in signature
    for param_name, param in sig.parameters.items():
        if param.kind == param.VAR_KEYWORD:
            return func  # return original function if any params are VAR_KEYWORD
        kw.append(param_name)

    # Eat kwargs that functional cannot handle
    def eat_unused_kwargs(*args, **kwargs):
        keep_kw = {}
        for key in kw:
            if key in kwargs:
                keep_kw[key] = kwargs[key]
        return func(*args, **keep_kw)

    return eat_unused_kwargs


def tuple_output(func):
    """Decorator for converting output of generators to pairs of tuples."""
    def convert_to_tuple(*args, **kwargs):
        output = eat_kwargs(func)(*args, **kwargs)
        if not isinstance(output, tuple):
            output = (output,)
        return output
    return convert_to_tuple


def worker_process(workers: typing.Iterable[typing.Callable], args: typing.Tuple, kwargs: typing.Mapping):
    """Implements pipeline of worker functions in a separate process."""
    if not isinstance(args, tuple):
        args = (args,)
    for worker in workers:
        args = tuple_output(worker)(*args, **kwargs)
    return args


def run_pipeline(n: int,
                 args: typing.Union[typing.Iterable, typing.Generator],
                 workers: typing.Iterable[typing.Callable] = (),
                 callback: typing.Callable[[typing.Any], typing.Any] = None,
                 kwargs: typing.Mapping = None):
    """Run an asynchronous pipeline of workers across n cores, iterating over args."""
    kwargs = kwargs or {}
    if n == 1:
        results = []
        for arg in args:
            result = worker_process(workers, arg, kwargs)
            results.append(result)
        if callback:
            callback(results)
        return
    # Start worker pool
    with Pool(processes=n) as pool:
        result = pool.starmap_async(worker_process, ((workers, arg, kwargs) for arg in args), callback=callback)
        result.get()


class Pipeline:
    """Class for creating analysis pipelines.

    Attributes
    ----------
    workers : callable
        Functions that do work in a separate process. The output of each feeds into the next. The first is called with
        the args passed to the Pipeline object when called.
    generator : callable (optional)
        Takes the input to the Pipeline object and yields arguments that are additionally passed to workers.
    callback : callable (optional)
        Processes aggregated outputs from final pipeline workers back in main process.
    kwargs
        Keyword arguments passed to pipeline functions.
    """

    params = Params

    def __init__(self, *workers, generator=None, callback=None, kwargs=None):
        self.workers = workers
        self.generator = generator
        self.callback = callback
        self.kwargs = kwargs or {}

    def __call__(self, *args, **kwargs):
        self.kwargs.update(kwargs)
        if self.generator:
            args = eat_kwargs(self.generator)(*args, **self.kwargs)
        self.run(self.params.n_cores, args, self.workers, self.callback, self.kwargs)

    run = staticmethod(run_pipeline)


class IOMapper:
    """Generates appropriate arguments to be passed to the analysis_pipeline function."""

    def __init__(self, generator, reader, worker, writer):
        self.generator = generator
        self.reader = reader or IOMapper.dummy
        self.worker = worker or IOMapper.dummy
        self.writer = writer or IOMapper.dummy

    @staticmethod
    def dummy(*args, **kwargs):
        return args

    def __call__(self, *args, **kwargs):
        for input_path, output_path in eat_kwargs(self.generator)(*args, **kwargs):
            yield input_path, self.reader, self.worker, self.writer, output_path


class PathGenerator:

    def __init__(self,
                 experiment: Experiment,
                 trial_md: str = "metadata",
                 input_dir: str = "",
                 input_ext: str = "",
                 output_dir: str = "",
                 output_ext: str = "",
                 overwrite: bool = False,
                 **kwargs):
        self.experiment = experiment
        self.trial_md_attr = trial_md
        self.input_dir = input_dir
        self.input_ext = input_ext
        self.output_dir = output_dir
        self.output_ext = output_ext
        self.overwrite = overwrite

    @property
    def trial_metadata(self) -> pd.DataFrame:
        return getattr(self.experiment, self.trial_md_attr)

    def path(self, directory, name, extension):
        if extension:
            name = ".".join([name, extension])
        if isinstance(directory, str):
            directory = self.experiment.directory.new_subdir(directory)
        else:
            directory = self.experiment.directory.new_subdir(*directory)
        return directory.joinpath(name)

    def input(self, **kwargs) -> typing.Union[Path, tuple]:
        return self.path(self.input_dir, kwargs["trial"], self.input_ext)

    def output(self, **kwargs) -> typing.Union[Path, tuple]:
        return self.path(self.output_dir, kwargs["trial"], self.output_ext)

    def validate_paths(self, input_path, output_path):
        # Account for multiple paths
        if isinstance(input_path, (Path, str)):
            input_path = (input_path,)
        if isinstance(output_path, (Path, str)):
            output_path = (output_path,)
        # Validate input
        for path in input_path:
            if not path.exists():
                warnings.warn(f"Input path: {path} does not exist.")
                return False
        # Check if output exists
        for path in output_path:
            path.parent.mkdir(exist_ok=True, parents=True)
            if not path.exists():
                return True
        # Finally check overwrite
        return self.overwrite

    def __call__(self, **kwargs):
        for idx, trial_info in self.trial_metadata.iterrows():
            input_path = self.input(**trial_info)
            output_path = self.output(**trial_info)
            if self.validate_paths(input_path, output_path):
                yield input_path, output_path

    @classmethod
    def from_experiment(cls, experiment, **kwargs):
        generator = cls(experiment, **kwargs)
        for input_path, output_path in generator(**kwargs):
            yield input_path, output_path


def analysis_pipeline(input_path: typing.Union[Path, str],
                      reader: typing.Callable,
                      worker: typing.Callable,
                      writer: typing.Callable,
                      output_path: typing.Union[Path, str],
                      **kwargs: typing.Mapping):
    """Implements a basic analysis pipeline."""
    kwargs = kwargs or {}
    args = tuple_output(reader)(input_path, **kwargs)
    result = tuple_output(worker)(*args, **kwargs)
    return eat_kwargs(writer)(output_path, *result, **kwargs)


def create_analysis_pipeline(*,
                             generator: typing.Union[typing.Generator, typing.Callable],
                             reader: typing.Callable = None,
                             worker: typing.Callable = None,
                             writer: typing.Callable = None,
                             callback: typing.Callable = None,
                             **kwargs: typing.Mapping):
    """Creates a parallelized analysis pipeline with a generator, reader, worker and writer function.

    Parameters
    ----------
    generator
        Generates (input_path, output_path) pairs from pipeline input.
    reader
        Receives an input path from the generator as an argument.
    worker
        Receives the output from the reader as an argument (unpacks wth * if reader returns multiple values).
    writer
        Receives the output path from the generator as the first argument. Subsequent arguments are passed from the
        worker (unpacks wth * if worker returns multiple values).
    callback
        Callable that receives the asynchronous result from the pipeline as its only argument.
    """
    generator = IOMapper(generator, reader, worker, writer)
    return Pipeline(analysis_pipeline, generator=generator, callback=callback, kwargs=kwargs)


class MultiWorker:

    def __init__(self, *workers):
        self.workers = workers

    def __call__(self, *args, **kwargs):
        results = []
        for worker in self.workers:
            results.append(eat_kwargs(worker)(*args, **kwargs))
        return tuple(results)
