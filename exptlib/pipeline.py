import time
from multiprocessing import Process, Queue, Lock
import typing
import inspect
from abc import ABC, abstractmethod
import shutil

from .experiment import Experiment


class Params:
    """Allows shared parameters to be set for all Pipeline instances."""

    n_cores = 4
    overwrite = False

    @classmethod
    def parallelize(cls):
        return not cls.n_cores == 1


def args_to_tuples(func):
    """Decorator for converting output of generators to pairs of tuples."""
    def wrapper(*args, **kwargs):
        args = eat_kwargs(func)(*args, **kwargs)
        formatted_args = []
        if args:
            for (arg1, arg2) in args:
                if not isinstance(arg1, tuple):
                    arg1 = (arg1,)
                if not isinstance(arg2, tuple):
                    arg2 = (arg2,)
                formatted_args.append((arg1, arg2))
        return formatted_args
    return wrapper


def eat_kwargs(func):
    """Decorator for eating unused keyword arguments passed to a function."""
    sig = inspect.signature(func)
    kw = []  # keep track of keywords in signature
    for param_name, param in sig.parameters.items():
        if param.kind == param.VAR_KEYWORD:
            return func  # return original function if any params are VAR_KEYWORD
        if param.kind == param.KEYWORD_ONLY:
            kw.append(param_name)

    # Eat all kwargs if functional cannot handle them
    if not len(kw):
        def eat_all_kwargs(*args, **kwargs):
            return func(*args)

        return eat_all_kwargs

    # Eat kwargs that functional cannot handle
    def eat_unused_kwargs(*args, **kwargs):
        keep_kw = {}
        for key in kw:
            if key in kwargs:
                keep_kw[key] = kwargs[key]
        return func(*args, **keep_kw)

    return eat_unused_kwargs


class Pipeline:
    """Pipeline class for running batch analyses.

    Attributes
    ----------
    generator
        Yields pairs of input args and output args. Inputs args are passed to worker.
    worker
        Takes first set of args from generator and does some work.
    handler
        Takes output of worker as first argument and second output of generator for additional arguments.
    kwargs
        Keyword arguments passed to generator, worker and handler functions.
    """

    params = Params

    def __init__(self,
                 generator: typing.Callable,
                 worker: typing.Callable,
                 handler: typing.Callable,
                 kwargs: typing.Mapping = None):
        self.generator = generator
        self.worker = worker
        self.handler = handler
        self.kwargs = kwargs or {}

    def __call__(self, *args, **kwargs):
        self.kwargs.update(kwargs)
        # Generate inputs
        io = args_to_tuples(self.generator)(*args, **self.kwargs)
        if not io:
            return
        # Add inputs to queue
        q = Queue()
        q_lock = Lock()
        for (inputs, outputs) in io:
            q.put((inputs, outputs))
        time.sleep(0.01)
        # Run in dummy process if not parallelized
        if not self.params.parallelize():
            self.process_from_queue(self.worker, self.handler, q, q_lock, **self.kwargs)
            return
        # Create worker pool
        workers = [Process(target=self.process_from_queue,
                           args=(self.worker, self.handler, q, q_lock),
                           kwargs=self.kwargs,
                           name=f'Worker-{i}')
                   for i in range(self.params.n_cores)]
        # Start all workers
        for p in workers:
            p.start()
        # Join all workers
        for p in workers:
            p.join()
        return

    @staticmethod
    def process_from_queue(worker: typing.Callable, handler: typing.Callable, queue, queue_lock, **kwargs):
        """Takes arguments from a queue, passes them to worker, and handles output."""
        while True:
            with queue_lock:
                if queue.empty():
                    return
                input_args, output_args = queue.get()
            result = eat_kwargs(worker)(*input_args, **kwargs)
            eat_kwargs(handler)(result, *output_args, **kwargs)


class ExperimentPipeline(Pipeline, ABC):
    """Base class for running pipelines from an Experiment object."""

    def __init__(self, **kwargs):
        super().__init__(self.generate_io, self.run, self.write, **kwargs)

    @abstractmethod
    def generate_io(self, experiment: Experiment, **kwargs):
        """Yields input, output pairs from the experiment.

        Parameters
        ----------
        experiment
            An Experiment object.
        """
        raise NotImplementedError

    @abstractmethod
    def run(self, *args, **kwargs):
        """Handle inputs from generate_io.

        Parameters
        ----------
        args:
            Passed from the first output of generate_io.
        """
        raise NotImplementedError

    @abstractmethod
    def write(self, result: typing.Any, output: typing.Any, *args, **kwargs):
        """Takes the output of run as the first arguments. Other args passed from output of generate_io.

        Parameters
        ----------
        result
            Passed from the output of run.
        output
            Passed from second output of generate_io.
        args
            Other arguments passed from second output of generate_io.
        """
        raise NotImplementedError

    def __call__(self, experiment: Experiment):
        super().__call__(experiment)


class TempMixin:

    @property
    def temp_directory(self):
        return self._temp_directory_will_be_deleted

    def __call__(self, experiment: Experiment):
        self._temp_directory_will_be_deleted = experiment.directory.joinpath("temp")
        self.temp_directory.mkdir(parents=True, exist_ok=False)
        super().__call__(experiment)
        shutil.rmtree(self.temp_directory)
