from multiprocessing import Process, Queue, Lock
import typing

from .experiment import Experiment


class Params:

    n_cores = 4
    overwrite = False

    @classmethod
    def parallelize(cls):
        return not cls.n_cores == 1


class Pipeline:

    params = Params

    def __init__(self, generator, worker, handler, kwargs=None):
        self.generator = generator
        self.worker = worker
        self.handler = handler
        self.kwargs = kwargs or {}

    def __call__(self, *args, **kwargs):
        self.kwargs.update(kwargs)
        # Generate inputs
        io = self.generator(*args, **self.kwargs)
        if not io:
            return
        # Add inputs to queue
        q = Queue()
        q_lock = Lock()
        for (args, output) in io:
            q.put((args, output))
        # Run in dummy process if not parallelized
        if not self.params.parallelize():
            self.process_from_queue(self.worker, self.handler, q, q_lock, **self.kwargs)
            return
        # Create worker pool
        workers = []
        for i in range(self.params.n_cores):
            p = Process(target=self.process_from_queue,
                        args=(self.worker, self.handler, q, q_lock),
                        kwargs=self.kwargs,
                        name=f'Worker-{i}')
            workers.append(p)
            p.start()
        # Join all workers
        for wp in workers:
            wp.join()
        return

    @staticmethod
    def process_from_queue(worker: typing.Callable,
                           handler: typing.Callable[[typing.Any, typing.Any, typing.Optional[typing.Mapping]], ...],
                           queue, queue_lock, **kwargs):
        while True:
            with queue_lock:
                if queue.empty():
                    return
                args, output = queue.get()
            result = worker(*args, **kwargs)
            handler(result, output, **kwargs)


class ExperimentPipeline(Pipeline):

    def __init__(self, **kwargs):
        super().__init__(self.generate_io, self.run, self.write, **kwargs)

    def generate_io(self,
                    experiment: Experiment,
                    **kwargs) -> typing.Sequence[typing.Tuple[typing.Iterable, typing.Any]]:
        raise NotImplementedError

    def run(self, *args, **kwargs) -> typing.Any:
        raise NotImplementedError

    def write(self, result, output, **kwargs):
        raise NotImplementedError

    def __call__(self, experiment: Experiment):
        super().__call__(experiment)
