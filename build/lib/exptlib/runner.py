from .multiprocessing import MultiProcessing


class WorkerRunner(MultiProcessing):

    def __init__(self, worker, n_processes, **kwargs):
        super().__init__(n_processes, **kwargs)
        self.worker = worker

    def run(self):
        pass
