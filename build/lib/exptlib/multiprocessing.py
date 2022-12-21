from multiprocessing import Process, Queue, Lock
import threading


class WorkerProcess(Process):
    """Worker process that takes arguments from a queue and passes them to a target function.

    Parameters
    ----------
    func : callable
        Target function.
    queue : Queue
        Contains queued arguments.
    queue_lock : Lock
        Lock for the queue.
    kw : dict
        Keyword arguments passed to the target function.
    *args, **kwargs :
        Arguments and keyword arguments passed to __init__ of Process.
    """

    def __init__(self, func, queue, queue_lock, kw, *args, **kwargs):
        super().__init__(target=self.process_from_queue, *args, **kwargs)
        self.func = func
        self.queue = queue
        self.queue_lock = queue_lock
        self.kwargs = kw

    def process_from_queue(self):
        """Target of super(). Handles queued arguments and passes them to target function."""
        while True:
            with self.queue_lock:
                if not self.queue.empty():
                    args = self.queue.get()
                else:
                    break
            self.func(*args, **self.kwargs)


class MultiProcessing:
    """Multiprocessing class that implements parallelization of a run method.

    Parameters
    ----------
    n_processes : int
        Number of processes to run in parallel.
    """

    def __init__(self, n_processes, verbose=False):
        self.n_processes = n_processes
        self.verbose = verbose

    def run(self, *args, **kwargs):
        """The run method to be parallelized. Overridden in subclasses."""
        return

    def _run(self, *args, **kwargs):
        """Private method that calls run. This allows the run method to be decorated in subclasses."""
        self.run(*args, **kwargs)

    @staticmethod
    def message_thread(q):
        """Handles printing to the console. Runs in a separate thread in the main process."""
        while True:
            message = q.get()
            if message is None:
                break
            print(message)

    def run_parallel(self, *args, **kwargs):
        """Implements parallelization of the run method using a pool of processes and passes arguments via a queue. Not
        to be overridden. Each argument in args should be tuple as they are passed to run with a star. Kwargs are passed
        directly to the run method.

        Parameters
        ----------
        *args :
            An iterable of tuple objects.
        **kwargs :
            Keyword arguments passed directly to run each call.
        verbose : bool (default=False)
            Whether messages should be printed to the console.
        """

        # Create queue for arguments
        q = Queue()
        q_lock = Lock()

        # Start messaging thread (if applicable)
        if self.verbose:
            messages = Queue()
            messaging_thread = threading.Thread(target=self.message_thread, args=(messages,))
            kwargs['messages'] = messages
            messaging_thread.start()

        # Populate queue with arguments
        with q_lock:
            for arg in args:
                q.put(arg)

        # Start pool of worker processes
        workers = []
        for i in range(self.n_processes):
            # Worker processes take the target method _run which in turn calls run
            wp = WorkerProcess(self._run, q, q_lock, kwargs, name=f'Worker-{i}')
            workers.append(wp)
            wp.start()

        # Join all workers
        for wp in workers:
            wp.join()

        # Join messaging thread (if applicable)
        if self.verbose:
            messages.put(None)
            messaging_thread.join()
