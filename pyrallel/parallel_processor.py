"""
ParallelProcessor utilizes multiple CPU cores to process compute-intensive tasks.


If you have a some time-consuming statements in a for-loop and no state is shared among loops, you can map these
statements to different processes. Assume you need to process couple of files, you can do this in parallel::

    def mapper(filename):
        with open(filename) as f_in, open(filename + '.out') as f_out:
            f_out.write(process_a_file(f_in.read()))

    pp = ParallelProcessor(2, mapper)
    pp.start()

    for fname in ['file1', 'file2', 'file3', 'file4']:
        pp.add_task(fname)

    pp.task_done()
    pp.join()

It's not required to have a loop statement if you have iterable object or type (list, generator, etc),
use a shortcut instead::

    pp = ParallelProcessor(2, mapper)
    pp.start()

    pp.map(['file1', 'file2', 'file3', 'file4'])

    pp.task_done()
    pp.join()

Usually, some files are small and some are big, it would be better if it can keep all cores busy.
One way is to send line by line to each process (assume content is line-separated)::

    def mapper(line, _idx):
        with open('processed_{}.out'.format(_idx), 'a') as f_out:
            f_out.write(process_a_line(line))

    pp = ParallelProcessor(2, mapper, enable_process_id=True)
    pp.start()

    for fname in ['file1', 'file2', 'file3', 'file4']:
        with open(fname) as f_in:
            for line in f_in:
                pp.add_task(line)

    pp.task_done()
    pp.join()

One problem here is you need to acquire file descriptor every time the mapper is called.
To avoid this, use Mapper class to replace mapper function.
It allows user to define how the process is constructed and deconstructed::

    class MyMapper(Mapper):
        def enter(self):
            self.f = open('processed_{}.out'.format(self._idx), 'w')

        def exit(self, *args, **kwargs):
            self.f.close()

        def process(self, line):
            self.f.write(process_a_line(line))

In some situations, you may need to use `collector` to collect data back from child processes to main process::

    processed = []

    def mapper(line):
        return process_a_line(line)

    def collector(data):
        processed.append(data)

    pp = ParallelProcessor(2, mapper, collector=collector)
    pp.start()

    for fname in ['file1', 'file2', 'file3', 'file4']:
        with open(fname) as f_in:
            for line in f_in:
                pp.add_task(line)

    pp.task_done()
    pp.join()

    print(processed)
"""

import multiprocessing as mp
import threading
import queue
import inspect
from typing import Callable, Iterable

from pyrallel import Paralleller


class Mapper(object):
    """
    Mapper class.

    This defines how mapper works.

    The methods will be called in following order::

        enter (one time) -> process (many times) -> exit (one time)
    """
    def __init__(self, idx):
        self._idx = idx

    def __enter__(self):
        self.enter()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.exit(exc_type, exc_val, exc_tb)

    def enter(self):
        """
        Invoked when subprocess is created and listening the queue.
        """
        pass

    def exit(self, *args, **kwargs):
        """
        Invoked when subprocess is going to exit. Arguments will be set if exception occurred.
        """
        pass

    def process(self, *args, **kwargs):
        """
        Same as mapper function, but `self` argument can provide additional context (e.g., `self._idx`).
        """
        raise NotImplementedError


class CollectorThread(threading.Thread):
    """
    Handle collector in main process.
    Create a thread and call ParallelProcessor.collect().
    """

    def __init__(self, instance, collector):
        super(CollectorThread, self).__init__()
        self.collector = collector
        self.instance = instance

    def run(self):
        for batched_collector in self.instance.collect():
            for o in batched_collector:
                self.collector(*o)


class ParallelProcessor(Paralleller):
    """
    Args:
        num_of_processor (int): Number of processes to use.
        mapper (Callable / Mapper): Function or subclass of `Mapper` class.
        max_size_per_mapper_queue (int, optional): Maximum size of mapper queue for one process.
                                    If it's full, the corresponding process will be blocked.
                                    0 by default means unlimited.
        collector (Callable, optional): If the collector data needs to be get in main process (another thread),
                                set this handler, the arguments are same to the return from mapper.
                                The return result is one by one, order is arbitrary.
        max_size_per_collector_queue (int, optional): Maximum size of collector queue for one process.
                                    If it's full, the corresponding process will be blocked.
                                    0 by default means unlimited.
        enable_process_id (bool, optional): If it's true, an additional argument `_idx` (process id) will be
                                passed to `mapper` function. This has no effect for `Mapper` class.
                                It defaults to False.
        batch_size (int, optional): Batch size, defaults to 1.


    Note:
        - Do NOT implement heavy compute-intensive operations in collector, they should be in mapper.
        - Tune the value for queue size and batch size will optimize performance a lot.
        - `collector` only collects returns from `mapper` or `Mapper.process`.
    """

    # Command format in queue. Represent in tuple.
    # The first element of tuple will be command, the rests are arguments or data.
    # (CMD_XXX, args...)
    CMD_DATA = 0
    CMD_STOP = 1

    def __init__(self, num_of_processor: int, mapper: Callable, max_size_per_mapper_queue: int = 0,
                 collector: Callable = None, max_size_per_collector_queue: int = 0,
                 enable_process_id: bool = False, batch_size: int = 1):
        self.num_of_processor = num_of_processor
        self.mapper_queues = [mp.Queue(maxsize=max_size_per_mapper_queue) for _ in range(num_of_processor)]
        self.collector_queues = [mp.Queue(maxsize=max_size_per_collector_queue) for _ in range(num_of_processor)]
        self.processes = [mp.Process(target=self._run, args=(i, self.mapper_queues[i], self.collector_queues[i]))
                          for i in range(num_of_processor)]

        ctx = self
        if not inspect.isclass(mapper) or not issubclass(mapper, Mapper):
            class DefaultMapper(Mapper):
                def process(self, *args, **kwargs):
                    if ctx.enable_process_id:
                        kwargs['_idx'] = self._idx
                    return mapper(*args, **kwargs)
            self.mapper = DefaultMapper
        else:
            self.mapper = mapper

        self.collector = collector
        self.mapper_queue_index = 0
        self.collector_queue_index = 0
        self.enable_process_id = enable_process_id
        self.batch_size = batch_size
        self.batch_data = []

        # collector can be handled in each process or in main process after merging (collector needs to be set)
        # if collector is set, it needs to be handled in main process;
        # otherwise, it assumes there's no collector.
        if collector:
            self.collector_thread = CollectorThread(self, collector)

    def start(self):
        """
        Start processes and threads.
        """
        if self.collector:
            self.collector_thread.start()
        for p in self.processes:
            p.start()

    def join(self):
        """
        Block until processes and threads return.
        """
        for p in self.processes:
            p.join()
        if self.collector:
            self.collector_thread.join()

    def task_done(self):
        """
        Indicate that all resources which need to add_task are added to processes.
        (main process, blocked)
        """
        if len(self.batch_data) > 0:
            self._add_task(self.batch_data)
            self.batch_data = []

        for q in self.mapper_queues:
            q.put((ParallelProcessor.CMD_STOP,))

    def add_task(self, *args, **kwargs):
        """
        Add data to one of the mapper queues.
        (main process, unblocked, using round robin to find next available queue)
        """
        self.batch_data.append((args, kwargs))

        if len(self.batch_data) == self.batch_size:
            self._add_task(self.batch_data)
            self.batch_data = []  # reset buffer

    def _add_task(self, batched_args):
        while True:
            q = self.mapper_queues[self.mapper_queue_index]
            self.mapper_queue_index = (self.mapper_queue_index + 1) % self.num_of_processor
            try:
                q.put_nowait((ParallelProcessor.CMD_DATA, batched_args))
                return  # put in
            except queue.Full:
                continue  # find next available

    def _run(self, idx: int, mapper_queue: mp.Queue, collector_queue: mp.Queue):
        """
        Process’s activity. It handles queue IO and invokes user's mapper handler.
        (subprocess, blocked, only two queues can be used to communicate with main process)
        """
        with self.mapper(idx) as mapper:
            while True:
                data = mapper_queue.get()
                if data[0] == ParallelProcessor.CMD_STOP:
                    # print(idx, 'stop')
                    if self.collector:
                        collector_queue.put((ParallelProcessor.CMD_STOP,))
                    return
                elif data[0] == ParallelProcessor.CMD_DATA:
                    batch_result = []
                    for d in data[1]:
                        args, kwargs = d[0], d[1]
                        # print(idx, 'data')
                        result = mapper.process(*args, **kwargs)
                        if self.collector:
                            if not isinstance(result, tuple):  # collector must represent as tuple
                                result = (result,)
                            batch_result.append(result)
                    if len(batch_result) > 0:
                        collector_queue.put((ParallelProcessor.CMD_DATA, batch_result))
                        batch_result = []  # reset buffer

    def collect(self):
        """
        Get data from collector queue sequentially.
        (main process, unblocked, using round robin to find next available queue)
        """
        if not self.collector:
            return
        while True:
            # print(self.collector_queues)
            q = self.collector_queues[self.collector_queue_index]
            try:
                data = q.get_nowait()  # get out
                if data[0] == ParallelProcessor.CMD_STOP:
                    del self.collector_queues[self.collector_queue_index]  # remove queue if it's finished
                elif data[0] == ParallelProcessor.CMD_DATA:
                    yield data[1]
            except queue.Empty:
                continue  # find next available
            finally:
                if len(self.collector_queues) == 0:  # all finished
                    return
                self.collector_queue_index = (self.collector_queue_index + 1) % len(self.collector_queues)
