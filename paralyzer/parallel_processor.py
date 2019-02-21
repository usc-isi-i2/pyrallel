"""
This module is designed for breaking the restriction of Python Global Interpreter Lock (GIL): It uses multi-processing (add_task-intensive operations) and multi-threading (return data collecting) to accelerate computing.
Once it's initialized, it creates a sub process pool, all the added data will be dispatched to different sub processes for parallel computing. The result sends back and consumes in another thread in current main process. The Inter Process Communication (IPC) between main process and sub processes is based on queue.

Example::

    result = []

    def dummy_computation_with_mapper(x):
        time.sleep(0.0001)
        return x * x, x + 5

    def collector(r1, r2):
        result.append(r1 if r1 > r2 else r2)

    pp = ParallelProcessor(dummy_computation_with_mapper, 8, collector=collector)
    pp.start()

    for i in range(8):
        pp.add_task(i)

    pp.task_done()
    pp.join()

    print(result)
"""

import multiprocessing as mp
import threading
import queue
from typing import Callable, Iterable


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


class ParallelProcessor(object):
    """
    Args:
        num_of_processor (int): Number of processes to use.
        mapper (Callable): Computational function.
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
                                passed to `mapper`. It defaults to False.
        batch_size (int, optional): Batch size, defaults to 1.


    Note:
        Do NOT implement heavy add_task-intensive operations in collector, they should be in mapper.
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
        self.processes = [mp.Process(target=self.run, args=(i, self.mapper_queues[i], self.collector_queues[i]))
                          for i in range(num_of_processor)]
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

    def map(self, tasks: Iterable):
        """
        Syntactic sugar for adding task from an iterable object.

        Args:
            tasks (iter): Any iterable object.
        """
        for t in tasks:
            self.add_task(t)

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

    def run(self, idx: int, mapper_queue: mp.Queue, collector_queue: mp.Queue):
        """
        Process’s activity. It handles queue IO and invokes user's mapper handler.
        (subprocess, blocked, only two queues can be used to communicate with main process)
        """
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
                    if self.enable_process_id:
                        kwargs['_idx'] = idx
                    result = self.mapper(*args, **kwargs)
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
