import time
import multiprocessing as mp

from pyrallel.parallel_processor import ParallelProcessor, Mapper


NUM_OF_PROCESSOR = max(2, int(mp.cpu_count() / 2))


def test_basic():
    def dummy_computation():
        time.sleep(0.0001)

    pp = ParallelProcessor(NUM_OF_PROCESSOR, dummy_computation)
    pp.start()

    for i in range(1000):
        pp.add_task()

    pp.task_done()
    pp.join()

    class MyMapper(Mapper):
        def enter(self):
            self.i = 0

        def process(self):
            dummy_computation()
            self.i += 1

    pp = ParallelProcessor(NUM_OF_PROCESSOR, MyMapper)
    pp.start()

    for i in range(1000):
        pp.add_task()

    pp.task_done()
    pp.join()


def test_with_input():
    def dummy_computation_with_input(x, _idx):
        time.sleep(0.0001)

    pp = ParallelProcessor(NUM_OF_PROCESSOR, dummy_computation_with_input, enable_process_id=True)
    pp.start()

    for i in range(1000):
        pp.add_task(i)

    pp.map(range(1000))

    pp.task_done()
    pp.join()

    class MyMapper(Mapper):
        def process(self, x):
            dummy_computation_with_input(x, _idx=self._idx)

    pp = ParallelProcessor(NUM_OF_PROCESSOR, MyMapper)
    pp.start()

    for i in range(1000):
        pp.add_task(i)

    pp.task_done()
    pp.join()


def test_with_multiple_input():
    def dummy_computation_with_input(x, y):
        assert x * 2 == y
        time.sleep(0.0001)

    pp = ParallelProcessor(NUM_OF_PROCESSOR, dummy_computation_with_input)
    pp.start()

    for i in range(1000):
        pp.add_task(i, y=i*2)

    pp.map([(i, i*2) for i in range(1000)])

    pp.task_done()
    pp.join()


def test_with_output():
    result = []

    def dummy_computation_with_input(x):
        time.sleep(0.0001)
        return x * x

    def collector(r):
        result.append(r)

    pp = ParallelProcessor(NUM_OF_PROCESSOR, dummy_computation_with_input, collector=collector)
    pp.start()

    for i in range(8):
        pp.add_task(i)

    pp.task_done()
    pp.join()

    for i in [0, 1, 4, 9, 16, 25, 36, 49]:
        assert i in result


def test_with_multiple_output():
    result = []

    def dummy_computation_with_input(x):
        time.sleep(0.0001)
        return x * x, x * x

    def collector(r1, r2):
        result.append(r1)

    pp = ParallelProcessor(NUM_OF_PROCESSOR, dummy_computation_with_input, collector=collector)
    pp.start()

    for i in range(8):
        pp.add_task(i)

    pp.task_done()
    pp.join()

    for i in [0, 1, 4, 9, 16, 25, 36, 49]:
        assert i in result
