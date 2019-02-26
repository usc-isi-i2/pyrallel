import multiprocessing as mp

from pyrallel.map_reduce import MapReduce


NUM_OF_PROCESSOR = max(2, int(mp.cpu_count() / 2))


def test_map_reduce_number():

    def mapper(x):
        return x

    def reducer(r1, r2):
        return r1 + r2

    mr = MapReduce(3, mapper, reducer)
    mr.start()
    mr.add_task(1)
    mr.task_done()
    assert mr.join() == 1

    mr = MapReduce(NUM_OF_PROCESSOR, mapper, reducer)
    mr.start()
    mr.add_task(1)
    mr.task_done()
    assert mr.join() == 1

    mr = MapReduce(1, mapper, reducer)
    mr.start()
    for i in range(1, 101):
        mr.add_task(i)
    mr.task_done()
    assert mr.join() == 5050

    mr = MapReduce(NUM_OF_PROCESSOR, mapper, reducer)
    mr.start()
    for i in range(1, 101):
        mr.add_task(i)
    mr.task_done()
    assert mr.join() == 5050

    mr = MapReduce(NUM_OF_PROCESSOR, mapper, reducer)
    mr.start()
    for i in range(1, 100001):
        mr.add_task(i)
    mr.task_done()
    assert mr.join() == 5000050000


def test_map_reduce_object():

    def mapper(k, v):
        return {k: v}

    def reducer(r1, r2):
        for k1, v1 in r1.items():
            if k1 in r2:
                r2[k1] += v1
            else:
                r2[k1] = v1
        return r2

    mr = MapReduce(1, mapper, reducer)
    mr.start()
    for i in range(100):
        if i % 2 == 0:
            mr.add_task('a', i)
        else:
            mr.add_task('b', i)
    mr.task_done()
    assert mr.join() == {'a': 2450, 'b': 2500}

    mr = MapReduce(NUM_OF_PROCESSOR, mapper, reducer)
    mr.start()
    for i in range(100):
        if i % 2 == 0:
            mr.add_task('a', i)
        else:
            mr.add_task('b', i)
    mr.task_done()
    assert mr.join() == {'a': 2450, 'b': 2500}
