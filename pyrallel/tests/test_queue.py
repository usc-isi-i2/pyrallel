import multiprocessing as mp
import queue
import pyrallel


def f(iq, oq):
    try:
        while True:
            oq.put(iq.get(timeout=2))
    except queue.Empty:
        return


def test_shmqueue():
    if not hasattr(pyrallel, 'ShmQueue'):
        return

    for mode in ['fork', 'spawn']:
        mp.set_start_method(mode, force=True)
        ShmQueueCls = getattr(pyrallel, 'ShmQueue')
        sq = ShmQueueCls(chunk_size=1024 * 4, maxsize=5)
        q = mp.Queue()
        p1 = mp.Process(target=f, args=(sq, q,))
        p2 = mp.Process(target=f, args=(sq, q,))
        p1.start()
        p2.start()

        items = list(range(10))

        for i in items:
            sq.put(i)

        while True:
            try:
                e = q.get(timeout=2)
                assert e in items
            except queue.Empty:
                break

        p1.join()
        p2.join()
        sq.close()
        q.close()
