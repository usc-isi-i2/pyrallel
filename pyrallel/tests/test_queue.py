import multiprocessing as mp
import queue
import pyrallel
import os


# 30 bytes each
# CONTENT = os.urandom(30)
CONTENT = b'\xaa' * 10 + b'\xbb' * 10 + b'\xcc' * 10


def sender(sq):
    for _ in range(10):
        sq.put(CONTENT)


def receiver(sq, q):
    try:
        while True:
            content = sq.get(timeout=2)
            q.put(content)
    except queue.Empty:
        return


class DummySerializer(object):
    def dumps(self, o):
        return o

    def loads(self, d):
        return d


def test_shmqueue():
    if not hasattr(pyrallel, 'ShmQueue'):
        return

    params = [  # chunk size, maxsize
        [50, 100],  # chunk size > content, maxsize is enough
        [10, 100],  # chunk size < content, maxsize is enough
        # [50, 1],  # chunk size > content, maxsize is limited
        # [10, 1],  # chunk size < content, maxsize is limited
    ]

    for mode in ['fork', 'spawn']:
        mp.set_start_method(mode, force=True)
        ShmQueueCls = getattr(pyrallel, 'ShmQueue')
        for param in params:
            sq = ShmQueueCls(chunk_size=param[0], maxsize=param[1], serializer=DummySerializer())
            q = mp.Queue()
            # 3 senders and 2 receivers
            # each sender process add 10 content, in total 30 * 10 = 300 bytes
            p_senders = [mp.Process(target=sender, args=(sq,)) for _ in range(3)]
            p_receivers = [mp.Process(target=receiver, args=(sq, q)) for _ in range(2)]

            for p in p_senders:
                p.start()
            for p in p_receivers:
                p.start()

            for p in p_senders:
                p.join()
            for p in p_receivers:
                p.join()
            sq.close()

            total_put = 30  # there should be in total 30 elements
            while True:
                try:
                    r = q.get(timeout=2)
                    total_put -= 1
                    assert r == CONTENT
                except queue.Empty:
                    break

            assert total_put == 0
