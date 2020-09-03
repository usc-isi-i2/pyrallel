import copy
import multiprocessing as mp
import multiprocessing.queues as mpq
from queue import Full, Empty
import pickle
import math
# import uuid
import os
import struct
import sys
import time
import dill
import zlib


if sys.version_info >= (3, 8):
    from multiprocessing.shared_memory import SharedMemory

    __all__ = ['ShmQueue']
else:
    __all__ = []


class ShmQueue(mpq.Queue):
    """
    ShmQueue depends on shared memory instead of pipe to efficiently exchange data among processes.
    Shared memory is "System V style" memory blocks which can be shared and accessed directly by processes.
    This implementation is based on `multiprocessing.shared_memory.SharedMemory` hence requires Python >= 3.8.
    Its interface is almost identical to `multiprocessing.queue <https://docs.python.org/3.8/library/multiprocessing.html#multiprocessing.Queue>`_.
    But it allows to specify serializer which by default is pickle.


    Args:
        chunk_size (int, optional): Size of each chunk. By default, it is 1*1024*1024.
        maxsize (int, optional): Maximum size of queue. If it is 0 (default), \
                                it will be set to `ShmQueue.MAX_CHUNK_SIZE`.
        serializer (int, optional): Serializer to serialize and deserialize data. \
                                If it is None (default), pickle will be used. \
                                The serialize should have implemented `loads(bytes data) -> object` \
                                and `dumps(object obj) -> bytes`.

    Note:
        - `close` needs to be invoked once to release memory and avoid memory leak.
        - `qsize`, `empty` and `full` are not currently implemented since they are not reliable in multiprocessing.

    Example::

        def run(q):
            e = q.get()
            print(e)

        if __name__ == '__main__':
            q = ShmQueue(chunk_size=1024 * 4, maxsize=10)
            p = Process(target=run, args=(q,))
            p.start()
            q.put(100)
            p.join()
            q.close()
    """
    MAX_CHUNK_SIZE = 512 * 1024 * 1024  # system limit is 2G, 512MB is enough
    META_BLOCK_SIZE = 36

    # if msg_id is empty, the block is considered as empty
    EMPTY_MSG_ID = b'\x00' * 12
    RESERVED_CHUNK_ID = 0xffff
    META_STRUCT = {
        'msg_id': (0, 12, '12s'),
        'msg_size': (12, 16, 'I'),
        'chunk_id': (16, 20, 'I'),
        'total_chunks': (20, 24, 'I'),
        'total_msg_size': (24, 28, 'I'),
        'checksum': (28, 32, 'I'),
        'src_pid': (32, 36, 'I')
    }

    qid_counter: int = 0

    def __init__(self,
                 chunk_size=1*1024*1024,
                 maxsize=2,
                 serializer=None,
                 integrity_check: bool=False,
                 deadlock_check: bool=True,
                 deadlock_immanent_check: bool=True,
                 watermark_check: bool = True,
                 verbose: bool=False):
        ctx = mp.get_context()

        super().__init__(maxsize, ctx=ctx)

        self.qid = self.__class__.qid_counter
        self.__class__.qid_counter += 1

        self.verbose = verbose
        if self.verbose:
            print("Starting ShmQueue qid=%d pid=%d chunk_size=%d maxsize=%d." % (self.qid, os.getpid(), chunk_size, maxsize), file=sys.stderr, flush=True) # ***

        self.chunk_size = min(chunk_size, self.__class__.MAX_CHUNK_SIZE) \
            if chunk_size > 0 else self.__class__.MAX_CHUNK_SIZE
        self.maxsize = maxsize

        self.serializer = serializer or pickle

        self.integrity_check = integrity_check
        self.deadlock_check = deadlock_check
        self.deadlock_immanent_check = deadlock_immanent_check
        self.watermark_check = watermark_check
        self.chunk_watermark = 0

        self.mid_counter = 0

        self.buf_msg_id = None
        self.buf_msg_body = None

        self.producer_lock = ctx.Lock()
        self.consumer_lock = ctx.Lock()
        self.block_locks = [ctx.Lock()] * maxsize
        self.meta_blocks = []
        for _ in range(maxsize):
            self.meta_blocks.append(SharedMemory(create=True, size=self.__class__.META_BLOCK_SIZE))
        self.data_blocks = []
        for _ in range(maxsize):
            self.data_blocks.append(SharedMemory(create=True, size=self.chunk_size))

    def __getstate__(self):
        return (dill.dumps(self.serializer), self.chunk_size, self.producer_lock,
                self.consumer_lock, self.block_locks, dill.dumps(self.meta_blocks), dill.dumps(self.data_blocks))

    def __setstate__(self, state):
        (self.serializer, self.chunk_size, self.producer_lock,
         self.consumer_lock, self.block_locks, self.meta_blocks, self.data_blocks) = state
        self.buf_msg_id = None
        self.buf_msg_body = None
        self.meta_blocks = dill.loads(self.meta_blocks)
        self.data_blocks = dill.loads(self.data_blocks)
        self.serializer = dill.loads(self.serializer)

    def get_meta(self, block, type_):
        addr_s, addr_e, ctype = self.__class__.META_STRUCT.get(type_)
        return struct.unpack(ctype, block.buf[addr_s : addr_e])[0]

    def set_meta(self, block, data, type_):
        addr_s, addr_e, ctype = self.__class__.META_STRUCT.get(type_)
        block.buf[addr_s : addr_e] = struct.pack(ctype, data)

    def generate_msg_id(self):
        # while True:
        #     cand = str(uuid.uuid4())[-12:].encode('utf-8')
        #     if cand != self.__class__.EMPTY_MSG_ID:
        #         return cand
        self.mid_counter += 1
        return ("%012x" % self.mid_counter).encode('utf-8')

    def next_writable_block_id(self, block, timeout, msg_id, src_pid):
        # Note: instead of scanning from the start of the
        # list of blocks, scan from where the previous scan stopped.
        # The loop criterion is more complex, but the's less
        # quadratic scan overhead.
        looped: bool = False
        loop_cnt: int = 0
        i: int = 0
        time_start = time.time()
        while True:
            with self.block_locks[i]:
                if self.get_meta(self.meta_blocks[i], 'msg_id') == self.__class__.EMPTY_MSG_ID:
                    if looped:
                        print("next_writable_block_id: src_pid=%d qid=%d: looping ended after %d loops." % (src_pid, self.qid, loop_cnt), file=sys.stderr, flush=True) # ***
                    meta_block = self.meta_blocks[i]
                    self.set_meta(meta_block, msg_id, 'msg_id')
                    self.set_meta(meta_block, src_pid, 'src_pid')

                    # Reserve this block for the specified message.  The
                    # reserved chunk ID won't be chunk ID 1, which would cause
                    # next_readable_msg_id(...)  to pick the block before it
                    # is ready to be read.
                    self.set_meta(meta_block, self.__class__.RESERVED_CHUNK_ID, 'chunk_id')
                    return i

            i += 1
            if i >= len(self.meta_blocks):
                if not block or (timeout and (time.time() - time_start) > timeout):
                    if self.verbose:
                        print("next_writable_block_id: src_pid=%d qid=%d: FULL" % (src_pid, self.qid), file=sys.stderr, flush=True) # ***
                    raise Full
                i = 0

                if self.deadlock_check or self.verbose:
                    loop_cnt += 1
                    if (self.verbose and loop_cnt == 2) or (self.deadlock_check and loop_cnt % 10000 == 0):
                        looped = True
                        print("next_writable_block_id: src_pid=%d qid=%d: looping (%d loops)" % (src_pid, self.qid, loop_cnt), file=sys.stderr, flush=True) # ***

    def next_readable_msg_block_id(self, block, timeout):
        i = 0
        time_start = time.time()
        while True:
            with self.block_locks[i]:
                meta_block = self.meta_blocks[i]
                msg_id = self.get_meta(meta_block, 'msg_id')
                if msg_id != self.__class__.EMPTY_MSG_ID:
                    if self.get_meta(meta_block, 'chunk_id') == 1:

                        # Reserve this chunk (and thus, this message) for
                        # eading by the current process by changing its
                        # chunk_id from 1 to the reserved chunk ID.
                        self.set_meta(meta_block, self.__class__.RESERVED_CHUNK_ID, 'chunk_id')
                        return i, self.get_meta(meta_block, 'src_pid'), msg_id

            i += 1
            if i >= len(self.meta_blocks):
                if not block or (timeout and (time.time() - time_start) > timeout):
                    raise Empty
                i = 0

    def read_next_block_id(self, src_pid, msg_id, chunk_id):
        i = 0
        while True:
            with self.block_locks[i]:
                meta_block = self.meta_blocks[i]
                if self.get_meta(meta_block, 'msg_id') == msg_id:
                    if self.get_meta(meta_block, 'chunk_id') == chunk_id:
                        if self.get_meta(meta_block, 'src_pid') == src_pid:
                            return i

            i += 1
            if i >= len(self.meta_blocks):
                i = 0

    # def debug_meta_block(self):
    #     for b in self.meta_blocks:
    #         print(bytes(b.buf[0:24]))

    def put(self, msg, block=True, timeout=None):
        """
        Put an object into queue.

        Args:
            msg (obj): The object which needs to put into queue.
            block (bool, optional): If it is set to True (default), it will return after an item is put into queue.
            timeout (int, optional): It can be any positive integer and only effective when `block` is set to True.

        Note:
            `queue.Full` exception will be raised if it times out or queue is full when `block` is False.
        """
        msg_id = self.generate_msg_id()
        src_pid = os.getpid()
        msg_body = self.serializer.dumps(msg)
        if self.integrity_check:
            total_msg_size = len(msg_body)
            msg2 = self.serializer.loads(msg_body)
            if self.verbose:
                print("put: src_pid=%d msg_qid=%s serialization integrity check is OK." % (src_pid, msg_id), file=sys.stderr, flush=True) # ***
            
        total_chunks = math.ceil(len(msg_body) / self.chunk_size)
        if self.verbose:
            print("put: src_pid=%d qid=%d: ***total_chunks=%d*** len(msg_body)=%d chunk_size=%d" % (src_pid, self.qid, total_chunks, len(msg_body), self.chunk_size), file=sys.stderr, flush=True) # ***
        if self.watermark_check or self.verbose:
            if total_chunks > self.chunk_watermark:
                print("put: src_pid=%d qid=%d: total_chunks=%d maxsize=%d new watermark" % (src_pid, self.qid, total_chunks, self.maxsize), file=sys.stderr, flush=True) # ***
                self.chunk_watermark = total_chunks

        if self.deadlock_immanent_check and total_chunks > self.maxsize:
            raise ValueError("DEADLOCK IMMANENT: src_pid=%d qid=%d: total_chunks=%d > maxsize=%d" % (src_pid, self.qid, total_chunks, self.maxsize))
        
        if self.verbose:
            print("put: src_pid=%d qid=%d: acquiring producer lock" % (src_pid, self.qid), file=sys.stderr, flush=True) # *** 
        time_start = time.time()
        lock = self.producer_lock.acquire(timeout=timeout)
        if timeout:
            timeout -= (time.time() - time_start)
        if block and not lock:
            if self.verbose:
                print("put: src_pid=%d qid=%d FULL" % (src_pid, self.qid), file=sys.stderr, flush=True) # ***
            raise Full # Note: a message ID has been consumed by the failed attempt.

        try:
            # In case we will process more than one cchunk and this is a
            # nonblocking or timed out request, start by reserving all the
            # blocks that we will need.
            block_id_list: typing.List[int] = [ ]
            for i in range(total_chunks):
                try:
                    block_id = self.next_writable_block_id(block, timeout, msg_id, src_pid)
                    block_id_list.append(block_id)

                except Full:
                    # We failed to find a free block and/or a timeout occured.
                    # Relese the reserved blocks.
                    if self.verbose:
                        print("put: src_pid=%d qid=%d releasing %d blocks" % (src_pid, self.qid, len(block_id_list)), file=sys.stderr, flush=True) # ***
                    for block_id in block_id_list:
                        with self.block_locks[block_id]:
                            meta_block = self.meta_blocks[block_id]
                            self.set_meta(meta_block, self.__class__.EMPTY_MSG_ID, 'msg_id')
                    raise Full # Note: a message ID has been consumed by the failed attempt.

            # Now that we have a full set of blocks, queue the
            # chunks:
            for i, block_id in enumerate(block_id_list):
                if self.verbose:
                    print("put: src_pid=%d qid=%d: i=%d" % (src_pid, self.qid, i), file=sys.stderr, flush=True) # *** 
               
                meta_block, data_block = self.meta_blocks[block_id], self.data_blocks[block_id]
                chunk_data = msg_body[i * self.chunk_size: (i + 1) * self.chunk_size]
                chunk_id = i + 1
                msg_size = len(chunk_data)
                if self.verbose:
                    print("put: src_pid=%d qid=%d: msg_id=%s msg_size=%d chunk_id=%d total_chunks=%d." % (src_pid, self.qid, msg_id, msg_size, chunk_id, total_chunks), file=sys.stderr, flush=True) # ***
                if self.integrity_check:
                    checksum = zlib.adler32(chunk_data)
                    if self.verbose:
                        print("put: src_pid=%d, qid=%d: msg_id=%s checksum=%x total_msg_size=%d" % (src_pid, self.qid, msg_id, checksum, total_msg_size), file=sys.stderr, flush=True) # ***

                with self.block_locks[block_id]:
                    self.set_meta(meta_block, msg_id, 'msg_id')
                    self.set_meta(meta_block, msg_size, 'msg_size')
                    self.set_meta(meta_block, chunk_id, 'chunk_id')
                    self.set_meta(meta_block, total_chunks, 'total_chunks')
                    if self.integrity_check:
                        self.set_meta(meta_block, total_msg_size, 'total_msg_size')
                        self.set_meta(meta_block, checksum, 'checksum')                        
                    data_block.buf[0:msg_size] = chunk_data
        finally:
            if self.verbose:
                print("put: src_pid=%d qid=%d: releasing producer lock" % (src_pid, self.qid), file=sys.stderr, flush=True) # *** 
            self.producer_lock.release()

    def get(self, block=True, timeout=None):
        """
        Return data from queue.

        Args:
            block (bool, optional): If it is set to True (default), it will only return when an item is available.
            timeout (int, optional): It can be any positive integer and only effective when `block` is set to True.

        Returns:
            object: Object.

        Note:
            `queue.Empty` exception will be raised if it times out or queue is empty when `block` is False.
        """
        if self.verbose:
            print("put: qid=%d: acquiring consumer lock" % self.qid, file=sys.stderr, flush=True) # *** 
        time_start = time.time()
        lock = self.consumer_lock.acquire(timeout=timeout)
        if timeout:
            timeout -= (time.time() - time_start)
        if block and not lock:
            raise Empty

        # We will build a list of message chunks.  We can't
        # release them until after we deserialize the data.
        msg_block_ids = [ ]
        
        try:
            block_id, src_pid, msg_id = self.next_readable_msg_block_id(block, timeout) # This call might raise Empty.
            chunk_id = 1
            chunk_count = 0
            while True:

                if chunk_count > 0:
                    block_id = self.read_next_block_id(src_pid, msg_id, chunk_count + 1)
                meta_block, data_block = self.meta_blocks[block_id], self.data_blocks[block_id]
                msg_block_ids.append(block_id)

                with self.block_locks[block_id]:
                    if chunk_count == 0:
                        total_chunks = self.get_meta(meta_block, 'total_chunks')
                    else:
                        chunk_id = self.get_meta(meta_block, 'chunk_id')
                    msg_size = self.get_meta(meta_block, 'msg_size')
                    if self.integrity_check:
                        if chunk_count == 0:
                            total_msg_size = self.get_meta(meta_block, 'total_msg_size')
                        checksum = self.get_meta(meta_block, 'checksum')
                    chunk_data = data_block.buf[0:msg_size] # This makes a reference, not a deep copy.
                chunk_count += 1
                if self.verbose:
                    print("get: qid=%d src_pid=%d msg_id=%s: msg_size=%d chunk_id=%d total_chunks=%d." % (self.qid, src_pid, msg_id, msg_size, chunk_id, total_chunks), file=sys.stderr, flush=True) # ***
                if self.integrity_check:
                    checksum2 = zlib.adler32(chunk_data)
                    if checksum == checksum2:
                        if self.verbose:
                            print("get: qid=%d src_pid=%d msg_id=%s: checksum=%x is OK" % (src_pid, self.qid, msg_id, checksum), file=sys.stderr, flush=True) # ***
                    else:
                        raise ValueError("ShmQueue.get: qid=%d src_pid=%d msg_id=%s: checksum=%x != checksum2=%x -- FAIL!" % (self.qid, src_pid, msg_id, checksum, checksum2)) # TODO: use a better exception

                if not self.buf_msg_id:
                    self.buf_msg_id = msg_id
                if not self.buf_msg_body:
                    self.buf_msg_body = [None] * total_chunks
                self.buf_msg_body[chunk_id-1] = chunk_data # This copies the reference

                if chunk_count == total_chunks:
                    msg_body = b''.join(self.buf_msg_body) # This might even copy the references.
                    if self.integrity_check:
                        if total_msg_size == len(msg_body):
                            if self.verbose:
                                 print("get: qid=%d src_pid=%d msg_id=%s: total_msg_size=%d is OK" % (self.qid, src_pid, msg_id, total_msg_size), file=sys.stderr, flush=True) # ***
                        else:
                            raise ValueError("get: qid=%d src_pid=%d msg_id=%s: total_msg_size=%d != len(msg_body)=%d -- FAIL!" % (self.qid, src_pid, msg_id, total_msg_size, len(msg_body))) # TODO: use a beter exception.
                    try:
                        msg = self.serializer.loads(msg_body)
                        # We could release the blocks here, but then we'd have to release
                        # them in the except clause, too.
                    except pickle.UnpicklingError as e:
                        print("get: Fail: qid=%d src_pid=%d msg_id=%s: msg_size=%d chunk_id=%d total_chunks=%d." % (self.qid, src_pid, msg_id, msg_size, chunk_id, total_chunks), file=sys.stderr, flush=True) # ***
                        if self.integrity_check:
                            print("get: Fail: qid=%d src_pid=%d msg_id=%s: total_msg_size=%d checksum=%x" % (self.pid, src_pid, msg_id, total_msg_size, checksum), file=sys.stderr, flush=True) # ***
                        raise e

                    self.buf_msg_id = None
                    self.buf_msg_body = None
                    return msg
        finally:
            # It is now safe to release the data blocks.  This is a good place
            # to release them, because it covers error paths as well as the main return.
            for block_id in msg_block_ids:
                with self.block_locks[block_id]:
                    self.set_meta(self.meta_blocks[block_id], self.__class__.EMPTY_MSG_ID, 'msg_id')
            if self.verbose:
                print("put: qid=%d: releasing consumer lock" % self.qid, file=sys.stderr, flush=True) # *** 
            self.consumer_lock.release()

    def get_nowait(self):
        """
        Equivalent to `get(False)`.
        """
        return self.get(False)

    def put_nowait(self, msg):
        """
        Equivalent to `put(obj, False)`.
        """
        return self.put(msg, False)

    def qsize(self):
        raise NotImplementedError

    def empty(self):
        raise NotImplementedError

    def full(self):
        raise NotImplementedError

    def close(self):
        """
        Indicate no more new data will be added and release the shared memory.
        """
        for block in self.meta_blocks:
            block.close()
            block.unlink()
        for block in self.data_blocks:
            block.close()
            block.unlink()

    def __del__(self):
        pass
