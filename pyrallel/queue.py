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
import typing
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


    Problem:  There is no guarantee that messages will be delivered in order.
    That means that if you have a START, DATA..., SHUTDOWN sequence, the first
    DATA might be delivered before the START, and the SHUTDOWN may be delivered
    before the last DATA.
    """
    MAX_CHUNK_SIZE = 512 * 1024 * 1024  # system limit is 2G, 512MB is enough

    # if msg_id is empty, the block is considered as empty
    RESERVED_BLOCK_ID = 0xffffffff
    META_STRUCT = {
        'msg_id': (0, 12, '12s'),
        'msg_size': (12, 16, 'I'),
        'chunk_id': (16, 20, 'I'),
        'total_chunks': (20, 24, 'I'),
        'total_msg_size': (24, 28, 'I'),
        'checksum': (28, 32, 'I'),
        'src_pid': (32, 36, 'I'),
        'next_chunk_block_id': (36, 40, 'I'),
        'next_block_id': (40, 44, 'I')
    }
    META_BLOCK_SIZE = 44

    LIST_HEAD_STRUCT = {
        'first_block': (0, 4, 'I'),
        'last_block': (4, 8, 'I'),
        'block_count': (8, 12, 'I')
    }
    LIST_HEAD_SIZE = 12
    FREE_LIST_HEAD = 0
    MSG_LIST_HEAD = 1

    qid_counter: int = 0

    def __init__(self,
                 chunk_size=1*1024*1024,
                 maxsize=2,
                 serializer=None,
                 integrity_check: bool=False,
                 deadlock_check: bool=False,
                 deadlock_immanent_check: bool=True,
                 watermark_check: bool = False,
                 use_semaphores: bool = True,
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

        self.producer_lock = ctx.Lock()
        self.free_list_lock = ctx.Lock()
        self.msg_list_lock = ctx.Lock()

        self.use_semaphores: bool = use_semaphores
        if use_semaphores:
            self.free_list_semaphore = ctx.Semaphore(0)
            self.msg_list_semaphore = ctx.Semaphore(0)
        else:
            self.free_list_semaphore = None
            self.msg_list_semaphore = None
        
        self.list_heads = SharedMemory(create=True, size=self.__class__.LIST_HEAD_SIZE * 2)
        self.init_list_head(self.__class__.FREE_LIST_HEAD)
        self.init_list_head(self.__class__.MSG_LIST_HEAD)

        self.block_locks = [ctx.Lock()] * maxsize
        self.data_blocks = []
        for block_id in range(maxsize):
            self.data_blocks.append(SharedMemory(create=True, size=self.__class__.META_BLOCK_SIZE + self.chunk_size))
            self.add_free_block(block_id)

    def __getstate__(self):
        return (self.qid,
                self.verbose,
                self.chunk_size,
                self.maxsize,
                dill.dumps(self.serializer),
                self.integrity_check,
                self.deadlock_check,
                self.deadlock_immanent_check,
                self.watermark_check,
                self.chunk_watermark,
                self.mid_counter,
                self.producer_lock,
                self.free_list_lock,
                self.msg_list_lock,
                self.use_semaphores,
                self.free_list_semaphore,
                self.msg_list_semaphore,
                dill.dumps(self.list_heads),
                self.block_locks,
                dill.dumps(self.data_blocks))

    def __setstate__(self, state):
        (self.qid,
         self.verbose,
         self.chunk_size,
         self.maxsize,
         self.serializer,
         self.integrity_check,
         self.deadlock_check,
         self.deadlock_immanent_check,
         self.watermark_check,
         self.chunk_watermark,
         self.mid_counter,
         self.producer_lock,
         self.free_list_lock,
         self.msg_list_lock,
         self.use_semaphores,
         self.free_list_semaphore,
         self.msg_list_semaphore,
         self.list_heads,
         self.block_locks,
         self.data_blocks) = state

        self.list_heads = dill.loads(self.list_heads)
        self.data_blocks = dill.loads(self.data_blocks)
        self.serializer = dill.loads(self.serializer)

    def get_list_head_field(self, lh: int, type_):
        addr_s, addr_e, ctype = self.__class__.LIST_HEAD_STRUCT.get(type_)
        return struct.unpack(ctype, self.list_heads.buf[(self.__class__.LIST_HEAD_SIZE * lh) + addr_s : (self.__class__.LIST_HEAD_SIZE * lh) + addr_e])[0]

    def set_list_head_field(self, lh: int, data, type_):
        addr_s, addr_e, ctype = self.__class__.LIST_HEAD_STRUCT.get(type_)
        self.list_heads.buf[(self.__class__.LIST_HEAD_SIZE * lh) + addr_s : (self.__class__.LIST_HEAD_SIZE * lh) + addr_e] = struct.pack(ctype, data)

    def get_meta(self, block, type_):
        addr_s, addr_e, ctype = self.__class__.META_STRUCT.get(type_)
        return struct.unpack(ctype, block.buf[addr_s : addr_e])[0]

    def set_meta(self, block, data, type_):
        addr_s, addr_e, ctype = self.__class__.META_STRUCT.get(type_)
        block.buf[addr_s : addr_e] = struct.pack(ctype, data)

    def get_data(self, block, data_size):
        return block.buf[self.__class__.META_BLOCK_SIZE:self.__class__.META_BLOCK_SIZE+data_size]

    def set_data(self, block, data, data_size):
        block.buf[self.__class__.META_BLOCK_SIZE:self.__class__.META_BLOCK_SIZE+data_size] = data

    def init_list_head(self, lh: int):
        self.set_list_head_field(lh, 0, 'block_count')
        self.set_list_head_field(lh, self.__class__.RESERVED_BLOCK_ID, 'first_block')
        self.set_list_head_field(lh, self.__class__.RESERVED_BLOCK_ID, 'last_block')

    def get_block_count(self, lh: int):
        return self.get_list_head_field(lh, 'block_count')

    def get_first_block(self, lh: int):
        block_count = self.get_block_count(lh)
        if block_count == 0:
            return None

        block_id = self.get_list_head_field(lh, 'first_block')

        block_count -= 1
        if block_count == 0:
            self.init_list_head(lh)
        else:
            with self.block_locks[block_id]:
                next_block_id = self.get_meta(self.data_blocks[block_id], 'next_block_id')
            self.set_list_head_field(lh, next_block_id, 'first_block')
            self.set_list_head_field(lh, block_count, 'block_count')
        return block_id

    def add_block(self, lh, block_id):
        block_count = self.get_list_head_field(lh, 'block_count')
        if block_count == 0:
            self.set_list_head_field(lh, block_id, 'first_block')
            self.set_list_head_field(lh, block_id, 'last_block')
            self.set_list_head_field(lh, 1, 'block_count')
        
        else:
            last_block = self.get_list_head_field(lh, 'last_block')
            with self.block_locks[last_block]:
                self.set_meta(self.data_blocks[last_block], block_id, 'next_block_id')
            self.set_list_head_field(lh, block_id, 'last_block')
            self.set_list_head_field(lh, block_count + 1, 'block_count')
                
    def get_free_block_count(self):
        with self.free_list_lock:
            return self.get_block_count(self.__class__.FREE_LIST_HEAD)

    def get_first_free_block(self, block: bool, timeout: typing.Optional[float]):
        if self.free_list_semaphore is not None:
            self.free_list_semaphore.acquire(block=block, timeout=timeout)
        with self.free_list_lock:
            return self.get_first_block(self.__class__.FREE_LIST_HEAD)

    def add_free_block(self, block_id: int):
        with self.free_list_lock:
            self.add_block(self.__class__.FREE_LIST_HEAD, block_id)
        if self.free_list_semaphore is not None:
            self.free_list_semaphore.release()

    def get_msg_count(self):
        with self.msg_list_lock:
            return self.get_block_count(self.__class__.MSG_LIST_HEAD)

    def get_first_msg(self, block: bool, timeout: typing.Optional[float]):
        if self.msg_list_semaphore is not None:
            self.msg_list_semaphore.acquire(block=block, timeout=timeout)
        with self.msg_list_lock:
            return self.get_first_block(self.__class__.MSG_LIST_HEAD)

    def add_msg(self, block_id):
        with self.msg_list_lock:
            self.add_block(self.__class__.MSG_LIST_HEAD, block_id)
        if self.msg_list_semaphore is not None:
            self.msg_list_semaphore.release()
        
    def generate_msg_id(self):
        return ("%012x" % (self.mid_counter + 1)).encode('utf-8')

    def consume_msg_id(self):
        self.mid_counter += 1

    def next_writable_block_id(self, block: bool, timeout: typing.Optional[float], msg_id, src_pid):
        looped: bool = False
        loop_cnt: int = 0
        time_start = time.time()
        while True:
            remaining_timeout = timeout
            if remaining_timeout is not None:
                remaining_timeout -= (time.time() - time_start)
                if remaining_timeout <= 0:
                    if self.verbose:
                        print("next_writable_block_id: qid=%d src_pid=%d: queue FULL (timeout)" % (self.qid, src_pid), file=sys.stderr, flush=True) # ***
                    raise Full

            block_id = self.get_first_free_block(block, remaining_timeout)
            if block_id is not None:
                break

            if not block:
                if self.verbose:
                    print("next_writable_block_id: qid=%d src_pid=%d: FULL (nonblocking)" % (self.qid, src_pid), file=sys.stderr, flush=True) # ***
                raise Full

            if self.deadlock_check or self.verbose:
                loop_cnt += 1
                if (self.verbose and loop_cnt == 2) or (self.deadlock_check and loop_cnt % 10000 == 0):
                    looped = True
                    print("next_writable_block_id: qid=%d src_pid=%d: looping (%d loops)" % (self.qid, src_pid, loop_cnt), file=sys.stderr, flush=True) # ***

        if looped:
            print("next_writable_block_id: qid=%d src_pid=%d: looping ended after %d loops." % (self.qid, src_pid, loop_cnt), file=sys.stderr, flush=True) # ***

        with self.block_locks[block_id]:
            data_block = self.data_blocks[block_id]
            self.set_meta(data_block, msg_id, 'msg_id')
            self.set_meta(data_block, src_pid, 'src_pid')

        return block_id

    def next_readable_msg(self, block: bool, timeout: typing.Optional[float]):
        i = 0
        time_start = time.time()
        while True:
            remaining_timeout = timeout
            if remaining_timeout is not None:
                remaining_timeout -= (time.time() - time_start)
                if remaining_timeout <= 0:
                    raise Empty
            block_id = self.get_first_msg(block=block, timeout=remaining_timeout)
            if block_id is not None:
                break

            if not block:
                raise Empty
            
        with self.block_locks[block_id]:
            data_block = self.data_blocks[block_id]
            return \
                self.get_meta(data_block, 'src_pid'), \
                self.get_meta(data_block, 'msg_id'), \
                block_id, \
                self.get_meta(data_block, 'total_chunks'), \
                self.get_meta(data_block, 'next_chunk_block_id')


    # def debug_data_block(self):
    #     for b in self.data_blocks:
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
        if timeout is not None:
            if not block:
                raise ValueError("A timeout is allowed only when not blocking.")
            if timemout < 0:
                raise Full

        msg_id = self.generate_msg_id()
        src_pid = os.getpid()
        msg_body = self.serializer.dumps(msg)
        if self.integrity_check:
            total_msg_size = len(msg_body)
            msg2 = self.serializer.loads(msg_body)
            if self.verbose:
                print("put: qid=%d src_pid=%d msg_id=%s: serialization integrity check is OK." % (self.qid, src_pid, msg_id), file=sys.stderr, flush=True) # ***
            
        total_chunks = math.ceil(len(msg_body) / self.chunk_size)
        if self.verbose:
            print("put: qid=%d src_pid=%d msg_id=%s: total_chunks=%d len(msg_body)=%d chunk_size=%d" % (self.qid, src_pid, msg_id, total_chunks, len(msg_body), self.chunk_size), file=sys.stderr, flush=True) # ***
        if self.watermark_check or self.verbose:
            if total_chunks > self.chunk_watermark:
                print("put: qid=%d src_pid=%d msg_id=%s: total_chunks=%d maxsize=%d new watermark" % (self.qid, src_pid, msg_id, total_chunks, self.maxsize), file=sys.stderr, flush=True) # ***
                self.chunk_watermark = total_chunks

        if self.deadlock_immanent_check and total_chunks > self.maxsize:
            raise ValueError("DEADLOCK IMMANENT: qid=%d src_pid=%d: total_chunks=%d > maxsize=%d" % (self.qid, src_pid, total_chunks, self.maxsize))
        
        time_start = time.time()

        # We acquire the producer lock to avoid deadlock if multiple
        # producers need multiple chunks each.
        lock = self.producer_lock.acquire(timeout=timeout)
        if not lock:
            # We must have timed out.
            if self.verbose:
                print("put: qid=%d src_pid=%d msg_id=%s: queue FULL" % (self.qid, src_pid, msg_id), file=sys.stderr, flush=True) # ***
            raise Full

        try:
            # In case we will process more than one chunk and this is a
            # nonblocking or timed out request, start by reserving all the
            # blocks that we will need.
            block_id_list: typing.List[int] = [ ]
            for i in range(total_chunks):
                try:
                    remaining_timeout = timeout
                    if remaining_timeout is not None:
                        remaining_timeout -= (time.time() - time_start)
                        if remaining_timeout <= 0:
                            if self.verbose:
                                print("put: qid=%d src_pid=%d msg_id=%s: queue FULL" % (self.qid, src_pid, msg_id), file=sys.stderr, flush=True) # ***
                            raise Full

                    block_id = self.next_writable_block_id(block, remaining_timeout, msg_id, src_pid)
                    block_id_list.append(block_id)

                except Full:
                    # We failed to find a free block and/or a timeout occured.
                    # Release the reserved blocks.
                    if self.verbose:
                        print("put: qid=%d src_pid=%d msg_id=%s: releasing %d blocks" % (self.qid, src_pid, msg_id, len(block_id_list)), file=sys.stderr, flush=True) # ***
                    for block_id in block_id_list:
                        self.add_free_block(block_id)
                    raise

        finally:
            # Now that we have acquired the full set of chunks, we can release
            # the producer lock.  We don't want to hold it while we transfer
            # data into the blocks.
            if self.verbose:
                print("put: qid=%d src_pid=%d msg_id=%s: releasing producer lock" % (self.qid, src_pid, msg_id), file=sys.stderr, flush=True) # *** 
            self.producer_lock.release()

            # Consume this message ID.
            self.consume_msg_id()

        if self.verbose:
            print("put: qid=%d src_pid=%d msg_id=%s: acquired %d blocks" % (self.qid, src_pid, msg_id, total_chunks), file=sys.stderr, flush=True) # *** 

        # Now that we have a full set of blocks, build the
        # chunks:
        for i, block_id in enumerate(block_id_list):
            chunk_id = i + 1
            if self.verbose:
                print("put: qid=%d src_pid=%d msg_id=%s: chunk_id=%d of total_chunks=%d" % (self.qid, src_pid, msg_id, chunk_id, total_chunks), file=sys.stderr, flush=True) # *** 
               
            data_block = self.data_blocks[block_id]
            chunk_data = msg_body[i * self.chunk_size: (i + 1) * self.chunk_size]
            msg_size = len(chunk_data)
            if self.verbose:
                print("put: qid=%d src_pid=%d msg_id=%s: chunk_id=%d: block_id=%d msg_size=%d." % (self.qid, src_pid, msg_id, chunk_id, block_id, msg_size), file=sys.stderr, flush=True) # ***
            if self.integrity_check:
                checksum = zlib.adler32(chunk_data)
                if self.verbose:
                    print("put: qid=%d src_pid=%d msg_id=%s: chunk_id=%d: checksum=%x total_msg_size=%d" % (self.qid, src_pid, msg_id, chunk_id, checksum, total_msg_size), file=sys.stderr, flush=True) # ***

            with self.block_locks[block_id]:
                self.set_meta(data_block, msg_id, 'msg_id')
                self.set_meta(data_block, msg_size, 'msg_size')
                self.set_meta(data_block, chunk_id, 'chunk_id')
                self.set_meta(data_block, total_chunks, 'total_chunks')
                if self.integrity_check:
                    self.set_meta(data_block, total_msg_size, 'total_msg_size')
                    self.set_meta(data_block, checksum, 'checksum')
                if chunk_id == total_chunks:
                    # No more chunks, store a reserved value to simplify debugging.
                    self.set_meta(data_block, self.__class__.RESERVED_BLOCK_ID, 'next_chunk_block_id')
                else:
                    # Store the block ID of the next chunk.
                    self.set_meta(data_block, block_id_list[i + 1], 'next_chunk_block_id')
                self.set_data(data_block, chunk_data, msg_size)

        # Now that the entire message has built, queue it:
        self.add_msg(block_id_list[0])
        if self.verbose:
            print("put: qid=%d src_pid=%d msg_id=%s: message sent" % (self.qid, src_pid, msg_id), file=sys.stderr, flush=True) # *** 

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
        time_start = time.time()

        # We will build a list of message chunks.  We can't
        # release them until after we deserialize the data.
        msg_block_ids = [ ]
        
        try:
            remaining_timeout = timeout
            if remaining_timeout is not None:
                remaining_timeout -= (time.time() - time_start)
                if remaining_timeout <= 0:
                    if self.verbose:
                        print("put: qid=%d src_pid=%d msg_id=%s: queue EMPTY" % (self.qid, src_pid, msg_id), file=sys.stderr, flush=True) # ***
                    raise Empty

            src_pid, msg_id, block_id, total_chunks, next_chunk_block_id = self.next_readable_msg(block, remaining_timeout) # This call might raise Empty.
            if self.verbose:
                print("get: qid=%d src_pid=%d msg_id=%s: total_chunks=%d next_chunk_block_id=%d." % (self.qid, src_pid, msg_id, total_chunks, next_chunk_block_id), file=sys.stderr, flush=True) # ***
            msg_block_ids.append(block_id)

            # Acquire the chunks for the rest of the message:
            for i in range(1, total_chunks):
                chunk_id = i + 1
                if self.verbose:
                    print("get: qid=%d src_pid=%d msg_id=%s: chunk_id=%d: block_id=%d." % (self.qid, src_pid, msg_id, chunk_id, next_chunk_block_id), file=sys.stderr, flush=True) # ***
                msg_block_ids.append(next_chunk_block_id)
                data_block = self.data_blocks[next_chunk_block_id]
                with self.block_locks[next_chunk_block_id]:
                    next_chunk_block_id = self.get_meta(data_block, 'next_chunk_block_id')

        except Exception:
            # Release the data blocks (losing the message) if we get an
            # unexpected exception:
            if self.verbose:
                print("put: qid=%d: releasing data blocks due to Exception" % self.qid, file=sys.stderr, flush=True) # *** 
            for block_id in msg_block_ids:
                self.add_free_block(block_id)
            msg_block_ids.clear()
            raise

        buf_msg_body = [None] * total_chunks
        try:
            for i, block_id in enumerate(msg_block_ids):
                chunk_id = i + 1
                data_block = self.data_blocks[block_id]
                with self.block_locks[block_id]:
                    msg_size = self.get_meta(data_block, 'msg_size')
                    if self.integrity_check:
                        if i == 0:
                            total_msg_size = self.get_meta(data_block, 'total_msg_size')
                        checksum = self.get_meta(data_block, 'checksum')
                    chunk_data = self.get_data(data_block, msg_size) # This may make a reference, not a deep copy.
                if self.verbose:
                    print("get: qid=%d src_pid=%d msg_id=%s: chunk_id=%d: block_id=%d msg_size=%d total_chunks=%d." % (self.qid, src_pid, msg_id, chunk_id, block_id, msg_size, total_chunks), file=sys.stderr, flush=True) # ***
                if self.integrity_check:
                    checksum2 = zlib.adler32(chunk_data)
                    if checksum == checksum2:
                        if self.verbose:
                            print("get: qid=%d src_pid=%d msg_id=%s: chunk_id=%d: checksum=%x is OK" % (self.qid, src_pid, msg_id, chunk_id, checksum), file=sys.stderr, flush=True) # ***
                    else:
                        raise ValueError("ShmQueue.get: qid=%d src_pid=%d msg_id=%s: chunk_id=%d: block_id=%d checksum=%x != checksum2=%x -- FAIL!" % (self.qid, src_pid, msg_id, chunk_id, block_id, checksum, checksum2)) # TODO: use a better exception

                buf_msg_body[i] = chunk_data # This may copy the reference.

            msg_body = b''.join(buf_msg_body) # Even this might copy the references.
            if self.integrity_check:
                if total_msg_size == len(msg_body):
                    if self.verbose:
                        print("get: qid=%d src_pid=%d msg_id=%s: total_msg_size=%d is OK" % (self.qid, src_pid, msg_id, total_msg_size), file=sys.stderr, flush=True) # ***
                else:
                    raise ValueError("get: qid=%d src_pid=%d msg_id=%s: total_msg_size=%d != len(msg_body)=%d -- FAIL!" % (self.qid, src_pid, msg_id, total_msg_size, len(msg_body))) # TODO: use a beter exception.

            try:
                msg = self.serializer.loads(msg_body) # Finally, we are guaranteed to copy the data.

                # We could release the blocks here, but then we'd have to
                # release them in the except clause, too.

                return msg

            except pickle.UnpicklingError as e:
                print("get: Fail: qid=%d src_pid=%d msg_id=%s: msg_size=%d chunk_id=%d total_chunks=%d." % (self.qid, src_pid, msg_id, msg_size, chunk_id, total_chunks), file=sys.stderr, flush=True) # ***
                if self.integrity_check:
                    print("get: Fail: qid=%d src_pid=%d msg_id=%s: total_msg_size=%d checksum=%x" % (self.pid, src_pid, msg_id, total_msg_size, checksum), file=sys.stderr, flush=True) # ***
                raise
    
        finally:
            # It is now safe to release the data blocks.  This is a good place
            # to release them, because it covers error paths as well as the main return.
            if self.verbose:
                print("get: qid=%d src_pid=%d msg_id=%s: releasing %d blocks." % (self.qid, src_pid, msg_id, len(msg_block_ids)), file=sys.stderr, flush=True) # ***
            for block_id in msg_block_ids:
                self.add_free_block(block_id)
            msg_block_ids.clear()
            buf_msg_body.clear()

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
        return self.get_msg_count()

    def empty(self):
        return self.get_msg_count() == 0

    def full(self):
        return self.get_free_count() == 0

    def close(self):
        """
        Indicate no more new data will be added and release the shared memory.
        """
        for block in self.data_blocks:
            block.close()
            block.unlink()

        self.list_heads.close()
        self.list_heads.unlink()

    def __del__(self):
        pass
