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
import dill # type: ignore
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
    But it allows one to specify the serializer, which by default is pickle.


    Args:
        chunk_size (int, optional): Size of each chunk. By default, it is 1*1024*1024.
        maxsize (int, optional): Maximum size of queue. If it is 0 (default), \
                                it will be set to `ShmQueue.MAX_CHUNK_SIZE`.
        serializer (obj, optional): Serializer to serialize and deserialize data. \
                                If it is None (default), pickle will be used. \
                                The serializer should implement `loads(bytes data) -> object` \
                                and `dumps(object obj) -> bytes`.
        integrity_check (bool, optional): When True, perform certain integrity checks on messages.
                                1) After serializing a message, immediately deserialize it to check for validity.
                                2) Save the length of a message after serialization.
                                3) Compute a checksum of each chunk of the message.
                                4) Include the total message size and chunk checksum in the metadata for each chunk.
                                5) When pulling a chunk from the queue, verify the chunk checksum.
                                6) After reassembling a message out of chunks, verify the total message size.
        deadlock_check (bool, optional): When fetching a writable block, print a message if two or more
                                loops are needed to get a free block. (default is False)
        deadlock_immanent_check (bool, optional): Raise a ValueError if a message submitted to
                                put(...) is too large to process.  (Default is True)
        watermark_check (bool, optional): When true, prit a mesage with the largest message size so far in chunks.
        use_semaphores (bool, optional): When true, use semaphores to control access to the free list and the 
                                message list. The system will sleep when accessing these shared resources,
                                instead of entering a polling loop.

    Note:
        - `close` needs to be invoked once to release memory and avoid a memory leak.
        - `qsize`, `empty` and `full` are implemented but may block.

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

    MAX_CHUNK_SIZE: int = 512 * 1024 * 1024
    """int: The maximum allowable size for a buffer chunk; also, the default size.  512MB should be a large enough
    value."""

    RESERVED_BLOCK_ID: int = 0xffffffff
    """int: RESERVED_BLOCK_ID is stored in the list head pointer and next chunk
    block id fields to indicate that thee is no next block.  This value is intended
    to simplify debugging by removing stale next-block values.  It is not used to
    test for blok chain termination;  counters are used for that purpose, instead."""

    META_STRUCT: typing.Mapping[str, typing.Tuple[int, int, str]] = {
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
    """The per-buffer metadata structure parameters for struct.pack(...) and
    struct.unpack(...)."""
    
    META_BLOCK_SIZE: int = 44
    """int: The length of the buffer metadata structure in bytes."""

    LIST_HEAD_STRUCT: typing.Mapping[str, typing.Tuple[int, int, str]] = {
        'first_block': (0, 4, 'I'),
        'last_block': (4, 8, 'I'),
        'block_count': (8, 12, 'I')
    }
    """The list head structure parameters for struct.pack(...) and
    struct.unpack(...). The list header structure maintains a block
    count in addition to first_block and last_block pointers."""

    LIST_HEAD_SIZE: int = 12
    """int: The length of a list head structure in bytes."""

    FREE_LIST_HEAD: int = 0
    """int: The index of the free buffer list head in the SharedMemory segment for
    sharing message queue list heads between processes."""
    
    MSG_LIST_HEAD: int = 1
    """int: The index of the queued message list head in the SharedMemory segment for
    sharing message queue list heads between processes."""

    qid_counter: int = 0
    """int: Each message queue has a queue ID (qid) that identifies the queue for
    debugging messages. This mutable class counter is used to create new queue ID
    values for newly-created queue. Implicitly, this assumes that message queues
    will be created by a single initialization process, then distributed to worker
    process.  If shared message queues will be created by multiple processes, then
    the queue ID should be altered to incorporate the process ID (pid) of the
    process that created the shared message queue, or an additional field should
    be created and presented with the shared message queue's creator's pid.."""

    def __init__(self,
                 chunk_size: int=1*1024*1024,
                 maxsize: int=2,
                 serializer=None,
                 integrity_check: bool=False,
                 deadlock_check: bool=False,
                 deadlock_immanent_check: bool=True,
                 watermark_check: bool = False,
                 use_semaphores: bool = True,
                 verbose: bool=False):
        ctx = mp.get_context() # TODO: What is the proper type hint here?

        super().__init__(maxsize, ctx=ctx)

        self.qid: int = self.__class__.qid_counter
        self.__class__.qid_counter += 1

        self.verbose: bool = verbose
        if self.verbose:
            print("Starting ShmQueue qid=%d pid=%d chunk_size=%d maxsize=%d." % (self.qid, os.getpid(), chunk_size, maxsize), file=sys.stderr, flush=True) # ***

        self.chunk_size: int = min(chunk_size, self.__class__.MAX_CHUNK_SIZE) \
            if chunk_size > 0 else self.__class__.MAX_CHUNK_SIZE
        self.maxsize: int = maxsize

        self.serializer = serializer or pickle

        self.integrity_check: bool = integrity_check
        self.deadlock_check: bool = deadlock_check
        self.deadlock_immanent_check: bool = deadlock_immanent_check
        self.watermark_check: bool = watermark_check
        self.chunk_watermark: int = 0

        self.mid_counter: int = 0

        self.producer_lock = ctx.Lock()
        self.free_list_lock = ctx.Lock()
        self.msg_list_lock = ctx.Lock()

        self.use_semaphores: bool = use_semaphores
        if not use_semaphores:
            # Put the None case first to make mypy happier.
            self.free_list_semaphore: typing.Optional[typing.Any] = None # TODO: what is the type returned by ctx.Semaphore(0)?
            self.msg_list_semaphore: typing.Optional[typing.Any] = None
        else:
            self.free_list_semaphore = ctx.Semaphore(0)
            self.msg_list_semaphore = ctx.Semaphore(0)
        
        self.list_heads: SharedMemory = SharedMemory(create=True, size=self.__class__.LIST_HEAD_SIZE * 2)
        self.init_list_head(self.__class__.FREE_LIST_HEAD)
        self.init_list_head(self.__class__.MSG_LIST_HEAD)

        self.block_locks: typing.List[typing.Any] = [ctx.Lock()] * maxsize # TODO: what is the type returned by ctx.Lock()?
        self.data_blocks: typing.List[SharedMemory] = []
        block_id: int
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

    def get_list_head_field(self, lh: int, type_)->int:
        addr_s: typing.Optional[int]
        addr_e: typing.Optional[int]
        ctype: typing.Optional[str]
        addr_s, addr_e, ctype = self.__class__.LIST_HEAD_STRUCT.get(type_, (None, None, None))
        if addr_s is None or addr_e is None or ctype is None:
            raise ValueError("get_list_head_field: unrecognized %s" % repr(type_))
        return struct.unpack(ctype, self.list_heads.buf[(self.__class__.LIST_HEAD_SIZE * lh) + addr_s : (self.__class__.LIST_HEAD_SIZE * lh) + addr_e])[0]

    def set_list_head_field(self, lh: int, data: int, type_: str):
        addr_s: typing.Optional[int]
        addr_e: typing.Optional[int]
        ctype: typing.Optional[str]
        addr_s, addr_e, ctype = self.__class__.LIST_HEAD_STRUCT.get(type_, (None, None, None))
        if addr_s is None or addr_e is None or ctype is None:
            raise ValueError("get_list_head_field: unrecognized %s" % repr(type_))

        # TODO: find a better way to calm mypy's annoyance at the following:
        self.list_heads.buf[(self.__class__.LIST_HEAD_SIZE * lh) + addr_s : (self.__class__.LIST_HEAD_SIZE * lh) + addr_e] = struct.pack(ctype, data) #type: ignore

    def get_meta(self, block: SharedMemory, type_: str):
        addr_s: typing.Optional[int]
        addr_e: typing.Optional[int]
        ctype: typing.Optional[str]
        addr_s, addr_e, ctype = self.__class__.META_STRUCT.get(type_, (None, None, None))
        if addr_s is None or addr_e is None or ctype is None:
            raise ValueError("get_meta: unrecognized %s" % repr(type_))
        return struct.unpack(ctype, block.buf[addr_s : addr_e])[0]

    def set_meta(self, block: SharedMemory, data, type_: str):
        addr_s: typing.Optional[int]
        addr_e: typing.Optional[int]
        ctype: typing.Optional[str]
        addr_s, addr_e, ctype = self.__class__.META_STRUCT.get(type_, (None, None, None))
        if addr_s is None or addr_e is None or ctype is None:
            raise ValueError("set_meta: unrecognized %s" % repr(type_))

        # TODO: find a better way to calm mypy's annoyance at the following:
        block.buf[addr_s : addr_e] = struct.pack(ctype, data) #type: ignore

    def get_data(self, block: SharedMemory, data_size: int)->bytes:
        return block.buf[self.__class__.META_BLOCK_SIZE:self.__class__.META_BLOCK_SIZE+data_size]

    def set_data(self, block: SharedMemory, data: bytes, data_size: int):
        # TODO: find a better way to calm mypy's annoyance at the following:
        block.buf[self.__class__.META_BLOCK_SIZE:self.__class__.META_BLOCK_SIZE+data_size] = data # type: ignore

    def init_list_head(self, lh: int):
        self.set_list_head_field(lh, 0, 'block_count')
        self.set_list_head_field(lh, self.__class__.RESERVED_BLOCK_ID, 'first_block')
        self.set_list_head_field(lh, self.__class__.RESERVED_BLOCK_ID, 'last_block')

    def get_block_count(self, lh: int)->int:
        return self.get_list_head_field(lh, 'block_count')

    def get_first_block(self, lh: int)->typing.Optional[int]:
        block_count: int = self.get_block_count(lh)
        if block_count == 0:
            return None

        block_id: int = self.get_list_head_field(lh, 'first_block')

        block_count -= 1
        if block_count == 0:
            self.init_list_head(lh)
        else:
            with self.block_locks[block_id]:
                next_block_id = self.get_meta(self.data_blocks[block_id], 'next_block_id')
            self.set_list_head_field(lh, next_block_id, 'first_block')
            self.set_list_head_field(lh, block_count, 'block_count')
        return block_id

    def add_block(self, lh: int, block_id: int):
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
                
    def get_free_block_count(self)->int:
        with self.free_list_lock:
            return self.get_block_count(self.__class__.FREE_LIST_HEAD)

    def get_first_free_block(self, block: bool, timeout: typing.Optional[float])->typing.Optional[int]:
        if self.free_list_semaphore is not None:
            self.free_list_semaphore.acquire(block=block, timeout=timeout)
        with self.free_list_lock:
            return self.get_first_block(self.__class__.FREE_LIST_HEAD)

    def add_free_block(self, block_id: int):
        with self.free_list_lock:
            self.add_block(self.__class__.FREE_LIST_HEAD, block_id)
        if self.free_list_semaphore is not None:
            self.free_list_semaphore.release()

    def get_msg_count(self)->int:
        with self.msg_list_lock:
            return self.get_block_count(self.__class__.MSG_LIST_HEAD)

    def get_first_msg(self, block: bool, timeout: typing.Optional[float])->typing.Optional[int]:
        if self.msg_list_semaphore is not None:
            self.msg_list_semaphore.acquire(block=block, timeout=timeout)
        with self.msg_list_lock:
            return self.get_first_block(self.__class__.MSG_LIST_HEAD)

    def add_msg(self, block_id: int):
        with self.msg_list_lock:
            self.add_block(self.__class__.MSG_LIST_HEAD, block_id)
        if self.msg_list_semaphore is not None:
            self.msg_list_semaphore.release()
        
    def generate_msg_id(self)->bytes:
        return ("%012x" % (self.mid_counter + 1)).encode('utf-8')

    def consume_msg_id(self):
        self.mid_counter += 1

    def next_writable_block_id(self, block: bool, timeout: typing.Optional[float], msg_id: bytes, src_pid: int)->int:
        looped: bool = False
        loop_cnt: int = 0
        time_start = time.time()
        while True:
            remaining_timeout: typing.Optional[float] = timeout
            if remaining_timeout is not None:
                remaining_timeout -= (time.time() - time_start)
                if remaining_timeout <= 0:
                    if self.verbose:
                        print("next_writable_block_id: qid=%d src_pid=%d: queue FULL (timeout)" % (self.qid, src_pid), file=sys.stderr, flush=True) # ***
                    raise Full

            block_id: typing.Optional[int] = self.get_first_free_block(block, remaining_timeout)
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

    def next_readable_msg(self, block: bool, timeout: typing.Optional[float]=None)->typing.Tuple[int, bytes, int, int, int]:
        i = 0
        time_start = time.time()
        while True:
            remaining_timeout: typing.Optional[float] = timeout
            if remaining_timeout is not None:
                remaining_timeout -= (time.time() - time_start)
                if remaining_timeout <= 0:
                    raise Empty
            block_id: typing.Optional[int] = self.get_first_msg(block=block, timeout=remaining_timeout)
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

    def put(self, msg: typing.Any, block: bool=True, timeout: typing.Optional[float]=None):

        """
        Put an object into a shaed memory queue.

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
            if timeout < 0:
                raise Full

        msg_id: bytes = self.generate_msg_id()
        src_pid: int = os.getpid()
        msg_body: bytes = self.serializer.dumps(msg) # type: ignore[union-attr]
        if self.integrity_check:
            total_msg_size: int = len(msg_body)
            msg2: typing.Any = self.serializer.loads(msg_body) # type: ignore[union-attr]
            if self.verbose:
                print("put: qid=%d src_pid=%d msg_id=%r: serialization integrity check is OK." % (self.qid, src_pid, msg_id), file=sys.stderr, flush=True) # ***
            
        total_chunks: int = math.ceil(len(msg_body) / self.chunk_size)
        if self.verbose:
            print("put: qid=%d src_pid=%d msg_id=%r: total_chunks=%d len(msg_body)=%d chunk_size=%d" % (self.qid, src_pid, msg_id, total_chunks, len(msg_body), self.chunk_size), file=sys.stderr, flush=True) # ***
        if self.watermark_check or self.verbose:
            if total_chunks > self.chunk_watermark:
                print("put: qid=%d src_pid=%d msg_id=%r: total_chunks=%d maxsize=%d new watermark" % (self.qid, src_pid, msg_id, total_chunks, self.maxsize), file=sys.stderr, flush=True) # ***
                self.chunk_watermark = total_chunks

        if self.deadlock_immanent_check and total_chunks > self.maxsize:
            raise ValueError("DEADLOCK IMMANENT: qid=%d src_pid=%d: total_chunks=%d > maxsize=%d" % (self.qid, src_pid, total_chunks, self.maxsize))
        
        time_start: float = time.time()

        # We acquire the producer lock to avoid deadlock if multiple
        # producers need multiple chunks each.
        lock_acquired: bool = self.producer_lock.acquire(timeout=timeout)
        if not lock_acquired:
            # We must have timed out.
            if self.verbose:
                print("put: qid=%d src_pid=%d msg_id=%r: queue FULL" % (self.qid, src_pid, msg_id), file=sys.stderr, flush=True) # ***
            raise Full

        block_id: int
        block_id_list: typing.List[int] = [ ]
        try:
            # In case we will process more than one chunk and this is a
            # nonblocking or timed out request, start by reserving all the
            # blocks that we will need.
            i: int
            for i in range(total_chunks):
                try:
                    remaining_timeout: typing.Optional[float] = timeout
                    if remaining_timeout is not None:
                        remaining_timeout -= (time.time() - time_start)
                        if remaining_timeout <= 0:
                            if self.verbose:
                                print("put: qid=%d src_pid=%d msg_id=%r: queue FULL" % (self.qid, src_pid, msg_id), file=sys.stderr, flush=True) # ***
                            raise Full

                    block_id = self.next_writable_block_id(block, remaining_timeout, msg_id, src_pid)
                    block_id_list.append(block_id)

                except Full:
                    # We failed to find a free block and/or a timeout occured.
                    # Release the reserved blocks.
                    if self.verbose:
                        print("put: qid=%d src_pid=%d msg_id=%r: releasing %d blocks" % (self.qid, src_pid, msg_id, len(block_id_list)), file=sys.stderr, flush=True) # ***
                    for block_id in block_id_list:
                        self.add_free_block(block_id)
                    raise

        finally:
            # Now that we have acquired the full set of chunks, we can release
            # the producer lock.  We don't want to hold it while we transfer
            # data into the blocks.
            if self.verbose:
                print("put: qid=%d src_pid=%d msg_id=%r: releasing producer lock" % (self.qid, src_pid, msg_id), file=sys.stderr, flush=True) # *** 
            self.producer_lock.release()

            # Consume this message ID.
            self.consume_msg_id()

        if self.verbose:
            print("put: qid=%d src_pid=%d msg_id=%r: acquired %d blocks" % (self.qid, src_pid, msg_id, total_chunks), file=sys.stderr, flush=True) # *** 

        # Now that we have a full set of blocks, build the
        # chunks:
        block_idx: int
        for block_idx, block_id in enumerate(block_id_list):
            chunk_id = block_idx + 1
            if self.verbose:
                print("put: qid=%d src_pid=%d msg_id=%r: chunk_id=%d of total_chunks=%d" % (self.qid, src_pid, msg_id, chunk_id, total_chunks), file=sys.stderr, flush=True) # *** 
               
            data_block: SharedMemory = self.data_blocks[block_id]
            chunk_data: bytes = msg_body[block_idx * self.chunk_size: (block_idx + 1) * self.chunk_size]
            msg_size: int = len(chunk_data)
            if self.verbose:
                print("put: qid=%d src_pid=%d msg_id=%r: chunk_id=%d: block_id=%d msg_size=%d." % (self.qid, src_pid, msg_id, chunk_id, block_id, msg_size), file=sys.stderr, flush=True) # ***
            if self.integrity_check:
                checksum: int = zlib.adler32(chunk_data)
                if self.verbose:
                    print("put: qid=%d src_pid=%d msg_id=%r: chunk_id=%d: checksum=%x total_msg_size=%d" % (self.qid, src_pid, msg_id, chunk_id, checksum, total_msg_size), file=sys.stderr, flush=True) # ***

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
                    self.set_meta(data_block, block_id_list[block_idx + 1], 'next_chunk_block_id')
                self.set_data(data_block, chunk_data, msg_size)

        # Now that the entire message has built, queue it:
        self.add_msg(block_id_list[0])
        if self.verbose:
            print("put: qid=%d src_pid=%d msg_id=%r: message sent" % (self.qid, src_pid, msg_id), file=sys.stderr, flush=True) # *** 

    def get(self, block: bool=True, timeout: typing.Optional[float]=None)->typing.Any:
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
        time_start: float = time.time()

        # We will build a list of message chunks.  We can't
        # release them until after we deserialize the data.
        block_id: int
        chunk_id: int
        msg_block_ids: typing.List[int] = [ ]
        data_block: SharedMemory
        
        try:
            remaining_timeout: typing.Optional[float] = timeout
            if remaining_timeout is not None:
                remaining_timeout -= (time.time() - time_start)
                if remaining_timeout <= 0:
                    if self.verbose:
                        print("put: qid=%d src_pid=%d msg_id=%r: queue EMPTY" % (self.qid, src_pid, msg_id), file=sys.stderr, flush=True) # ***
                    raise Empty

            src_pid: int
            msg_id: bytes
            total_chunks: int
            next_chunk_block_id: int
            src_pid, msg_id, block_id, total_chunks, next_chunk_block_id = self.next_readable_msg(block, remaining_timeout) # This call might raise Empty.
            if self.verbose:
                print("get: qid=%d src_pid=%d msg_id=%r: total_chunks=%d next_chunk_block_id=%d." % (self.qid, src_pid, msg_id, total_chunks, next_chunk_block_id), file=sys.stderr, flush=True) # ***
            msg_block_ids.append(block_id)

            # Acquire the chunks for the rest of the message:
            i: int
            for i in range(1, total_chunks):
                chunk_id = i + 1
                if self.verbose:
                    print("get: qid=%d src_pid=%d msg_id=%r: chunk_id=%d: block_id=%d." % (self.qid, src_pid, msg_id, chunk_id, next_chunk_block_id), file=sys.stderr, flush=True) # ***
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

        buf_msg_body: typing.List[bytes] = []
        try:
            block_idx: int
            for block_idx, block_id in enumerate(msg_block_ids):
                chunk_id = block_idx + 1
                data_block = self.data_blocks[block_id]
                with self.block_locks[block_id]:
                    msg_size: int = self.get_meta(data_block, 'msg_size')
                    if self.integrity_check:
                        if block_idx == 0:
                            total_msg_size: int = self.get_meta(data_block, 'total_msg_size')
                        checksum: int = self.get_meta(data_block, 'checksum')
                    chunk_data: bytes = self.get_data(data_block, msg_size) # This may make a reference, not a deep copy.
                if self.verbose:
                    print("get: qid=%d src_pid=%d msg_id=%r: chunk_id=%d: block_id=%d msg_size=%d total_chunks=%d." % (self.qid, src_pid, msg_id, chunk_id, block_id, msg_size, total_chunks), file=sys.stderr, flush=True) # ***
                if self.integrity_check:
                    checksum2: int = zlib.adler32(chunk_data)
                    if checksum == checksum2:
                        if self.verbose:
                            print("get: qid=%d src_pid=%d msg_id=%r: chunk_id=%d: checksum=%x is OK" % (self.qid, src_pid, msg_id, chunk_id, checksum), file=sys.stderr, flush=True) # ***
                    else:
                        raise ValueError("ShmQueue.get: qid=%d src_pid=%d msg_id=%r: chunk_id=%d: block_id=%d checksum=%x != checksum2=%x -- FAIL!" % (self.qid, src_pid, msg_id, chunk_id, block_id, checksum, checksum2)) # TODO: use a better exception

                buf_msg_body.append(chunk_data) # This may copy the reference.

            msg_body: bytes = b''.join(buf_msg_body) # Even this might copy the references.
            if self.integrity_check:
                if total_msg_size == len(msg_body):
                    if self.verbose:
                        print("get: qid=%d src_pid=%d msg_id=%r: total_msg_size=%d is OK" % (self.qid, src_pid, msg_id, total_msg_size), file=sys.stderr, flush=True) # ***
                else:
                    raise ValueError("get: qid=%d src_pid=%d msg_id=%r: total_msg_size=%d != len(msg_body)=%d -- FAIL!" % (self.qid, src_pid, msg_id, total_msg_size, len(msg_body))) # TODO: use a beter exception.

            try:
                # Finally, we are guaranteed to copy the data.
                msg: typing.Any = self.serializer.loads(msg_body)  # type: ignore[union-attr]

                # We could release the blocks here, but then we'd have to
                # release them in the except clause, too.

                return msg

            except pickle.UnpicklingError as e:
                print("get: Fail: qid=%d src_pid=%d msg_id=%r: msg_size=%d chunk_id=%d total_chunks=%d." % (self.qid, src_pid, msg_id, msg_size, chunk_id, total_chunks), file=sys.stderr, flush=True) # ***
                if self.integrity_check:
                    print("get: Fail: qid=%d src_pid=%d msg_id=%r: total_msg_size=%d checksum=%x" % (self.qid, src_pid, msg_id, total_msg_size, checksum), file=sys.stderr, flush=True) # ***
                raise
    
        finally:
            # It is now safe to release the data blocks.  This is a good place
            # to release them, because it covers error paths as well as the main return.
            if self.verbose:
                print("get: qid=%d src_pid=%d msg_id=%r: releasing %d blocks." % (self.qid, src_pid, msg_id, len(msg_block_ids)), file=sys.stderr, flush=True) # ***
            for block_id in msg_block_ids:
                self.add_free_block(block_id)
            msg_block_ids.clear()
            buf_msg_body.clear()

    def get_nowait(self)->typing.Any:
        """
        Equivalent to `get(False)`.
        """
        return self.get(False)

    def put_nowait(self, msg: typing.Any):
        """
        Equivalent to `put(obj, False)`.
        """
        return self.put(msg, False)

    def qsize(self)->int:
        return self.get_msg_count()

    def empty(self)->bool:
        return self.get_msg_count() == 0

    def full(self)->bool:
        return self.get_free_block_count() == 0

    def close(self):
        """
        Indicate no more new data will be added and release the shared memory.
        """
        block: SharedMemory
        for block in self.data_blocks:
            block.close()
            block.unlink()

        self.list_heads.close()
        self.list_heads.unlink()

    def __del__(self):
        pass
