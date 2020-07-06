import multiprocessing as mp
import multiprocessing.queues as mpq
from multiprocessing.shared_memory import SharedMemory
from queue import Full, Empty
import pickle
import math
import uuid
import struct


class ShmQueue(mpq.Queue):
    MAX_CHUNK_SIZE = 512 * 1024 * 1024  # system limit is 2G, 512MB is enough
    META_BLOCK_SIZE = 24

    # if msg_id is empty, the block is considered as empty
    EMPTY_MSG_ID = b'\x00' * 12
    META_STRUCT = {
        'msg_id': (0, 12, '12s'),
        'msg_size': (12, 16, 'I'),
        'chunk_id': (16, 20, 'I'),
        'total_chunks': (20, 24, 'I')
    }

    def __init__(self, chunk_size=1*1024*1024, maxsize=0, serializer=None):
        ctx = mp.get_context()

        super().__init__(maxsize, ctx=ctx)
        self.chunk_size = min(chunk_size, self.__class__.MAX_CHUNK_SIZE) \
            if chunk_size > 0 else self.__class__.MAX_CHUNK_SIZE

        self.serializer = serializer or pickle

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

    def get_meta(self, block, type_):
        addr_s, addr_e, ctype = self.__class__.META_STRUCT.get(type_)
        return struct.unpack(ctype, block.buf[addr_s : addr_e])[0]

    def set_meta(self, block, data, type_):
        addr_s, addr_e, ctype = self.__class__.META_STRUCT.get(type_)
        block.buf[addr_s : addr_e] = struct.pack(ctype, data)

    def generate_msg_id(self):
        while True:
            cand = str(uuid.uuid4())[-12:].encode('utf-8')
            if cand != self.__class__.EMPTY_MSG_ID:
                return cand

    def next_writable_block_id(self, block):
        i = 0
        while True:
            if self.get_meta(self.meta_blocks[i], 'msg_id') == self.__class__.EMPTY_MSG_ID:
                return i

            i += 1
            if i >= len(self.meta_blocks):
                if not block:
                    raise Full
                i = 0

    def next_readable_msg_id(self, block):
        i = 0
        while True:
            if self.get_meta(self.meta_blocks[i], 'msg_id') != self.__class__.EMPTY_MSG_ID:
                if self.get_meta(self.meta_blocks[i], 'chunk_id') == 1:
                    return self.get_meta(self.meta_blocks[i], 'msg_id')

            i += 1
            if i >= len(self.meta_blocks):
                if not block:
                    raise Empty
                i = 0

    def read_next_block_id(self, msg_id):
        i = 0
        while True:
            if self.get_meta(self.meta_blocks[i], 'msg_id') == msg_id:
                return i

            i += 1
            if i >= len(self.meta_blocks):
                i = 0

    # def debug_meta_block(self):
    #     for b in self.meta_blocks:
    #         print(bytes(b.buf[0:24]))

    def put(self, msg, block=True, timeout=None):
        msg_id = self.generate_msg_id()
        msg_body = self.serializer.dumps(msg)
        total_chunks = math.ceil(len(msg_body) / self.chunk_size)

        lock = self.producer_lock.acquire(timeout=timeout)
        if block and not lock:
            raise Full

        try:
            for i in range(total_chunks):
                block_id = self.next_writable_block_id(block)
                meta_block, data_block = self.meta_blocks[block_id], self.data_blocks[block_id]
                chunk_data = msg_body[i * self.chunk_size: (i + 1) * self.chunk_size]
                chunk_id = i + 1
                msg_size = len(chunk_data)

                with self.block_locks[block_id]:
                    self.set_meta(meta_block, msg_id, 'msg_id')
                    self.set_meta(meta_block, msg_size, 'msg_size')
                    self.set_meta(meta_block, chunk_id, 'chunk_id')
                    self.set_meta(meta_block, total_chunks, 'total_chunks')
                    data_block.buf[0:msg_size] = msg_body
        finally:
            self.producer_lock.release()

    def get(self, block=True, timeout=None):
        lock = self.consumer_lock.acquire(timeout=timeout)
        if block and not lock:
            raise Empty

        try:
            msg_id = self.next_readable_msg_id(block)
            while True:
                block_id = self.read_next_block_id(msg_id)
                meta_block, data_block = self.meta_blocks[block_id], self.data_blocks[block_id]

                msg_id = self.get_meta(meta_block, 'msg_id')
                msg_size = self.get_meta(meta_block, 'msg_size')
                chunk_id = self.get_meta(meta_block, 'chunk_id')
                total_chunks = self.get_meta(meta_block, 'total_chunks')

                if not self.buf_msg_id:
                    self.buf_msg_id = msg_id
                if not self.buf_msg_body:
                    self.buf_msg_body = [None] * total_chunks
                self.buf_msg_body[chunk_id-1] = data_block.buf[0:msg_size]

                if chunk_id == total_chunks:
                    msg = self.serializer.loads(b''.join(self.buf_msg_body))
                    self.buf_msg_id = None
                    self.buf_msg_body = None
                    with self.block_locks[block_id]:
                        self.set_meta(meta_block, self.__class__.EMPTY_MSG_ID, 'msg_id')
                    return msg
        finally:
            self.consumer_lock.release()

    def get_nowait(self):
        return self.get(False)

    def put_nowait(self, msg):
        return self.put(msg, False)

    def qsize(self):
        raise NotImplementedError

    def empty(self):
        raise NotImplementedError

    def full(self):
        raise NotImplementedError

    def close(self):
        for block in self.meta_blocks:
            block.close()
            block.unlink()
        for block in self.data_blocks:
            block.close()
            block.unlink()

    def __del__(self):
        pass
