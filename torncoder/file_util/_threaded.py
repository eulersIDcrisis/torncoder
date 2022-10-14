"""_aiofiles.py.

Specific overrides for the 'aiofiles' module.
"""
import os
import hashlib
from contextlib import AsyncExitStack
# 'aiofiles' import
import aiofiles
# Local Imports
from tornproxy.file_util._core import AbstractFileDelegate, CacheError


DEFAULT_BUFF_SIZE = 4096
"""Default buffer size to assume when loading data in chunks."""


class ThreadedFileDelegate(AbstractFileDelegate):

    def __init__(self, root_dir):
        super(ThreadedFileDelegate, self).__init__()
        self._root_dir = root_dir
        self._stream_mapping = dict()

    async def start_write(self, key):
        path = os.path.join(self._root_dir, key)
        stm = await aiofiles.open(path, 'wb')
        self._stream_mapping[key] = stm

    async def write(self, key, data):
        stm = self._stream_mapping.get(key)
        if not stm:
            raise CacheError('No stream open for key: {}'.format(key))
        return await stm.write(data)

    async def finish_write(self, key):
        stm = self._stream_mapping.get(key)
        if not stm:
            raise CacheError('No stream open for key: {}'.format(key))
        await stm.close()

    async def read_generator(self, key, start=None, end=None):
        path = os.path.join(self._root_dir, key)
        async with aiofiles.open(path, 'rb') as stm:
            if start is not None:
                await stm.seek(start)
            else:
                start = 0

            # If 'end is None', read till the end of the file.
            if end is None:
                while True:
                    chunk = await stm.read(DEFAULT_BUFF_SIZE)
                    # Indicates the end of the file.
                    if len(chunk) <= 0:
                        return
                    yield chunk
                return

            # Otherwise, iterate over the chunks until we reach 'end'.
            # Just exit if end < start already.
            if end - start <= 0:
                return

            while True:
                to_read = end - start
                if to_read <= 0:
                    return

                chunk = await stm.read(DEFAULT_BUFF_SIZE)
                # Indicates EOF
                if len(chunk) <= 0:
                    return

                bytes_read = len(chunk)
                if bytes_read > to_read:
                    yield chunk[:to_read]
                    return
                # Otherwise, just keep going.
                yield chunk
                start += bytes_read

    async def remove(self, key):
        pass
