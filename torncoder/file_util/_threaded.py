"""_aiofiles.py.

Specific overrides for the 'aiofiles' module.
"""
import io
import os
import hashlib
from contextlib import AsyncExitStack
from typing import Mapping
# 'aiofiles' import
import aiofiles
import aiofiles.os
# Local Imports
from torncoder.file_util._core import (
    FileInfo, AbstractFileDelegate, CacheError
)


class ThreadedFileDelegate(AbstractFileDelegate):

    def __init__(self, root_dir: str):
        super(ThreadedFileDelegate, self).__init__()
        self._root_dir = root_dir
        self._stream_mapping = dict()

    async def start_write(self, key: str, headers: Mapping[str, str]):
        internal_key = os.path.join(self._root_dir, key)
        info = FileInfo(key, internal_key)
        stm = await aiofiles.open(internal_key, 'wb')
        self._stream_mapping[key] = stm
        return info

    async def write(self, file_info: FileInfo, data):
        stm = self._stream_mapping.get(file_info.key)
        if not stm:
            raise CacheError('No stream open for key: {}'.format(
                file_info.key))
        return await stm.write(data)

    async def finish_write(self, file_info: FileInfo):
        stm = self._stream_mapping.get(file_info.key)
        if not stm:
            raise CacheError('No stream open for key: {}'.format(
                file_info.key))
        await stm.close()

    async def read_generator(self, file_info, start=None, end=None):
        async with aiofiles.open(file_info.internal_key, 'rb') as stm:
            if start is not None:
                await stm.seek(start)
            else:
                start = 0

            # If 'end is None', read till the end of the file.
            if end is None:
                while True:
                    chunk = await stm.read(io.DEFAULT_BUFFER_SIZE)
                    # Indicates the end of the file.
                    if len(chunk) <= 0:
                        return
                    yield chunk

            while end > start:
                to_read = min(end - start, io.DEFAULT_BUFFER_SIZE)
                chunk = await stm.read(to_read)
                start += to_read
                yield chunk

    async def remove(self, file_info):
        await aiofiles.os.remove(file_info.internal_key)
