"""_aiofile.py.

Specific overrides for the 'aiofile' module.

NOTE: The 'aiofile' module is distinct from the 'aiofiles' module':
 - 'aiofile' uses caio and kernel-level modules if supported, falling back
        to threadpools otherwise.
 - 'aiofiles' (plural) wraps synchronous file operations in a threadpool.
"""
import os
from contextlib import AsyncExitStack
# AIOFile import
import aiofile
# Local Imports
from torncoder.file_util._core import AbstractFileDelegate, CacheError


class NativeAioFileDelegate(AbstractFileDelegate):

    def __init__(self, root_dir):
        super(NativeAioFileDelegate, self).__init__()
        self._root_dir = root_dir

        self._stream_mapping = dict()
        self._path_mapping = dict()

    async def start_write(self, key):
        path = os.path.join(self._root_dir, key)
        stm = await aiofile.async_open(path, 'wb')
        self._path_mapping[key] = path
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
        async with aiofile.async_open(path, 'rb') as stm:
            if start is not None:
                stm.seek(start)
            else:
                start = 0

            # If 'end' isn't set, just iterate over everything to the end.
            if end is None:
                async for chunk in stm:
                    yield chunk
                return
            # Assert that end > start. Just exit if not.
            elif end <= start:
                return

            # If we get here, start < end and we've already seeked to start.
            # Iterate over the chunks until we reach the quota.
            #
            # NOTE: We need to create this reader explicitly like this so that
            # the resulting iterator properly respects the necessary offsets.            
            reader = aiofile.Reader(stm.file, offset=start)
            async for chunk in reader:
                to_read = end - start
                if to_read <= 0:
                    return

                bytes_read = len(chunk)
                if bytes_read < to_read:
                    yield chunk
                    start += bytes_read
                else:
                    yield chunk[:to_read]
                    return


    async def remove(self, key):
        pass
