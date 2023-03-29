"""_aiofile.py.

Specific overrides for the 'aiofile' module.

NOTE: The 'aiofile' module is distinct from the 'aiofiles' module':
 - 'aiofile' uses caio and kernel-level modules if supported, falling back
        to threadpools otherwise.
 - 'aiofiles' (plural) wraps synchronous file operations in a threadpool.
"""
import os
import asyncio
from typing import Optional

# AIOFile import
import aiofile

# Local Imports
from torncoder.file_util._core import (
    AbstractFileDelegate,
    CacheError,
    FileInfo,
    create_file_info_from_os_stat,
    force_abspath_inside_root_dir,
    is_path_inside_directory,
)


class NativeAioFileDelegate(AbstractFileDelegate):
    def __init__(self, root_dir: str):
        super(NativeAioFileDelegate, self).__init__()
        self._root_dir = root_dir

        self._stream_mapping = dict()
        self._path_mapping = dict()

    async def get_file_info(self, key: str) -> Optional[FileInfo]:
        try:
            path = force_abspath_inside_root_dir(self._root_dir, key)
            # Unfortunately, the aiofile driver does not have a way to do
            # this internally, so run this part in a thread.
            loop = asyncio.get_event_loop()
            stat_result = await loop.run_in_executor(None, os.stat, path)
            return create_file_info_from_os_stat(key, path, stat_result)
        except Exception:
            return None

    async def start_write(self, key, headers) -> FileInfo:
        internal_key = force_abspath_inside_root_dir(self._root_dir, key)
        if not internal_key:
            raise Exception("Cannot open file at: {}".format(internal_key))
        file_info = FileInfo(key, internal_key)
        stm = await aiofile.async_open(internal_key, "wb")
        self._path_mapping[key] = internal_key
        self._stream_mapping[key] = stm
        return file_info

    async def write(self, file_info, data):
        stm = self._stream_mapping.get(file_info.key)
        if not stm:
            raise CacheError("No stream open for key: {}".format(file_info.key))
        return await stm.write(data)

    async def finish_write(self, file_info):
        stm = self._stream_mapping.get(file_info.key)
        if not stm:
            raise CacheError("No stream open for key: {}".format(file_info.key))
        await stm.close()
        return file_info

    async def read_generator(self, file_info, start=None, end=None):
        assert is_path_inside_directory(self._root_dir, file_info.internal_key)
        async with aiofile.async_open(file_info.internal_key, "rb") as stm:
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

    async def remove(self, file_info):
        assert is_path_inside_directory(self._root_dir, file_info.internal_key)
        loop = asyncio.get_event_loop()
        # 'aiofile' does not offer a direct way to remove files, so just
        # defer this operation to the default threadpool.
        await loop.run_in_executor(None, os.remove, file_info.internal_key)
