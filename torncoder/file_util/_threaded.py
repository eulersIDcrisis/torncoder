"""_aiofiles.py.

Specific overrides for the 'aiofiles' module.
"""
import io
import os
from typing import Mapping, Optional

# 'aiofiles' import
import aiofiles
import aiofiles.os

# Local Imports
from torncoder.utils import is_path_inside_directory
from torncoder.file_util._core import (
    FileInfo,
    AbstractFileDelegate,
    CacheError,
    force_abspath_inside_root_dir,
    create_file_info_from_os_stat,
)


class ThreadedFileDelegate(AbstractFileDelegate):
    def __init__(self, root_dir: str):
        super(ThreadedFileDelegate, self).__init__()
        self._root_dir = root_dir
        self._stream_mapping = dict()

    async def get_file_info(self, key: str) -> Optional[FileInfo]:
        try:
            path = force_abspath_inside_root_dir(self._root_dir, key)
            stat_result = await aiofiles.os.stat(path)
            return create_file_info_from_os_stat(key, path, stat_result)
        except Exception:
            return None

    async def start_write(self, key: str, headers: Mapping[str, str]):
        path = force_abspath_inside_root_dir(self._root_dir, key)
        info = FileInfo(key, path)
        stm = await aiofiles.open(path, "wb")
        self._stream_mapping[key] = stm
        return info

    async def write(self, file_info: FileInfo, data):
        stm = self._stream_mapping.get(file_info.key)
        if not stm:
            raise CacheError("No stream open for key: {}".format(file_info.key))
        return await stm.write(data)

    async def finish_write(self, file_info: FileInfo):
        stm = self._stream_mapping.get(file_info.key)
        if not stm:
            raise CacheError("No stream open for key: {}".format(file_info.key))
        await stm.close()
        return file_info

    async def read_generator(self, file_info, start=None, end=None):
        async with aiofiles.open(file_info.internal_key, "rb") as stm:
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
        assert is_path_inside_directory(self._root_dir, file_info.internal_key)
        await aiofiles.os.remove(file_info.internal_key)
