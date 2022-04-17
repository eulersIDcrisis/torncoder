"""core.py.

Implementation of an AIO-capable file handler in torncoder.
"""
import asyncio
# Third-party Imports
import aiofile
# Local Imports
from torncoder.file_util.core import (
    AbstractFileHandler, sanitize_file_path, fetch_file_info_basic
)


class BasicAIOFileHandler(AbstractFileHandler):
    """Basic file handler that serves asynchronously serves files."""

    def initialize(self, root_path=None, etag_cache_key='default'):
        self.root_path = root_path
        self.cache_key = etag_cache_key

    async def fetch_file_info(self, path):
        path = sanitize_file_path(self.root_path, path)
        # Stat the file and extract the relevant fields.
        loop = asyncio.get_event_loop()
        info = await loop.run_in_executor(
            None, fetch_file_info_basic, self.cache_key, path)
        return info

    async def get_iter_content(self, path, start, end):
        async with aiofile.async_open(path, 'rb') as stm:
            if start:
                stm.seek(start)
            async for chunk in stm.iter_chunked():
                yield chunk
