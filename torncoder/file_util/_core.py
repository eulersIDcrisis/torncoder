"""_core.py.

Implementation of a simple File cache.

The simple cache is designed to be subclassed for more sophisticated
operations and different cache types.
"""
import os
import io
import uuid
import hashlib
from abc import abstractmethod, ABC
from collections import OrderedDict
from datetime import datetime, timezone
# Typing import
from typing import AsyncGenerator, Generator, Mapping, Union, Optional

from torncoder.utils import parse_header_date
# Local Imports

# Typing Helpers
DataContent = Union[bytes, bytearray, memoryview]


DEFAULT_CONTENT_TYPE = 'application/octet-stream'


class CacheError(Exception):
    """Error implying an issue with the cache."""


class FileInfo(object):

    @classmethod
    def from_http_headers(
            cls, key: str, internal_key: str=None,
            headers: Mapping[str, str]=None):
        content_type = headers.get(
            'Content-Type', DEFAULT_CONTENT_TYPE)
        e_tag = headers.get('ETag')
        last_modified_str = headers.get('Last-Modified')
        if last_modified_str:
            last_modified = parse_header_date(last_modified_str)
        else:
            last_modified = None
        
        size = headers.get('Content-Length', -1)
        size = int(size) if size else None

        return cls(
            key, internal_key, last_modified=last_modified, e_tag=e_tag,
            size=size, content_type=content_type
        )

    def __init__(self, key: str, internal_key: str =None,
                 last_modified: Optional[datetime] =None,
                 e_tag: Optional[str] =None, size: Optional[int] =None,
                 content_type: str =DEFAULT_CONTENT_TYPE,
                 metadata=None):
        # Store the delegate to proxy how the data is written to file.
        self._key = key
        # Store the key used for this item; the key is used to identify the
        # file in the cache to use.
        self._internal_key = internal_key
        # Store the access times for this field.
        self._created_at = datetime.utcnow()
        self._last_modified = last_modified or datetime.utcnow()
        self._last_accessed = datetime.utcnow()
        self._content_type = content_type
        self._etag = e_tag
        # Also store additional (custom) metadata here.
        self._metadata = metadata if metadata else {}
        # Store the size of the file.
        self._size = size or 0

    @property
    def key(self) -> str:
        """External key used to identify this file.

        Often, this is simply a relative path to the file.
        """
        return self._key

    @property
    def internal_key(self) -> str:
        """Internal key used to identify this file.

        This key is used internally and is passed to: AbstractFileDelegate
        when performing various operations. This key is intentionally
        different in order to support different delegates and should only
        ever be set by the underlying delegate or internally!
        """
        return self._internal_key

    @property
    def content_type(self):
        """The MIME type used for this header."""
        return self._content_type

    @property
    def created_at(self) -> datetime:
        return self._created_at

    @property
    def last_modified(self) -> datetime:
        """Returns the datetime this file was last modified.

        NOTE: This field is intended to be used by the various HTTP
        caching headers!
        """
        return self._last_modified

    @property
    def last_accessed(self):
        return self._last_accessed

    @property
    def size(self):
        """Return the length of the file, in bytes."""
        return self._size

    @property
    def e_tag(self):
        """Return the ETag (unique identifier) for the file.

        The delegate can decide how to set this if they so choose.
        """
        return self._etag

    def to_dict(self):
        """Return the info about this file as a JSON-serializable dict."""
        return dict(
            key=self.key, internal_key=self.internal_key,
            content_type=self.content_type, size=self.size,
            etag=self.e_tag
        )


def generate_path(path: str, key_level: int=0):
    path = path.lstrip('/')
    if key_level <= 0:
        return path
    if isinstance(path, str):
        path = path.encode('utf-8')
    if key_level == 1:
        return '{}/{}'.format(path[:2], path[2:])
    return '{}/{}/{}'.format(path[:2], path[2:4], path[4:])


class AbstractFileDelegate(ABC):
    """Base Interface for all things pertaining to async file management.

    This defines a high-level interface for managing files, as follows:
     - start_write(key): Start writing (open) a 'file' at the given key.
     - write(key, data): Append the data to the 'file' at the given key.
     - finish_write(key): Finish (flush?) data to the 'file' at the given key.
     - read_generator(key, start, end): Return an (async) interator that
            yields the data for this 'file' in the given start/end slice.

    As best as is reasonable, the rest of the utilities try to simplify
    file-management to this interface. Each call above is expected to be
    asynchronous and the interface is intentionally kept simple.
    """

    def generate_internal_key_from_path(self, path):
        """Generate and return an 'internal_key' for the given key.

        The internal key is the key that this delegate will pass as the 'key'
        argument for the other operations of this class.
        """
        # By default, just return a random key.
        return uuid.uuid1().hex

    @abstractmethod
    async def start_write(
            self, key: str, headers: Mapping[str, str]
        ) -> FileInfo:
        """Called when starting a write at this key.

        This operation should be thought of as "opening a file" for the
        first time.
        """
        pass

    @abstractmethod
    async def write(
        self, file_info: FileInfo, data: DataContent
    ) -> int:
        """Write/append the given data to the stream referenced by key.

        NOTE: Currently, this only supports "appending" to the end of the
        file; subclasses _could_ hypothetically seek themselves if more
        sophisticated behavior is needed.
        """
        pass

    @abstractmethod
    async def finish_write(self, file_info: FileInfo):
        """Called when done writing data for the given FileInfo.

        This operation should be thought of as "closing a file" or
        otherwise flushing its contents. The contents should still exist after
        this operation and be accessible by: `read_generator()`.
        """
        pass

    @abstractmethod
    async def read_generator(
        self, file_info: FileInfo,
        start: Optional[int]=None, end: Optional[int]=None
    ) -> AsyncGenerator[DataContent, None]:
        """Iterate over the data referenced by the given FileInfo.

        This iterates over the data in chunks. 'start' and 'end' are treated
        the same as python's "slice" notation. Also, the chunk size and so
        forth are subject to the delegate's implementation; callers will
        need to manually aggregate the results of this iterator if they want
        the data in a larger chunk.
        """
        pass

    @abstractmethod
    async def remove(self, file_info: FileInfo):
        pass

    # Helper to read data explicitly into a bytearray.
    async def read_into_bytes(
        self, file_info: FileInfo,
        start: Optional[int] =None, end: Optional[int] =None
    ) -> bytearray:
        """Helper to read data from an AbstractFileDelegate into a bytearray.

        This is a convenience to manage reading data from 'read_generator()'
        which might chunk the results based on the delegate's internal
        buffering. Subclasses can optionally override this if they have a
        more efficient way of managing this.
        """
        result = bytearray()
        async for chunk in self.read_generator(file_info, start, end):
            result.extend(chunk)
        return result


# Constant defining the number of bytes in one gigabyte.
ONE_GIGABYTE = 2 ** 30
ONE_HOUR = 60 * 60


class SimpleFileManager(object):
    """File Manager for reading and writing files.

    This is basically designed to be a key/value store where the content is
    written out to file instead of stored in memory (kind of like S3).
    Subclasses can implement this key/value store however they choose as long
    as they preserve this structure.

    This basic implementation stores the file information in an in-memory
    dictionary, and the files themselves in a manner prescribed by the passed
    delegate.
    """

    def __init__(
            self,
            delegate: AbstractFileDelegate,
            max_entry_size: Optional[int] =None,
            max_cache_size: Optional[int] =None,
            max_cache_count: Optional[int] =None):
        """Create a SimpleFileManager that stores files for the given delegate.

        This tracks the different files stored and permits simple 'vacuum' and
        quota operations on the resulting files, which are disabled if both
        'max_size' and 'max_count' are falsey/None.
        """
        self._delegate = delegate
        # Use an ordered dict for the cache.
        #
        # NOTE: The cache should be ordered based on the most likely items to
        # expire.
        self._cache_mapping = OrderedDict()

        # Cache status items.
        self._max_count = max_cache_count
        self._max_size = max_cache_size
        self._max_file_size = max_entry_size

        # Store the current (tracked) size of the file directory.
        self._current_size = 0

    @property
    def delegate(self) -> AbstractFileDelegate:
        """Return the underlying delegate for this manager."""
        return self._delegate

    @property
    def max_item_count(self) -> Optional[int]:
        """Return the count of tracked items/files in this manager."""
        return len(self._cache_mapping)

    @property
    def max_byte_count(self) -> Optional[int]:
        """Return byte count for the tracked items/files in this manager."""
        return self._current_size

    @property
    def max_file_size(self) -> Optional[int]:
        """Return the maximum number of bytes for a given file."""
        return self._max_file_size

    def initialize(self):
        pass

    def close(self):
        """Close this SimpleFileManager.

        NOTE: This will write out the current contents
        """
        pass

    def get_file_info(self, key: str) -> Optional[FileInfo]:
        """Return the FileInfo for the given key.

        If no entry exists for this key, None is returned instead.
        """
        return self._cache_mapping.get(key)

    def set_file_info(
        self, key: str, file_info: FileInfo
    ) -> Optional[FileInfo]:
        """Set the FileInfo to the given value

        If a previous entry existed at this key, it is returned.
        """
        old_info = self._cache_mapping.get(key)
        self._cache_mapping[key] = file_info
        # Update the current size of the cached data.
        self._current_size += file_info.size
        if old_info:
            self._current_size -= old_info.size

        # Return the old FileInfo object as applicable.
        return old_info

    async def remove_file_info_async(self, file_info: FileInfo) -> Optional[FileInfo]:
        item = self._cache_mapping.pop(file_info.key, None)
        if item:
            self._current_size -= item.size
            await self.delegate.remove(item)

    async def vacuum(self):
        """Vacuum and assert the constraints of the cache by removing items.

        This is designed to be run at some regular interval.
        """
        if self._max_count:
            while len(self._cache_mapping) > self._max_count:
                item = self._cache_mapping.popitem(last=True)
                await self.delegate.remove(item.internal_key)
        if self._max_size:
            while self._current_size > self._max_size:
                item = self._cache_mapping.popitem(last=True)
                await self.delegate.remove(item.internal_key)


#
# Core AbstractFileDelegate Implementations
#
class MemoryFileDelegate(AbstractFileDelegate):

    def __init__(self):
        self._stream_mapping = {}
        self._data_mapping = {}
        self._header_mapping = {}
        self._info_mapping = {}

    @property
    def keys(self):
        return list(self._data_mapping.keys())

    def get_file_info(self, key: str) -> Optional[FileInfo]:
        return self._info_mapping.get(key)

    def get_headers(self, key: str) -> Optional[Mapping[str, str]]:
        return self._header_mapping.get(key)

    # AbstractFileDelegate Overrides
    async def start_write(self, key: str, headers: Mapping[str, str]):
        self._stream_mapping[key] = io.BytesIO()
        self._header_mapping[key] = headers
        # For the 'memory' delegate, let's just blindly assign over the
        # internal_key as the key.
        internal_key = key
        # Parse the headers for FileInfo types?
        info = FileInfo.from_http_headers(key, internal_key, headers)
        self._info_mapping[key] = info
        return info

    async def write(self, file_info: FileInfo,
                    data: DataContent):
        stm = self._stream_mapping.get(file_info.key)
        if stm:
            stm.write(data)

    async def finish_write(self, file_info: FileInfo):
        key = file_info.key
        stm = self._stream_mapping.pop(key, None)
        if stm:
            self._data_mapping[key] = stm.getvalue()

    async def read_generator(
        self, file_info: FileInfo,
        start: Optional[int] =None, end: Optional[int] =None
    ) -> AsyncGenerator[DataContent, None]:
        key = file_info.key
        data = self._data_mapping.get(key)
        if not data:
            return
        if start is None:
            start = 0
        if end is None:
            end = len(data)
        while start < end:
            chunk = min(end - start, io.DEFAULT_BUFFER_SIZE)
            yield data[start:start + chunk]
            start += chunk

    async def remove(self, file_info: FileInfo):
        self._data_mapping.pop(file_info.key, None)
        self._stream_mapping.pop(file_info.key, None)
        self._info_mapping.pop(file_info.key, None)
        self._header_mapping.pop(file_info.key, None)


class SynchronousFileDelegate(AbstractFileDelegate):

    def __init__(self, root_path, key_level=1):
        self._root_path = root_path
        self._key_level = key_level
        self._stream_mapping = {}
        self._path_mapping = {}

    async def start_write(
        self, key: str, headers: Mapping[str, str]
    ) -> FileInfo:
        internal_key = os.path.join(self._root_path, key)
        info = FileInfo.from_http_headers(key, internal_key, headers)
        self._stream_mapping[key] = open(internal_key, 'wb')
        # self._path_mapping[key] = path
        return info

    async def write(self, file_info: FileInfo, data: DataContent):
        stm = self._stream_mapping.get(file_info.key)
        if not stm:
            raise CacheError('Stream is not set for the cache!')
        stm.write(data)

    async def finish_write(self, file_info: FileInfo):
        # Mark that the file has been fully written.
        stm = self._stream_mapping.get(file_info.key)
        if not stm:
            raise CacheError('Stream is not set for the cache!')
        stm.close()

    async def read_generator(
        self, file_info: FileInfo,
        start: Optional[int]=None, end: Optional[int]=None
    ) -> AsyncGenerator[DataContent, None]:
        # Wait for the file to be written before reading it back. This opens
        # the file locally and closes it when this context is exitted.
        with open(file_info.internal_key, 'rb') as stm:
            if start is not None:
                stm.seek(start)
            else:
                start = 0

            # When 'end is None', just read to the end of the file, then exit.
            if end is None:
                for line in stm:
                    yield line
                return
                # Assert that end > start. If not, just exit.
            elif end <= start:
                return

            # Otherwise, read start + (start - end) bytes and yield them.
            for line in stm:
                to_read = end - start
                if to_read <= 0:
                    return
                bytes_read = len(line)
                if bytes_read < to_read:
                    yield line
                    start += bytes_read
                else:
                    yield line[:to_read]

    async def remove(self, file_info):
        path = self._path_mapping.pop(file_info.key, None)
        if not path:
            return
        try:
            os.remove(path)
        except OSError:
            pass
