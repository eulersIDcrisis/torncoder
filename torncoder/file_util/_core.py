"""simple.py.

Implementation of a simple File cache.

The simple cache is designed to be subclassed for more sophisticated
operations and different cache types.
"""
import os
import io
import uuid
from abc import abstractmethod, ABC
from collections import OrderedDict
from datetime import datetime
# Typing import
from typing import Union, Optional
# Local Imports


class CacheError(Exception):
    """Error implying an issue with the cache."""


class AbstractFileDelegate(ABC):

    def generate_internal_key_from_path(self, path):
        """Generate and return an 'internal_key' for the given key.

        The internal key is the key that this delegate will pass as the 'key'
        argument for the other operations of this class.
        """
        # By default, just return a random path.
        return uuid.uuid1().hex

    @abstractmethod
    async def start_write(self, internal_key: str):
        pass

    @abstractmethod
    async def write(self, internal_key: str,
                    data: Union[memoryview, bytes, bytearray]):
        pass

    @abstractmethod
    async def finish_write(self, internal_key: str):
        pass

    @abstractmethod
    async def read_generator(self, internal_key: str,
                             start: Optional[int]=None,
                             end: Optional[int]=None):
        pass

    @abstractmethod
    async def remove(self, key: str):
        pass


class FileInfo(object):

    def __init__(self, key: str, internal_key: str =None,
                 last_modified: Optional[datetime] =None,
                 etag: Optional[str] =None, size: Optional[int] =None,
                 content_type: str ='application/octet-stream'):
        # Store the delegate to proxy how the data is written to file.
        self._key = key
        # Store the key used for this item; the key is used to identify the
        # file in the cache to use.
        self._internal_key = internal_key
        # Store the access times for this field.
        self._created_at = datetime.utcnow()
        self._last_modified = datetime.utcnow()
        self._last_accessed = datetime.utcnow()
        self._content_type = content_type
        self._etag = None
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

    @property
    def last_accessed(self):
        return self._last_accessed

    @property
    def size(self):
        """Return the length of the file, in bytes."""
        return self._size

    @property
    def etag(self):
        """Return the ETag (unique identifier) for the file.

        The delegate can decide how to set this if they so choose.
        """
        return self._etag

    def to_dict(self):
        """Return the info about this file as a JSON-serializable dict."""
        return dict(
            key=self.key, internal_key=self.internal_key,
            content_type=self.content_type, size=self.size,
            etag=self.etag
        )


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
            max_size: Optional[int] =None,
            max_count: Optional[int] =None):
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
        self._max_count = max_count
        self._max_size = max_size

        # Store the current (tracked) size of the file directory.
        self._current_size = 0

    @property
    def delegate(self) -> AbstractFileDelegate:
        """Return the underlying delegate for this manager."""
        return self._delegate

    @property
    def item_count(self):
        """Return the count of tracked items/files in this manager."""
        return len(self._cache_mapping)

    @property
    def byte_count(self):
        """Return byte count for the tracked items/files in this manager."""
        return self._current_size

    def create_internal_key(self, key: str) -> str:
        """Create an internal_key for a new, future FileInfo object.

        This internal key is the _actual_ path to use for the file.
        By default, this will just be the original key, but different
        delegates might be better suited to handle different paths.
        """
        return uuid.uuid1().hex
        # Return the original key.
        return key.lstrip('/')

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

    async def remove_file_info_async(self, key: str) -> Optional[FileInfo]:
        item = self._cache_mapping.pop(key, None)
        if item:
            self._current_size -= item.size
            await self.delegate.remove(item.internal_key)

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

    def serialize_listing_to_stream(self, stm):
        """Serialize the current metadata to the given stream."""
        pass


#
# Core AbstractFileDelegate Implementations
#
class MemoryFileDelegate(AbstractFileDelegate):

    def __init__(self):
        self._stream_mapping = {}
        self._data_mapping = {}

    async def start_write(self, key):
        self._stream_mapping[key] = io.BytesIO()

    async def write(self, key, data):
        stm = self._stream_mapping.get(key)
        if stm:
            stm.write(data)

    async def finish_write(self, key):
        stm = self._stream_mapping.pop(key, None)
        if stm:
            self._data_mapping[key] = stm.getvalue()

    async def read_generator(self, key, start=None, end=None):
        data = self._data_mapping.get(key)
        if not data:
            return
        if start is None:
            start = 0
        if end is None:
            yield data[start:]
        else:
            yield data[start:end]

    async def remove(self, key):
        self._data_mapping.pop(key, None)


class SynchronousFileDelegate(AbstractFileDelegate):

    def __init__(self, root_path):
        self._root_path = root_path
        self._stream_mapping = {}
        self._path_mapping = {}

    async def start_write(self, key):
        path = os.path.join(self._root_path, key)
        self._stream_mapping[key] = open(path, 'wb')
        self._path_mapping[key] = path

    async def write(self, key, data):
        stm = self._stream_mapping.get(key)
        if not stm:
            raise CacheError('Stream is not set for the cache!')
        stm.write(data)

    async def finish_write(self, key):
        # Mark that the file has been fully written.
        stm = self._stream_mapping.get(key)
        if not stm:
            raise CacheError('Stream is not set for the cache!')
        stm.close()

    async def read_generator(self, key, start=None, end=None):
        # Wait for the file to be written before reading it back. This opens
        # the file locally and closes it when this context is exitted.
        path = self._path_mapping.get(key)
        if not path:
            return
        with open(path, 'rb') as stm:
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

    async def remove(self, key):
        path = self._path_mapping.pop(key, None)
        if not path:
            return
        try:
            os.remove(path)
        except OSError:
            pass
