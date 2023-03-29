"""_core.py.

Implementation of a simple File cache.

The simple cache is designed to be subclassed for more sophisticated
operations and different cache types.
"""
import os
import io
import uuid
import hashlib
import mimetypes
from contextlib import AsyncExitStack
from collections import OrderedDict
from abc import abstractmethod, ABC
from datetime import datetime

# Typing import
from typing import AsyncGenerator, Mapping, Union, Optional

from torncoder.utils import (
    parse_header_date,
    force_abspath_inside_root_dir,
    is_path_inside_directory,
)

# Local Imports

# Typing Helpers
DataContent = Union[bytes, bytearray, memoryview]


DEFAULT_CONTENT_TYPE = "application/octet-stream"


class CacheError(Exception):
    """Error implying an issue with the cache."""


if "md5" in hashlib.algorithms_available:

    def _default_hash_factory(initial_data: bytes):
        return hashlib.md5(initial_data)

else:

    def _default_hash_factory(initial_data: bytes):
        return hashlib.sha256(initial_data)


def calculate_content_type(path) -> str:
    mime_type, _ = mimetypes.guess_type(path)
    if mime_type:
        return mime_type
    return DEFAULT_CONTENT_TYPE


def calculate_etag_hash(version: bytes, content: bytes) -> str:
    """Calculate the ETag header value from the version and content."""
    hash_obj = _default_hash_factory(version)
    hash_obj.update(content)
    return hash_obj.hexdigest()


class FileInfo(object):
    @classmethod
    def from_http_headers(
        cls, key: str, internal_key: str = None, headers: Mapping[str, str] = None
    ):
        content_type = headers.get("Content-Type", DEFAULT_CONTENT_TYPE)
        e_tag = headers.get("ETag")
        last_modified_str = headers.get("Last-Modified")
        if last_modified_str:
            last_modified = parse_header_date(last_modified_str)
        else:
            last_modified = None

        size = headers.get("Content-Length", -1)
        size = int(size) if size else None

        return cls(
            key,
            internal_key,
            last_modified=last_modified,
            e_tag=e_tag,
            size=size,
            content_type=content_type,
        )

    def __init__(
        self,
        key: str,
        internal_key: str,
        last_modified: Optional[datetime] = None,
        e_tag: Optional[str] = None,
        size: Optional[int] = None,
        content_type: str = DEFAULT_CONTENT_TYPE,
        metadata=None,
    ):
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

    @content_type.setter
    def content_type(self, new_val):
        self._content_type = new_val

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

    @size.setter
    def size(self, new_val):
        assert new_val >= 0
        self._size = new_val

    @property
    def e_tag(self):
        """Return the ETag (unique identifier) for the file.

        The delegate can decide how to set this if they so choose.
        """
        return self._etag

    @e_tag.setter
    def e_tag(self, new_val):
        self._etag = new_val

    def to_dict(self):
        """Return the info about this file as a JSON-serializable dict."""
        return dict(
            key=self.key,
            internal_key=self.internal_key,
            content_type=self.content_type,
            size=self.size,
            etag=self.e_tag,
        )


def generate_path(path: str, key_level: int = 0):
    path = path.lstrip("/")
    if key_level <= 0:
        return path
    if isinstance(path, str):
        path = path.encode("utf-8")
    if key_level == 1:
        return "{}/{}".format(path[:2], path[2:])
    return "{}/{}/{}".format(path[:2], path[2:4], path[4:])


def create_file_info_from_os_stat(
    key: str,
    internal_key: str,
    stat_result: os.stat_result,
    content_type: Optional[str] = None,
    version: bytes = b"(not set)",
) -> FileInfo:
    """Create a FileInfo object after parsing the given stat results.

    This call will calculate the file size, last_modified, and an ETag value
    (based on 'last_modified'). This will also attempt to determine the file
    type based on the file extension using the 'mimetypes' library, though
    this should not necessarily be relied upon.
    """
    size = int(stat_result.st_size)
    last_modified = datetime.utcfromtimestamp(stat_result.st_mtime)
    etag = calculate_etag_hash(version, last_modified.isoformat().encode("utf-8"))
    if not content_type:
        content_type = calculate_content_type(internal_key)
    return FileInfo(key, internal_key, last_modified, etag, size, content_type)


class AbstractFileDelegate(ABC):
    """Base Interface for all things pertaining to async file management.

    This defines a high-level interface for managing files, as follows:
     - start_write(file_info): Start writing (open) a 'file' at the given key.
     - write(file_info, data): Append data to the 'file' at the given key.
     - finish_write(file_info): Finish (flush?) data to the 'file' at the
            given key. This should return the "finished" FileInfo as a result.
     - read_generator(key, start, end): Return an (async) interator that
            yields the data for this 'file' in the given start/end slice.
     - get_file_info(key): Return the FileInfo for the given key or None if no
            FileInfo exists for this key.

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
    async def get_file_info(self, key: str) -> Optional[FileInfo]:
        return None

    @abstractmethod
    async def start_write(self, key: str, headers: Mapping[str, str]) -> FileInfo:
        """Called when starting a write at this key.

        This operation should be thought of as "opening a file" for the
        first time.
        """
        pass

    @abstractmethod
    async def write(self, file_info: FileInfo, data: DataContent) -> int:
        """Write/append the given data to the stream referenced by key.

        NOTE: Currently, this only supports "appending" to the end of the
        file; subclasses _could_ hypothetically seek themselves if more
        sophisticated behavior is needed.
        """
        pass

    @abstractmethod
    async def finish_write(self, file_info: FileInfo) -> FileInfo:
        """Called when done writing, returning the finalized FileInfo.

        This operation should be thought of as "closing a file" or
        otherwise flushing its contents. The contents should still exist after
        this operation and be accessible by: `read_generator()`.

        Implementation Assumptions:
        This should return the "newest" form of the FileInfo, since the write
        operation was finished; this might update the 'e_tag' field (for
        example) if the value was calculated as a hash of the contents.
        """
        pass

    @abstractmethod
    async def read_generator(
        self,
        file_info: FileInfo,
        start: Optional[int] = None,
        end: Optional[int] = None,
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
        self,
        file_info: FileInfo,
        start: Optional[int] = None,
        end: Optional[int] = None,
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


#
# Core AbstractFileDelegate Implementations
#
class MemoryFileDelegate(AbstractFileDelegate):
    """FileDelegate that stores all of its contents in memory."""

    def __init__(self):
        self._stream_mapping = {}
        self._data_mapping = {}
        self._header_mapping = {}
        self._info_mapping = {}

    @property
    def keys(self):
        return list(self._data_mapping.keys())

    async def get_file_info(self, key: str) -> Optional[FileInfo]:
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

    async def write(self, file_info: FileInfo, data: DataContent):
        stm = self._stream_mapping.get(file_info.key)
        if stm:
            stm.write(data)

    async def finish_write(self, file_info: FileInfo):
        key = file_info.key
        stm = self._stream_mapping.pop(key, None)
        if stm:
            data = stm.getvalue()
            self._data_mapping[key] = data
            file_info.size = len(data)
        return self._info_mapping.get(key)

    async def read_generator(
        self,
        file_info: FileInfo,
        start: Optional[int] = None,
        end: Optional[int] = None,
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
            yield data[start : start + chunk]
            start += chunk

    async def remove(self, file_info: FileInfo):
        self._data_mapping.pop(file_info.key, None)
        self._stream_mapping.pop(file_info.key, None)
        self._info_mapping.pop(file_info.key, None)
        self._header_mapping.pop(file_info.key, None)


class SynchronousFileDelegate(AbstractFileDelegate):
    """Delegate that stores contents to the filesystem synchronously.

    This delegate does not _really_ handle file I/O operations async in cases
    that need higher performance, but is a suitable delegate for common usage.
    """

    def __init__(self, root_path, version=b"(not set)"):
        self._root_path = root_path
        self._stream_mapping = {}
        self._version = version

    async def get_file_info(self, key: str) -> Optional[FileInfo]:
        path = force_abspath_inside_root_dir(self._root_path, key)
        try:
            stat_result = os.stat(path)
            return create_file_info_from_os_stat(
                key, path, stat_result, version=self._version
            )
        except FileNotFoundError:
            return None

    async def start_write(self, key: str, headers: Mapping[str, str]) -> FileInfo:
        path = force_abspath_inside_root_dir(self._root_path, key)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        info = FileInfo.from_http_headers(key, path, headers)
        self._stream_mapping[key] = open(path, "wb")
        return info

    async def write(self, file_info: FileInfo, data: DataContent) -> int:
        stm = self._stream_mapping.get(file_info.key)
        if not stm:
            raise CacheError("Stream is not set for the cache!")
        return stm.write(data)

    async def finish_write(self, file_info: FileInfo) -> FileInfo:
        # Mark that the file has been fully written.
        stm = self._stream_mapping.get(file_info.key)
        if not stm:
            raise CacheError("Stream is not set for the cache!")
        stm.close()
        return file_info

    async def read_generator(
        self,
        file_info: FileInfo,
        start: Optional[int] = None,
        end: Optional[int] = None,
    ) -> AsyncGenerator[DataContent, None]:
        assert is_path_inside_directory(self._root_path, file_info.internal_key)

        # Wait for the file to be written before reading it back. This opens
        # the file locally and closes it when this context is exitted.
        with open(file_info.internal_key, "rb") as stm:
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
        assert is_path_inside_directory(self._root_path, file_info.internal_key)
        try:
            os.remove(file_info.internal_key)
        except OSError:
            pass


#
# Compound Delegates
#
# These delegates have the same AbstractFileDelegate interface, but they
# are tailored to store additional metadata that might not strictly be
# attached to the filesystem. These delegates can also implement custom
# functionality on top of another delegate, such as vacuuming, mandating
# READONLY behavior, etc.
#
class ReadOnlyDelegate(AbstractFileDelegate):
    """Delegate that wraps another delegate to prevent write operations.

    This delegate simply wraps another delegate, but will raise Exceptions
    when write operations are attempted. This can be useful to mandate some
    types of permission access in some cases.
    """

    def __init__(self, delegate: AbstractFileDelegate):
        self._parent = delegate

    async def start_write(self, key: str, headers: Mapping[str, str]) -> FileInfo:
        raise PermissionError("Delegate is readonly!")

    async def write(self, file_info: FileInfo, data: DataContent) -> int:
        raise PermissionError("Delegate is readonly!")

    async def finish_write(self, file_info: FileInfo) -> FileInfo:
        raise PermissionError("Delegate is readonly!")

    async def remove(self, file_info: FileInfo):
        raise PermissionError("Delegate is readonly!")

    async def get_file_info(self, key: str) -> Optional[FileInfo]:
        return await self._parent.get_file_info(key)

    async def read_generator(
        self,
        file_info: FileInfo,
        start: Optional[int] = None,
        end: Optional[int] = None,
    ) -> AsyncGenerator[DataContent, None]:
        async for chunk in self._parent.read_generator(file_info, start, end):
            yield chunk

    async def read_into_bytes(
        self,
        file_info: FileInfo,
        start: Optional[int] = None,
        end: Optional[int] = None,
    ) -> bytearray:
        return await self._parent.read_into_bytes(file_info, start, end)


class SimpleCacheFileDelegate(AbstractFileDelegate):
    """Delegate that is intended to be used as a (temporary) cache."""

    def __init__(
        self,
        parent_delegate: AbstractFileDelegate,
        max_entries: Optional[int] = None,
        max_size: Optional[int] = None,
        key_level: int = 0,
    ):
        # Store the delegate to proxy the requests to.
        self._delegate = parent_delegate
        self._key_level = key_level
        self._max_entries = max_entries
        self._max_size = max_size
        self._curr_size = 0
        # Store an index of the current files in the store.
        self._info_mapping = OrderedDict()
        self._write_mapping = dict()
        # typing: Mapping[str, Tuple[FileInfo, Any]]

    async def get_file_info(self, key: str) -> Optional[FileInfo]:
        curr_info, _ = self._info_mapping.get(key, (None, None))
        return curr_info

    async def start_write(self, key: str, headers: Mapping[str, str]) -> FileInfo:
        internal_key = uuid.uuid4().hex
        # Create a new key dynamically.
        result_info = FileInfo(
            key,
            internal_key,
            e_tag=internal_key,
            content_type=headers.get("Content-Type", DEFAULT_CONTENT_TYPE),
        )
        async with AsyncExitStack() as exit_stack:
            internal_info = await self._delegate.start_write(internal_key, headers)
            exit_stack.push_async_callback(self._delegate.remove, internal_info)
            exit_stack.push_async_callback(self._delegate.finish_write, internal_info)

            if key in self._write_mapping:
                raise FileExistsError("Key {} already pending write.".format(key))
            self._write_mapping[key] = (result_info, internal_info)
            # If we get here, the pending write was added. Pop off all of the
            # error callbacks appended above.
            exit_stack.pop_all()
        return result_info

    async def write(self, file_info: FileInfo, data: DataContent) -> int:
        _, internal_info = self._write_mapping.get(file_info.key, (None, None))
        if not internal_info:
            raise FileNotFoundError(
                "No writes started for key: {}".format(file_info.key)
            )
        return await self._delegate.write(internal_info, data)

    async def finish_write(self, file_info: FileInfo) -> FileInfo:
        # Finish the write, and update the FileInfo as appropriate.
        try:
            curr_info, internal_info = self._write_mapping.get(
                file_info.key, (None, None)
            )
            if not curr_info:
                raise FileNotFoundError(
                    "No writes started for key: {}".format(file_info.key)
                )
            # 'curr_info' is the latest FileInfo now that the underlying
            # delegate finished writing the file. For our purposes, this
            # will be ignored since the etag and so forth is tracked
            # separately.
            internal_info = await self._delegate.finish_write(internal_info)
            self._info_mapping[file_info.key] = (curr_info, internal_info)
            # Update the current size.
            self._curr_size += internal_info.size
            return curr_info
        finally:
            # Remove the current info from the write_mapping because the
            # write has finished, one way or another.
            self._write_mapping.pop(file_info.key, None)

    async def read_generator(
        self,
        file_info: FileInfo,
        start: Optional[int] = None,
        end: Optional[int] = None,
    ) -> AsyncGenerator[DataContent, None]:
        _, internal_info = self._info_mapping.get(file_info.key, (None, None))
        if not internal_info:
            return
        async for chunk in self._delegate.read_generator(internal_info, start, end):
            yield chunk

    async def remove(self, file_info: FileInfo):
        try:
            _, internal_info = self._info_mapping.get(file_info.key, (None, None))
            if internal_info:
                await self._delegate.remove(internal_info)
                self._curr_size -= internal_info.size
        finally:
            self._info_mapping.pop(file_info.key, None)

    # Helper to read data explicitly into a bytearray.
    async def read_into_bytes(
        self,
        file_info: FileInfo,
        start: Optional[int] = None,
        end: Optional[int] = None,
    ) -> bytearray:
        _, internal_info = self._info_mapping.get(file_info.key, (None, None))
        # Use the parent delegate's version of the call in case there are
        # optimizations implemented.
        if not internal_info:
            return b""
        return await self._delegate.read_into_bytes(internal_info, start, end)

    async def vacuum(self):
        """Run a vacuum operation to try to preserve the state of the cache."""
        if not self._info_mapping:
            return
        if self._max_size:
            while self._curr_size > self._max_size:
                _, info_tuple = self._info_mapping.popitem(last=False)
                internal_info = info_tuple[1]
                await self._delegate.remove(internal_info)
                self._curr_size -= internal_info.size

        if self._max_entries:
            while len(self._info_mapping) > self._max_entries:
                _, info_tuple = self._info_mapping.popitem(last=False)
                internal_info = info_tuple[1]
                await self._delegate.remove(internal_info)
                self._curr_size -= internal_info.size
