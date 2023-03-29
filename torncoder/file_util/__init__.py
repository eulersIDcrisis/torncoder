"""tornado.file_util module.

Common Caching utilities.
"""
from torncoder.file_util._core import (
    # Core Classes
    AbstractFileDelegate,
    FileInfo,
    # Default Implementations
    MemoryFileDelegate,
    SynchronousFileDelegate,
    # Compound Delegates
    SimpleCacheFileDelegate,
    ReadOnlyDelegate,
)

# Import the parser library utilities.
from torncoder.file_util._parser import MultipartFormDataParser

#
# Specialized File Delegates
#
# Register the available file delegates.
_ENGINE_MAPPING = {"synchronous": SynchronousFileDelegate, "memory": MemoryFileDelegate}

try:
    from torncoder.file_util._aiofile import NativeAioFileDelegate

    _ENGINE_MAPPING["aio"] = NativeAioFileDelegate

    NATIVE_AIO_FILE_DELEGATE_ENABLED = True
except ImportError:
    # 'aiofile' likely could not be imported, so skip these.
    NATIVE_AIO_FILE_DELEGATE_ENABLED = False


try:
    from torncoder.file_util._threaded import ThreadedFileDelegate

    _ENGINE_MAPPING["threaded"] = ThreadedFileDelegate

    THREADED_FILE_DELEGATE_ENABLED = True
except ImportError:
    # 'aiofiles' likely could not be imported, so skip these.
    THREADED_FILE_DELEGATE_ENABLED = False


def get_available_delegate_types():
    return list(_ENGINE_MAPPING.keys())


def create_delegate(delegate_type, *args, **kwargs):
    engine_type = _ENGINE_MAPPING.get(delegate_type)
    if not engine_type:
        raise ValueError("Unrecognized delegate type!")
    return engine_type(*args, **kwargs)
