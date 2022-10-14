"""tornado.cache module.

Common Caching utilities.
"""
from torncoder.file_util._core import (
    # Abstract Classes
    AbstractFileDelegate,
    # Default Implementations
    MemoryFileDelegate, SynchronousFileDelegate,
    # File Manager Implementations
    FileInfo, SimpleFileManager
)
# Import the parser library utilities.
from torncoder.file_util._parser import (
    MultipartFormDataParser
)

#
# Specialized File Delegates
#
try:
    from torncoder.file_util._aiofile import NativeAioFileDelegate

    NATIVE_AIO_FILE_DELEGATE_ENABLED = True
except ImportError:
    # 'aiofile' likely could not be imported, so skip these.
    NATIVE_AIO_FILE_DELEGATE_ENABLED = False


try:
    from torncoder.file_util._threaded import ThreadedFileDelegate

    THREADED_FILE_DELEGATE_ENABLED = True
except ImportError:
    # 'aiofiles' likely could not be imported, so skip these.
    THREADED_FILE_DELEGATE_ENABLED = False
