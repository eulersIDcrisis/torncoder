"""handlers.py.

'torncoder.handlers' module with some base file handlers and logic
that handle conventional GET/HEAD requests and so forth.
"""
import re
from tempfile import tempdir
from typing import Any, Optional, Awaitable, Union
# Third-party Imports
from tornado import web
# Local Imports
from torncoder.utils import parse_header_date, parse_range_header, logger
from torncoder.file_util import (
    FileInfo, SimpleFileManager, AbstractFileDelegate
)


ETAGS_FROM_IF_NONE_MATCH_REGEX = re.compile(r'\"(?P<etag>.+)\"')
"""Regex that should map the 'If-None-Match' header to a list of ETags."""


def check_if_304(file_info, headers):
    if file_info.etag:
        etag_values = headers.get('If-None-Match', '')
        if etag_values:
            matching_etags = ETAGS_FROM_IF_NONE_MATCH_REGEX.findall(
                etag_values
            )
            # Check if the file_etag matches one of the values.
            if file_info.etag in matching_etags:
                return True
    # After the ETag check, check Last-Modified.
    # NOTE: According to the spec, the ETag checks should take priority
    # over the Last-Modified checks.
    if file_info.last_modified:
        modified_since = headers.get('If-Modified-Since', '')
        if modified_since:
            modified_dt = parse_header_date(modified_since)
            if modified_dt <= file_info.last_modified:
                return True
    return False


async def serve_get_from_file_info(
        delegate: AbstractFileDelegate,
        file_info: FileInfo,
        req_handler: web.RequestHandler,
        head_only: bool =False,
        ignore_caching: bool=False):
    # First, check the request headers and process them.
    request = req_handler.request

    # Set these headers regardless of anything since they pertain to the
    # content directly.
    if file_info.etag:
        req_handler.set_header('ETag', file_info.etag)
    if file_info.last_modified:
        req_handler.set_header('Last-Modified', file_info.last_modified)
    if file_info.content_type:
        req_handler.set_header('Content-Type', file_info.content_type)
    # This should support partial requests, so add the Accept-Ranges header.
    req_handler.set_header('Accept-Ranges', 'bytes')

    if not ignore_caching:
        if check_if_304(file_info, request.headers):
            req_handler.set_status(304)
            return

    # If only serving headers, return a 204 to explicitly avoid reading the
    # content body and just exit.
    if head_only:
        req_handler.set_status(204)
        return

    # Support handling 'Range' header requests as well.
    content_range = request.headers.get('Range')
    partial_response = False
    if content_range:
        start, end = parse_range_header(content_range)
    else:
        start, end = None, None
    partial_response = not(start is None and end is None)
    if partial_response:
        req_handler.set_status(206)
    else:
        req_handler.set_status(200)
    async for chunk in delegate.read_generator(
            file_info.internal_key, start=start, end=end):
        # TODO -- How frequently should this await and flush?
        req_handler.write(chunk)
        await req_handler.flush()


class UploadFileProxy(object):
    """Object that helps upload a file to some AbstractFileDelegate."""

    def __init__(self, key: str, delegate: AbstractFileDelegate):
        self._key = key
        self._error = None
        self._delegate = delegate
        self._is_started = False
        self._is_finished = False

    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_value, tb):
        await self.aclose()

    def mark_error(self, exc):
        """Mark the current upload with an error.

        NOTE: This marks the upload as finished internally.
        """
        self._error = exc
        self._is_finished = True

    async def aclose(self):
        # Close and remove the file if the delegate started the
        # write, but did not finish.
        if self._is_started and not self._is_finished:
            try:
                await self._delegate.remove(self._key)
            except Exception:
                logger.exception("Error in remove after incomplete upload!")
        # Always mark the request as finished on a close operation.
        self._is_finished = True

    async def start(self):
        await self._delegate.start_write(self._key)
        self._is_started = True

    async def data_received(
        self, data: Union[bytes, memoryview, bytearray]
    ) -> int:
        try:
            if not self._error:
                await self._delegate.write(self._key, data)
        except Exception as exc:
            self.mark_error(exc)
        # Return the length of the data processed, even if we drop it.
        return len(data)

    async def finish(self):
        if not self._is_finished:
            await self._delegate.finish_write(self._key)
        self._is_finished = True


@web.stream_request_body
class ServeFileHandler(web.RequestHandler):
    """Basic handler that serves files from a file_manager.

    This handler supports the following API by default:
     - GET: Fetch the current content.
     - PUT: Create or Update the current content.
     - DELETE: Remove the current content.
     - HEAD: Get content Metadata (same as GET without content).

    This handler expects exactly one argument to be passed via the
    'path' input. In other words, this route should be used like this:
    ```
    fm = SimpleFileManager()  # Or whatever
    app = web.Application([
        (r'/data/(?P<path>.+)', ServeFileHandler, dict(file_manager=fm)),
    ])
    ```
    """

    def initialize(self, file_manager: SimpleFileManager =None):
        self.file_manager = file_manager
        self.delegate = file_manager.delegate
        self._internal_key = None
        self._error = None

    def send_status(self, status_code, message):
        self.set_status(status_code)
        self.write(dict(
            status_code=status_code, message=message
        ))

    async def prepare(self):
        # Parse the path as the first argument.
        try:
            path = self.path_kwargs.get('path')
            if not path:
                path = self.path_args[0]
        except Exception:
            self.send_status(400, "Bad arguments!")
            return

        # If the request is a PUT, we are likely expecting a request
        # body, so initialize the file here.
        if self.request.method.upper() == 'PUT':
            key = self.delegate.generate_internal_key_from_path(path)
            await self.delegate.start_write(key)
            self._internal_key = key

    async def data_received(self, chunk: bytes) -> Optional[Awaitable[None]]:
        try:
            # If we are supposed to receive a file and there are no errors,
            # write the contents to the given key.
            if self._internal_key and not self._error:
                await self.delegate.write(self._internal_key, chunk)
        except Exception as exc:
            self._error = exc

    async def put(self, path):
        try:
            if self._error:
                self.send_status(400, "Invalid file upload!")
                return
            # Finish the write operation.
            await self.delegate.finish_write(self._internal_key)

            self.send_status(200, "Success")
        except Exception:
            self.send_status(500, 'Internal Server Error')

    async def get(self, path):
        try:
            item = self.file_manager.get_file_info(path)
            if not item:
                self.set_status(404)
                self.write(dict(code=404, message="File not found!"))
                return
            # Proxy the request handling to the generalized call.
            await serve_get_from_file_info(
                self.file_manager.delegate, item, self,
                head_only=False)
        except Exception:
            self.set_status(500)
            self.write(dict(code=500, message="Internal server error!"))

    async def head(self, path):
        try:
            item = self.file_manager.get_file_info(path)
            if not item:
                self.set_status(404)
                self.write(dict(code=404, message="File not found!"))
                return
            # Proxy the request handling to the generalized call.
            await serve_get_from_file_info(
                self.file_manager.delegate, item, self,
                head_only=True)
        except Exception:
            self.set_status(500)
            self.write(dict(code=500, message="Internal server error!"))
