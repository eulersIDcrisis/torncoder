"""handlers.py.

'torncoder.handlers' module with some base file handlers and logic
that handle conventional GET/HEAD requests and so forth.
"""
import re
from tempfile import tempdir
from contextlib import AsyncExitStack
from typing import Any, Optional, Awaitable, Union

# Third-party Imports
from tornado import web, ioloop

# Local Imports
from torncoder.utils import parse_header_date, parse_range_header, logger
from torncoder.file_util import FileInfo, AbstractFileDelegate


ETAGS_FROM_IF_NONE_MATCH_REGEX = re.compile(r"\"(?P<etag>.+?)\",?")
"""Regex that should map the 'If-None-Match' header to a list of ETags.

Like most regexes, this one is a pain :/ It uses '.+?' where it does to avoid
greedy matches as well as an optional ',' character at the end to properly
split multiple match candidates from the header.
"""


def check_if_304(file_info, headers) -> bool:
    if file_info.e_tag:
        etag_values = headers.get("If-None-Match", "")
        if etag_values:
            matching_etags = ETAGS_FROM_IF_NONE_MATCH_REGEX.findall(etag_values)
            expected_match = file_info.e_tag.strip('"')
            # Check if the file_etag matches one of the values.
            # If there is a match, we should return 304.
            if expected_match in matching_etags:
                return True
    # After the ETag check, check Last-Modified.
    # NOTE: According to the spec, the ETag checks should take priority
    # over the Last-Modified checks.
    if file_info.last_modified:
        modified_since = headers.get("If-Modified-Since", "")
        if modified_since:
            modified_dt = parse_header_date(modified_since)
            if modified_dt >= file_info.last_modified:
                return True
    return False


def check_if_412(curr_file_info: FileInfo, headers) -> bool:
    """Check if the current headers should return a 412.

    This is supposed to check if the current FileInfo matches what is
    requested by the given headers; if not, then the request should
    return a 412. The details are described somewhat in the MDN docs here:
    https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/412
    """
    e_tag = headers.get("If-Match")
    if e_tag:
        # The requested ETag does not match the ETag of the current
        # entry; this should imply a 412.
        if curr_file_info.e_tag and curr_file_info.e_tag != e_tag:
            return True


def set_headers_for_file_info(
    req_handler: web.RequestHandler, file_info: FileInfo
) -> None:
    if file_info.e_tag:
        req_handler.set_header("ETag", file_info.e_tag)
    if file_info.last_modified:
        req_handler.set_header("Last-Modified", file_info.last_modified)
    if file_info.content_type:
        req_handler.set_header("Content-Type", file_info.content_type)


async def serve_get_from_file_info(
    delegate: AbstractFileDelegate,
    file_info: FileInfo,
    req_handler: web.RequestHandler,
    head_only: bool = False,
    ignore_caching: bool = False,
):
    # First, check the request headers and process them.
    request = req_handler.request

    # Set these headers regardless of anything since they pertain to the
    # content directly.
    set_headers_for_file_info(req_handler, file_info)
    # This should support partial requests, so add the Accept-Ranges header.
    req_handler.set_header("Accept-Ranges", "bytes")

    if not ignore_caching:
        if check_if_304(file_info, request.headers):
            req_handler.set_status(304)
            return

    # If only serving headers, return a 204 to explicitly avoid reading the
    # content body and just exit.
    if head_only:
        req_handler.set_status(204)
        # Cryptic: We need to flush() to explicitly write out the headers
        # that were set above; otherwise, tornado might try and override
        # some of these headers like Content-Length or similar since it
        # never received any content to actually write and would write
        # headers implying an empty response.
        await req_handler.flush()
        return

    # Support handling 'Range' header requests as well.
    content_range = request.headers.get("Range")
    partial_response = False
    if content_range:
        start, end = parse_range_header(content_range)
    else:
        start, end = None, None
    if end is None:
        if start is None:
            partial_response = False
        elif start == 0:
            # NOTE: If given a range like: 0-, then just assume a 200
            # status code instead, since this is basically asking for
            # the full content anyway.
            partial_response = False
        else:
            partial_response = True
    else:
        # Otherwise, we are stopping early, so the request is partial.
        partial_response = True
    if partial_response:
        req_handler.set_status(206)
    else:
        req_handler.set_status(200)
    async for chunk in delegate.read_generator(file_info, start=start, end=end):
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

    async def data_received(self, data: Union[bytes, memoryview, bytearray]) -> int:
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


class ReadonlyFileHandler(web.RequestHandler):
    """Basic handler that serves files from a file_manager.

    This handler supports the following API by default:
     - GET: Fetch the current content.
     - HEAD: Get content Metadata (same as GET without content).
    """

    def initialize(self, delegate: AbstractFileDelegate = None):
        self.delegate = delegate

    def send_status(self, status_code, message):
        self.set_status(status_code)
        self.write(dict(status_code=status_code, message=message))

    async def get(self, path):
        try:
            info = await self.delegate.get_file_info(path)
            if not info:
                self.set_status(404)
                self.write(dict(code=404, message="File not found!"))
                return
            # Proxy the request handling to the generalized call.
            await serve_get_from_file_info(self.delegate, info, self, head_only=False)
        except Exception:
            self.set_status(500)
            self.write(dict(code=500, message="Internal server error!"))

    async def head(self, path):
        try:
            info = await self.delegate.get_file_info(path)
            if not info:
                self.set_status(404)
                self.write(dict(code=404, message="File not found!"))
                return
            # Proxy the request handling to the generalized call.
            await serve_get_from_file_info(self.delegate, info, self, head_only=True)
        except Exception:
            self.set_status(500)
            self.write(dict(code=500, message="Internal server error!"))


@web.stream_request_body
class ServeFileHandler(ReadonlyFileHandler):
    """Basic handler that serves files from a file_manager.

    Unlike 'ReadonlyFileHandler', this _also_ supports PUT and DELETE
    requests to update and remove the content of files.

    This handler supports the following API by default:
     - GET: Fetch the current content.
     - HEAD: Get content Metadata (same as GET without content).
     - PUT: Create or Update the current content.
     - DELETE: Remove the current content.

    This handler expects exactly one argument to be passed via the
    'path' input. In other words, this route should be used like this:
    ```
    fm = SimpleFileManager()  # Or whatever
    app = web.Application([
        (r'/data/(?P<path>.+)', ServeFileHandler, dict(file_manager=fm)),
    ])
    ```
    """

    def initialize(self, delegate: AbstractFileDelegate = None):
        self.delegate = delegate
        self._error = None
        self._exit_stack = AsyncExitStack()
        # Stores the possible new FileInfo that might be uploaded.
        self._new_info = None
        # Stores the FileInfo that might currently exist.
        self._curr_info = None

    def on_finish(self):
        if self._exit_stack:
            exit_stack = self._exit_stack
            ioloop.IOLoop().current().add_callback(exit_stack.aclose)
            self._exit_stack = None

    def on_connection_close(self):
        if self._exit_stack:
            exit_stack = self._exit_stack
            ioloop.IOLoop().current().add_callback(exit_stack.aclose)
            self._exit_stack = None

    async def prepare(self):
        # Parse the path as the first argument.
        try:
            path = self.path_kwargs.get("path")
            if not path:
                path = self.path_args[0]
        except Exception:
            self.send_status(400, "Bad arguments!")
            return

        try:
            # If the request is a PUT, we are likely expecting a request
            # body, so initialize the file here.
            if self.request.method.upper() == "PUT":
                self._curr_info = await self.delegate.get_file_info(path)
                if self._curr_info:
                    if check_if_412(self._curr_info, self.request.headers):
                        self.send_status(412, "Precondition failed!")
                        return
                self._new_info = await self.delegate.start_write(
                    path, self.request.headers
                )
            else:
                # Fetch the current file info.
                self._curr_info = await self.delegate.get_file_info(path)
        except PermissionError:
            # Implies a READONLY situation. Return a 405.
            self.send_status(405, "This path is readonly!")
            self.finish()
            return
        except Exception:
            self.send_status(404, "File not found.")
            self.finish()
            return

    async def data_received(self, chunk: bytes):
        try:
            # If we are supposed to receive a file and there are no errors,
            # write the contents to the given key.
            if self._new_info and not self._error:
                await self.delegate.write(self._new_info, chunk)
        except Exception as exc:
            self._error = exc

    async def put(self, path):
        try:
            if self._error:
                self.send_status(400, "Invalid file upload!")
                return
            # Finish the write operation.
            info = await self.delegate.finish_write(self._new_info)

            # Set the ETag and Last-Modified headers (if appropriate) for
            # future reference and caching.
            if info.e_tag:
                self.set_header("ETag", info.e_tag)
            if info.last_modified:
                self.set_header("Last-Modified", info.last_modified)

            # If a previous entry existed, return a 200.
            if self._curr_info:
                self.send_status(200, "Successfully updated resource.")
            else:
                self.send_status(201, "Created new resource.")
        except Exception:
            logger.exception("Internal Error in PUT request!")
            self.send_status(500, "Internal Server Error")

    async def delete(self, path):
        try:
            if not self._curr_info:
                self.send_status(404, "File not found!")
                return
            await self.delegate.remove(self._curr_info)
            self.send_status(200, "File removed successfully.")
        except Exception:
            logger.exception("Error in DELETE: %s", self.request.uri)
            self.send_status(500, "Internal Server Error")
