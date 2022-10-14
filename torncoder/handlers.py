"""handlers.py.

'torncoder.handlers' module with some base file handlers and logic
that handle conventional GET/HEAD requests and so forth.
"""
import re
from typing import Any
# Third-party Imports
from tornado import web
# Local Imports
from torncoder.utils import parse_header_date, parse_range_header
from torncoder.file_util import FileInfo, AbstractFileDelegate


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


class ServeFileHandler(web.RequestHandler):
    """Basic handler that serves files from a file_manager.

    This handler expects exactly one argument to be passed via the
    'path' input. In other words, this route should be used like this:
    ```
    fm = SimpleFileManager()  # Or whatever
    app = web.Application([
        (r'/data/(?P<path>.+)', ServeFileHandler, dict(file_manager=fm)),
    ])
    ```
    """

    def initialize(self, file_manager=None):
        self.file_manager = file_manager

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
