"""core.py.

Core utilities for custom file handlers in torncoder.
"""
import os
import re
import abc
import asyncio
import hashlib
import logging
import email.utils
from datetime import datetime
from collections import namedtuple
# Import tornado
from tornado import web
from tornado.escape import utf8


logger = logging.getLogger('torncoder')
"""Define the logger for the 'torncoder' module."""


FileInfo = namedtuple("FileInfo", [
    "path", "size", "modified_utc_secs", "etag"
])
"""Tuple to store the file information as returned by 'fetch_file_info()'.

This is designed to return all of the different pieces that might be needed
to calculate the headers and so forth for 'FileHandler' subclasses. This is
preferred to returning each piece separately, in case some operations want to
take advantage of handling the operation in bulk.
"""

CHUNK_SIZE = 64 * 1024  # 64Kb


class AbstractFileHandler(web.RequestHandler, metaclass=abc.ABCMeta):
    """Basic file handler.

    NOTE: Much of the caching "implementation" details were borrowed from
    tornado; the license there is permissive enough for this, and also this is
    a library for 'tornado' as it is ;)
    """

    @abc.abstractmethod
    async def fetch_file_info(self, path: str):
        pass

    @abc.abstractmethod
    async def get_iter_content(self, path, start: int=None, end: int=None):
        """Return an async generator over the content of the file."""
        pass

    def _check_if_none_match(self, file_info):
        computed_etag = utf8(file_info.etag)
        # Find all weak and strong etag values from If-None-Match header
        # because RFC 7232 allows multiple etag values in a single header.
        etags = re.findall(
            br'\*|(?:W/)?"[^"]*"', utf8(
                self.request.headers.get("If-None-Match", ""))
        )
        if not computed_etag or not etags:
            return False
        if etags[0] == b"*":
            return True

        # Use a weak comparison when comparing entity-tags.
        def val(x: bytes) -> bytes:
            return x[2:] if x.startswith(b"W/") else x

        for etag in etags:
            if val(etag) == val(computed_etag):
                return True
        return False

    def _check_if_modified_since(self, file_info):
        # Check the If-Modified-Since, and don't send the result if the
        # content has not been modified
        ims_value = self.request.headers.get("If-Modified-Since")
        if ims_value is not None:
            date_tuple = email.utils.parsedate(ims_value)
            if date_tuple is not None:
                if_since = datetime(*date_tuple[:6])
                if if_since >= info.modified_utc_secs:
                    return True
        return False


    def should_return_304(self, file_info) -> bool:
        """Returns True if the headers indicate that we should return 304.

        .. versionadded:: 3.1
        """
        # If-None-Match takes priority.
        if self._check_if_none_match(file_info):
            return True
        return self._check_if_modified_since(file_info)

    def process_response_headers(self, file_info):
        size = file_info.size
        # Calculate the content-length, based on the requested range header,
        # if applicable. If the header does not exist, assume the full size.
        range_header = self.request.headers.get('Range')
        if range_header:
            rng = _parse_range_header(rng)
            start, end = rng
            if start is not None and start < 0:
                start = max(0, start + size)

            # Borrow what tornado.web.StaticFileHandler does for this.
            if (
                start is not None
                and (start >= size or (end is not None and start >= end))
            ) or end == 0:
                # As per RFC 2616 14.35.1, a range is not satisfiable only: if
                # the first requested byte is equal to or greater than the
                # content, or when a suffix with length 0 is specified.
                # https://tools.ietf.org/html/rfc7233#section-2.1
                # A byte-range-spec is invalid if the last-byte-pos value is
                # present and less than the first-byte-pos.
                self.set_status(416)  # Range Not Satisfiable
                self.set_header("Content-Type", "text/plain")
                self.set_header("Content-Range", "bytes */%s" % (size,))
                return
            if end is not None and end > size:
                # Clients sometimes blindly use a large range to limit their
                # download size; cap the endpoint at the actual file size.
                end = size
            # Note: only return HTTP 206 if less than the entire range has been
            # requested. Not only is this semantically correct, but Chrome
            # refuses to play audio if it gets an HTTP 206 in response to
            # ``Range: bytes=0-``.
            if size != (end or size) - (start or 0):
                self.set_status(206)  # Partial Content
                self.set_header(
                    "Content-Range", _get_content_range(start, end, size)
                )
        else:
            start = end = None

        # # Process the 'Content-Length' header.
        # if start is not None and end is not None:
        #     content_length = end - start
        # elif end is not None:
        #     content_length = end
        # elif start is not None:
        #     content_length = size - start
        # else:
        #     content_length = size
        # self.set_header("Content-Length", content_length)
        self.set_header('Etag', file_info.etag)

        # Return the requested range.
        return start, end

    async def head(self, path):
        try:
            info = await self.fetch_file_info(path)
            if self.should_return_304(info):
                self.set_status(304)
                self.finish()
                return
        except Exception as exc:
            logger.error("Invalid path: %s", exc)
            self.set_status(404)
            self.finish()
            return
        # For 'head' requests, just write the headers.
        try:
            self.set_status(200)
            self.process_response_headers(info)
        except Exception as exc:
            logger.exception('Error processing headers: %s', exc)
            # Since this is a HEAD request, no content should be written.
            # So, we can set the status code here. It might not be standard,
            # but it should be ok to flag the error differently than for the
            # GET request variant.
            self.set_status(500)

    async def get(self, path):
        try:
            info = await self.fetch_file_info(path)
            if self.should_return_304(info):
                self.set_status(304)
                return
        except Exception as exc:
            logger.error("Invalid path: %s", exc)
            self.set_status(404)
            return

        # For the GET request, write out the requested content.

        # At this point, fetch the content and write it out as expected.
        try:
            self.set_status(200)
            start, end = self.process_response_headers(info)
            async for chunk in self.get_iter_content(info.path, start, end):
                self.write(chunk)
                # Flush the buffer and write out the content.
                await self.flush()

            # Write out the content for the (parsed) range. If start and end
            # are both None, return the whole file.
        except Exception as exc:
            logger.exception('Unexpected error processing content: %s', exc)

            # NOTE: We probably can't send a 500 status code because it was
            # very likely already sent. So, just finish the request outright
            # and exit.
            self.finish()


#
# Common Utilities for File-Handling
#
def sanitize_file_path(root, path):
    # Necessary so that faulty paths with the same prefix as the root path
    # do not pass validation.
    if not root.endswith(os.path.sep):
        root += os.path.sep
    result_path = os.path.abspath(os.path.join(root, path))
    if result_path.startswith(root):
        return result_path
    raise ValueError("Invalid path outside of root directory!")


def fetch_file_info_basic(key, path):
    result = os.stat(path)
    modified_dt = datetime.fromtimestamp(result.st_mtime)
    file_size = str(result.st_size).encode('utf-8')

    m = hashlib.md5(utf8(key))
    m.update(modified_dt.isoformat().encode('utf-8'))
    m.update(file_size)
    etag = '"{}"'.format(m.hexdigest())
    return FileInfo(
        path, result.st_size, datetime.fromtimestamp(result.st_mtime),
        etag)


class BasicStaticFileHandler(AbstractFileHandler):
    """Basic file handler that serves files directly.

    This does NOT use AIO and otherwise functions identically to tornado's
    StaticFileHandler, but with different 'caching' Etag calculations.
    """

    def initialize(self, root_path=None, etag_cache_key='default'):
        self.root_path = root_path
        self.cache_key = etag_cache_key

    async def get_iter_content(self, path, start, end):
        if not start:
            start = 0
        with open(path, 'rb') as stm:
            if start:
                stm.seek(start)
            if end:
                remaining = end - start
            else:
                remaining = None
            while True:
                read_count = CHUNK_SIZE
                if remaining is not None and remaining < read_count:
                    read_count = remaining
                chunk = stm.read(CHUNK_SIZE)
                if not chunk:
                    # Reached EOF
                    return

                # Yield the current chunk.
                yield chunk
                if remaining is not None:
                    remaining -= len(chunk)

    async def fetch_file_info(self, path):
        path = sanitize_file_path(self.root_path, path)
        # Stat the file and extract the relevant fields.
        loop = asyncio.get_event_loop()
        info = await loop.run_in_executor(
            None, fetch_file_info_basic, self.cache_key, path)
        return info
