"""file_cache_server.py.

Basic Fileserver implementation with the following REST API:
 - GET /data/<path>: Get the file at <path>
 - HEAD /data/<path>: Get file info (headers) at <path>
 - DELETE /data/<path> : Remove the file path <path>
 - PUT /upload/<path>: Upload a file to <path>. File should then be
        accessible via: GET /data/<path>
"""
from contextlib import AsyncExitStack
import os
import signal
import logging
import hashlib
import argparse
from datetime import datetime
from typing import Any
# Third-party Imports
from tornado import web, ioloop, httpserver
# Local Imports
from torncoder.utils import logger
from torncoder.file_util import (
    NATIVE_AIO_FILE_DELEGATE_ENABLED, SimpleFileManager,
    SynchronousFileDelegate, FileInfo, get_available_delegate_types,
    create_delegate
)
from torncoder.handlers import (
    serve_get_from_file_info,
    ServeFileHandler
)


class BaseHandler(web.RequestHandler):

    def write_error(self, status_code: int, **kwargs: Any) -> None:
        self.set_status(status_code)
        if status_code == 405:
            message = 'Method Not Allowed!'
        elif status_code == 400:
            message = 'Bad Arguments!'
        elif status_code == 500:
            message = 'Internal Server Error!'
        else:
            message = 'Unknown Error!'
        self.send_status(status_code, message)

    def send_status(self, status_code: int, message: str) -> None:
        self.set_status(status_code)
        self.write(dict(
            code=status_code, message=message
        ))


class FileInfoHandler(BaseHandler):

    def initialize(self, file_manager=None):
        self.file_manager = file_manager

    async def head(self, path):
        try:
            info = self.file_manager.get_file_info(path)
            if not info:
                self.send_status(404, 'File not found: {}'.format(path))
                return
            await serve_get_from_file_info(
                self.file_manager.delegate, info, self, head_only=True)
            return
        except Exception:
            logger.exception("Unknown error in HEAD: %s", path)
            self.set_status(500)
            self.write(dict(
                code=500, message='Internal Server Error'
            ))

    async def get(self, path):
        try:
            info = self.file_manager.get_file_info(path)
            if not info:
                self.send_status(404, 'File not found: {}'.format(path))
                return
            await serve_get_from_file_info(
                self.file_manager.delegate, info, self, head_only=False)
            return
        except Exception:
            logger.exception("Unknown error in GET: %s", path)
            self.send_status(500, 'Internal Server Error')

    async def delete(self, path):
        try:
            await self.file_manager.remove_file_info_async(path)
            self.send_status(200, "File removed at path: {}".format(path))
        except Exception:
            logger.exception("Unknown error in DELETE %s", path)
            self.send_status(500, "Internal Server Error")


@web.stream_request_body
class FileUploadHandler(BaseHandler):

    def initialize(self, file_manager=None):
        self.file_manager = file_manager
        self.delegate = self.file_manager.delegate
        self.internal_key = None
        self._exit_stack = AsyncExitStack()

    # HACKY: Tornado doesn't seem to support these calls being coroutines,
    # so we'll manually add this cleanup code to the IOLoop to run.
    def on_connection_close(self):
        ioloop.IOLoop.current().add_callback(self._exit_stack.aclose)

    def on_finish(self):
        ioloop.IOLoop.current().add_callback(self._exit_stack.aclose)

    async def prepare(self):
        try:
            path = self.path_kwargs.get('path')
            if not path:
                self.path_args[0]
        except Exception:
            self.send_status(400, 'Invalid arguments!')
            return
        self.internal_key = self.file_manager.create_internal_key(path)
        self.size = 0
        self.content_type = self.request.headers.get(
            'Content-Type', 'application/octet-stream'
        )
        self._hash = hashlib.sha1()
        # Start the write, and register that this should remove the key upon
        # exit unless it completes successfully.
        try:
            await self.delegate.start_write(self.internal_key)
            # Register the file for removal, in case the connection is closed
            # midtransfer. If the upload completes, we'll clear this and reset
            # the callbacks properly.
            self._exit_stack.push_async_callback(
                self.delegate.remove, self.internal_key)
            self._exit_stack.push_async_callback(
                self.delegate.finish_write,
                self.internal_key)
        except Exception:
            logger.exception('Error in upload: %s', path)
            self.send_status(500, 'Internal Server Error!')
            return

    async def data_received(self, chunk: bytes):
        self.size += len(chunk)
        self._hash.update(chunk)
        await self.delegate.write(self.internal_key, chunk)

    async def put(self, path):
        # All of the data has been uploaded. Mark the write for finish,
        # then set the item as expected.
        try:
            # Get the hash and the content_type.
            etag = self._hash.hexdigest()
            last_modified = datetime.utcnow()
            info = FileInfo(
                path, self.internal_key,
                last_modified=last_modified, etag=etag,
                size=self.size, content_type=self.content_type)
            await self.delegate.finish_write(self.internal_key)

            # Set the file info, replacing the old.
            old_info = self.file_manager.set_file_info(
                path, info)
            self._exit_stack.pop_all()

            # Remove the old info, if it exists.
            if old_info:
                await self.delegate.remove(old_info.internal_key)
            self.send_status(200, 'Successfully uploaded: {}'.format(path))
        except Exception:
            logger.exception("Error in PUT: %s", path)
            self.send_status(500, 'Internal Server Error!')


def start():
    parser = argparse.ArgumentParser(description=(
        "HTTP server designed to serve files. It can operate either as a "
        "cache with a simple API, or it can serve static content from some "
        "directory."
    ))
    parser.add_argument('--port', '-p', type=int, default=7070, help=(
        'Port to listen on.'
    ))
    parser.add_argument('--cache-dir', '-d', default=None, help=(
        'Root directory to use for the cache.'
    ))
    # parser.add_argument('--key-level', '-k', type=int, default=0)
    # parser.add_argument('--max-count', '-c', type=int, default=-1, help=(
    #     'Limit the maximum number of files in the cache. If negative, '
    #     'assume unlimited.'
    # ))
    # parser.add_argument('--max-size', '-s', type=int, default=None, help=(
    #     'Limit the total combined size of the files in the cache. If '
    #     'negative, assume unlimited (up to whatever the OS allows).'
    # ))
    # parser.add_argument('--max-entry-size', '-m', type=int, default=-1,
    #     help=('Maximum size for a single entry. If negative, assume '
    #           'unlimited.'))
    parser.add_argument('--verbose', '-v', action='count', default=0, help=(
        'Increase verbosity. This option stacks for increasing verbosity.'
    ))
    # Dynamically determine the available choices.
    engines = get_available_delegate_types()
    if 'synchronous' in engines:
        default_engine = 'synchronous'
    else:
        default_engine = engines[0]

    parser.add_argument('--use-engine', help=(
        'Use the given engine when serving files.'),
        choices=engines, default=default_engine)

    options = parser.parse_args()
    # Parse the logging options first.
    logger.setLevel(logging.INFO)
    if options.verbose > 0:
        logger.setLevel(logging.DEBUG)
    logging.basicConfig()

    # Parse the server options.
    port = options.port
    cache_dir = options.cache_dir
    if not cache_dir:
        cache_dir = os.path.join(os.getcwd(), '_cache')
    os.makedirs(cache_dir, exist_ok=True)

    loop = ioloop.IOLoop.current()

    delegate = SynchronousFileDelegate(cache_dir)
    file_manager = SimpleFileManager(delegate)
    context = dict(file_manager=file_manager)
    app = web.Application([
        (r'/(?P<path>.+)', ServeFileHandler, context),
        (r'.*', BaseHandler)
    ])
    server = httpserver.HTTPServer(app)
    server.listen(port)
    server.start()
    logger.info("Running server on port: %d", port)

    async def _drain_server():
        logger.info("Stopping server and draining connections.")
        server.stop()
        await server.close_all_connections()
        # Stop the IOLoop as well.
        loop.stop()

    def _sighandler(*_):
        loop.add_callback_from_signal(_drain_server)
    signal.signal(signal.SIGINT, _sighandler)
    signal.signal(signal.SIGTERM, _sighandler)

    try:
        loop.start()
    except Exception:
        logger.exception('Unknown error!')


if __name__ == '__main__':
    start()
