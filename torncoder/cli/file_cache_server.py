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
    get_available_delegate_types,
    create_delegate,
    ReadOnlyDelegate,
)
from torncoder.handlers import ServeFileHandler


def start():
    parser = argparse.ArgumentParser(
        description=(
            "HTTP server designed to serve files. It can operate either as a "
            "cache with a simple API, or it can serve static content from some "
            "directory."
        )
    )
    parser.add_argument(
        "--port", "-p", type=int, default=7070, help=("Port to listen on.")
    )
    parser.add_argument(
        "--cache-dir", "-d", default=None, help=("Root directory to use for the cache.")
    )
    parser.add_argument(
        "--readonly",
        action="store_true",
        help=(
            "Serve static files; do not permit updating the files at the given directory."
        ),
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="count",
        default=0,
        help=("Increase verbosity. This option stacks for increasing verbosity."),
    )
    # Dynamically determine the available choices.
    engines = get_available_delegate_types()
    if "threaded" in engines:
        default_engine = "threaded"
    else:
        default_engine = engines[0]

    parser.add_argument(
        "--use-engine",
        help=("Use the given engine when serving files."),
        choices=engines,
        default=default_engine,
    )

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
        cache_dir = os.path.join(os.getcwd(), "_cache")
    os.makedirs(cache_dir, exist_ok=True)

    loop = ioloop.IOLoop.current()

    delegate = create_delegate(options.use_engine, cache_dir)

    # Do not permit write operations if configured in readonly mode.
    if options.readonly:
        delegate = ReadOnlyDelegate(delegate)

    context = dict(delegate=delegate)
    app = web.Application(
        [
            (r"/(?P<path>.+)", ServeFileHandler, context),
            # (r'.*', BaseHandler)
        ]
    )
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
        logger.exception("Unknown error!")


if __name__ == "__main__":
    start()
