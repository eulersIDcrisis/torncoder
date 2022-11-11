"""file_util_test.py.

Test cases for file_utils.
"""
import unittest
import uuid
import tempfile
from datetime import timedelta
from contextlib import AsyncExitStack, asynccontextmanager
# Third-party imports.
from tornado import web, httpserver
from tornado.testing import bind_unused_port
# Use httpx for easier testing.
import httpx
# Local imports
from torncoder.file_util import (
    SynchronousFileDelegate
)
from torncoder.handlers import (
    ServeFileHandler, ReadonlyFileHandler
)
from torncoder.utils import parse_header_date, format_header_date


@asynccontextmanager
async def serve_file_context():
    async with AsyncExitStack() as exit_stack:
        tempdir = exit_stack.enter_context(
            tempfile.TemporaryDirectory()
        )
        delegate = SynchronousFileDelegate(tempdir)
        context = dict(delegate=delegate)
        app = web.Application([
            (r'/(.*)', ServeFileHandler, context)
        ])
        server = httpserver.HTTPServer(app)
        socket, port = bind_unused_port()
        server.add_sockets([socket])
        server.start()
        exit_stack.push_async_callback(server.close_all_connections)
        exit_stack.callback(server.stop)

        url = 'http://127.0.0.1:{}'.format(port)
        yield url, context


class ServeFileHandlerTest(unittest.IsolatedAsyncioTestCase):

    async def test_basic_acid_requests(self):
        async with serve_file_context() as (
            base_url, context
        ), httpx.AsyncClient() as client:
            delegate = context.get('delegate')
            self.assertIsNotNone(delegate)

            url = '{}/test.txt'.format(base_url)
            # No content should be returned.
            res = await client.get(url)
            self.assertEqual(404, res.status_code)
            # Also check HEAD requests.
            res = await client.head(url)
            self.assertEqual(404, res.status_code)

            DATA = b'asdfasdfasdfasdffdsajen'

            # PUT some data to this route.
            res = await client.put(
                url, content=DATA, headers={
                    'Content-Type': 'text/plain',
                })
            # Status code should _technically_ be 201 because a new
            # resource was created here.
            self.assertEqual(201, res.status_code)

            # Get the route again. The data should be there.
            res = await client.get(url)
            self.assertEqual(200, res.status_code)
            # self.assertEqual(
            #     'text/plain', res.headers.get('Content-Type'))
            self.assertEqual(DATA, res.content)
            res = await client.head(url)
            # self.assertEqual(
            #     'text/plain', res.headers.get('Content-Type'))
            self.assertEqual(204, res.status_code)

            # DELETE the file.
            res = await client.delete(url)
            self.assertEqual(200, res.status_code)
            # File should no longer exist.
            res = await client.get(url)
            self.assertEqual(404, res.status_code)
            res = await client.head(url)
            self.assertEqual(404, res.status_code)

    async def test_caching_response_etag(self):
        async with serve_file_context() as (
            base_url, context
        ), httpx.AsyncClient() as client:
            delegate = context.get('delegate')
            self.assertIsNotNone(delegate)

            path = uuid.uuid1().hex
            url = '{}/{}'.format(base_url, path)
            # No content should be returned.
            res = await client.get(url)
            self.assertEqual(404, res.status_code)
            # Also check HEAD requests.
            res = await client.head(url)
            self.assertEqual(404, res.status_code)

            # Write out a file into the server.
            ETAG = '"abcdefg"'
            DATA = b'asdfasdfasdfasdffdsajen'
            res = await client.put(
                url, content=DATA, headers={
                    'Content-Type': 'text/plain',
                    # NOTE: Since one primary use of this handler is to
                    # function as a cache, we permit the caller to actually
                    # pass their own ETag header to use for the response.
                    'ETag': ETAG
                })
            # Status code should _technically_ be 201 because a new
            # resource was created here.
            self.assertEqual(201, res.status_code)
            self.assertIn('Last-Modified', res.headers)
            self.assertIn('ETag', res.headers)
            # The response ETag header should have the same contents as what
            # we requested.
            self.assertEqual(ETAG, res.headers['ETag'])

            res = await client.get(url)
            self.assertEqual(200, res.status_code)
            self.assertIn('Content-Type', res.headers)
            # self.assertEqual('text/plain', res.headers['Content-Type'])
            self.assertEqual(DATA, res.content)
            # Check the HEAD request too.
            res = await client.head(url)
            self.assertEqual(204, res.status_code)
            self.assertIn('Content-Type', res.headers)
            # self.assertEqual('text/plain', res.headers['Content-Type'])

            # Make the GET request, but set the If-None-Match header.
            res = await client.get(url, headers={
                'If-None-Match': ETAG
            })
            self.assertEqual(304, res.status_code)
            res = await client.head(url, headers={
                'If-None-Match': ETAG
            })
            self.assertEqual(304, res.status_code)
            # Test multiple ETag headers passed.
            res = await client.get(url, headers={
                'If-None-Match': '"abc", {}'.format(ETAG)
            })
            self.assertEqual(304, res.status_code)
            res = await client.head(url, headers={
                'If-None-Match': '"abc", {}'.format(ETAG)
            })
            self.assertEqual(304, res.status_code)

            # Test if the ETag header doesn't match.
            res = await client.get(url, headers={
                'If-None-Match': '"abc"'
            })
            self.assertEqual(200, res.status_code)
            self.assertIn('ETag', res.headers)
            self.assertEqual(ETAG, res.headers['ETag'])
            res = await client.head(url, headers={
                'If-None-Match': '"abc"'
            })
            self.assertEqual(204, res.status_code)

            res = await client.delete(url)
            self.assertEqual(200, res.status_code)

    async def test_caching_response_last_modified(self):
        async with serve_file_context() as (
            base_url, context
        ), httpx.AsyncClient() as client:
            delegate = context.get('delegate')
            self.assertIsNotNone(delegate)

            path = uuid.uuid1().hex
            url = '{}/{}'.format(base_url, path)
            # No content should be returned.
            res = await client.get(url)
            self.assertEqual(404, res.status_code)
            # Also check HEAD requests.
            res = await client.head(url)
            self.assertEqual(404, res.status_code)

            # Write out a file into the server.
            DATA = b'asdfasdfasdfasdffdsajen'
            res = await client.put(
                url, content=DATA, headers={
                    'Content-Type': 'text/plain',
                })
            # Status code should _technically_ be 201 because a new
            # resource was created here.
            self.assertEqual(201, res.status_code)
            self.assertIn('Last-Modified', res.headers)
            # Get the date.
            dt = parse_header_date(res.headers['Last-Modified'])

            res = await client.get(url, headers={
                'If-Modified-Since': format_header_date(
                    dt - timedelta(seconds=2)
                )
            })
            self.assertEqual(200, res.status_code)
            self.assertIn('Content-Type', res.headers)
            # self.assertEqual('text/plain', res.headers['Content-Type'])
            self.assertEqual(DATA, res.content)
            # Check the HEAD request too.
            res = await client.head(url, headers={
                'If-Modified-Since': format_header_date(
                    dt - timedelta(seconds=2)
                )
            })
            self.assertEqual(204, res.status_code)
            self.assertIn('Content-Type', res.headers)
            # self.assertEqual('text/plain', res.headers['Content-Type'])

            # Make the GET request, but set the If-Modified-Since
            # header to some value _after_ dt.
            res = await client.get(url, headers={
                'If-Modified-Since': format_header_date(
                    dt + timedelta(seconds=2)
                )
            })
            self.assertEqual(304, res.status_code)
            res = await client.head(url, headers={
                'If-Modified-Since': format_header_date(
                    dt + timedelta(seconds=2)
                )
            })
            self.assertEqual(304, res.status_code)

            res = await client.delete(url)
            self.assertEqual(200, res.status_code)


if __name__ == '__main__':
    unittest.main()
