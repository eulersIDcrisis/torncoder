"""file_util_test.py.

Test cases for file_utils.
"""
import unittest
import tempfile
from contextlib import AsyncExitStack, asynccontextmanager
# Third-party imports.
from tornado import web, httpserver
from tornado.testing import bind_unused_port
# Use httpx for easier testing.
import httpx
# Local imports
from torncoder.file_util import (
    SynchronousFileDelegate, SimpleFileManager
)
from torncoder.handlers import (
    ServeFileHandler
)


@asynccontextmanager
async def application_context():
    async with AsyncExitStack() as exit_stack:
        tempdir = exit_stack.enter_context(
            tempfile.TemporaryDirectory()
        )
        delegate = SynchronousFileDelegate(tempdir)
        manager = SimpleFileManager(delegate)
        context = dict(file_manager=manager)
        app = web.Application([
            (r'(.*)', ServeFileHandler, context)
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

    async def test_get_request(self):
        async with application_context() as (
            base_url, context
        ), httpx.AsyncClient() as client:
            # Preload a file into the file_manager.
            file_manager = context.get('file_manager')
            self.assertIsNotNone(file_manager)

            url = '{}/test.txt'.format(base_url)
            res = await client.get(url)
            self.assertEqual(404, res.status_code)

            # PUT some data to this route.
            res = await client.put(
                url, data=b'asdfasdf', headers={
                    'Content-Type': 'text/plain',
                })
            self.assertEqual(200, res.status_code)

    async def test_head_request(self):
        pass


# # Hacky, but effective. Basically, this "nested" class definition prevents
# # this base unittest.TestCase subclass from being invoked directly by the
# # unittest module (which only scans classes defined at the module level),
# # but still allows for defining most of the test infrastructure in a common
# # base class. Each subclass _can_ be defined at the module level in order
# # to be found for test discovery. See here:
# # https://stackoverflow.com/questions/1323455/python-unit-test-with-base-and-sub-class
# class Base:

#     class FileHandlerTestBase(AsyncHTTPTestCase):

#         # NOTE: Each of these test cases will be 'inherited' by subclasses.
#         #
#         # They _can_ be overridden (and otherwise passed or decorated with
#         # `@unittest.skip('...')`).
#         @gen_test
#         async def test_basic_head_request(self):
#             url = self.get_url('/A.txt')
#             client = self.get_http_client()
#             req = httpclient.HTTPRequest(url, method='HEAD')
#             res = await client.fetch(req)
#             self.assertEqual(200, res.code)
#             self.assertIn('Etag', res.headers)
#             # No content should actually be sent.
#             self.assertFalse(res.body)

#         @gen_test
#         async def test_basic_get_request(self):
#             url = self.get_url('/A.txt')
#             client = self.get_http_client()
#             req = httpclient.HTTPRequest(url, method='GET')
#             res = await client.fetch(req)
#             self.assertEqual(200, res.code)
#             self.assertIn('Etag', res.headers)
#             self.assertEqual(b'a' * 1024, res.body)

#         @gen_test
#         async def test_basic_head_request_cache(self):
#             url = self.get_url('/A.txt')
#             client = self.get_http_client()
#             req = httpclient.HTTPRequest(url, method='HEAD')
#             res = await client.fetch(req)
#             self.assertEqual(200, res.code)
#             self.assertIn('Etag', res.headers)
#             etag = res.headers['Etag']
#             req = httpclient.HTTPRequest(url, method='HEAD', headers={
#                 "If-None-Match": etag
#             })
#             res = await client.fetch(req, raise_error=False)
#             self.assertEqual(304, res.code)

#         @gen_test
#         async def test_basic_get_request_cache(self):
#             url = self.get_url('/A.txt')
#             client = self.get_http_client()
#             req = httpclient.HTTPRequest(url, method='GET')
#             res = await client.fetch(req)
#             self.assertEqual(200, res.code)
#             self.assertIn('Etag', res.headers)
#             self.assertEqual(b'a' * 1024, res.body)
#             etag = res.headers['Etag']
#             req = httpclient.HTTPRequest(url, method='HEAD', headers={
#                 "If-None-Match": etag
#             })
#             res = await client.fetch(req, raise_error=False)
#             self.assertEqual(304, res.code)


# class BasicStaticFileHandlerTest(Base.FileHandlerTestBase):
#     """Test cases for the BasicStaticFileHandler class."""

#     def get_app(self):
#         return web.Application([
#             (r'/(.+)', BasicStaticFileHandler, dict(root_path=self.temp_dir))
#         ])


# if AIO_IMPORTED:
#     class BasicAIOFileHandlerTest(Base.FileHandlerTestBase):
#         """Test cases for the BasicAIOFileHandler class."""

#         def get_app(self):
#             return web.Application([
#                 (r'/(.+)', BasicAIOFileHandler, dict(root_path=self.temp_dir))
#             ])


if __name__ == '__main__':
    unittest.main()
