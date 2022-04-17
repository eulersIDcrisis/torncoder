"""file_util_test.py.

Test cases for file_utils.
"""
import unittest
import os
import shutil
import tempfile
# Third-party imports.
from tornado import web, httpclient
from tornado.testing import AsyncHTTPTestCase, gen_test
# Local imports
from torncoder.file_util import BasicStaticFileHandler

# Try optional imports and flag which ones were successful. If an import
# succeeds, we should run the additional tests for that section.
try:
    from torncoder.file_util import BasicAIOFileHandler

    AIO_IMPORTED = True
except ImportError:
    AIO_IMPORTED = False


# Hacky, but effective. Basically, this "nested" class definition prevents
# this base unittest.TestCase subclass from being invoked directly by the
# unittest module (which only scans classes defined at the module level),
# but still allows for defining most of the test infrastructure in a common
# base class. Each subclass _can_ be defined at the module level in order
# to be found for test discovery. See here:
# https://stackoverflow.com/questions/1323455/python-unit-test-with-base-and-sub-class
class Base:

    class FileHandlerTestBase(AsyncHTTPTestCase):

        @classmethod
        def setUpClass(cls):
            cls.temp_dir = tempfile.mkdtemp()

            # Create some files:
            # A.txt
            # B.data
            path = os.path.join(cls.temp_dir, 'A.txt')
            with open(path, 'wb') as stm:
                stm.write(b'a' * 1024)

            path = os.path.join(cls.temp_dir, 'B.data')
            with open(path, 'wb') as stm:
                for i in range(1000000):
                    stm.write(b'b')

        @classmethod
        def tearDownClass(cls):
            shutil.rmtree(cls.temp_dir, ignore_errors=True)

        def tearDown(self):
            # Close the asyncio's executor.
            super().tearDown()

        # NOTE: Each of these test cases will be 'inherited' by subclasses.
        #
        # They _can_ be overridden (and otherwise passed or decorated with
        # `@unittest.skip('...')`).
        @gen_test
        async def test_basic_head_request(self):
            url = self.get_url('/A.txt')
            client = self.get_http_client()
            req = httpclient.HTTPRequest(url, method='HEAD')
            res = await client.fetch(req)
            self.assertEqual(200, res.code)
            self.assertIn('Etag', res.headers)
            # No content should actually be sent.
            self.assertFalse(res.body)

        @gen_test
        async def test_basic_get_request(self):
            url = self.get_url('/A.txt')
            client = self.get_http_client()
            req = httpclient.HTTPRequest(url, method='GET')
            res = await client.fetch(req)
            self.assertEqual(200, res.code)
            self.assertIn('Etag', res.headers)
            self.assertEqual(b'a' * 1024, res.body)

        @gen_test
        async def test_basic_head_request_cache(self):
            url = self.get_url('/A.txt')
            client = self.get_http_client()
            req = httpclient.HTTPRequest(url, method='HEAD')
            res = await client.fetch(req)
            self.assertEqual(200, res.code)
            self.assertIn('Etag', res.headers)
            etag = res.headers['Etag']
            req = httpclient.HTTPRequest(url, method='HEAD', headers={
                "If-None-Match": etag
            })
            res = await client.fetch(req, raise_error=False)
            self.assertEqual(304, res.code)

        @gen_test
        async def test_basic_get_request_cache(self):
            url = self.get_url('/A.txt')
            client = self.get_http_client()
            req = httpclient.HTTPRequest(url, method='GET')
            res = await client.fetch(req)
            self.assertEqual(200, res.code)
            self.assertIn('Etag', res.headers)
            self.assertEqual(b'a' * 1024, res.body)
            etag = res.headers['Etag']
            req = httpclient.HTTPRequest(url, method='HEAD', headers={
                "If-None-Match": etag
            })
            res = await client.fetch(req, raise_error=False)
            self.assertEqual(304, res.code)


class BasicStaticFileHandlerTest(Base.FileHandlerTestBase):
    """Test cases for the BasicStaticFileHandler class."""

    def get_app(self):
        return web.Application([
            (r'/(.+)', BasicStaticFileHandler, dict(root_path=self.temp_dir))
        ])


if AIO_IMPORTED:
    class BasicAIOFileHandlerTest(Base.FileHandlerTestBase):
        """Test cases for the BasicAIOFileHandler class."""

        def get_app(self):
            return web.Application([
                (r'/(.+)', BasicAIOFileHandler, dict(root_path=self.temp_dir))
            ])


if __name__ == '__main__':
    unittest.main()
