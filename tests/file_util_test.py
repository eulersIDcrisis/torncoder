"""file_util_test.py.

Test cases for file_utils.
"""
import unittest
import os
import json
import shutil
import tempfile
# Third-party imports.
from tornado import web, httpclient, httputil
from tornado.testing import AsyncTestCase, AsyncHTTPTestCase, gen_test
# Local imports
from torncoder.file_util import (
    BasicStaticFileHandler,
    # For the MultipartFormData parsing tests.
    MultipartFormDataParser
)

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

#
# FormData Parsing Test Cases
#
MULTIPART_DATA = b"""----boundarything\r
Content-Disposition: form-data; name="a.txt"\r
\r
a----boundarything\r
Content-Disposition: form-data; name=b.csv"\r
Content-Type: text/csv\r
\r
col1,col2
a,b
--boundarythin,thatwasclose
----boundarything--\r
"""


class BasicDelegate(object):

    def __init__(self):
        self.parsed_data = dict()
        self.parsed_info = dict()
        self.finished_files = []

    def start_new_file(self, name, headers):
        self.parsed_info[name] = headers

    def file_data_received(self, name, data):
        if name not in self.parsed_data:
            self.parsed_data[name] = bytearray(data)
        else:
            self.parsed_data[name].extend(data)

    def finish_file(self, name):
        self.finished_files.append(name)


class BasicAsyncDelegate(object):

    def __init__(self):
        self.parsed_data = dict()
        self.parsed_info = dict()
        self.finished_files = []

    async def start_new_file(self, name, headers):
        self.parsed_info[name] = headers

    async def file_data_received(self, name, data):
        if name not in self.parsed_data:
            self.parsed_data[name] = bytearray(data)
        else:
            self.parsed_data[name].extend(data)

    async def finish_file(self, name):
        self.finished_files.append(name)


class StreamFormData(AsyncTestCase):

    @gen_test
    async def test_multipart_form_data(self):
        boundary = b'--boundarything'

        headers_a_txt = list(httputil.HTTPHeaders({
            'Content-Disposition': 'form-data; name="a.txt"',
        }).get_all())
        headers_b_csv = list(httputil.HTTPHeaders({
            'Content-Disposition': 'form-data; name=b.csv',
            'Content-Type': 'text/csv;'
        }))

        # Test all possible splits and chunks of the given data. This will
        # verify the parser with all possible corner cases.
        for i in range(len(MULTIPART_DATA)):
            delegate = BasicDelegate()
            parser = MultipartFormDataParser(delegate, boundary)
            chunk1 = MULTIPART_DATA[:i]
            chunk2 = MULTIPART_DATA[i:]
            await parser.data_received(chunk1)
            await parser.data_received(chunk2)

            # Verify that the delegate contents are correct.
            self.assertEqual(
                set(['a.txt', 'b.csv']), set(delegate.parsed_data.keys()),
                "Expected files not found for slicing at: {}".format(i))
            # Assert the 'headers' match what is expected.
            self.assertEqual(
                headers_a_txt,
                list(delegate.parsed_info['a.txt'].get_all()),
                '"a.txt" header mismatch on slice: {}'.format(i))
            self.assertEqual(
                headers_b_csv,
                list(delegate.parsed_info['b.csv']),
                '"b.csv" header mismatch on slice: {}'.format(i))
            # Assert that the file contents match what is expected.
            self.assertEqual(
                b'a', bytes(delegate.parsed_data['a.txt']),
                '"a.txt" file contents mismatch on slice: {}'.format(i))
            self.assertEqual(
                b'col1,col2\na,b\n--boundarythin,thatwasclose\n',
                bytes(delegate.parsed_data['b.csv']),
                '"b.csv" file contents mismatch on slice: {}'.format(i))

    @gen_test
    async def test_multipart_form_data_async(self):
        # Same test as above, but with async methods for the delegate.
        boundary = b'--boundarything'

        headers_a_txt = list(httputil.HTTPHeaders({
            'Content-Disposition': 'form-data; name="a.txt"',
        }).get_all())
        headers_b_csv = list(httputil.HTTPHeaders({
            'Content-Disposition': 'form-data; name=b.csv',
            'Content-Type': 'text/csv;'
        }))

        # Test all possible splits and chunks of the given data. This will
        # verify the parser with all possible corner cases.
        for i in range(len(MULTIPART_DATA)):
            delegate = BasicAsyncDelegate()
            parser = MultipartFormDataParser(delegate, boundary)
            chunk1 = MULTIPART_DATA[:i]
            chunk2 = MULTIPART_DATA[i:]
            await parser.data_received(chunk1)
            await parser.data_received(chunk2)

            # Verify that the delegate contents are correct.
            self.assertEqual(
                set(['a.txt', 'b.csv']), set(delegate.parsed_data.keys()),
                "Expected files not found for slicing at: {}".format(i))
            # Assert the 'headers' match what is expected.
            self.assertEqual(
                headers_a_txt,
                list(delegate.parsed_info['a.txt'].get_all()),
                '"a.txt" header mismatch on slice: {}'.format(i))
            self.assertEqual(
                headers_b_csv,
                list(delegate.parsed_info['b.csv']),
                '"b.csv" header mismatch on slice: {}'.format(i))
            # Assert that the file contents match what is expected.
            self.assertEqual(
                b'a', bytes(delegate.parsed_data['a.txt']),
                '"a.txt" file contents mismatch on slice: {}'.format(i))
            self.assertEqual(
                b'col1,col2\na,b\n--boundarythin,thatwasclose\n',
                bytes(delegate.parsed_data['b.csv']),
                '"b.csv" file contents mismatch on slice: {}'.format(i))


#
# MultipartFormDataParser Route Test Utilities
#
@web.stream_request_body
class UploadRoute(web.RequestHandler):

    def initialize(self):
        self._delegate = BasicDelegate()
        self._parser = None

    def prepare(self):
        header = self.request.headers.get('Content-Type', '')
        self._parser = MultipartFormDataParser.from_content_type_header(
            self._delegate, header)

    async def data_received(self, chunk):
        await self._parser.data_received(chunk)

    def post(self):
        self.set_status(200)
        # Write out the contents of the delegate into a dictionary.
        self.write({
            name: data.decode('utf-8')
            for name, data in self._delegate.parsed_data.items()
        })


# NOTE: This request was generated by invoking cURL with -F on a netcat
# server. To duplicate, open two shells as follows:
# (1) In the first shell, run netcat to dump any incoming connections with:
# $ nc -l 8080
#
# (2) In the second shell, run cURL to "send" the file:
# $ curl 'http://localhost:8080' -F "a.txt=@a.txt" -F "b.txt=@b.txt"
#
CURL_REQUEST = b"""--------------------------0eae778966f91290\r
Content-Disposition: form-data; name="a.txt"; filename="a.txt"\r
Content-Type: text/plain\r
\r
asdf
--------------------------0eae778966f91290\r
Content-Disposition: form-data; name="b.txt"; filename="b.txt"\r
Content-Type: text/plain\r
\r
bbb
--------------------------0eae778966f91290--\r
"""


class FileHandlerTestBase(AsyncHTTPTestCase):

    def get_app(self):
        return web.Application([
            (r'/upload', UploadRoute)
        ])

    @gen_test
    async def test_file_upload_route(self):
        headers={
            'Content-Type': (
            'multipart/form-data; '
            'boundary=------------------------0eae778966f91290')
        }

        client = httpclient.AsyncHTTPClient()
        response = await client.fetch(
            self.get_url('/upload'), method='POST', body=CURL_REQUEST,
            headers=headers
        )
        result = json.loads(response.body)
        self.assertEqual(result['a.txt'], 'asdf\n')
        self.assertEqual(result['b.txt'], 'bbb\n')


if __name__ == '__main__':
    unittest.main()
