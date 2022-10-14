"""test_file_util.py.

Test Cases for the 'tornproxy.file_util' module.
"""
import json
import uuid
import asyncio
import unittest
import tempfile
from contextlib import AsyncExitStack
# Third-party Imports
from tornado import httputil, httpclient, web
from tornado.testing import (
    gen_test, AsyncTestCase, AsyncHTTPTestCase
)
# Local Imports
from torncoder.file_util import (
    # Parser Imports
    MultipartFormDataParser,
    # Delegate Imports
    AbstractFileDelegate,
    MemoryFileDelegate,
    SynchronousFileDelegate,
    FileInfo,
    SimpleFileManager,
    # Import these to check which delegates are available.
    NATIVE_AIO_FILE_DELEGATE_ENABLED,
    THREADED_FILE_DELEGATE_ENABLED
)

#
# Parser Assertions
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


#
# AbstractFileDelegate Assertions
#
# Define some common test cases that should pass for any compliant
# AbstractFileDelegate. These tests are defined abstractly so the test can be
# repeated for different delegates.
#
async def assert_file_delegate_operations(
    test_case: unittest.IsolatedAsyncioTestCase,
    delegate: AbstractFileDelegate
):
    # Create a key with some data.
    key = uuid.uuid1().hex
    data = b'abcdefghijklmnopqrstuvwxyz'

    await delegate.start_write(key)
    await delegate.write(key, data)
    await delegate.finish_write(key)

    result = bytearray()
    async for chunk in delegate.read_generator(key):
        result.extend(chunk)
    result = bytes(result)
    test_case.assertEqual(data, result)

    result = bytearray()
    async for chunk in delegate.read_generator(
            key, start=5, end=15):
        result.extend(chunk)
    result = bytes(result)
    test_case.assertEqual(data[5:15], result)


async def assert_parallel_file_operations(
    test_case: unittest.IsolatedAsyncioTestCase,
    delegate: AbstractFileDelegate
):
    key = uuid.uuid1().hex
    write_started_event = asyncio.Event()
    write_finished_event = asyncio.Event()

    async def _first_request():
        # Wait for the write event to start.
        await delegate.start_write(key)
        # Introduce a wait here to make sure the other request starts.
        write_started_event.set()
        await delegate.write(key, b'a' * 1000)
        await delegate.write(key, b'b' * 1000)
        await delegate.finish_write(key)
        write_finished_event.set()

        result = bytearray()
        async for chunk in delegate.read_generator(key):
            result.extend(chunk)

        test_case.assertEqual(2000, len(result))
        test_case.assertEqual(b'a' * 1000, result[:1000])
        test_case.assertEqual(b'b' * 1000, result[1000:])

    async def _second_request():
        res = bytearray()
        await write_started_event.wait()
        await write_finished_event.wait()
        async for chunk in delegate.read_generator(key):
            test_case.assertTrue(write_finished_event.is_set())
            # NOTE: Before we start reading back data, this
            res.extend(chunk)

        test_case.assertEqual(2000, len(res))
        test_case.assertEqual(b'a' * 1000, res[:1000])
        test_case.assertEqual(b'b' * 1000, res[1000:])

    req1_fut = asyncio.create_task(_first_request())
    req2_fut = asyncio.create_task(_second_request())

    await asyncio.gather(req1_fut, req2_fut)


#
# SimpleFileManager Assertions
#
async def assert_simple_file_cache_basic(
    test_case: unittest.IsolatedAsyncioTestCase,
    delegate: AbstractFileDelegate
):
    # Create the file cache to perform some operations.
    cache = SimpleAsyncFileCache(delegate)

    path = '/basic.txt'

    item = cache.get_item(path)
    test_case.assertIsNone(item)

    # Now, get or create the item here.
    item = cache.get_or_create_item(path)
    test_case.assertIsNotNone(item)

    to_write = b'q' * 128
    to_write2 = b'w' * 128
    expected = bytearray()
    expected.extend(to_write)
    expected.extend(to_write2)
    expected = bytes(expected)

    async def _reader():
        # Fetch the item and start reading. This should work because the
        # reader should wait for 'finish_write()' to be called.
        read_item = cache.get_item(path)
        test_case.assertIsNotNone(read_item)

        data = bytearray()
        async for chunk in item.read_generator():
            data.extend(chunk)
        test_case.assertEqual(expected, bytes(data))

    # Start the readers before the write is even started.
    reader1 = asyncio.create_task(_reader())
    reader2 = asyncio.create_task(_reader())

    # 'item' should be some instance of: AsyncCacheItem
    await item.start_write()
    await item.write(to_write)
    await item.write(to_write2)
    await item.finish_write()

    # Wait for both readers.
    await asyncio.gather(reader1, reader2)


async def assert_simple_file_cache_write_lock(
    test_case: unittest.IsolatedAsyncioTestCase,
    delegate: AbstractFileDelegate
):
    pass


#
# Test Case Container
#
class DelegateContainer(object):

    class MainDelegateTests(unittest.IsolatedAsyncioTestCase):

        def get_delegate(self, temp_dir):
            raise NotImplementedError(
                '"get_delegate()" should be overridden!')

        # Test the delegates
        async def test_basic_file_operations(self):
            with tempfile.TemporaryDirectory() as temp_dir:
                delegate = self.get_delegate(temp_dir)
                await assert_file_delegate_operations(self, delegate)

        async def test_parallel_file_operations(self):
            with tempfile.TemporaryDirectory() as temp_dir:
                delegate = self.get_delegate(temp_dir)
                await assert_parallel_file_operations(self, delegate)

        # Test the SimpleAsyncFileCache operations.
        async def test_simple_file_cache(self):
            with tempfile.TemporaryDirectory() as temp_dir:
                delegate = self.get_delegate(temp_dir)
                await assert_simple_file_cache_basic(self, delegate)

        async def test_simple_file_cache_write_contention(self):
            with tempfile.TemporaryDirectory() as temp_dir:
                delegate = self.get_delegate(temp_dir)
                await assert_simple_file_cache_write_lock(self, delegate)


class MemoryDelegateTest(DelegateContainer.MainDelegateTests):

    def get_delegate(self, temp_dir):
        return MemoryFileDelegate()


class SynchronousFileDelegateTest(DelegateContainer.MainDelegateTests):

    def get_delegate(self, temp_dir):
        return SynchronousFileDelegate(temp_dir)


@unittest.skipIf(
    not NATIVE_AIO_FILE_DELEGATE_ENABLED,
    "'aiofile' module not installed; skipping relevant tests."
)
class NativeAioFileDelegateTest(DelegateContainer.MainDelegateTests):

    def get_delegate(self, temp_dir):
        from torncoder.file_util import NativeAioFileDelegate

        return NativeAioFileDelegate(temp_dir)


@unittest.skipIf(
    not THREADED_FILE_DELEGATE_ENABLED,
    "'aiofiles' module not installed; skipping relevant tests."
)
class ThreadedFileDelegateTest(DelegateContainer.MainDelegateTests):

    def get_delegate(self, temp_dir):
        from torncoder.file_util import ThreadedFileDelegate

        return ThreadedFileDelegate(temp_dir)


if __name__ == '__main__':
    unittest.main()
