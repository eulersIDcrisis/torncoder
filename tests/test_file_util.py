"""test_file_util.py.

Test Cases for the 'tornproxy.file_util' module.
"""
import uuid
import asyncio
import unittest
import tempfile
# Third-party Imports
from tornado import httputil
# Local Imports
from torncoder.file_util import (
    # Parser Imports
    MultipartFormDataParser,
    # Delegate Imports
    AbstractFileDelegate,
    MemoryFileDelegate,
    SynchronousFileDelegate,
    # Compound delegates
    SimpleCacheFileDelegate,
    ReadOnlyDelegate,
    # Import these to check which delegates are available.
    NATIVE_AIO_FILE_DELEGATE_ENABLED,
    THREADED_FILE_DELEGATE_ENABLED
)


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
) -> None:
    # Create two keys with some data; intermix the write operation.
    key1 = uuid.uuid1().hex
    data1 = b'abcdefghijklmnopqrstuvwxyz'
    key2 = uuid.uuid1().hex
    data2 = b'zyxwvutsrqponmlkjihgfedcba'

    info1 = await delegate.start_write(key1, {})
    await delegate.write(info1, data1[:5])
    info2 = await delegate.start_write(key2, {})
    await delegate.write(info2, data2[:6])
    await delegate.write(info1, data1[5:])
    await delegate.write(info2, data2[6:])
    await delegate.finish_write(info1)
    await delegate.finish_write(info2)

    # Test Full reads
    result1 = bytearray()
    async for chunk in delegate.read_generator(info1):
        result1.extend(chunk)
    result1 = bytes(result1)
    test_case.assertEqual(data1, result1)

    result2 = bytearray()
    async for chunk in delegate.read_generator(info2):
        result2.extend(chunk)
    result2 = bytes(result2)
    test_case.assertEqual(data2, result2)

    # Test Partial reads
    result1 = bytearray()
    async for chunk in delegate.read_generator(
            info1, start=5, end=15):
        result1.extend(chunk)
    result1 = bytes(result1)
    test_case.assertEqual(data1[5:15], result1)

    result2 = bytearray()
    async for chunk in delegate.read_generator(
            info2, start=4, end=13):
        result2.extend(chunk)
    result2 = bytes(result2)
    test_case.assertEqual(data2[4:13], result2)


async def assert_parallel_file_operations(
    test_case: unittest.IsolatedAsyncioTestCase,
    delegate: AbstractFileDelegate
) -> None:
    key = uuid.uuid1().hex
    write_started_event = asyncio.Event()
    write_finished_event = asyncio.Event()
    info = await delegate.start_write(key, {})

    async def _first_request():
        # Wait for the write event to start.
        # Introduce a wait here to make sure the other request starts.
        write_started_event.set()
        await delegate.write(info, b'a' * 1000)
        await delegate.write(info, b'b' * 1000)
        await delegate.finish_write(info)
        write_finished_event.set()

        result = bytearray()
        async for chunk in delegate.read_generator(info):
            result.extend(chunk)

        test_case.assertEqual(2000, len(result))
        test_case.assertEqual(b'a' * 1000, result[:1000])
        test_case.assertEqual(b'b' * 1000, result[1000:])

    async def _second_request():
        res = bytearray()
        await write_started_event.wait()
        await write_finished_event.wait()
        async for chunk in delegate.read_generator(info):
            test_case.assertTrue(write_finished_event.is_set())
            # NOTE: Before we start reading back data, this
            res.extend(chunk)

        test_case.assertEqual(2000, len(res))
        test_case.assertEqual(b'a' * 1000, res[:1000])
        test_case.assertEqual(b'b' * 1000, res[1000:])

    req1_fut = asyncio.create_task(_first_request())
    req2_fut = asyncio.create_task(_second_request())

    await asyncio.gather(req1_fut, req2_fut)


async def assert_readonly_delegate(
    test_case: unittest.IsolatedAsyncioTestCase,
    delegate: AbstractFileDelegate
):
    # Create two keys with some data; intermix the write operation.
    key = uuid.uuid1().hex
    data = b'YTREWQASDFVCXZ'

    info = await delegate.start_write(key, {})
    await delegate.write(info, data)
    info = await delegate.finish_write(info)

    readonly_delegate = ReadOnlyDelegate(delegate)
    # Readonly delegate should work with the given key.
    new_info = await readonly_delegate.get_file_info(key)
    test_case.assertIsNotNone(new_info)
    test_case.assertEqual(info.key, new_info.key)
    test_case.assertEqual(info.internal_key, new_info.internal_key)

    read_data = await readonly_delegate.read_into_bytes(new_info)
    test_case.assertEqual(data, read_data)

    data2 = bytearray()
    async for chunk in readonly_delegate.read_generator(new_info):
        data2.extend(chunk)
    test_case.assertEqual(data, data2)

    new_key = uuid.uuid1().hex
    with test_case.assertRaises(Exception):
        await readonly_delegate.start_write(new_key, {})
    with test_case.assertRaises(Exception):
        await readonly_delegate.write(info, data)
    with test_case.assertRaises(Exception):
        await readonly_delegate.finish_write(info)
    with test_case.assertRaises(Exception):
        await readonly_delegate.remove(info)

    # The value at 'key' should still exist and the original delegate should
    # still work as expected.
    new_info = await delegate.get_file_info(key)
    test_case.assertEqual(info.key, new_info.key)
    test_case.assertEqual(info.internal_key, new_info.internal_key)
    new_data = await delegate.read_into_bytes(new_info)
    test_case.assertEqual(data, new_data)
    data2 = bytearray()
    async for chunk in delegate.read_generator(info):
        data2.extend(chunk)
    test_case.assertEqual(data, data2)

    # Using the original delegate, remove the file.
    await delegate.remove(info)
    new_info = await delegate.get_file_info(key)
    test_case.assertIsNone(new_info)

    new_info = await readonly_delegate.get_file_info(key)
    test_case.assertIsNone(new_info)

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

        async def test_readonly_delegate_wrapping(self):
            with tempfile.TemporaryDirectory() as temp_dir:
                delegate = self.get_delegate(temp_dir)
                await assert_readonly_delegate(self, delegate)


class MemoryDelegateTest(DelegateContainer.MainDelegateTests):

    def get_delegate(self, temp_dir):
        return MemoryFileDelegate()


class SynchronousFileDelegateTest(DelegateContainer.MainDelegateTests):

    def get_delegate(self, temp_dir):
        return SynchronousFileDelegate(temp_dir)


class SimpleCacheFileDelegateTest(DelegateContainer.MainDelegateTests):

    def get_delegate(self, temp_dir):
        parent_delegate = SynchronousFileDelegate(temp_dir)
        return SimpleCacheFileDelegate(parent_delegate)

    # TODO: This delegate type should also test vacuuming and so forth to
    # make sure that various limits are enforced.
    async def test_vacuum_size(self):
        # For this test, let's just use the memory delegate.
        root_delegate = MemoryFileDelegate()
        # Create the delegate with a maximum size constraint of 1kb.
        delegate = SimpleCacheFileDelegate(
            root_delegate, max_size=1024)

        # Write a file.
        info = await delegate.start_write(
            'a.txt', {'Content-Type': 'text/plain'})
        await delegate.write(info, b'a' * 1024)
        info = await delegate.finish_write(info)

        # Make sure the file exists.
        new_info = await delegate.get_file_info('a.txt')
        self.assertIsNotNone(new_info)
        self.assertEqual('a.txt', new_info.key)
        result = await delegate.read_into_bytes(new_info)
        self.assertEqual(b'a' * 1024, bytes(result))

        # Now, add a new file.
        new_info = await delegate.start_write(
            'b.txt', {"Content-Type": 'text/plain'})
        await delegate.write(new_info, b'b' * 1024)
        new_info = await delegate.finish_write(new_info)
        self.assertIsNotNone(new_info)

        # After a vacuum, only 'b.txt' should remain.
        await delegate.vacuum()
        info = await delegate.get_file_info('a.txt')
        self.assertIsNone(info)
        info = await delegate.get_file_info('b.txt')
        self.assertIsNotNone(info)


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


#
# MultipartFormDataParser Assertions
#
MULTIPART_DATA = b"""----boundarything\r
Content-Disposition: form-data; name="a.txt"\r
\r
a----boundarything\r
Content-Disposition: form-data; name="b.csv"\r
Content-Type: text/csv\r
\r
col1,col2
a,b
--boundarythin,thatwasclose
----boundarything--\r
"""


class TestMultipartFormDataParser(unittest.IsolatedAsyncioTestCase):

    async def test_multipart_form_data(self):
        boundary = b'--boundarything'

        headers_a_txt = list(httputil.HTTPHeaders({
            'Content-Disposition': 'form-data; name="a.txt"',
        }).get_all())
        headers_b_csv = list(httputil.HTTPHeaders({
            'Content-Disposition': 'form-data; name="b.csv"',
            'Content-Type': 'text/csv'
        }).get_all())

        # Test all possible splits and chunks of the given data. This will
        # verify the parser with all possible corner cases.
        for i in range(len(MULTIPART_DATA)):
            delegate = MemoryFileDelegate()
            parser = MultipartFormDataParser(delegate, boundary)
            chunk1 = MULTIPART_DATA[:i]
            chunk2 = MULTIPART_DATA[i:]
            await parser.data_received(chunk1)
            await parser.data_received(chunk2)

            # Verify that the delegate contents are correct.
            self.assertEqual(
                set(['a.txt', 'b.csv']), set(delegate.keys),
                "Expected files not found for slicing at: {}".format(i))
            # Assert the 'headers' match what is expected.
            self.assertEqual(
                headers_a_txt,
                list(delegate.get_headers('a.txt').get_all()),
                '"a.txt" header mismatch on slice: {}'.format(i))
            self.assertEqual(
                headers_b_csv,
                list(delegate.get_headers('b.csv').get_all()),
                '"b.csv" header mismatch on slice: {}'.format(i))
            # Assert that the file contents match what is expected.
            a_info = await delegate.get_file_info('a.txt')
            self.assertIsNotNone(a_info)
            a_data = await delegate.read_into_bytes(a_info)
            self.assertEqual(
                b'a', a_data,
                '"a.txt" file contents mismatch on slice: {}'.format(i))
            b_info = await delegate.get_file_info('b.csv')
            self.assertIsNotNone(b_info)
            b_data = await delegate.read_into_bytes(b_info)
            self.assertEqual(
                b'col1,col2\na,b\n--boundarythin,thatwasclose\n',
                b_data,
                # bytes(delegate.parsed_data['b.csv']),
                '"b.csv" file contents mismatch on slice: {}'.format(i))

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
            delegate = MemoryFileDelegate()
            parser = MultipartFormDataParser(delegate, boundary)
            chunk1 = MULTIPART_DATA[:i]
            chunk2 = MULTIPART_DATA[i:]
            await parser.data_received(chunk1)
            await parser.data_received(chunk2)

            # Verify that the delegate contents are correct.
            self.assertEqual(
                set(['a.txt', 'b.csv']), set(delegate.keys),
                "Expected files not found for slicing at: {}".format(i))
            # Assert the 'headers' match what is expected.
            self.assertEqual(
                headers_a_txt,
                list(delegate.get_headers('a.txt').get_all()),
                '"a.txt" header mismatch on slice: {}'.format(i))
            self.assertEqual(
                headers_b_csv,
                list(delegate.get_headers('b.csv')),
                '"b.csv" header mismatch on slice: {}'.format(i))
            # Assert that the file contents match what is expected.
            a_info = await delegate.get_file_info('a.txt')
            self.assertIsNotNone(a_info)
            a_data = await delegate.read_into_bytes(a_info)
            self.assertEqual(
                b'a', a_data,
                '"a.txt" file contents mismatch on slice: {}'.format(i))
            b_info = await delegate.get_file_info('b.csv')
            self.assertIsNotNone(b_info)
            b_data = await delegate.read_into_bytes(b_info)
            self.assertEqual(
                b'col1,col2\na,b\n--boundarythin,thatwasclose\n',
                b_data,
                '"b.csv" file contents mismatch on slice: {}'.format(i))


if __name__ == '__main__':
    unittest.main()
