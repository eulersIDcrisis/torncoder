"""test_utils.py.

Unittests for the torncoder.utils module.
"""
import unittest
from datetime import datetime
# Local Imports
from torncoder import utils


class TestUtils(unittest.TestCase):

    def test_parse_range_header(self):
        self.assertEqual((0, 10), utils.parse_range_header(
            'bytes=0-9'
        ))
        self.assertEqual((-10, None), utils.parse_range_header(
            'bytes=-10'
        ))
        # The empty string should parse.
        self.assertEqual((None, None), utils.parse_range_header(''))
        self.assertEqual((10, None), utils.parse_range_header(
            'bytes=10-'
        ))

    def test_parse_header_date(self):
        self.assertEqual(
            datetime(2022, 10, 14, 18, 16, 49),
            utils.parse_header_date('Fri, 14 Oct 2022 18:16:49 GMT')
        )
        self.assertEqual(
            datetime(2022, 9, 26, 12, 44, 47),
            utils.parse_header_date('Mon, 26 Sep 2022 12:44:47 GMT')
        )
        self.assertIsNone(utils.parse_header_date(''))

    def test_force_abspath_inside_root_dir(self):
        self.assertEqual(utils.force_abspath_inside_root_dir(
            '/mnt/test', '/asdf'
        ), '/mnt/test/asdf')
        self.assertEqual(utils.force_abspath_inside_root_dir(
            '/mnt/test', '   asdf'
        ), '/mnt/test/asdf')
        self.assertEqual(utils.force_abspath_inside_root_dir(
            '/mnt/test', '   /asdf'
        ), '/mnt/test/asdf')

        # Test these quirky corner cases.
        self.assertEqual(utils.force_abspath_inside_root_dir(
            '/mnt/test', 'asdf/../fdsa'
        ), '/mnt/test/fdsa')
        self.assertEqual(utils.force_abspath_inside_root_dir(
            '/mnt/test', 'asdf/a/b/c/../../fdsa'
        ), '/mnt/test/asdf/a/fdsa')

        # Test bad cases
        self.assertIsNone(utils.force_abspath_inside_root_dir(
            '/mnt/test', '../../asdf'))
        self.assertIsNone(utils.force_abspath_inside_root_dir(
            '/mnt/test', 'asdf/../../../asdf'))
        self.assertIsNone(utils.force_abspath_inside_root_dir(
            '/mnt/test', 'asdf/fdsa/../../../f'))


if __name__ == '__main__':
    unittest.main()
