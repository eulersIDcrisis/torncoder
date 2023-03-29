"""utils.py.

Module with miscellaneous functions.
"""
import os
import re
import logging
import asyncio
import datetime
import email.utils
from typing import Optional, Tuple, Any


logger = logging.getLogger("torncoder")


MULTIPART_FORM_DATA_TYPE = "multipart/form-data"
"""Constant storing the 'multipart/form-data' content type."""


NAME_REGEX = re.compile(r'name="?(?P<name>[^"]+)"?')
"""Regex to match the name field."""


def format_header_date(header_value: datetime.datetime) -> str:
    """Format a datetime into a string suitable for an HTTP header."""
    return email.utils.format_datetime(header_value)


def parse_header_date(header_value: str) -> datetime.datetime:
    """Parse a date in a header to a datetime object.

    NOTE: Headers store the date using some format like:
        'Fri, 14 Oct 2022 18:16:49 GMT'
    which is not the same as datetime.isoformat or similar.
    """
    date_tuple = email.utils.parsedate(header_value)
    if date_tuple is not None:
        return datetime.datetime(*date_tuple[:6])
    return None


def parse_range_header(header_value: str) -> Tuple[int, Optional[int]]:
    """Parse a range header and return the implied python slice.

    This only parses the 'bytes' type.
    """
    try:
        if not header_value:
            return None, None
        range_type, value = header_value.split("=", 1)
        if range_type.strip() != "bytes":
            return None, None
        # Now, check 'value', which should be the range.
        start_str, end_str = value.split("-", 1)
        if end_str:
            end = int(end_str)
            if not start_str and end != 0:
                return -end, None
            start = int(start_str)
            return start, end + 1
        elif not start_str:
            # Both end_str AND start_str are empty.
            return None, None
        else:
            # start is set, end is None
            start = int(start_str)
            return start, None
    except Exception:
        return None, None


def parse_content_name(content_disp: str) -> str:
    """Parse the 'name' field from Content-Disposition."""
    for field in content_disp.split(";"):
        m = NAME_REGEX.search(field)
        if m:
            return m.group("name")
    raise ValueError('No "name" field found in Content-Disposition!')


def is_awaitable(obj: Any) -> bool:
    """Return whether this object is 'Awaitable'."""
    return asyncio.isfuture(obj) or asyncio.iscoroutine(obj)


def is_path_inside_directory(root_dir: str, full_path: str) -> bool:
    """Return whether the given path is contained inside root_dir.

    NOTE: Both given paths are expected to be absolute paths!
    """
    full_root = os.path.abspath(root_dir)
    if not full_root.endswith("/"):
        full_root += "/"
    return full_path.startswith(full_root)


def force_abspath_inside_root_dir(root_dir: str, path: str):
    """Return the joined path inside root_dir, or None if not possible.

    NOTE: This call handles a few subtleties, such as paths that have
    leading spaces and/or '/' (os.pathsep) characters, assuming that
    these were actually intended to be relative to root_dir.
    For example, this call should behave like the following:
     - force_abspath_inside_root_dir('/mnt/test', '/asdf')
      --> '/mnt/test/asdf'
     - force_abspath_inside_root_dir('/mnt/test', '  asdf')
      --> '/mnt/test/asdf'
     - force_abspath_inside_root_dir('/mnt/test', 'asdf')
      --> '/mnt/test/asdf'
     - force_abspath_inside_root_dir('/mnt/test', '../../../asdf')
      --> None
     - force_abspath_inside_root_dir('/mnt/test', '/asdf/../../fda')
      --> None
    (This case is curious but relevant; the resolved 'relative' path would
    be inside 'root_dir', and thus should be valid?)
     - force_abspath_inside_root_dir('/mnt/test', '/asdf/../fda')
      --> '/mnt/test/fda'
    """
    full_root = os.path.abspath(root_dir)
    if not full_root.endswith("/"):
        full_root += "/"
    # Strip any space or possible path separator characters.
    path = path.lstrip(" \\/")
    full_path = os.path.abspath(os.path.join(full_root, path))
    if full_path.startswith(full_root):
        return full_path
    return None
