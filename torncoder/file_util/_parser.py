"""_parser.py.

Module for handling file request parsing.
"""
import re
import enum
import warnings

# Third-party Imports
from tornado.httputil import HTTPHeaders

# Local Imports
from torncoder.utils import MULTIPART_FORM_DATA_TYPE, parse_content_name
from torncoder.file_util._core import AbstractFileDelegate


BOUNDARY_REGEX = re.compile(r'boundary="?(?P<boundary>[^"]+)"?')
"""Regex to match the boundary option."""


class ParserState(enum.Enum):
    """Enum to store the state of the parser."""

    PARSE_BOUNDARY_LINE = 1
    """State that parses the initial boundary."""

    PARSE_FILE_HEADERS = 2
    """State that parses the 'headers' for the next file/object."""

    PARSE_BODY = 3
    """State that parses some body text."""

    PARSING_DONE = 4
    """State that denotes the parser is finished."""


class MultipartFormDataParser(object):
    """Basic parser that accepts data and parses it into distinct files.

    This parser handles 'multipart/form-data' Content-Type uploads, which
    permits multiple file uploads in a single request.
    """

    @classmethod
    def from_content_type_header(cls, delegate, header):
        if isinstance(header, bytes):
            header = header.decode("utf-8")
        boundary = None
        # Make sure the header is the multipart/form-data.
        parts = [part.strip() for part in header.split(";")]
        if parts[0].lower() != MULTIPART_FORM_DATA_TYPE:
            raise ValueError("Invalid Content-Type: {}".format(parts[0]))

        # Search for 'boundary='
        for part in parts:
            m = BOUNDARY_REGEX.match(part)
            if m:
                boundary = m.group("boundary")
                return cls(delegate, boundary)
        raise ValueError("Required 'boundary' option not found in header!")

    def __init__(self, delegate: AbstractFileDelegate, boundary: str):
        # Be nice and decode the boundary if it is a bytes object.
        if isinstance(boundary, bytes):
            boundary = boundary.decode("utf-8")
        # Store the delegate to write out the data.
        self._delegate = delegate
        self._boundary = boundary
        self._name = None
        self._info = None

        # Variables to store the current state of the parser.
        self._state = ParserState.PARSE_BOUNDARY_LINE
        self._buffer = bytearray()

        # Variables to hold the boundary matches.
        self._boundary_next = "--{}\r\n".format(self._boundary).encode()
        self._boundary_end = "--{}--\r\n".format(self._boundary).encode()
        self._boundary_base = self._boundary_next[:-2]

        # Variables for caching boundary matching.
        self._last_idx = 0
        self._boundary_idx = 0

    @property
    def boundary(self):
        """Return the boundary text that denotes the end of a file."""
        return self._boundary

    def change_state(self, state, name=None):
        """Helper to change the state of the parser.

        This also clears some variables used in different states.
        """
        self._state = state
        self._last_idx = 0
        self._boundary_idx = 0
        self._name = name

    async def data_received(self, chunk):
        # Process the data received, based on the current state.
        self._buffer.extend(chunk)

        # Iterate over and over while there is sufficient data in the buffer.
        # Each loop should either consume data, or move to a state where not
        # enough data is available, in which case this should exit to await
        # more data.
        while True:
            # NOTE: As a almost useless optimization, we order the states in
            # the "expected" most common state ordering:
            # (1) PARSE_BODY is likely the most common state.
            # (2) PARSE_BOUNDARY_LINE
            # (3) PARSE_HEADERS
            # (4) PARSING_DONE

            # PARSE_BODY state --> Expecting to parse the file contents.
            if self._state == ParserState.PARSE_BODY:
                # Search for the boundary characters.
                idx = self._buffer.find(b"-")
                if idx < 0:
                    # No match against any boundary character. Write out the
                    # whole buffer.
                    data = self._buffer
                    self._buffer = bytearray()
                    await self._delegate.write(self._info, data)

                    # Return because the whole buffer was written out.
                    return

                # If 'idx > 0', write the data _up to_ this boundary point,
                # then proceed in the same manner as 'idx == 0'.
                if idx > 0:
                    # Write out all of the data, _up to_ this boundary point,
                    # then cycle around to check whether we are at the bounary
                    # or not. This simplifies the logic for checking against
                    # the boundary cases.
                    data = self._buffer[:idx]
                    self._buffer = self._buffer[idx:]
                    await self._delegate.write(self._info, data)

                # Not enough data (technically) to check against. Wait for
                # more data to be certain whether the boundary was parsed.
                if len(self._buffer) < len(self._boundary_next):
                    return

                # If the buffer starts with the same contents as
                # 'self._boundary_base', switch states and let that state
                # handle this case more cleanly.
                if self._buffer.startswith(self._boundary_next):
                    # Mark the current file as finished.
                    await self._delegate.finish_write(self._info)
                    self.change_state(ParserState.PARSE_BOUNDARY_LINE)
                    continue

                # Check the end boundary as well. The end boundary _might_
                # match if the 'self._boundary_base' matches, but the
                # 'self._boundary_next' does not. Wait for more data if the
                # buffer does not have enough data to be sure.
                if len(self._buffer) < len(self._boundary_end):
                    return

                if self._buffer.startswith(self._boundary_end):
                    await self._delegate.finish_write(self._info)
                    self.change_state(ParserState.PARSE_BOUNDARY_LINE)
                    continue

                # No match so far, so write out the data up to the next
                # boundary delimiter.
                next_idx = self._buffer.find(b"-", 1)
                if next_idx < 0:
                    data = self._buffer
                    self._buffer = bytearray()
                else:
                    data = self._buffer[:next_idx]
                    self._buffer = self._buffer[next_idx:]
                await self._delegate.write(self._info, data)

                # Continue and run the check after this update.
                continue

            # PARSE_BOUNDARY_LINE state --> Expecting to parse either:
            # - self._boundary_next (for the next file)
            # - self._boundary_end (for the end of the request)
            if self._state == ParserState.PARSE_BOUNDARY_LINE:
                # Parse the first boundary chunk.
                if len(self._buffer) < len(self._boundary_next):
                    # Not enough data, so exit.
                    return
                # This implies we are parsing another file, so transition to
                # the 'PARSE_HEADER' state. Also, continue to run through the
                # loop again with the new state.
                if self._buffer.startswith(self._boundary_next):
                    self._buffer = self._buffer[len(self._boundary_next) :]
                    self.change_state(ParserState.PARSE_FILE_HEADERS)
                    continue
                # Check against 'self._boundary_end' as well. There is a slim
                # chance that we are at the self._boundary_end case, but still
                # do not have enough data, so handle that here.
                if len(self._buffer) < len(self._boundary_end):
                    # Hope we get more data to confirm the boundary end case.
                    return
                elif self._buffer.startswith(self._boundary_end):
                    # Done parsing. We should probably sanity-check that all
                    # data was consumed.
                    self._buffer = self._buffer[len(self._boundary_end) :]
                    self.change_state(ParserState.PARSING_DONE)
                    continue
                else:
                    warnings.warn("Invalid boundary parsed!")

            # PARSE_HEADERS state --> Expecting to parse headers with CRLF.
            if self._state == ParserState.PARSE_FILE_HEADERS:
                idx = self._buffer.find(b"\r\n\r\n", self._last_idx)
                # Implies no match. Update the next index to search to be:
                # max(0, len(buffer) - 3)
                # as an optimization to speed up future comparisons. This
                # should work; if there is no match, then the buffer could
                # (in the worst case) have '\r\n\r', but not the final '\n'
                # so we might need to rescan the previous 3 characters, but
                # not 4. (Cap at 0 in case the buffer is too small for some
                # reason.)
                #
                # In any case, there is not enough data, so just exit.
                if idx < 0:
                    self._last_idx = max(0, len(self._buffer) - 3)
                    return
                # Otherwise, we have a match. Parse this into a dictionary of
                # headers and pass the result to create a new file.
                data = self._buffer[: idx + 4].decode("utf-8")
                self._buffer = self._buffer[idx + 4 :]
                headers = HTTPHeaders.parse(data)
                content_disp = headers.get("Content-Disposition", "")
                name = parse_content_name(content_disp)

                # Call the delegate with the new file.
                self._info = await self._delegate.start_write(name, headers=headers)

                # Update the buffer and the state.
                self.change_state(ParserState.PARSE_BODY, name=name)
                continue

            # PARSE_DONE state --> Expect no more data, but break the loop.
            if self._state == ParserState.PARSING_DONE:
                if len(self._buffer) > 0:
                    # WARNING: Data is left in the buffer when we should be
                    # finished...
                    warnings.warn(
                        "Finished with non-empty buffer ({} bytes "
                        "remaining).".format(len(self._buffer))
                    )

                # Even if there is data remaining, we should exit the loop.
                return
