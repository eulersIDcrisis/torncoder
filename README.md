# Torncoder

Tornado Utility Library for various features.

This library contains a few common classes and helpers that:
 - Make file serving easier.
 - Make file uploads easier.
 - Permit piping output from processes easier.
 - Basic pool management.

## (Static) FileHandler Utilities

`tornado`'s default `web.StaticFileHandler` is a bit onerous and confusing to
subclass or otherwise use; `torncoder` instead defines a slightly different
interface for similar purposes, but consolidates much of the work:

```python
class MyFileHandler(AbstractFileHandler):

    async def fetch_file_info(self, path):
        # Validate path, then return a FileInfo tuple.
        return FileInfo(...)

    async def get_iter_content(self, path_handle, start, end):
        # Iterate over the content from start/end.
        # NOTE: 'path_handle' is the path argument by the above FileInfo.
```
