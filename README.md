# Torncoder

Tornado Utility Library for various features.

This library contains a few common classes and helpers that:
 - Make file serving easier.
 - Make file uploads easier.

## File Server CLI

This also implements a simple tool: `file-cache-server`.
This CLI tool implements a super simple File server API (with caching headers and so forth).
Simple usage is:
```sh
file-cache-server -p 7070 -d mydirectory --readonly
```
to serve the files at `mydirectory` on port 7070.
Note that without the `--readonly` flag, this supports PUT and DELETE operations on these files which will create, modify and delete them.

# TBD

Add an interface that is more S3-compatible.
