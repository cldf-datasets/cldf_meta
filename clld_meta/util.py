"""Utility functions for clld_meta."""

import re
import sys


def loggable_progress(things, file=sys.stderr):
    """'Progressbar' that doesn't clog up logs with escape codes.

    Loops over `things` and prints a status update every 10 elements.
    Writes status updates to `file` (standard error by default).

    Yields elements in `things`.
    """
    for index, thing in enumerate(things):
        if (index + 1) % 10 == 0:
            print(index + 1, '....', sep='', end='', file=file, flush=True)
        yield thing
    print('done.', file=file, flush=True)


def file_basename(file):
    basename = re.search(
        r'/([^/]+?)(?:\?[^/]*)?(?:#[^/]*)?$',
        file['links']['self']).group(1)
    assert basename
    if not basename.endswith('.{}'.format(file['type'])):
        basename = '{}.{}'.format(basename, file['type'])
    return basename


def path_contains(path, regex):
    """Return `True` iff an element in `path` matches `regex`."""
    while True:
        parent, name = path.parent, path.name
        if re.fullmatch(regex, name):
            return True
        elif parent == path:
            return False
        else:
            path = parent
