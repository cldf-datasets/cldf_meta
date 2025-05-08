"""Utility functions for cldf_meta."""

import re
import sys


def loggable_progress(things, file=sys.stderr):
    """'Progressbar' that doesn't clog up logs with escape codes.

    Loops over `things` and prints a status update every 10 elements.
    Writes status updates to `file` (standard error by default).

    Yields elements in `things`.
    """
    for ord, thing in enumerate(things, 1):
        if ord % 10 == 0:
            print(ord, '....', sep='', end='', file=file, flush=True)
        yield thing
    print('done.', file=file, flush=True)


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
