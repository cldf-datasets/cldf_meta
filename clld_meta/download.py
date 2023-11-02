"""Code for downloading data or metadata."""

import hashlib
import os
import time
import sys
from urllib import request
from urllib.error import HTTPError
from urllib.parse import urlparse


def retrieve_access_token():
    """Get access token from environment.

    Uses the `CLLD_META_ACCESS_TOKEN` environment variable.
    """
    access_token = os.environ.get('CLLD_META_ACCESS_TOKEN') or ''
    if access_token:
        print('NOTE: Access token detected.', file=sys.stderr, flush=True)
    else:
        print(
            'WARNING: No zenodo access token detected!',
            file=sys.stderr, flush=True)
    return access_token


def add_access_token(url, token):
    """Add Zenodod access token to a URL."""
    if not token:
        return url

    o = urlparse(url)
    if o.query:
        o = o._replace(query='{}&access_token={}'.format(o.query, token))
    else:
        o = o._replace(query='access_token={}'.format(token))

    return o.geturl()


def time_secs():
    return time.time_ns() // 1000000000


def fmt_time_period(secs):
    mins, secs = secs // 60, secs % 60
    hrs, mins = mins // 60, mins % 60
    days, hrs = hrs // 24, hrs % 24
    if days:
        return '{}d{}h{}m{}s'.format(days, hrs, mins, secs)
    elif hrs:
        return '{}h{}m{}s'.format(hrs, mins, secs)
    elif mins:
        return '{}m{}s'.format(mins, secs)
    else:
        return '{}s'.format(secs)


def wait_until(secs_since_epoch):
    dt = secs_since_epoch - time_secs()
    print(
        'hit rate limit -- waiting', fmt_time_period(dt),
        'until', time.ctime(secs_since_epoch),
        file=sys.stderr, flush=True)
    time.sleep(dt)


def download_or_wait(url):
    """Download data from one url waiting for the ratelimit."""
    retries = 3
    for attempt in range(retries):
        try:
            with request.urlopen(url) as response:
                return response.read()
        except HTTPError as e:
            if e.code == 429:
                # too many requests
                limit_reset = int(e.headers['X-RateLimit-Reset'])
                retry_after = int(e.headers['Retry-After'])
                wait_until(max(limit_reset, time_secs() + retry_after))
            else:
                print(
                    f'Unexpected http response: {e.code}',
                    e.read().decode('utf-8').strip(),
                    f'Attempt {attempt + 1} of {retries}; retrying...',
                    sep='\n', file=sys.stderr, flush=True)
    else:
        raise IOError(f'Tried {retries} times to no avail.  Giving up...')


def download_all(urls):
    """Download data from multiple urls at a ratelimit-friendly pace."""
    retries = 3
    for url in urls:
        for attempt in range(retries):
            try:
                with request.urlopen(url) as response:
                    yield response.read()
                    limit_remaining = int(response.headers['X-RateLimit-Remaining'])
                    if limit_remaining == 0:
                        limit_reset = int(response.headers['X-RateLimit-Reset'])
                        retry_after = int(response.headers['Retry-After'])
                        wait_until(max(limit_reset, time_secs() + retry_after))
                    # no retries needed
                    break
            except HTTPError as e:
                if e.code == 429:
                    # too many requests
                    limit_reset = int(e.headers['X-RateLimit-Reset'])
                    retry_after = int(e.headers['Retry-After'])
                    wait_until(max(limit_reset, time_secs() + retry_after))
                else:
                    print(
                        f'Unexpected http response: {e.code}',
                        e.read().decode('utf-8').strip(),
                        f'Attempt {attempt + 1} of {retries}; retrying...',
                        sep='\n', file=sys.stderr, flush=True)
        else:
            raise IOError(f'Tried {retries} times to no avail.  Giving up...')


def validate_checksum(checksum, data):
    """Validate `data` by comparing its hash to `checksum`.

    `checksum` is assumed to look like `hashing_algorithm:hex_checksum`
    (e.g. `md5:6f5902ac237024bdd0c176cb93063dc4`).
    """
    fields = checksum.split(':', maxsplit=1)
    if len(fields) != 2:
        raise ValueError('Could not determine hashing algorithm')

    algo, expected_sum = fields
    if algo not in hashlib.algorithms_available:
        raise ValueError(
            "Hashing algorithm '%s' not available in hashlib" % algo)

    h = hashlib.new(algo)
    h.update(data)
    real_sum = h.hexdigest()

    if real_sum != expected_sum:
        raise ValueError(
            'Checksum validation failed: '
            "Expected %s sum '%s'; got '%s'." % (algo, expected_sum, real_sum))
