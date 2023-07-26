"""\
Update Zenodo metadata in `raw/zenodo-metadata.json`.
"""

import json
import os
import sys
import time
from urllib import request
from urllib.error import HTTPError
from urllib.parse import urlparse, quote

from cldfbench.cli_util import add_dataset_spec, with_dataset


SEARCH_KEYWORDS = [
    'CLDF',
    'cldf:StructureDataset',
    'cldf:Wordlist',
    'cldf:Dictionary',
    'cldf:Generic',
]


### Stuff that needs to be put in some sort of library ###

# FIXME code duplication
def get_access_token():
    """Get access token from environment.

    Uses the `CLLD_META_ACCESS_TOKEN` environment variable.
    """
    access_token = os.environ.get('CLLD_META_ACCESS_TOKEN') or ''
    if access_token:
        print('NOTE: Access token detected.', file=sys.stderr)
    return access_token


# FIXME code duplication
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


# FIXME code duplication
def time_secs():
    return time.time_ns() // 1000000000


# FIXME code duplication
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


# FIXME code duplication
def wait_until(secs_since_epoch):
    dt = secs_since_epoch - time_secs()
    print(
        'hit rate limit -- waiting', fmt_time_period(dt),
        'until', time.ctime(secs_since_epoch),
        file=sys.stderr, flush=True)
    time.sleep(dt)


### JSON metadata download ###

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
                    'Unexpected http response:', e.code,
                    '\nRetrying (attempt', attempt + 1,
                    'of', '%s)...' % retries,
                    file=sys.stderr, flush=True)
    else:
        print(
            'Tried', retries, 'times to no avail.  Giving up...',
            file=sys.stderr)
        return


def download_records_paginated(url):
    while url:
        raw_data = download_or_wait(url)
        json_data = json.loads(raw_data)
        yield json_data['hits']['hits']
        url = json_data['links'].get('next')


def register(parser):
    add_dataset_spec(parser)


def updatemd(dataset, args):
    access_token = get_access_token()

    print('reading existing zenodo metadata...', file=sys.stderr, flush=True)
    metadata_file = dataset.raw_dir / 'zenodo-metadata.json'
    if metadata_file.exists():
        with open(metadata_file, encoding='utf-8') as f:
            json_data = json.load(f)
        records = {
            record['id']: record
            for record in json_data['records']}
    else:
        records = {}

    print('downloading records...', file=sys.stderr, flush=True)
    entity = 'records'
    api = 'https://zenodo.org/api'
    query = 'keywords:({})'.format(
        ' OR '.join('"{}"'.format(kw) for kw in SEARCH_KEYWORDS))
    params = [
        ('sort', 'mostrecent'),
        ('all_versions', 'true'),
        ('q', query),
        ('type', 'dataset'),
        ('status', 'published'),
        ('size', '100'),
    ]
    if access_token:
        params.append(('access_token', access_token))

    param_str = '&'.join(
        '{}={}'.format(quote(k, safe=''), quote(v, safe=''))
        for k, v in params)
    search_url = '{api}/{entity}/{param_prefix}{params}'.format(
        api=api, entity=entity,
        param_prefix='?' if param_str else '',
        params=param_str)

    records.update(
        (hit['id'], hit)
        for hits in download_records_paginated(search_url)
        for hit in hits)

    new_metadata = {
        'records': sorted(
            records.values(),
            key=lambda r: r['updated'],
            reverse=True),
    }
    with open(metadata_file, 'w', encoding='utf-8') as f:
        json.dump(new_metadata, f, indent=2)


def run(args):
    with_dataset(args, updatemd)
