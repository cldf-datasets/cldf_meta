"""\
Update Zenodo metadata in `raw/zenodo-metadata.json`.
"""

from itertools import chain
import json
import os
import re
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
SEARCH_COMMUNITIES = [
    'user-lexibank',
    'user-dictionaria',
    'user-calc',
    'user-cldf-datasets',
    'user-clics',
    'user-clld',
    'user-diachronica',
    'user-dighl',
    'user-digling',
    'user-tular',
]

TYPE_BLACKLIST = {
    'lesson',
    'poster',
    'presentation',
    'publication-annotationcollection',
    'publication-article',
    'publication-book',
    'publication-conferencepaper',
    'publication-other',
    'publication-proposal',
    'publication-report',
    'publication-softwaredocumentation',
    'video',
}
TITLE_BLACKLIST_REGEX = r'''
    ^Glottolog\ database
    | ^Cross-Linguistic\ Transcription\ Systems:\ Final\ Version
    | ^CLTS\.\ Cross-Linguistic\ Transcription\ Systems
    | ^Cross-Linguistic\ Transcription\ Systems$
    | ^CLLD\ Concepticon
    | ^(?:clld/)?(?:clld:)?\s*clld\ (?:-\ )?(?:a\ )?toolkit\ for
    | ^PYCLTS\.
    | ^cldf/cldf:
    | ^cldf:\ Baseline\ for\ first\ experiments
    | ^clics/pyclics:
    | ^clics/pyclics-clustering:
    | ^clld/clics:\ CLLD\ app
    | ^clld/asjp:\ The\ ASJP\ Database
    | ^CL\ Toolkit\.\ A\ Python\ Library
    | ^DAFSA:\ a\ Python\ Library
    | ^edictor:\ EDICTOR\ version
    | ^EDICTOR\.\ A\ web-based\ interactive\ tool
    | ^glottobank/cldf:
    | ^glottolog/glottolog-cldf:
    | ^LingPy[-:. ]
    | ^lingpy/lingpy:
    | ^lingpy/lingpy-tutorial:\ LingPy\ Tutorial
    | ^LingRex[:.]\ Linguistic\ Reconstruction
    | ^lingpy/lingrex:
    | ^paceofchange:
    | ^PoePy\.\ A\ Python\ library
    | ^PyBor:\ A\ Python\ library
'''


### Stuff that needs to be put in some sort of library ###

# FIXME: code duplication
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

def build_search_url(params):
    """Build url for downloading record metadata from Zenodo."""
    entity = 'records'
    api = 'https://zenodo.org/api'
    param_str = '&'.join(
        '{}={}'.format(quote(k, safe=''), quote(v, safe=''))
        for k, v in params)
    return '{api}/{entity}/{param_prefix}{params}'.format(
        api=api, entity=entity,
        param_prefix='?' if param_str else '',
        params=param_str)


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


def is_valid(record):
    """Filter for possible CLDF datasets.

     1. Ignore non-data entries (posters, books, videos, etc.).
     2. Ignore cldf catalogues.
     3. Ignore everything made before 2018 (CLDF didn't exist, yet).
    """
    # TODO: date
    if (date := record.get('created')):
        #date = record.get('date')[0].strip()
        match = re.match(r'(\d\d\d\d)-(\d\d)-(\d\d)', date)
        assert match, '`date` needs to be YYYY-MM-DD, not {}'.format(repr(date))
        if int(match.group(1)) < 2018:
            return False

    md = record.get('metadata') or {}
    if (type_ := md.get('resource_type', {}).get('type')):
        if type_ in TYPE_BLACKLIST:
            return False

    if (title := md.get('title')):
        if re.search(TITLE_BLACKLIST_REGEX, title, re.VERBOSE):
            return False
        elif re.match(r'(?:\S*?)glottolog(?:\S*?):', title.strip()):
            return False
        elif re.match(r'(?:\S*?)clts(?:\S*?):', title.strip()):
            return False
        elif re.match(r'(?:\S*?)concepticon(?:\S*?):', title.strip()):
            return False

    return True


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

    query_kw = 'keywords:({})'.format(
        ' OR '.join('"{}"'.format(kw) for kw in SEARCH_KEYWORDS))
    params_kw = [
        ('sort', 'mostrecent'),
        ('all_versions', 'true'),
        ('q', query_kw),
        ('type', 'dataset'),
        ('status', 'published'),
        ('size', '100'),
    ]
    if access_token:
        params_kw.append(('access_token', access_token))

    query_comm = 'communities:({})'.format(
        ' OR '.join('"{}"'.format(kw) for kw in SEARCH_COMMUNITIES))
    params_comm = [
        ('sort', 'mostrecent'),
        ('all_versions', 'true'),
        ('q', query_comm),
        ('status', 'published'),
        ('size', '100'),
    ]
    if access_token:
        params_comm.append(('access_token', access_token))

    keyword_url = build_search_url(params_kw)
    community_url = build_search_url(params_comm)

    records.update(loggable_progress(
        (hit['id'], hit)
        for hits in chain(
            download_records_paginated(keyword_url),
            download_records_paginated(community_url))
        for hit in hits
        if is_valid(hit)))

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
