"""\
Update Zenodo metadata in `raw/zenodo-metadata.csv`.
"""

from collections import defaultdict, OrderedDict
import csv
import json
import re
import os
import sys
import time
from urllib import request
from urllib.error import HTTPError
from urllib.parse import urlparse

from bs4 import BeautifulSoup
from sickle import Sickle

from cldfbench.cli_util import add_dataset_spec, with_dataset


OAI_URL = 'https://zenodo.org/oai2d'
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

DOI_REGEX = r'(?:doi:)?10(?:\.[0-9]+)+/'
# ZENODO_DOI_REGEX = r'(?:doi:)?10\.5281/zenodo\.'
GITHUB_LINK_REGEX = r'(?:url:)?(?:https?://)?github.com'
# COMMUNITY_LINK_REGEX = r'(?:url:)?(?:https?://)?zenodo.org/communities'

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
    | ^CL\ Toolkit\.\ A\ Python\ Library
    | ^DAFSA:\ a\ Python\ Library
    | ^edictor:\ EDICTOR\ version
    | ^EDICTOR\.\ A\ web-based\ interactive\ tool
    | ^glottobank/cldf:
    | ^LingPy[-:. ]
    | ^lingpy/lingpy:
    | ^lingpy/lingpy-tutorial:\ LingPy\ Tutorial
    | ^LingRex[:.]\ Linguistic\ Reconstruction
    | ^lingpy/lingrex:
    | ^paceofchange:
    | ^PoePy\.\ A\ Python\ library
    | ^PyBor:\ A\ Python\ library
'''

ZENODO_METADATA_LISTSEP = r'\t'
ZENODO_METADATA_ROWS = [
    'id',
    'date',
    'title',
    'version',
    'description',
    'author',
    'contributor',
    'creator',
    'github-link',
    'zenodo-link',
    'doi',
    'doi-related',
    'communities',
    'rights',
    'source',
    'subject',
    'type',
    'file-links',
    'file-types',
    'file-checksums',
    'json-downloaded',
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


# FIXME code duplication
def download_all(urls):
    """Download data from multiple urls at a ratelimit-friendly pace."""
    limit = 60
    limit_remaining = 60
    retry_after = 60
    limit_reset = time_secs() + retry_after

    retries = 3
    for url in urls:
        for attempt in range(retries):
            try:
                with request.urlopen(url) as response:
                    limit = int(response.headers['X-RateLimit-Limit'])
                    limit_remaining = int(response.headers['X-RateLimit-Remaining'])
                    limit_reset = int(response.headers['X-RateLimit-Reset'])
                    retry_after = int(response.headers['Retry-After'])

                    yield response.read()

                    if limit_remaining == 0:
                        wait_until(max(limit_reset, time_secs() + retry_after))
                    # no retries needed
                    break
            except HTTPError as e:
                if e.code == 429:
                    # too many requests
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


# FIXME code duplication
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


### OAI metadata download ###

def _transform_key(k, v):
    if k == 'identifier':
        if re.match('https?://', v):
            return 'zenodo-link'
        elif v.startswith('oai:zenodo.org:'):
            return 'id'
        elif re.match(DOI_REGEX, v, re.I):
            return 'doi'
        else:
            return None
    elif k == 'relation':
        if re.match(DOI_REGEX, v, re.I):
            return 'doi-related'
        elif re.match(GITHUB_LINK_REGEX, v, re.I):
            return 'github-link'
        else:
            return None
    else:
        return k


def parse_record(record):
    """Turn OAI metadata record into an easy-to-use dictionary."""
    md = defaultdict(list)
    md['communities'] = record.header.setSpecs
    for k, vs in record.metadata.items():
        for v in vs:
            new_k = _transform_key(k, v)
            if not new_k:
                continue

            v = v.strip()\
                .replace('\\', '\\\\')\
                .replace('\n', '\\n')\
                .replace('\t', ' ')
            md[new_k].append(v)
    return md


def is_valid(record):
    """Filter for possible CLDF datasets.

     1. Ignore non-data entries (posters, books, videos, etc.).
     2. Ignore cldf catalogues.
    """
    for type_ in record.get('type', ()):
        if type_ in TYPE_BLACKLIST:
            return False

    for title in record.get('title', ()):
        if re.search(TITLE_BLACKLIST_REGEX, title, re.VERBOSE):
            return False
        elif re.match(r'(?:\S*?)glottolog(?:\S*?):', title.strip()):
            return False
        elif re.match(r'(?:\S*?)clts(?:\S*?):', title.strip()):
            return False
        elif re.match(r'(?:\S*?)concepticon(?:\S*?):', title.strip()):
            return False

    return True


def uniq(iterable, key=None):
    seen_before = set()
    for item in iterable:
        keyed = key(item) if key else item
        if keyed not in seen_before:
            seen_before.add(keyed)
            yield item


def download_oai_metadata(communities):
    """Return OAI metadata records from Zenodo.

    Searches for all Zenodo records that are in at least one of the communities
    defined in `communities`.

    Return a dictionary 'link to Zenodo page' -> 'record'.
    """
    dl = Sickle(
        OAI_URL,
        retry_status_codes=[503, 429],
        max_retries=3,
        default_retry_after=60)

    records = (
        parse_record(record)
        for community in communities
        for record in dl.ListRecords(
            metadataPrefix='oai_dc',
            set=community))
    records = filter(is_valid, records)
    records = uniq(records, key=lambda r: '\t'.join(r['id']))
    records = OrderedDict((record['zenodo-link'][0], record) for record in records)
    return records


### JSON metadata download ###

def extract_json(html_string):
    """Parse JSON data inside an html webpage downloaded from Zenodo."""
    soup = BeautifulSoup(html_string, 'lxml')
    pre_tags = soup.find_all('pre', style="white-space: pre-wrap;")
    if not pre_tags:
        raise ValueError('no <pre> tags found that could contain the json input')
    elif len(pre_tags) > 1:
        raise ValueError('more than one candidate for a json <pre> tag')
    else:
        return json.loads(pre_tags[0].text)


def download_json_data(json_links):
    """Batch-download JSON metadata from Zenodo.

    Return a list containing parsed json data for each `json_link`.
    """
    json_data = list(download_all(loggable_progress(json_links, sys.stderr)))
    json_data = list(map(extract_json, json_data))
    return json_data


def merge_json_data(records, json_data):
    """Merge the `json_data` into the OAI metadata records (in-place)."""
    for json_record in json_data:
        zenodo_link = json_record.get('links', {}).get('html')
        if not zenodo_link:
            continue
        md = json_record.get('metadata') or {}
        records[zenodo_link]['version'] = md.get('version') or ''
        for filedata in json_record.get('files', ()):
            records[zenodo_link]['file-links'].append(
                filedata.get('links', {}).get('self', ''))
            records[zenodo_link]['file-types'].append(
                filedata.get('type', ''))
            records[zenodo_link]['file-checksums'].append(
                filedata.get('checksum', ''))
        records[zenodo_link]['json-downloaded'] = 'y'


def merge_previous_records(records, previous_md):
    """Merge metadata from a previous download into `records` (in-place)."""
    for zenodo_link, previous_record in previous_md.items():
        if previous_record.get('json-downloaded') != 'y':
            continue
        if zenodo_link not in records:
            continue
        records[zenodo_link]['version'] = previous_record.get('version') or ''
        for k in (
            'file-links',
            'file-types',
            'file-checksums',
        ):
            v = previous_record.get(k) or ''
            records[zenodo_link][k] = v.split(ZENODO_METADATA_LISTSEP)
        records[zenodo_link]['json-downloaded'] = 'y'


def write_zenodo_metadata(records, filename):
    """Write collected metadata to disk."""

    def merge_lists(v):
        return ZENODO_METADATA_LISTSEP.join(uniq(v)) if isinstance(v, list) else v
    csv_rows = [
        [merge_lists(record.get(k) or '') for k in ZENODO_METADATA_ROWS]
        for record in records.values()]

    def _id_sort_key(record_row):
        # XXX this assumes zenodo doesn't change their id generation pattern
        return int(re.fullmatch(r'oai:zenodo.org:(\d+)', record_row[0]).group(1))
    csv_rows.sort(key=_id_sort_key)

    with open(filename, 'w', encoding='utf-8') as f:
        wrt = csv.writer(f)
        wrt.writerow(ZENODO_METADATA_ROWS)
        wrt.writerows(csv_rows)


def register(parser):
    add_dataset_spec(parser)


def updatemd(dataset, args):
    print('reading existing zenodo metadata...', file=sys.stderr, flush=True)
    try:
        previous_md = {
            record['zenodo-link']: record
            for record in dataset.raw_dir.read_csv(
                'zenodo-metadata.csv', dicts=True)
        }
    except IOError:
        previous_md = {}

    access_token = os.environ.get('CLLD_META_ACCESS_TOKEN') or ''
    if access_token:
        print('NOTE: Access token detected.', file=sys.stderr, flush=True)

    print('downloading OAI-PH metadata...', file=sys.stderr, flush=True)
    records = download_oai_metadata(SEARCH_COMMUNITIES)

    json_links = [
        '{}/export/json'.format(zenodo_link)
        for zenodo_link, rec in records.items()
        if previous_md.get(zenodo_link, {}).get('json-downloaded') != 'y']
    if access_token:
        json_links = [
            add_access_token(url, access_token)
            for url in json_links]

    if json_links:
        print(
            'downloading', len(json_links), 'json metadata files...',
            file=sys.stderr, flush=True)
        json_data = download_json_data(json_links)
    else:
        json_data = ()

    merge_json_data(records, json_data)
    merge_previous_records(records, previous_md)

    print('additional communities mentioned:', file=sys.stderr, flush=True)
    old_comms = set(SEARCH_COMMUNITIES)
    new_comms = {
        c
        for record in records.values()
        for c in record.get('communities', ())
        if c not in old_comms}
    print(
        '\n'.join(' * {}'.format(c) for c in sorted(new_comms)),
        file=sys.stderr, flush=True)

    print('writing zenodo metadata...', file=sys.stderr, flush=True)
    write_zenodo_metadata(
        records,
        dataset.raw_dir / 'zenodo-metadata.csv')


def run(args):
    with_dataset(args, updatemd)
