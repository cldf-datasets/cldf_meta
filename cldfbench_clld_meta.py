from collections import defaultdict, namedtuple, OrderedDict
import csv
from functools import partial, reduce
import json
import pathlib
import re
import sys
import time
from urllib import request
from urllib.error import HTTPError
from xml.etree import ElementTree as ET

from bs4 import BeautifulSoup
from sickle import Sickle
from sickle.iterator import OAIResponseIterator
from tqdm import tqdm

from cldfbench import Dataset as BaseDataset


OAI_URL = 'https://zenodo.org/oai2d'

DOI_REGEX = r'(?:doi:)?10(?:\.[0-9]+)+/'
#ZENODO_DOI_REGEX = r'(?:doi:)?10\.5281/zenodo\.'
GITHUB_REGEX = r'(?:url:)?(?:https?://)?github.com'
#COMMUNITY_REGEX = r'(?:url:)?(?:https?://)?zenodo.org/communities'

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


TITLE_BLACKLIST = {
    'Glottolog database 2.2',
    'Glottolog database 2.3',
    'PYCLTS. A Python library for the handling of phonetic transcription systems',
    'CLTS. Cross-Linguistic Transcription Systems',
    'CLTS. Cross-Linguistic Transcription Systems',
    'CLTS. Cross-Linguistic Transcription Systems',
    'CLLD Concepticon 2.3.0',
    'CLLD Concepticon 2.4.0-rc.1',
    'CLLD Concepticon 2.4.0',
    'CLLD Concepticon 2.5.0',
}


def is_valid(record):
    for type_ in record.get('type', ()):
        if type_ in TYPE_BLACKLIST:
            return False

    for title in record.get('title', ()):
        if title in TITLE_BLACKLIST:
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
        elif re.match(GITHUB_REGEX, v, re.I):
            return 'github-link'
        else:
            return None
    else:
        return k


def parse_record(record):
    md = defaultdict(list)
    md['communities'] = record.header.setSpecs
    for k, vs in record.metadata.items():
        for v in vs:
            new_k = _transform_key(k ,v)
            if not new_k:
                continue

            v = v.strip()\
                .replace('\\', '\\\\')\
                .replace('\n', '\\n')\
                .replace('\t', ' ')
            md[new_k].append(v)
    return md


def _id_sort_key(record_row):
    # XXX this assumes zenodo doesn't change their id generation pattern
    return int(re.fullmatch(r'oai:zenodo.org:(\d+)', record_row[0]).group(1))


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


def time_secs():
    return time.time_ns() // 1000000000


def wait_until(secs_since_epoch):
    dt = secs_since_epoch - time_secs()
    print(
        'hit rate limit -- waiting', fmt_time_period(dt),
        'until', time.ctime(secs_since_epoch),
        file=sys.stdout)
    time.sleep(dt)


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
                       'Unexpected http response:', response.status,
                       '\nRetrying (attempt', attempt + 1,
                       'of', '%s)...' % retries,
                       file=sys.stderr)
        else:
            print(
                'Tried', retries, 'times to no avail.  Giving up...',
                file=sys.stderr)
            return


def extract_json(html_string):
    soup = BeautifulSoup(html_string, 'lxml')
    pre_tags = soup.find_all('pre', style="white-space: pre-wrap;")
    if not pre_tags:
        raise ValueError('no <pre> tags found that could contain the json input')
    elif len(pre_tags) > 1:
        raise ValueError('more than one candidate for a json <pre> tag')
    else:
        return json.loads(pre_tags[0].text)


class Dataset(BaseDataset):
    dir = pathlib.Path(__file__).parent
    id = "clld_meta"

    def cldf_specs(self):  # A dataset must declare all CLDF sets it creates.
        return super().cldf_specs()

    def cmd_download(self, args):
        """
        Download files to the raw/ directory. You can use helpers methods of `self.raw_dir`, e.g.

        >>> self.raw_dir.download(url, fname)
        """
        # TODO find a way to search for all records
        #  (ideally on the server-side, rather than downloading *all* the records)

        dl = Sickle(
            OAI_URL,
            retry_status_codes=[503, 429],
            max_retries=3,
            default_retry_after=60)

        communities = (
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
        )
        records = (
            parse_record(record)
            for community in communities
            for record in dl.ListRecords(
                metadataPrefix='oai_dc',
                set=community))
        records = filter(is_valid, records)
        records = uniq(records, key=lambda r: '\t'.join(r['id']))
        records = OrderedDict((record['zenodo-link'][0], record) for record in records)

        print('downloading json metadata...', file=sys.stderr)

        json_links = [
            '{}/export/json'.format(record['zenodo-link'][0])
            for record in records.values()
            if record.get('zenodo-link')]
        # TODO replace tqdm with less cursor-move-y status msg
        json_data = list(download_all(tqdm(json_links)))
        json_data = list(map(extract_json, json_data))

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

        print('additional communities mentioned:')
        old_comms = set(communities)
        new_comms = {
            c
            for record in records.values()
            for c in record.get('communities', ())
            if c not in old_comms}
        print('\n'.join(' * {}'.format(c) for c in sorted(new_comms)))

        def merge_lists(v):
            return '\\t'.join(uniq(v)) if isinstance(v, list) else v
        csv_rows = [
            [merge_lists(record.get(k) or '') for k in ZENODO_METADATA_ROWS]
            for record in records.values()]
        csv_rows.sort(key=_id_sort_key)
        with open(self.raw_dir / 'zenodo-metadata.csv', 'w', encoding='utf-8') as f:
            wrt = csv.writer(f)
            wrt.writerow(ZENODO_METADATA_ROWS)
            wrt.writerows(csv_rows)

    def cmd_makecldf(self, args):
        """
        Convert the raw data to a CLDF dataset.

        >>> args.writer.objects['LanguageTable'].append(...)
        """
