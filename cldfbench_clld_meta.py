from collections import defaultdict, namedtuple
import csv
from functools import partial, reduce
import pathlib
import re
import sys
import time
from urllib import request
from xml.etree import ElementTree as ET

from sickle import Sickle
from sickle.iterator import OAIResponseIterator

from cldfbench import Dataset as BaseDataset


OAI_URL = 'https://zenodo.org/oai2d'

DOI_REGEX = r'(?:doi:)?10(?:\.[0-9]+)+/'
#ZENODO_DOI_REGEX = r'(?:doi:)?10\.5281/zenodo\.'
GITHUB_REGEX = r'(?:url:)?(?:https?://)?github.com'
COMMUNITY_REGEX = r'(?:url:)?(?:https?://)?zenodo.org/communities'

ZENODO_METADATA_ROWS = [
    'id',
    'date',
    'title',
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
]


def uniq(iterable):
    seen_before = set()
    for item in iterable:
        if item not in seen_before:
            seen_before.add(item)
            yield item


def zenodo_records():
    # TODO find a way to search for all records
    #  (ideally on the server-side, rather than downloading *all* the records)

    dl = Sickle(
        OAI_URL,
        retry_status_codes=[503, 429],
        max_retries=3,
        default_retry_after=60)

    #set='user-dictionaria')
    return dl.ListRecords(metadataPrefix='oai_dc', set='user-lexibank')


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
        elif re.match(COMMUNITY_REGEX, v, re.I):
            return 'communities'
        else:
            return None
    else:
        return k


def parse_record_md(record_md):
    md = defaultdict(list)
    for k, vs in record_md.items():
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
        records = [
            parse_record_md(record.metadata)
            for record in zenodo_records()]

        def merge_lists(v):
            return '\\t'.join(uniq(v)) if isinstance(v, list) else v
        csv_rows = [
            [merge_lists(record.get(k) or '') for k in ZENODO_METADATA_ROWS]
            for record in records]
        csv_rows.sort(key=lambda r: r[0])
        with open(self.raw_dir / 'zenodo-metadata.csv', 'w', encoding='utf-8') as f:
            wrt = csv.writer(f)
            wrt.writerow(ZENODO_METADATA_ROWS)
            wrt.writerows(csv_rows)

    def cmd_makecldf(self, args):
        """
        Convert the raw data to a CLDF dataset.

        >>> args.writer.objects['LanguageTable'].append(...)
        """
