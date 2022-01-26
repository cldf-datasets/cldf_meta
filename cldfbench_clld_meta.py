from collections import defaultdict, namedtuple
import csv
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
GITHUB_REGEX = r'(?:url:)?(?:https?://)?github.com'
COMMUNITY_REGEX = r'(?:url:)?(?:https?://)?zenodo.org/communities'
ZENODO_METADATA_ROWS = [
    'ID',
    'Title',
    'Description',
    'Authors',
    'Creators',
    'Contributors',
    'Communities',
    'License',
    'DOI',
    'DOI_Latest',
    'Zenodo_Link',
    'Github_Link',
    'Subjects']


def oai_ns(elem):
    return '{http://www.openarchives.org/OAI/2.0/}%s' % elem


def oai_dc_ns(elem):
    return '{http://www.openarchives.org/OAI/2.0/oai_dc/}%s' % elem


def dc_ns(elem):
    return '{http://purl.org/dc/elements/1.1/}%s' % elem


def zenodo_records():
    # TODO find a way to search for all records
    #  (ideally on the server-side, rather than downloading *all* the records)

    dl = Sickle(
        OAI_URL,
        retry_status_codes=[503, 429],
        max_retries=3,
        default_retry_after=60,
        iterator=OAIResponseIterator)

    #set='user-dictionaria')
    for resp in dl.ListRecords(metadataPrefix='oai_dc', set='user-lexibank'):
        xml_data = ET.fromstring(resp.raw)

        # TODO get info out of sickle's pre-parsed record objects
        records = xml_data.find(oai_ns('ListRecords')).iter(oai_ns('record'))
        for xml_record in records:
            yield xml_record


def _parse_md(elem):
    if elem.tag == dc_ns('identifier'):
        if re.match('https?://', elem.text):
            return 'Zenodo_Link', elem.text
        elif elem.text.startswith('oai:zenodo.org:'):
            return 'ID', elem.text
        elif re.match(DOI_REGEX, elem.text, re.I):
            return 'DOI', elem.text
        else:
            return None, None
    elif elem.tag == dc_ns('title'):
        return 'Title', elem.text
    elif elem.tag == dc_ns('description'):
        return 'Description', elem.text
    elif elem.tag == dc_ns('author'):
        return 'Authors', elem.text
    elif elem.tag == dc_ns('creator'):
        return 'Creators', elem.text
    elif elem.tag == dc_ns('contributor'):
        return 'Contributors', elem.text
    elif elem.tag == dc_ns('rights'):
        return 'License', elem.text
    elif elem.tag == dc_ns('relation'):
        if re.match(DOI_REGEX, elem.text, re.I):
            return 'DOI_Latest', elem.text
        elif re.match(GITHUB_REGEX, elem.text, re.I):
            return 'Github_Link', elem.text
        elif re.match(COMMUNITY_REGEX, elem.text, re.I):
            return 'Communities', elem.text
        else:
            return None, None
    elif elem.tag == dc_ns('subject'):
        return 'Subjects', elem.text
    else:
        return None, None


def parse_xml_metadata(elem):
    md = defaultdict(lambda y: None)
    list_fields = (
        'Authors', 'Creators', 'Contributors', 'Subjects', 'Communities',
        'License')
    for k in list_fields:
        md[k] = []

    dc = elem.find(oai_ns('metadata')).find(oai_dc_ns('dc'))
    for node in dc:
        k, v = _parse_md(node)
        if not k or not v:
            continue

        v = v.strip()\
            .replace('\\', '\\\\')\
            .replace('\n', '\\n')\
            .replace('\t', ' ')

        if k in list_fields:
            md[k].append(v)
        else:
            md[k] = v

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
        records = list(map(parse_xml_metadata, zenodo_records()))
        def merge_lists(v):
            # TODO use a separator that is guaranteed to not appear in the cells
            return '\\t'.join(v) if isinstance(v, list) else v
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
