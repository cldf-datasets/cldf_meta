from collections import defaultdict, namedtuple
import csv
import pathlib
import re
import sys
import time
from urllib import request
from xml.etree import ElementTree as ET

from cldfbench import Dataset as BaseDataset


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


def wait_a_minute(msg):
    sys.stderr.write(msg)
    sys.stderr.write('\n')
    secs = 60
    interval = 5
    while secs > 0:
        sys.stderr.write('{}..'.format(secs))
        # flush the stream -- a status bar that only updates
        # when everything is done isn't particularly useful
        sys.stderr.flush()
        time.sleep(interval)
        secs -= interval
    sys.stderr.write('0\n')


class MetaDataDownloader:

    def __init__(self):
        self.rate_limit_remaining = None

    def next_batch(self, url):
        if self.rate_limit_remaining == 0:
            wait_a_minute(
                'We hit the rate limit\n'
                'Waiting 60s before next request, '
                "so Zenodo doesn't get mad at us...\n")
        # try three times
        retries = 3
        for n in range(retries):
            with request.urlopen(url) as response:
                self.rate_limit_remaining = response.headers.get(
                    'X-RateLimit-Remaining')
                if response.status == 200:
                    # ok
                    return response.read()
                elif response.status == 429:
                    # too many requests
                    wait_a_minute(
                        'Request failed due to rate limit\n'
                        'Waiting 60s before retrying, '
                        "so Zenodo doesn't get mad at us...\n"
                        '(attempt {} of {})'.format(n + 1, retries))
                else:
                    print(
                        'Unexpected http response:', response.status,
                        '\nRetrying (attempt', n + 1, 'of', '%s)...' % retries,
                        file=sys.stderr)
        else:
            print('Tried 3 times to no avail.  Giving up...', file=sys.stderr)
            return None


def zenodo_records():
    OAI_URL = "https://zenodo.org/oai2d"
    # TODO find a way to search for all records
    #  (ideally on the server-side, rather than downloading *all* the records)

    dl = MetaDataDownloader()

    # TODO escape community
    #set='user-dictionaria')
    url = '{url}?verb={verb}&metadataPrefix={md_prefix}&set={set}'.format(
        url=OAI_URL,
        verb='ListRecords',
        md_prefix='oai_dc',
        set='user-lexibank')

    # TODO remove debug code
    import time
    nr = 0
    date = time.strftime('%Y-%m-%d-%Hh%Mm%Ss', time.localtime())

    download_more = True
    while download_more:
        download_more = False

        raw_data = dl.next_batch(url)
        if not raw_data:
            break

        # TODO remove debug code
        nr += 1
        with open('tmp/{}-{}'.format(date, nr), 'wb') as f:
            f.write(raw_data)

        xml_data = ET.fromstring(raw_data.decode('utf-8'))

        records = xml_data.find(oai_ns('ListRecords')).iter(oai_ns('record'))
        for xml_record in records:
            yield xml_record

        resumption_token_xml = xml_data\
            .find(oai_ns('ListRecords'))\
            .find(oai_ns('resumptionToken'))
        resumption_token = getattr(resumption_token_xml, 'text', None)
        if resumption_token:
            url = '{url}?verb={verb}&resumptionToken={token}'.format(
                url=OAI_URL,
                verb='ListRecords',
                token=resumption_token)
            download_more = True


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
