"""\
Update Zenodo metadata in `raw/zenodo-metadata.json`.
"""

from itertools import chain, islice
import csv
import json
import re
import sys
from urllib.parse import quote

from cldfbench.cli_util import add_dataset_spec, with_dataset

from clld_meta import download as dl
from clld_meta.util import loggable_progress


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


def build_search_url(params):
    """Build url for downloading record metadata from Zenodo."""
    entity = 'records'
    api = 'https://zenodo.org/api'
    param_str = '&'.join(
        '{}={}'.format(quote(k, safe=''), quote(v, safe=''))
        for k, v in params)
    return '{api}/{entity}{param_prefix}{params}'.format(
        api=api, entity=entity,
        param_prefix='?' if param_str else '',
        params=param_str)


def build_doi_url(access_token, doi):
    query_doi = 'doi:"{0}" OR conceptdoi:"{0}"'.format(doi)
    params_doi = [
        ('sort', 'mostrecent'),
        ('all_versions', 'true'),
        ('q', query_doi),
        ('status', 'published'),
    ]
    if access_token:
        params_doi.append(('access_token', access_token))
    return build_search_url(params_doi)


def download_records_paginated(url):
    chunk_size = 100
    page = 1
    record_total = None
    record_count = 0
    while record_total is None or record_count < record_total:
        the_url = f'{url}&size={chunk_size}&page={page}'
        raw_data = dl.download_or_wait(the_url)
        json_data = json.loads(raw_data)
        if record_total is None:
            record_total = json_data['hits']['total']
        hits = json_data['hits']['hits']
        record_count += len(hits)
        page += 1
        yield hits


def register(parser):
    add_dataset_spec(parser)


def is_valid(record):
    """Filter for possible CLDF datasets.

     1. Ignore non-data entries (posters, books, videos, etc.).
     2. Ignore cldf catalogues.
     3. Ignore everything made before 2018 (CLDF didn't exist, yet).
    """
    if (date := record.get('created')):
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
    access_token = dl.retrieve_access_token()

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

    with open(dataset.etc_dir / 'whitelist.csv', encoding='utf-8') as f:
        rdr = csv.reader(f)
        whitelist = [doi for doi, _ in islice(rdr, 1, None) if doi]

    print('downloading records...', file=sys.stderr, flush=True)

    query_kw = 'keywords:({})'.format(
        ' OR '.join('"{}"'.format(kw) for kw in SEARCH_KEYWORDS))
    params_kw = [
        ('sort', 'mostrecent'),
        ('all_versions', 'true'),
        ('q', query_kw),
        ('type', 'dataset'),
        ('status', 'published'),
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
    ]
    if access_token:
        params_comm.append(('access_token', access_token))

    keyword_url = build_search_url(params_kw)
    community_url = build_search_url(params_comm)
    doi_urls = [
        build_doi_url(access_token, doi)
        for doi in whitelist]

    def _download_individual_dois(doi_urls):
        for hits in map(download_records_paginated, doi_urls):
            yield from hits

    try:
        records.update(loggable_progress(
            (hit['id'], hit)
            for hits in chain(
                download_records_paginated(keyword_url),
                download_records_paginated(community_url),
                _download_individual_dois(doi_urls))
            for hit in hits
            if is_valid(hit)))
    except IOError as err:
        print(err, file=sys.stderr)
        sys.exit(74)

    # We don't need Zenodo's view/download stats; they just create unnecessary
    # diffs.
    for record in records.values():
        if 'stats' in record:
            del record['stats']

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
