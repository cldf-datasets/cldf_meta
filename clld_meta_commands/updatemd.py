"""\
Update Zenodo metadata in `raw/zenodo-metadata.json`.
"""

import csv
import json
import pprint
import re
import sys
from itertools import chain, islice
from urllib.parse import quote

from cldfbench.cli_util import add_dataset_spec, with_dataset

from cerberus import Validator
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


ZENODO_METADATA_SCHEMA = {
    'title': {'type': 'string'},
    'description': {'type': 'string'},
    'version': {'type': 'string'},
    'access_right': {'type': 'string'},
    'publication_date': {'type': 'string'},
    'relations': {
        'type': 'dict',
        'schema': {
            'version': {
                'type': 'list',
                'schema': {
                    'type': 'dict',
                    'schema': {
                        'index': {'type': 'integer'},
                        'is_last': {'type': 'boolean'},
                        'parent': {
                            'type': 'dict',
                            'schema': {
                                'pid_type': {'type': 'string'},
                                'pid_value': {'type': 'string'},
                            },
                        },
                    },
                },
            },
        },
    },
    'related_identifiers': {
        'type': 'list',
        'required': False,
        'schema': {
            'type': 'dict',
            'schema': {
                'identifier': {'type': 'string'},
                'relation': {'type': 'string'},
            },
        },
    },
    'license': {'type': 'dict', 'schema': {'id': {'type': 'string'}}},
    'resource_type': {
        'type': 'dict',
        'required': False,
        'schema': {'type': {'type': 'string'}},
    },
    'keywords': {
        'type': 'list',
        'required': False,
        'schema': {'type': 'string'},
    },
    'creators': {
        'type': 'list',
        'schema': {
            'type': 'dict',
            'schema': {
                'affiliation': {
                    'type': 'string',
                    'nullable': True,
                },
                'name': {'type': 'string'},
            },
        },
    },
    'contributors': {
        'type': 'list',
        'required': False,
        'schema': {
            'type': 'dict',
            'schema': {
                'affiliation': {'type': 'string', 'nullable': True},
                'name': {'type': 'string'},
                'type': {'type': 'string'},
            },
        },
    },
}

ZENODO_FILE_SCHEMA = {
    'type': 'dict',
    'schema': {
        'key': {'type': 'string'},
        'checksum': {'type': 'string'},
        'links': {'type': 'dict', 'schema': {'self': {'type': 'string'}}},
    },
}

ZENODO_RECORD_SCHEMA = {
    'id': {'type': 'integer'},
    'doi': {'type': 'string'},
    'conceptrecid': {'type': 'string'},
    'conceptdoi': {'type': 'string'},
    'created': {'type': 'string'},
    'updated': {'type': 'string'},
    'modified': {'type': 'string'},
    'metadata': {'type': 'dict', 'schema': ZENODO_METADATA_SCHEMA},
    'files': {'type': 'list', 'schema': ZENODO_FILE_SCHEMA},
}

ZENODO_JSON_SCHEMA = {
    'hits': {
        'type': 'dict',
        'schema': {
            'hits': {
                'type': 'list',
                'schema': {'type': 'dict', 'schema': ZENODO_RECORD_SCHEMA},
            },
            'total': {'type': 'integer'},
        },
    },
}

ZENODO_JSON_VALIDATOR = Validator(
    schema=ZENODO_JSON_SCHEMA,
    require_all=True,
    allow_unknown=True)


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
    query_doi = f'doi:"{doi}" OR conceptdoi:"{doi}"'
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
        if not ZENODO_JSON_VALIDATOR.validate(json_data):
            msg = pprint.pformat(ZENODO_JSON_VALIDATOR.errors)
            raise ValueError(f"Zenodo's response has changed\n{msg}")
        if record_total is None:
            record_total = json_data['hits']['total']
        hits = json_data['hits']['hits']
        record_count += len(hits)
        page += 1
        yield hits


def make_flat_record(record):
    new_record = {
        'id': record['id'],
        'doi': record['doi'],
        'conceptid': record['conceptrecid'],
        'conceptdoi': record['conceptdoi'],
        'created': record['created'],
        'updated': record['updated'],
        'modified': record['modified'],
        'title': record['metadata']['title'],
        'description': record['metadata']['description'],
        'version': record['metadata']['version'],
        'access_right': record['metadata']['access_right'],
        'publication_date': record['metadata']['publication_date'],
        'license': record['metadata']['license']['id'],
        'creators': list(map(drop_nulls, record['metadata']['creators'])),
        'files': list(map(flatten_file, record['files'])),
    }
    type_struct = record['metadata'].get('resource_type')
    if type_struct and (type_ := type_struct.get('type')):
        new_record['resource_type'] = type_
    if (keywords := record['metadata'].get('keywords')):
        new_record['keywords'] = keywords
    if (contributors := record['metadata'].get('contributors')):
        new_record['contributors'] = list(map(drop_nulls, contributors))
    if (git_link := retrieve_git_link(record)):
        new_record['git-link'] = git_link
    return new_record


def flatten_file(file):
    return {
        'file_path': file['key'],
        'checksum': file['checksum'],
        'url': file['links']['self'],
    }


def drop_nulls(mapping):
    return {k: v for k, v in mapping.items() if k and v}


def retrieve_git_link(record):
    # I've only seen github so far but I want to at least check for these
    hosts = (
        'bitbucket.org', 'codeberg.org', 'gitlab.', 'sr.ht', 'github.com')
    git_links = [
        relid['identifier']
        for relid in record['metadata'].get('related_identifiers', ())
        if any(host in relid['identifier'] for host in hosts)]
    if len(git_links) < 1:
        return None
    elif len(git_links) == 1:
        return git_links[0]
    else:
        msg = 'WARN {}: multiple git links: {}'.format(
            record['id'], ', '.join(git_links))
        print(msg, file=sys.stderr)
        return git_links[0]


def register(parser):
    add_dataset_spec(parser)


def might_have_cldf_in_it(record):
    """Filter for possible CLDF datasets.

     1. Ignore non-data entries (posters, books, videos, etc.).
     2. Ignore cldf catalogues.
     3. Ignore everything made before 2018 (CLDF didn't exist, yet).
    """
    if (date := record.get('created')):
        match = re.match(r'(\d\d\d\d)-(\d\d)-(\d\d)', date)
        assert match, '`date` needs to be YYYY-MM-DD, not {repr(date)}'
        if int(match.group(1)) < 2018:
            return False

    if (type_ := record.get('resource_type')):
        if type_ in TYPE_BLACKLIST:
            return False

    if (title := record.get('title')):
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
        ' OR '.join(f'"{kw}"' for kw in SEARCH_KEYWORDS))
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
        ' OR '.join(f'"{kw}"' for kw in SEARCH_COMMUNITIES))
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
            (record['id'], record)
            for hits in chain(
                download_records_paginated(keyword_url),
                download_records_paginated(community_url),
                _download_individual_dois(doi_urls))
            for hit in hits
            if might_have_cldf_in_it((record := make_flat_record(hit)))))
    except IOError as err:
        print(err, file=sys.stderr)
        sys.exit(74)

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
