from collections import Counter, namedtuple
import contextlib
import csv
import hashlib
from itertools import chain, islice
import io
import json
from multiprocessing import Pool
import os
from pathlib import Path
import re
import sys
import time
from urllib import request
from urllib.error import HTTPError
from urllib.parse import urlparse
import zipfile
from cldfbench import Dataset as BaseDataset
from cldfbench.cldf import CLDFSpec
from pycldf.dataset import Dataset as CLDFDataset, SchemaError, sniff
from pycldf.terms import URL as TERMS_URL


CLDFError = namedtuple('CLDFError', 'record_no file reason')


### Stuff that needs to be put in some sort of library ###

# FIXME code duplication
def get_access_token():
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
                        'Unexpected http response:', e.code,
                        '\nRetrying (attempt', attempt + 1,
                        'of', '%s)...' % retries,
                        file=sys.stderr, flush=True)
        else:
            print(
                'Tried', retries, 'times to no avail.  Giving up...',
                file=sys.stderr, flush=True)
            return


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


### Data download ###

def file_basename(file):
    basename = re.search(
        r'/([^/]+?)(?:\?[^/]*)?(?:#[^/]*)?$',
        file['links']['self']).group(1)
    assert basename
    if not basename.endswith('.{}'.format(file['type'])):
        basename = '{}.{}'.format(basename, file['type'])
    return basename


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


def _download_datasets(raw_dir, files, access_token=None):
    urls = (file['links']['self'] for _, file in files)
    if access_token:
        urls = (add_access_token(url, access_token) for url in urls)
    dls = download_all(loggable_progress(urls, file=sys.stderr))
    for raw_data, (id_, file) in zip(dls, files):
        validate_checksum(file['checksum'], raw_data)
        basename = file_basename(file)
        output_folder = raw_dir / id_
        output_folder.mkdir(parents=True, exist_ok=True)
        output_file = output_folder / basename
        output_file.write_bytes(raw_data)


### Loading data ###

def _has_downloaded_data(datadir, record):
    record_dir = datadir.joinpath(str(record['id']))
    return record_dir.exists() and any(record_dir.iterdir())


def _is_blacklisted(blacklist, record):
    return (
        record.get('doi') in blacklist
        or record.get('conceptdoi') in blacklist)


def _has_zip(record):
    """Return True if a record might contain a cldf dataset."""
    return any(file['type'] == 'zip' for file in record.get('files', ()))


def _dataset_exists(raw_dir, record):
    record_no = record['id']
    dataset_dir = Path(raw_dir) / 'datasets' / str(record_no)

    if not dataset_dir.exists():
        return False, '{}: dataset folder not found'.format(dataset_dir)
    elif not any(dataset_dir.iterdir()):
        return False, '{}: dataset folder empty'.format(dataset_dir)
    else:
        return True, ''


def find_missing_datasets(raw_dir, records):
    results = [_dataset_exists(raw_dir, rec) for rec in records]
    return [err for exists, err in results if not exists]


def get_cldf_json(f):
    try:
        if not f.read(10).lstrip().startswith(b'{'):
            return None
        f.seek(0)
        json_data = json.load(f, encoding='utf-8')
        if not json_data.get('dc:conformsTo', '').startswith(TERMS_URL):
            return None
        return json_data
    except Exception:
        return None


class ZipDataReader:
    def __init__(self, zip_file, zip_infos, md_root, cldf_md):
        self._zip_file = zip_file
        self._zip_infos = zip_infos
        self._md_root = md_root
        self._cldf_md = cldf_md

    def cldf_module(self):
        return self._cldf_md['dc:conformsTo'].split('#')[-1]

    def get_table(self, name_or_url):
        url = '{}#{}'.format(TERMS_URL, name_or_url)
        for table in self._cldf_md['tables']:
            if table.get('dc:conformsTo', '') == url:
                return table
        else:
            raise ValueError('table not found: {}'.format(name_or_url))

    def iterrows(self, table, *columns):
        try:
            table = self.get_table(table)
        except ValueError:
            return
        col_urls = {'{}#{}'.format(TERMS_URL, col): col for col in columns}

        def get_colname(spec):
            purl = spec.get('propertyUrl')
            if purl in col_urls:
                return col_urls[purl]
            else:
                return None

        col_names = list(map(get_colname, table['tableSchema']['columns']))

        relpath = table['url']
        root = self._md_root
        while relpath.startswith('../'):
            relpath, root = relpath[3:], root.parent
        relpath_zip = '{}.zip'.format(relpath)

        zip_info = (
            self._zip_infos.get(root / relpath_zip)
            or self._zip_infos.get(root / relpath))
        if zip_info is None:
            # TODO: maybe show an error message?
            return

        with contextlib.ExitStack() as withs:
            csv_f = withs.enter_context(self._zip_file.open(zip_info))
            if zip_info.filename.endswith('.zip'):
                internal_zip = withs.enter_context(zipfile.ZipFile(csv_f))
                internal_info = [
                    info
                    for info in internal_zip.infolist()
                    if info.filename.endswith(Path(relpath).name)][0]
                csv_f = withs.enter_context(internal_zip.open(internal_info))
            decoder = io.TextIOWrapper(csv_f, encoding='utf-8')
            rdr = csv.reader(decoder)
            for row in islice(rdr, 1, None):
                yield {
                    colname: cell
                    for colname, cell in zip(col_names, row)
                    if colname and cell}


def _stats_from_zip(args):
    record_no, zip_path = args
    found_data = False
    with zipfile.ZipFile(zip_path) as zip:
        file_tree = {Path(info.filename): info for info in zip.infolist()}
        for path, info in file_tree.items():
            # TODO: try and filter out raw/ and test/ folders
            if path.suffix != '.json':
                continue
            with zip.open(info) as f:
                cldf_md = get_cldf_json(f)
            if cldf_md is None:
                continue
            zipreader = ZipDataReader(zip, file_tree, path.parent, cldf_md)
            found_data = True
            yield collect_dataset_stats(zipreader), None
    if not found_data:
        yield None, CLDFError(record_no, zip_path.name, 'nocldf')


def stats_from_zip(args):
    return list(_stats_from_zip(args))


# FIXME not happy with that function name
def collect_dataset_stats(zipreader):
    values = [
        (r['languageReference'], r.get('parameterReference'))
        for r in zipreader.iterrows(
            'ValueTable', 'languageReference', 'parameterReference')
        if r.get('languageReference')]
    lang_values = Counter(l for l, _ in values)
    # XXX: count parameters and concepts separately?
    #  if so -- how?
    lang_features = Counter((l, p) for l, p in values if p)

    lang_forms = Counter(
        r['languageReference']
        for r in zipreader.iterrows('FormTable', 'languageReference')
        if r.get('languageReference'))

    lang_entries = Counter(
        r['languageReference']
        for r in zipreader.iterrows('EntryTable', 'languageReference')
        if r.get('languageReference'))

    lang_examples = Counter(
        r['languageReference']
        for r in zipreader.iterrows('ExampleTable', 'languageReference')
        if r.get('languageReference'))

    lang_iter = chain(lang_values, lang_forms, lang_examples, lang_entries)
    langtable = {
        r['id']: r.get('glottocode') or r.get('iso639P3code') or r.get('id')
        for r in zipreader.iterrows(
            'LanguageTable', 'id', 'glottocode', 'iso639P3code')
        if r.get('id')}
    langs = {v: (langtable.get(v) or v) for v in lang_iter}

    # TODO count concepticon ids?

    return {
        'module': zipreader.cldf_module(),
        'value_count': len(values),
        'langs': langs,
        'lang_values': lang_values,
        'lang_features': lang_features,
        'lang_forms': lang_forms,
        'lang_entries': lang_entries,
        'lang_examples': lang_examples,
    }


def raw_stats_to_glottocode_stats(stats, by_glottocode, by_isocode):
    lang_map = {
        lid: (by_glottocode.get(guess) or by_isocode[guess]).id
        for lid, guess in stats['langs'].items()
        if guess in by_glottocode or guess in by_isocode}
    return {
        'module': stats['module'],
        'value_count': stats['value_count'],
        'lang_count': len(stats['langs']),
        'glottocode_count': len(lang_map),
        'langs': sorted(set(lang_map.values())),
        'lang_values': {
            lang_map[l]: c
            for l, c in stats['lang_values'].items()
            if l in lang_map},
        'lang_features': {
            lang_map[l]: c
            for l, c in stats['lang_features'].items()
            if l in lang_map},
        'lang_forms': {
            lang_map[l]: c
            for l, c in stats['lang_forms'].items()
            if l in lang_map},
        'lang_entries': {
            lang_map[l]: c
            for l, c in stats['lang_entries'].items()
            if l in lang_map},
        'lang_examples': {
            lang_map[l]: c
            for l, c in stats['lang_examples'].items()
            if l in lang_map},
    }


class ErrorFilter:
    def __init__(self):
        self.errors = []

    def filter(self, iterable):
        for val, err in iterable:
            if err is not None:
                self.errors.append(err)
            if val is not None:
                yield val


### CLDFbench ###

class Dataset(BaseDataset):
    dir = Path(__file__).parent
    id = "clld_meta"

    def cldf_specs(self):  # A dataset must declare all CLDF sets it creates.
        return CLDFSpec(
            dir=self.cldf_dir,
            module='Generic',
            metadata_fname='cldf-metadata.json')

    def cmd_download(self, args):
        """
        Download files to the raw/ directory. You can use helpers methods of `self.raw_dir`, e.g.

        >>> self.raw_dir.download(url, fname)
        """
        access_token = get_access_token()

        try:
            records = self.raw_dir.read_json('zenodo-metadata.json')['records']
        except IOError:
            args.log.error(
                'No zenodo metadata found.'
                '  Run `cldfbench clld-meta.updatemd cldfbench_clld_meta.py`'
                '  to download the metadata.')
            return

        files_without_cldf = {
            (record_no, file)
            for record_no, file, _ in islice(
                self.etc_dir.read_csv('not-cldf.csv'), 1, None)}

        datadir = self.raw_dir / 'datasets'
        # only download if raw/<id> folder is missing or empty

        # TODO: add 'All Versions' DOI for the meta database itself, once we have one.
        with open(self.etc_dir / 'blacklist.csv', encoding='utf-8') as f:
            rdr = csv.reader(f)
            blacklist = {doi for doi, _ in islice(rdr, 1, None) if doi}

        records = [
            rec
            for rec in records
            if not _has_downloaded_data(datadir, rec)
            and not _is_blacklisted(blacklist, rec)]
        # XXX how will I know if someone packages a cldf dataset as a tarball…?
        file_urls = [
            (str(rec['id']), file)
            for rec in records
            for file in rec.get('files', ())
            if file['type'] == 'zip'
            and (str(rec['id']), file_basename(file)) not in files_without_cldf]

        if file_urls:
            print(
                'downloading', len(file_urls), 'datasets...',
                file=sys.stderr, flush=True)
            _download_datasets(datadir, file_urls, access_token=access_token)
        else:
            print(
                'Datasets already up-to-date.',
                file=sys.stderr, flush=True)

    def cmd_makecldf(self, args):
        """
        Convert the raw data to a CLDF dataset.

        >>> args.writer.objects['LanguageTable'].append(...)
        """
        # Prepare metadata

        with open(self.etc_dir / 'blacklist.csv', encoding='utf-8') as f:
            rdr = csv.reader(f)
            blacklist = {doi for doi, _ in islice(rdr, 1, None) if doi}

        records = [
            rec
            for rec in self.raw_dir.read_json('zenodo-metadata.json')['records']
            if _has_zip(rec)
            and not _is_blacklisted(blacklist, rec)]
        not_cldf_full = [
            CLDFError(*row)
            for row in islice(self.etc_dir.read_csv('not-cldf.csv'), 1, None)]

        # Read CLDF data

        print('finding cldf datasets..', file=sys.stderr, flush=True)
        not_cldf = {(err.record_no, err.file) for err in not_cldf_full}
        data_archives = [
            (rec['id'], self.raw_dir / 'datasets' / str(rec['id']) / fname)
            for rec in records
            for fname in map(file_basename, rec['files'])
            if fname.endswith('.zip')
            and (str(rec['id']), fname) not in not_cldf]

        missing_files = [
            (record_no, path)
            for record_no, path in data_archives
            if not path.is_file()]
        if missing_files:
            print(
                '\n'.join(
                    '{}:{}: file not found'.format(record_no, path.name)
                    for record_no, path in missing_files),
                file=sys.stderr)
            print(
                'ERROR: Some datasets seem to be missing in raw/.',
                'You might have to re-run `cldfbench download`.',
                sep='\n', file=sys.stderr, flush=True)
            return

        print(
            'extracting databases from', len(data_archives), 'zip files...',
            file=sys.stderr, flush=True)
        cldf_errors = ErrorFilter()
        with Pool() as pool:
            dataset_stats = list(cldf_errors.filter(
                (stats, err)
                for chunk in loggable_progress(
                    pool.imap(stats_from_zip, data_archives))
                for stats, err in chunk))
        if cldf_errors.errors:
            print(
                '\n'.join(
                    '{}:{}: no cldf data found'.format(err.record_no, err.file)
                    for err in cldf_errors.errors),
                file=sys.stderr)
            not_cldf_full.extend(cldf_errors.errors)
            not_cldf_full.sort(key=lambda err: int(err.record_no))
            not_cldf_path = self.etc_dir / 'not-cldf.csv'
            with open(not_cldf_path, 'w', encoding='utf-8') as f:
                wtr = csv.writer(f)
                wtr.writerow(CLDFError._fields)
                wtr.writerows(not_cldf_full)

        print(
            'loading language info from glottolog...',
            file=sys.stderr, flush=True)
        by_glottocode = {l.id: l for l in args.glottolog.api.languoids()}
        by_isocode = {l.iso: l for l in by_glottocode.values() if l.iso}

        dataset_stats = [
            raw_stats_to_glottocode_stats(stats, by_glottocode, by_isocode)
            for stats in dataset_stats]

        # Create CLDF tables

        print('assembling language table...', file=sys.stderr, flush=True)

        all_glottocodes = sorted({
            lid
            for stats in dataset_stats
            for lid in stats['langs']})

        def macroarea(l):
            m = l.macroareas
            return m[0].name if m else ''
        languages = [
            {
                'ID': lid,
                'Name': by_glottocode[lid].name,
                'Macroarea': macroarea(by_glottocode[lid]),
                'Latitude': by_glottocode[lid].latitude,
                'Longitude': by_glottocode[lid].longitude,
                'Glottocode': lid,
                'ISO639P3code': (by_glottocode[lid].iso or ''),
            }
            for lid in all_glottocodes]

        # TODO count all teh things! o/

        datasets_per_contrib = Counter()

        def count_datasets(record_no):
            datasets_per_contrib[record_no] += 1
            return datasets_per_contrib[record_no]

        print('assembling dataset tables...', file=sys.stderr, flush=True)

        # # XXX how idempotent is this?
        datasets = [
            {
                'ID': '{}-{}'.format(record_no, count_datasets(record_no)),
                'Contribution_ID': record_no,
                'Module': stats['module'],
                'Language_Count': len(stats['langs']),
                'Value_Count': stats['value_count'],
                'Glottocode_Count': stats['glottocode_count'],
            }
            for ((record_no, _), stats) in zip(data_archives, dataset_stats)]

        dataset_languages = [
            {
                'ID': '{}-{}'.format(ds['ID'], lid),
                'Language_ID': lid,
                'Dataset_ID': ds['ID'],
                'Value_Count': stats['lang_values'].get(lid, 0),
                'Parameter_Count': stats['lang_features'].get(lid, 0),
                'Form_Count': stats['lang_forms'].get(lid, 0),
                'Entry_Count': stats['lang_entries'].get(lid, 0),
                'Example_Count': stats['lang_examples'].get(lid, 0),
            }
            for ds, stats in zip(datasets, dataset_stats)
            for lid in stats['langs']]

        contributions = [
            {
                'ID': rec['id'],
                'Name': rec['metadata']['title'],
                'Description': rec['metadata']['description'],
                'Version': rec['metadata']['version'],
                'Creators': [
                    c['name'] for c in rec['metadata']['creators']],
                'Contributors': [
                    c['name'] for c in rec['metadata'].get('contributors', ())],
                'DOI': rec['doi'],
                'Concept_DOI': rec['conceptdoi'],
                'Parent_ID': rec['conceptrecid'],
                # TODO: extract github link somehow...
                # 'GitHub_Link': contrib_md['github-link'],
                'Date_Created': rec['created'],
                'Date_Updated': rec['updated'],
                'Communities': [
                    c['id'] for c in rec['metadata'].get('communities', ())],
                'License': rec['metadata']['license']['id'],
                'Zenodo_ID': rec['id'],
                'Zenodo_Link': rec['links']['html'],
                'Zenodo_Keywords': rec['metadata']['keywords'],
                'Zenodo_Type': rec['metadata']['resource_type']['type'],
            }
            for rec in records]

        # Write CLDF data

        print('writing cldf data...', file=sys.stderr, flush=True)

        args.writer.cldf.add_component('LanguageTable')

        args.writer.cldf.add_table(
            'contributions.csv',
            'http://cldf.clld.org/v1.0/terms.rdf#id',
            'http://cldf.clld.org/v1.0/terms.rdf#name',
            'http://cldf.clld.org/v1.0/terms.rdf#description',
            'Version',
            {'name': 'Creators', 'separator': ' ; '},
            {'name': 'Contributors', 'separator': ' ; '},
            'DOI',
            'Concept_DOI',
            'Date',
            {'name': 'Communities', 'separator': ';'},
            'License',
            'Zenodo_Link',
            'Zenodo_ID',
            'Parent_ID',
            {'name': 'Zenodo_Keyword', 'separator': ';'},
            'Zenodo_Type',
            'GitHub_Link')

        args.writer.cldf.add_table(
            'datasets.csv',
            'http://cldf.clld.org/v1.0/terms.rdf#id',
            'Contribution_ID',
            'Module',
            {'name': 'Language_Count', 'datatype': 'integer'},
            {'name': 'Value_Count', 'datatype': 'integer'},
            {'name': 'Glottocode_Count', 'datatype': 'integer'})
        args.writer.cldf.add_foreign_key(
            'datasets.csv', 'Contribution_ID', 'contributions.csv', 'ID')

        args.writer.cldf.add_table(
            'dataset-languages.csv',
            'http://cldf.clld.org/v1.0/terms.rdf#id',
            'Dataset_ID',
            'http://cldf.clld.org/v1.0/terms.rdf#languageReference',
            {'name': 'Value_Count', 'datatype': 'integer'},
            {'name': 'Parameter_Count', 'datatype': 'integer'},
            {'name': 'Form_Count', 'datatype': 'integer'},
            {'name': 'Entry_Count', 'datatype': 'integer'},
            {'name': 'Example_Count', 'datatype': 'integer'})
        args.writer.cldf.add_foreign_key(
            'dataset-languages.csv', 'Dataset_ID',
            'datasets.csv', 'ID')

        args.writer.objects['LanguageTable'] = languages
        args.writer.objects['contributions.csv'] = contributions
        args.writer.objects['datasets.csv'] = datasets
        args.writer.objects['dataset-languages.csv'] = dataset_languages
