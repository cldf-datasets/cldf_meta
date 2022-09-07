from collections import Counter, OrderedDict
import hashlib
import io
from itertools import chain, repeat
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
from pycldf.dataset import Dataset as CLDFDataset, SchemaError, sniff


### Helpers ###

def zenodo_id(zenodo_link):
    match = re.fullmatch(r'https://zenodo\.org/record/(\d+)', zenodo_link)
    if match:
        return match.group(1)
    else:
        raise ValueError('Zenodo link looks funny: {}'.format(zenodo_link))


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
                file=sys.stderr, flush=True)
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


### Data download ###

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


def _download_datasets(raw_dir, file_urls):
    dls = download_all(loggable_progress(
        (furl for _, furl, _, _ in file_urls),
        file=sys.stderr))
    for raw_data, (id_, furl, ftype, fsum) in zip(dls, file_urls):
        validate_checksum(fsum, raw_data)
        output_folder = raw_dir / id_
        output_folder.mkdir(parents=True, exist_ok=True)
        # XXX support more file types?
        # XXX it is possible that several zip files are dumped into the same folder
        # whether that's a problem or not, I don't know
        with zipfile.ZipFile(io.BytesIO(raw_data)) as zipped_data:
            zipped_data.extractall(output_folder)


### Loading data ###

def _dataset_exists(raw_dir, contrib_md):
    record_no = zenodo_id(contrib_md.get('zenodo-link') or '')
    dataset_dir = Path(raw_dir) / 'datasets' / record_no

    if not dataset_dir.exists():
        return False, '{}: dataset folder not found'.format(dataset_dir)
    elif not any(dataset_dir.iterdir()):
        return False, '{}: dataset folder empty'.format(dataset_dir)
    else:
        return True, ''


def find_missing_datasets(raw_dir, json_md):
    results = [_dataset_exists(raw_dir, row) for row in json_md]
    return [msg for success, msg in results if not success]


def try_to_load_table(dataset, table, *rows):
    try:
        # we need to actually loop here, otherwise the ValueError gets thrown
        # at the caller.
        for row in dataset.iter_rows(table, *rows):
            yield row
    except SchemaError:
        # If a table doesn't have the columns we need, we won't read it
        return
    except ValueError:
        # If a table is invalid, we simply won't read it
        return
    except IOError:
        # If we can't read a table, we can't read the table
        return


# FIXME not happy with that function name
def collect_dataset_stats(dataset):
    if 'ValueTable' in dataset:
        values = [
            (r['languageReference'], r.get('parameterReference'))
            for r in try_to_load_table(
                dataset, 'ValueTable', 'languageReference', 'parameterReference')
            if r.get('languageReference')]
        lang_values = Counter(l for l, _ in values)
        # XXX: count parameters and concepts separately?
        #  if so -- how?
        lang_features = Counter((l, p) for l, p in values if p)
    else:
        values = []
        lang_values = Counter()
        lang_features = Counter()

    if 'FormTable' in dataset:
        lang_forms = Counter(
            r['languageReference']
            for r in try_to_load_table(
                dataset, 'FormTable', 'languageReference')
            if r.get('languageReference'))
    else:
        lang_forms = Counter()

    if 'EntryTable' in dataset:
        lang_entries = Counter(
            r['languageReference']
            for r in try_to_load_table(
                dataset, 'EntryTable', 'languageReference')
            if r.get('languageReference'))
    else:
        lang_entries = Counter()

    if 'ExampleTable' in dataset:
        lang_examples = Counter(
            r['languageReference']
            for r in try_to_load_table(
                dataset, 'ExampleTable', 'languageReference')
            if r.get('languageReference'))
    else:
        lang_examples = Counter()

    lang_iter = chain(lang_values, lang_forms, lang_examples, lang_entries)
    if 'LanguageTable' in dataset:
        langtable = {
            row.get('id'): (
                row.get('glottocode')
                or row.get('iso639P3code')
                or row.get('id'))
            for row in try_to_load_table(
                dataset, 'LanguageTable', 'id', 'glottocode', 'iso639P3code')}
        langs = {v: (langtable.get(v) or v) for v in lang_iter}
    else:
        langs = {v: v for v in lang_iter}

    # TODO count concepticon ids?

    return {
        'module': dataset.module,
        'value_count': len(values),
        'langs': langs,
        'lang_values': lang_values,
        'lang_features': lang_features,
        'lang_forms': lang_forms,
        'lang_examples': lang_examples,
    }


def load_dataset_helper(file_path):
    return collect_dataset_stats(CLDFDataset.from_metadata(file_path))


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
        'langs': list(lang_map.values()),
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
        'lang_examples': {
            lang_map[l]: c
            for l, c in stats['lang_examples'].items()
            if l in lang_map},
    }


### CLDFbench ###

class Dataset(BaseDataset):
    dir = Path(__file__).parent
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

        access_token = get_access_token()

        try:
            records = {
                record['zenodo-link']: record
                for record in self.raw_dir.read_csv(
                    'zenodo-metadata.csv',
                    dicts=True)
            }
        except IOError:
            records = {}

        def _ftypes(ftypes):
            return chain(ftypes, repeat(ftypes[-1])) if ftypes else ()

        dataset_dir = self.raw_dir / 'datasets'
        # XXX how will I know if someone packages a cldf dataset as a tarball…?
        file_urls = [
            (zenodo_id(zenodo_link), furl, ftype, fsum)
            for zenodo_link, record in records.items()
            for furl, ftype, fsum in zip(
                record.get('file-links').split('\\t') or (),
                _ftypes(record.get('file-types').split('\\t') or ()),
                record.get('file-checksums').split('\\t') or ())
            if ftype == 'zip']
        # only download if raw/<id> folder is missing or empty
        file_urls = [
            (id_, furl, ftype, fsum)
            for (id_, furl, ftype, fsum) in file_urls
            if (not dataset_dir.joinpath(id_).exists()
                or not any(dataset_dir.joinpath(id_).iterdir()))]
        if access_token:
            file_urls = [
                (id_, add_access_token(furl, access_token), ftype, fsum)
                for (id_, furl, ftype, fsum) in file_urls]

        if file_urls:
            print(
                'downloading', len(file_urls), 'datasets...',
                file=sys.stderr, flush=True)
            _download_datasets(dataset_dir, file_urls)
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

        json_md = self.raw_dir.read_csv('zenodo-metadata.csv', dicts=True)
        json_md = [
            row
            for row in json_md
            if 'zip' in row.get('file-types', '').split('\\t')]

        # Read CLDF data

        # before doing anything, check that the datasets have all been
        # downloaded propery
        error_messages = find_missing_datasets(self.raw_dir, json_md)
        if error_messages:
            print(
                '\n'.join(error_messages),
                'ERROR: Some datasets seem to be missing in raw/.',
                'You might have to re-run `cldfbench download`.',
                sep='\n', file=sys.stderr, flush=True)
            return

        print('finding cldf datasets..', file=sys.stderr, flush=True)
        record_nos = [
            zenodo_id(contrib_md.get('zenodo-link') or '')
            for contrib_md in json_md]
        data_dirs = [
            (record_no, self.raw_dir / 'datasets' / record_no)
            for record_no in record_nos
            if (self.raw_dir / 'datasets' / record_no).exists()]
        # FIXME Remove debug code
        data_dirs = data_dirs[200:300]
        cldf_metadata_files = [
            (record_no, Path(dirpath) / fname)
            for record_no, data_dir in data_dirs
            for dirpath, _, filenames in os.walk(data_dir)
            for fname in filenames
            if fname.endswith('.json') and sniff(Path(dirpath) / fname)]

        print(
            'loading', len(cldf_metadata_files), 'cldf databases...',
            file=sys.stderr, flush=True)
        with Pool() as pool:
            dataset_stats = list(loggable_progress(pool.imap(
                load_dataset_helper,
                (p for _, p in cldf_metadata_files))))

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
                'Glottocode': lid,
                'ISO639P3code': (by_glottocode[lid].iso or ''),
                'Macroarea': macroarea(by_glottocode[lid]),
                'Latitude': by_glottocode[lid].latitude,
                'Longitude': by_glottocode[lid].longitude,
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
            for ((record_no, _), stats) in zip(cldf_metadata_files, dataset_stats)]

        dataset_languages = [
            {
                'ID': '{}-{}'.format(ds['ID'], lid),
                'Language_ID': lid,
                'Dataset_ID': ds['ID'],
                'Value_Count': stats['lang_values'].get(lid, 0),
                'Parameter_Count': stats['lang_features'].get(lid, 0),
                'Form_Count': stats['lang_forms'].get(lid, 0),
                'Example_Count': stats['lang_examples'].get(lid, 0),
            }
            for ds, stats in zip(datasets, dataset_stats)
            for lid in stats['langs']]

        contributions = [
            {
                'ID': zenodo_id(contrib_md['zenodo-link'] or ''),
                'Name': contrib_md['title'],
                'Description': contrib_md['description'],
                'Version': contrib_md['version'],
                'Author': contrib_md['author'],
                'Contributor': contrib_md['contributor'],
                'Creator': contrib_md['creator'],
                'Zenodo_ID': contrib_md['id'],
                'DOI': contrib_md['doi'],
                'DOI_Related': contrib_md['doi-related'],
                'GitHub_Link': contrib_md['github-link'],
                'Zenodo_Link': contrib_md['zenodo-link'],
                'Date': contrib_md['date'],
                'Communities': contrib_md['communities'],
                'License': contrib_md['rights'],
                'Source': contrib_md['source'],
                'Zenodo_Subject': contrib_md['subject'],
                'Zenodo_Type': contrib_md['type'],
            }
            for contrib_md in json_md]

        # TODO CLDF schema
        #
        # contributions:
        #   ID, Name, Description, Version, Author, Contributor, Creator,
        #   Zenodo_ID, DOI, DOI_Related, GitHub_Link, Zenodo_Link, Date,
        #   Communities, License, Source, Zenodo_Subject, Zenodo_Type
        #
        # datasets
        #   ID, Contribution_ID, Module, Language_Count, Glottocode_Count,
        #   Value_Count
