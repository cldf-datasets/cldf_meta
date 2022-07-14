from collections import OrderedDict
import hashlib
import io
from itertools import chain, repeat
import os
import pathlib
import re
import sys
import time
from urllib import request
from urllib.error import HTTPError
from urllib.parse import urlparse
import zipfile


from cldfbench import Dataset as BaseDataset


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
            "Hashing algorigthm '%s' not available in hashlib" % algo)

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
    zenodo_link = contrib_md.get('zenodo-link') or ''
    m = re.fullmatch(r'https://zenodo\.org/record/(\d+)', zenodo_link)
    assert m, 'the json data should contain valid zenodo links'
    record_no = m.group(1)
    dataset_dir = pathlib.Path(raw_dir) / 'datasets' / record_no

    if not dataset_dir.exists():
        return False, '{}: dataset folder not found'.format(dataset_dir)
    elif not any(dataset_dir.iterdir()):
        return False, '{}: dataset folder empty'.format(dataset_dir)
    else:
        return True, ''

def find_missing_datasets(raw_dir, json_md):
    results = [_dataset_exists(raw_dir, row) for row in json_md]
    return [msg for success, msg in results if not success]


### CLDFbench ###

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

        def zenodo_id(zenodo_link):
            m = re.fullmatch(r'https://zenodo.org/record/(\d+)', zenodo_link)
            if not m:
                raise ValueError(
                    'Zenodo link looks funny: {}'.format(zenodo_link))
            return m.group(1)

        def _ftypes(ftypes):
            return chain(ftypes, repeat(ftypes[-1])) if ftypes else ()

        dataset_dir = self.raw_dir / 'datasets'
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
        json_md = self.raw_dir.read_csv('zenodo-metadata.csv', dicts=True)
        json_md = [
            row
            for row in json_md
            if 'zip' in row.get('file-types', '').split('\\t')]

        # before doing anything, check that the datasets have all been
        # downloaded propery
        error_messages = find_missing_datasets(self.raw_dir, json_md)
        if error_messages:
            print(
                '\n'.join(error_messages),
                '\nERROR: Some datasets seem to be missing in raw/.',
                '\nYou might have to re-run `cldfbench download`.',
                sep='', file=sys.stderr, flush=True)
            return
