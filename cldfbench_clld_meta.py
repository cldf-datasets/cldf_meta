from collections import Counter, namedtuple
import csv
from itertools import chain, islice
from multiprocessing import Pool
from pathlib import Path
import sys
import zipfile

from cldfbench import Dataset as BaseDataset
from cldfbench.cldf import CLDFSpec

from clld_meta import download as dl, zipdata
from clld_meta.util import loggable_progress, file_basename

CLDFError = namedtuple('CLDFError', 'record_no file reason')


def _download_datasets(raw_dir, files, access_token=None):
    urls = (file['links']['self'] for _, file in files)
    if access_token:
        urls = (dl.add_access_token(url, access_token) for url in urls)
    dls = dl.download_all(loggable_progress(urls, file=sys.stderr))
    for raw_data, (id_, file) in zip(dls, files):
        dl.validate_checksum(file['checksum'], raw_data)
        basename = file_basename(file)
        output_folder = raw_dir / id_
        output_folder.mkdir(parents=True, exist_ok=True)
        output_file = output_folder / basename
        output_file.write_bytes(raw_data)


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
                cldf_md = zipdata.get_cldf_json(f)
            if cldf_md is None:
                continue
            zipreader = zipdata.ZipDataReader(
                zip, file_tree, path.parent, cldf_md)
            found_data = True
            yield collect_dataset_stats(zipreader), None
    if not found_data:
        yield None, CLDFError(record_no, zip_path.name, 'nocldf')


def stats_from_zip(args):
    return list(_stats_from_zip(args))


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
        access_token = dl.retrieve_access_token()

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
                'Zenodo_Keywords': rec['metadata'].get('keywords', ()),
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
