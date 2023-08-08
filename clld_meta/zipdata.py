"""Handling cldf data inside of a zip file."""

import contextlib
import csv
import io
from itertools import islice
import json
from pathlib import Path
import zipfile

from pycldf.terms import URL as TERMS_URL


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
