"""Handling cldf data inside of a zip file."""

import contextlib
import csv
import io
import json
import zipfile
from pathlib import Path

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


def rename_columns(column_specs, column_names, raw_rows):
    col_urls = {f'{TERMS_URL}#{col}': col for col in column_names}
    name_map = {
        col['name']: col_urls[purl]
        for col in column_specs
        if (purl := col.get('propertyUrl')) and purl in col_urls}

    row_i = iter(raw_rows)
    header = [
        name_map.get(orig_name, orig_name)
        for orig_name in next(row_i, ())]
    for row in row_i:
        yield {
            colname: cell
            for colname, cell in zip(header, row)
            if colname and cell and colname in column_names}


class ZipDataReader:
    def __init__(self, zip_file, zip_infos, md_root, cldf_md):
        self._zip_file = zip_file
        self._zip_infos = zip_infos
        self._md_root = md_root
        self._cldf_md = cldf_md

    def cldf_module(self):
        return self._cldf_md['dc:conformsTo'].split('#')[-1]

    def get_table(self, name_or_url):
        url = f'{TERMS_URL}#{name_or_url}'
        for table in self._cldf_md['tables']:
            if table.get('dc:conformsTo', '') == url:
                return table
        else:
            raise ValueError(f'table not found: {name_or_url}')

    def iterrows(self, table, *column_names):
        try:
            table = self.get_table(table)
        except ValueError:
            return

        relpath = table['url']
        root = self._md_root
        while relpath.startswith('../'):
            relpath, root = relpath[3:], root.parent
        relpath_zip = f'{relpath}.zip'

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
                internal_info = next(
                    info
                    for info in internal_zip.infolist()
                    if info.filename.endswith(Path(relpath).name))
                csv_f = withs.enter_context(internal_zip.open(internal_info))
            decoder = io.TextIOWrapper(csv_f, encoding='utf-8-sig')
            rdr = csv.reader(decoder)
            yield from rename_columns(
                table['tableSchema']['columns'], column_names, rdr)
