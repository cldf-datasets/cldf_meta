"""Handling cldf data inside of a zip file."""

import contextlib
import csv
import io
import json
import zipfile
from collections import ChainMap
from itertools import islice
from pathlib import Path

from pycldf.terms import URL as TERMS_URL


def get_cldf_json(f):
    try:
        # bytes([123]) is a single opening curly brace, which messes up the
        # automatic indentation of my editor for some reason.  It is what it is.
        if not f.read(10).lstrip().startswith(bytes([123])):
            return None
        f.seek(0)
        json_data = json.load(f, encoding='utf-8')
        if not json_data.get('dc:conformsTo', '').startswith(TERMS_URL):
            return None
        return json_data
    except Exception:
        return None


def skip_rows(rows, skip_count):
    if skip_count > 0:
        return islice(rows, skip_count, None)
    else:
        return rows


def trim_column_ends(rows):
    return (row.rstrip() for row in rows)


def skip_blank_rows(rows):
    return (row for row in rows if any(row))


def skip_comments(rows, comment_str):
    if comment_str:
        return (
            row
            for row in rows
            if not row or not row[0].startswith(comment_str))
    else:
        return rows


def skip_columns(rows, skip_count):
    if skip_count > 0:
        return (row[skip_count:] for row in rows)
    else:
        return rows


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

        default_dialect = {
            'commentPrefix': '#',
            'delimiter': ',',
            'doubleQuote': True,
            'encoding': 'utf-8',
            'header': True,
            # `headerRowCount` left blank for `header`
            # `lineTerminators` left blank, because csv.reader ignores it anyways
            'quoteChar': '"',
            'skipBlankRows': False,
            'skipColumns': 0,
            'skipRows': 0,
            'skipInitialSpace': False,
            # `trim` left blank for `skipInitialSpace`
        }
        dataset_dialect = self._cldf_md.get('dialect') or {}
        dialect = ChainMap(
            table.get('dialect', {}), dataset_dialect, default_dialect)

        # We'll cross these bridges when we get to them
        assert dialect.get('header'), 'dataset without header detected'
        assert (hrc := dialect.get('headerRowCount')) is None or hrc != 1, 'dataset with odd header detected'

        # we don't want to get BOM'd
        if dialect['encoding'] == 'utf-8':
            encoding = 'utf-8-sig'
        else:
            encoding = dialect['encoding']

        if (trim := dialect.get('trim')) is not None:
            trim_left = trim in {True, 'true', 'start'}
            trim_rght = trim in {True, 'true', 'end'}
        else:
            trim_left = dialect['skipInitialSpace']
            trim_rght = False

        if dialect['quoteChar'] is None:
            escapechar = None
        elif dialect['doubleQuote']:
            # `escapechar` needs to be None here.
            # I tried to set it to '"' and then the parser was unable to finish
            # quoted fields.
            escapechar = None
        else:
            escapechar = '\\'

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
            decoder = io.TextIOWrapper(csv_f, encoding=encoding)
            rdr = csv.reader(
                decoder,
                doublequote=dialect['doubleQuote'],
                delimiter=dialect['delimiter'],
                skipinitialspace=trim_left,
                escapechar=escapechar)
            rows = skip_rows(rdr, dialect['skipRows'])
            if trim_rght:
                rows = trim_column_ends(rows)
            if dialect['skipBlankRows']:
                rows = skip_blank_rows(rows)
            rows = skip_comments(rows, dialect['commentPrefix'])
            rows = skip_columns(rows, dialect['skipColumns'])
            yield from rename_columns(
                table['tableSchema']['columns'], column_names, rows)
