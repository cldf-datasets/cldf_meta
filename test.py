import unittest
from pathlib import Path

from clld_meta.util import path_contains
from clld_meta.zipdata import rename_columns


def test_valid(cldf_dataset, cldf_logger):
    assert cldf_dataset.validate(log=cldf_logger)


def test_pathcontains():
    path1 = Path('/usr/share/icons/Adwaita/index.theme')
    assert not path_contains(path1, 'local')
    assert path_contains(path1, 'share')
    assert path_contains(path1, 'icons?')


class NormaliseColumnNames(unittest.TestCase):

    def setUp(self):
        custom_col = {'name': 'my_custom_col'}
        lang_id_col = {
            'name': 'my_language_col',
            'propertyUrl': 'http://cldf.clld.org/v1.0/terms.rdf#languageReference',
        }
        id_col = {
            'name': 'my_id_col',
            'propertyUrl': 'http://cldf.clld.org/v1.0/terms.rdf#id',
        }
        self.colspecs = [custom_col, lang_id_col, id_col]
        self.rows = [
            ['my_id_col', 'my_language_col', 'my_custom_col'],
            ['a', 'b', 'c'],
            ['d', 'e', 'f'],
        ]

    def test_verbatim(self):
        cols = ['my_custom_col']
        expected = [
            {'my_custom_col': 'c'},
            {'my_custom_col': 'f'}]
        self.assertEqual(
            list(rename_columns(self.colspecs, cols, self.rows)),
            expected)

    def test_mapped(self):
        cols = ['my_custom_col', 'id']
        expected = [
            {'id': 'a', 'my_custom_col': 'c'},
            {'id': 'd', 'my_custom_col': 'f'}]
        self.assertEqual(
            list(rename_columns(self.colspecs, cols, self.rows)),
            expected)
