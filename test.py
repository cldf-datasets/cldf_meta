from pathlib import Path

from clld_meta.util import path_contains


def test_valid(cldf_dataset, cldf_logger):
    assert cldf_dataset.validate(log=cldf_logger)


def test_pathcontains():
    path1 = Path('/usr/share/icons/Adwaita/index.theme')
    assert not path_contains(path1, 'local')
    assert path_contains(path1, 'share')
    assert path_contains(path1, 'icons?')
