"""\
Cleanup data download folder in `raw/datasets/`.

Removes all downloaded datasets that are listed in `etc/not-cldf.csv`.
"""

from itertools import islice
import sys

from cldfbench.cli_util import add_dataset_spec, with_dataset


def register(parser):
    add_dataset_spec(parser)


def cleanup(dataset, args):
    download_dir = dataset.raw_dir / 'datasets'
    not_cldf = islice(dataset.etc_dir.read_csv('not-cldf.csv'), 1, None)
    not_cldf = [
        file_path
        for record_no, filename, _ in not_cldf
        if (file_path := download_dir / record_no / filename).exists()]

    print('\n'.join(map(str, not_cldf)), file=sys.stderr)
    answer = input('The files above will be DELETED.  Continue? [y|N] ')
    if answer.strip().lower() not in {'y', 'yes'}:
        return

    for file_path in not_cldf:
        print('rm', file_path, file=sys.stderr)
        file_path.unlink()
        if next(file_path.parent.iterdir(), None) is None:
            print('rmdir', file_path, file=sys.stderr)
            file_path.parent.rmdir()


def run(args):
    with_dataset(args, cleanup)
