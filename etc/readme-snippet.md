## Basic Workflow

Creating the meta database is a three-step process:

 1. Download metadata for existing datasets from Zenodo.  This will update the
   metadata in `raw/zenodo-metadata.json`.

    $ cldfbench cldf-meta.updatemd cldfbench_cldf_meta.py

 2. Download the datasets themselves.  They will be downloaded into the
   `raw/datasets/` folder.

    $ cldfbench download cldfbench_cldf_meta.py

 3. Look through the datasets and create the meta database.  This will update
   the CLDF dataset in `cldf/` and also add files that don't contain any CLDF
   data to `etc/not-cldf.csv`, so they can be avoided in the future.

    $ cldfbench makecldf cldfbench_cldf_meta.py

## Important files

 * `raw/zenodo-metadata.json`: contains the metadata downloaded from Zenodo.
   This file is updated automatically by the `updatemd` command.
 * `etc/blacklist.csv`: contains DOIs for datasets that should be excluded from
   the meta database (e.g. the CLDF version of [Glottolog][glottolog]).  This
   file is meant to be edited manually.
 * `etc/whitelist.csv`: contains DOIs for datasets that should explicitly be
   added to the meta database.  This file meant to be edited manually.
 * `etc/not-cldf.csv`: contains a list of dataset files that are known to not
   contain CLDF.  These files will not be downloaded or scanned for CLDF data.
   This file is updated automatically by the `makecldf` command.

[glottolog]: https://glottolog.org/

## Using a Personal Access Token to access Zenodo

Since this project involves downloading a lot of data, there is a non-zero
chance that the `updatemd` or `download` commands might hit [the rate limits for
Zenodo's API][zenodo-lim].

If you need to extend the rate limit, you can [set up a Personal Access
Token][zenodo-pat] and add it to the `$CLDF_META_ACCESS_TOKEN` environment
variable before running `cldfbench`:

    $ export CLDF_META_ACCESS_TOKEN=AbCdEfG[…]
    $ cldfbench download cldfbench_cldf_meta.py

[zenodo-lim]: https://developers.zenodo.org/#rate-limiting
[zenodo-pat]: https://developers.zenodo.org/#authentication
