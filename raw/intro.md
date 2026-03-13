The CLDF meta database
======================

Welcome to the CLDF meta database, a catalogue of known datasets in the
[Cross-Linguistic Data Formats (CLDF)][cldf] that have been published on
[Zenodo][zenodo].

[cldf]: https://cldf.clld.org/
[zenodo]: https://zenodo.org/

## What am I looking at?

The structure of the meta database mirrors the structure of Zenodo itself, so it 

<!-- TODO understanding zenodo's structure is necessary to understand this -->
<!-- TODO 'dataset' is a bit loaded as a term -->

### CLDF datasets

For the purpose of this database, a CLDF dataset is a JSON file, which describes
a dataset based on the CLDF spec, and a number of CSV files, which contain the
actual data (technically the CLDF standard allows CLDF datasets consisting of
bare CSV files but this is not supported here).

### Zenodo records

On Zenodo a *record* is an individual upload of a specific version of a dataset.
Every record gets assigned a [Digital Object Identifier (DOI)][doi] that makes
the record uniquely identifiable (and thus citable).

[doi]: https://www.doi.org/

Zenodo records can contain multiple files and folders, which means all the JSON
and CSV files can be uploaded as one neat package.  A consequence of this is
that *a Zenodo record can be home to more than one CLDF dataset*.

<!-- TODO for mpi stuff, a record corresponds to a cldfbench  -->
<!-- TODO we don't stipulate any project structure/programming language -->

### Zenodo concepts

<!-- TODO all versions -->
<!-- TODO 'latest' DOI -->

### Example: Lexibank-analysed

<!-- TODO text -->
<!-- TODO diagram -->

## What kind of datasets are in here

Known datasets using the CLDF format that have been published on Zenodo.

## What kind of datasets are *not* in here

The following kinds of datasets are not included in the meta database (mostly
for technical reasons):

 * Unpublished datasets.
 * Datasets in places other than Zenodo.
 * Datasets in formats other than CLDF.
 * CLDF datasets that don't have a JSON meta data file.
 * Reference catalogues like Glottolog.
 * The meta database itself (yes, the meta database is just a CLDF dataset
   itself).

## I want my dataset to show up in the meta database

The steps for inclusion are:

 * Have your dataset in CLDF.
 * Release your dataset on Zenodo.
 * Add known [keywords][keywords] to your Zenodo record (e.g. the keyword
   `CLDF`).
 * Add your Zenodo record to a known [Zenodo community][communities].
 * If you have different keywords or communities that you use for your data,
   [tell me about them][mail] and I'll add them to the program.
 * If your data don't use any keywords or isn't part of any communities, tell me
   the DOI of your dataset and I'll add it manually.

If you want to know what keywords and communities are currently supported, the
most accurate place to look is the [Python code][updatemd] that searches Zenodo.
Search for the variables `SEARCH_KEYWORDS` and `SEARCH_COMMUNITIES`.

[keywords]: https://help.zenodo.org/docs/deposit/describe-records/keywords-and-subjects/
[communities]: https://help.zenodo.org/docs/communities/
[mail]: mailto:johannes_englisch@eva.mpg.de
[updatemd]: https://github.com/cldf-datasets/cldf_meta/blob/master/cldf_meta_commands/updatemd.py
