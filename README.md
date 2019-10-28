# Invenio VLP Data Script
This project uses VLP PostgreSQL, CSV and OCR data to create the corresponding import files for Invenio.

## Usage
This script is commandline-based. The JSON files for the data import are created in the execution folder.

VLP OCR files are currently not supported.

Parse VLP essays, sites, people, journals and books (see db queries in main.py)

`$ python main.py cli`

Parse VLP literature

`$ python main.py vlp`

Parse VLP sites

`$ python main.py sites`

Parse VLP persons

`$ python main.py VL-People2`
