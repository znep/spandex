# Spandex Log Scraper

This is a Python script for fetching Spandex logs from the Sumo API and dumping them to S3.

## Identifying datasets to delete

Here's the idea: collect a bunch of Spandex request logs, and determine which domains and datasets
are actually servicing suggestion requests. Those that aren't should be removed from the index. The
`analyze.py` script may be handy:

``` sh
spandex-log-scraper$ python analyze.py --fxf_domain_map_file 20171117.fxf_domain_map.tsv --dataset_id_fxf_map_file 20171117.dataset_id_fxf_version.tsv --spandex_dataset_ids 20171117.spandex_dataset_copies.txt --logfile_dir logfiles_all --output_file 20171117.spandex_logs_df.pickle

Found 2421 datasets in the Spandex index
Reading logfiles: ['logfiles_all/2017-10-22T00:00:00_2017-10-29T00:00:00.logs.json', 'logfiles_all/2017-10-29T00:00:00_2017-11-05T00:00:00.logs.json', 'logfiles_all/2017-10-08T00:00:00_2017-10-15T00:00:00.logs.json', 'logfiles_all/2017-09-24T00:00:00_2017-10-01T00:00:00.logs.json', 'logfiles_all/2017-10-01T00:00:00_2017-10-08T00:00:00.logs.json', 'logfiles_all/2017-11-05T00:00:00_2017-11-12T00:00:00.logs.json', 'logfiles_all/2017-10-15T00:00:00_2017-10-22T00:00:00.logs.json']
...
```

This script will list all of the datasets that received no suggestion requests in the specified
time period (along with some other potentially useful information). Additionally, it will output
this list to a file (specified by the `zero_request_datasets` command-line parameter).


