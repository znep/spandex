import argparse
import logging
import os
import re
from collections import namedtuple
from datetime import datetime
from functools import partial
from itertools import chain
from urllib.parse import parse_qs

import pandas as pd
import simplejson as json


def parse_args():
    parser = argparse.ArgumentParser(
        description="Combine data from Spandex logs and other sources into a "
        "single DataFrame and report some statistics")

    parser.add_argument("--fxf_domain_map_file", required=True,
                        help="A tab delimited file mapping FXFs to domains")
    parser.add_argument("--dataset_id_fxf_map_file", required=True,
                        help="A tab delimited file mapping dataset IDs to FXFs")
    parser.add_argument("--spandex_dataset_ids", required=True,
                        help="A file containing the set of dataset IDs currently indexed in \
                        Spandex")
    parser.add_argument("--counted_logs", required=True,
                        help="Counts and dataset_ids from spandex logs")
    parser.add_argument("--log_dataframe",
                        help="Instead of reading in raw logs, read previously extracted logs from "
                        "a pickled DataFrame")
    parser.add_argument("--whitelist_fxfs_file", required=True,
                        help="A path to a TSV file containing the app URLs and fxfs for datasets "
                        "that power Bellerophon apps")
    parser.add_argument("--zero_request_datasets", required=True,
                        help="A path to a text file containing the dataset_ids for the "
                        "datasets that received 0 requests")
    parser.add_argument("--store_dataframe", help="Whether to pickle the resulting dataframe. "
                        "Helpful for local development, but irrelevant for a chronos task.",
                        action="store_true")
    parser.add_argument("--output_file", help="Where to write the resulting DataFrame",
                        default="spandex_logs_df.pickle")
    parser.add_argument("--non_indexed", help="Where to write the file of possible problem "
                        "datasets that should be poked back into spandex",
                        default="non_indexed_with_counts")
    parser.add_argument("--threshold", help="Threshold over which a non-indexed should possibly "
                        "be looked into as to whether it should be poked back into spandex",
                        default=1000,
                        type=int)

    return parser.parse_args()


SpandexDatasetInfo = namedtuple("SpandexDatasetInfo", ["fxf", "version", "created_at"])
Domain = namedtuple("Domain", ["cname", "salesforce_id", "deleted_at"])
BellerophonInfo = namedtuple("BellerophonInfo", ["fxf", "domain", "app_type", "app_urls"])

def dataset_id_fxf_map_from_file(input_file):
    """
    Read in a tab-delimited file with dataset system IDs, FXFs, version numbers,
    and creation dates (approximate date of insertion into spandex, barring delays).

    Args:
        input_file (str): The path to the tab-delimited metadata file

    Returns:
        A dictionary mapping dataset system IDs to `SpandexDatasetInfo`
    """
    with open(input_file, "r") as infile:
        data = (tuple(line.strip().split("\t")) for line in infile if line.strip())
        return {t[0]: SpandexDatasetInfo(*t[1:]) for t in data}


def fxf_domain_map_from_file(input_file):
    """
    Read in a tab-delimited file with lens FXFs and domain metadata.

    Args:
        input_file (str): The path to the tab-delimited metadata file

    Returns:
        A dictionary mapping lens FXFs to `Domain`
    """
    def pad(elems):
        return elems + ([None] * (4 - len(elems)))

    with open(input_file, "r") as infile:
        data = (tuple(pad(line.strip().split("\t"))) for line in infile if line.strip())
        return {t[0]: Domain(*t[1:]) for t in data}


def bellerophon_fxf_app_map_from_file(input_file):
    """
    Read in a TSV file with fxfs, platform domains, app types, and app URLs of
    datasets that power Bellerophon (Open Budget/Performance/Expenditures, etc.)
    apps.

    Args:
        input_file (str): The path to the tab-delimited file

    Returns:
        A dictionary mapping lens FXFs to `BellerophonInfo`
    """
    with open(input_file) as infi:
        lines = [line.strip().split("\t") for line in infi.readlines()]
        return {fxf: BellerophonInfo(fxf, domain, app_type, app_name)
                for fxf, domain, app_type, app_name in lines}


def merge_dataset_metadata(dataset_id, request_count, dataset_id_fxf_map, fxf_domain_map,
                           bellerophon_fxf_app_map):
    """
    Return dataset metadata as a dictionary.

    Args:
        dataset_id (str): The dataset system ID
        request_count (int): The number of requests found in the log files
        dataset_id_fxf_map (dict): A map of dataset IDs to SpandexDatasetInfos
        fxf_domain_map (dict): A map of dataset FXFs to NameDomainPairs
        bellerophon_fxf_app_map (dict): A map of FXFs to other information for datasets
            which Bellerophon's search engine relies on

    Returns:
        A dictionary with some additional dataset metadata
    """
    fxf, version, created_at = dataset_id_fxf_map.get(dataset_id, (None, None, None))
    domain = fxf_domain_map.get(fxf, Domain(None, None, None))
    cname, salesforce_id, deleted_at = domain
    bellerophon_info = bellerophon_fxf_app_map.get(fxf)
    bellerophon_apps = bellerophon_info.app_urls if bellerophon_info else None

    return {
        "dataset_id": dataset_id,
        "request_count": request_count,
        "fxf": fxf,
        "version": version,
        "created_at": created_at,
        "domain": cname,
        "salesforce_id": salesforce_id,
        "domain_deleted_at": deleted_at,
        "bellerophon_apps": bellerophon_apps
    }


def read_log_file(input_file):
    """
    Read a file containing counts and dataset_ids, in the format output by sort | uniq -c

    Args:
        input_file (str): The path to counted dataset_ids grepped, sedded, sorted, and counted
            beforehand for memory reasons.

    Returns:
        A dict of {dataset_id: count}
    """
    logging.info("Reading request logs from {}".format(input_file))
    with open(input_file, "r") as infi:
        lines = [line.strip().split() for line in infi]

    return {dsid: int(count) for count, dsid in lines}


def spandex_dataset_ids(input_file):
    """
    Read all dataset IDs currently in the Spandex index.

    Args:
        input_file (str): The path to a flat file with a list of dataset IDs currently in spandex

    Returns:
        A set of dataset IDs
    """
    return {x.strip() for x in open(input_file, "r") if x.strip()}


def main():
    # setup logging
    logging.basicConfig(format="%(message)s", level=logging.INFO)

    # parse command-line args
    args = parse_args()

    # read in some extra dataset metadata
    dataset_id_fxf_map = dataset_id_fxf_map_from_file(args.dataset_id_fxf_map_file)
    fxf_domain_map = fxf_domain_map_from_file(args.fxf_domain_map_file)
    bellerophon_fxf_app_map = bellerophon_fxf_app_map_from_file(args.whitelist_fxfs_file)

    # read spandex dataset information
    spandex_datasets = spandex_dataset_ids(args.spandex_dataset_ids)
    print("Found {} datasets in the Spandex index".format(len(spandex_datasets)))

    # read log information
    dataset_ids_to_counts = read_log_file(args.counted_logs)

    # Reusable partial that combines spandex log data with data from other sources
    merge = partial(
        merge_dataset_metadata, dataset_id_fxf_map=dataset_id_fxf_map,
        fxf_domain_map=fxf_domain_map, bellerophon_fxf_app_map=bellerophon_fxf_app_map)

    # read request logs either from previously pickled DataFrame or JSON
    if args.log_dataframe:
        dataset_df = pd.read_pickle(args.log_dataframe)
    else:
        dataset_df = pd.DataFrame(merge(dataset_id, request_count) for dataset_id, request_count
                                  in dataset_ids_to_counts.items())

    print("Found {} suggest requests".format(dataset_df["request_count"].sum()))
    pd.options.display.max_rows = 50

    print("Top datasets by request count")
    print(dataset_df.sort_values(by=["request_count"],
                                 ascending=False)[["dataset_id",
                                                   "fxf", "request_count"]].head(50))

    num_unique_datasets = len(dataset_df["dataset_id"].unique())
    print("Number of unique datasets receiving suggestion "
          "requests: {}".format(num_unique_datasets))

    nonzero_request_datasets = set(dataset_df["dataset_id"].unique())
    zero_request_datasets = spandex_datasets - nonzero_request_datasets
    print("{} datasets in Spandex received 0 suggest requests".format(len(zero_request_datasets)))
    zero_request_datasets_df = pd.DataFrame(
        merge(dsid, 0) for dsid in zero_request_datasets)

    print("Top domains among datasets receiving zero requests")
    domain_counts = zero_request_datasets_df.groupby("domain")\
                                            .apply(len)\
                                            .reset_index()\
                                            .rename(columns={0: "dataset_count"})\
                                            .sort_values("dataset_count", ascending=False)
    print(domain_counts.head(50))

    fxfs_to_dataset_ids = {fxf: dsid for dsid, (fxf, _, _) in dataset_id_fxf_map.items()}
    bellerophon_dataset_ids = {fxfs_to_dataset_ids.get(fxf)
                               for fxf in bellerophon_fxf_app_map.keys()}

    zero_request_non_bellerophon_datasets = zero_request_datasets - bellerophon_dataset_ids
    print("{} datasets in Spandex received 0 suggest requests "
          "and don't back a Bellerophon app".format(len(zero_request_non_bellerophon_datasets)))
    with open(args.zero_request_datasets, "w") as outfile:
        outfile.write("\n".join(zero_request_non_bellerophon_datasets))

    requested_datasets_missing_from_spandex = nonzero_request_datasets - spandex_datasets
    print("{} datasets that are missing from Spandex received one or more suggest requests".format(
        len(requested_datasets_missing_from_spandex)))
    missing_from_spandex_df = dataset_df[dataset_df["dataset_id"].apply(
        lambda dataset_id: dataset_id in requested_datasets_missing_from_spandex)]

    over_threshold_df = missing_from_spandex_df[missing_from_spandex_df["request_count"].apply(
        lambda count: count > args.threshold)]

    num_over_threshold = len(over_threshold_df)
    if num_over_threshold > 0:
        ds = "datasets" if num_over_threshold > 1 else "dataset"
        pd.set_option('display.max_colwidth', -1)
        with open(args.non_indexed, "w") as outfi:
            outfi.write("[SPANDEX] {} {} to potentially reinsert "
                        "into Spandex\n\n".format(num_over_threshold, ds))
            outfi.write("<h3>Found {} {} that potentially ought to "
                        "be put into spandex</h3>\n<p>They each have over {} requests.</p>\n" \
                        .format(num_over_threshold, ds, args.threshold))
            outfi.write(over_threshold_df[["domain", "fxf", "dataset_id", "request_count",
                                           "bellerophon_apps"]].to_html())

    if not args.log_dataframe and args.store_dataframe:
        logging.info("Pickling logs dataframe at path {}".format(args.output_file))
        dataset_df.to_pickle(args.output_file)


if __name__ == "__main__":
    main()
