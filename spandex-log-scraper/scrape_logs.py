import argparse
import dateutil.parser
import logging
import os
import simplejson as json
from datetime import datetime, timedelta
from tempfile import NamedTemporaryFile

from clients_common import S3Client, SumoLogicClient


def json_serialize(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError("Type not serializable")


def isodate(s):
    return dateutil.parser.parse(s)


def today():
    now = datetime.now()
    return now.replace(hour=0, minute=0, second=0, microsecond=0)


def one_week_ago():
    now = datetime.now()
    one_week_ago = now - timedelta(weeks=1)
    return one_week_ago.replace(hour=0, minute=0, second=0, microsecond=0)

ENVIRONMENTS = ("us-west-2-staging", "us-west-2-rc", "eu-west-1-prod", "us-east-1-fedramp-prod")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Extract JSON request logs from the Sumo Logic API")

    parser.add_argument(
        "-f", "--from_date", type=isodate, default=one_week_ago(),
        help="The ISO-formatted date to begin querying event data from (defaults to 2 weeks ago)")

    parser.add_argument(
        "-t", "--to_date", type=isodate, default=today(),
        help="The ISO-formatted date to end querying event data at")

    parser.add_argument(
        "--s3_bucket", help="The S3 bucket to which logs will be written", required=True)

    parser.add_argument("--aws_profile", help="The AWS profile name for writing logs to S3")
    parser.add_argument("--sumo_access_id", help="The access ID to use for the Sumo Logic API")
    parser.add_argument("--sumo_access_key", help="The access key to use for the Sumo Logic API")
    parser.add_argument("--environment", help="The environment", choices=ENVIRONMENTS,
                        required=True)

    return parser.parse_args()


def sumo_logic_query(environment):
    return f'"/suggest" | where backend_name = "spandex-http" | where env = "{environment}"'

REQUEST_FIELDS = ["_raw", "_messagetime"]


def main():
    logging.basicConfig(format='%(message)s', level=logging.INFO)

    args = parse_args()

    from_date = args.from_date
    to_date = args.to_date

    s3_client = S3Client.connect_with_profile(args.aws_profile)
    s3_bucket = args.s3_bucket
    s3_key = "{}_{}.logs.json".format(from_date.isoformat(), to_date.isoformat())
    sumo_access_id = args.sumo_access_id or os.environ["SUMOLOGIC_ACCESS_ID"]
    sumo_access_key = args.sumo_access_key or os.environ["SUMOLOGIC_ACCESS_KEY"]
    sumo_logic_client = SumoLogicClient(sumo_access_id, sumo_access_key)

    logging.info(
        "Searching for request logs in {} in range {} to {}".format(
            args.environment, from_date.isoformat(), to_date.isoformat()))

    request_logs = sumo_logic_client.search(
        sumo_logic_query(args.environment), start_dt=from_date, end_dt=to_date, fields=REQUEST_FIELDS)

    tmp_file = NamedTemporaryFile(mode='w', encoding='utf-8', delete=False)
    logging.debug("Writing out logs to temporary file {}".format(tmp_file.name))

    try:
        for request in request_logs:
            tmp_file.write(json.dumps(request, default=json_serialize) + '\n')

        s3_client.upload_from_file(s3_bucket, s3_key, tmp_file.name)
    finally:
        logging.debug("Removing temporary file {}".format(tmp_file.name))
        os.remove(tmp_file.name)


if __name__ == "__main__":
    main()
