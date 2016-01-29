#!/bin/bash

ES=spandex-5.elasticsearc.aws-us-west-2-prod.socrata.net
dsid=$1

if [ "" == "$dsid" ]; then
  echo "Usage: $0 <dataset_id>"
  exit 1
fi

query="{\"query\":{\"match\":{\"dataset_id\":\"alpha.$dsid\"}},\"_source\":\"value.output\"}"
curl -s $ES/spandex/field_value/_search?size=1000 -d $query |jq '.hits.hits[]._source.value.output |length' |sort |uniq -c |sort -nr -k 2

