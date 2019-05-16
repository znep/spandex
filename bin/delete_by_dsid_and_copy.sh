#!/bin/bash

STACK=$1
DCU=alpha # data coordinator universe or whatever
DSID=$2
COPY_NUMBER=$3

case $STACK in
  "local" )
    DCU=alpha
    CLUSTER="http://localhost:9200" ;;
  "staging" )
    CLUSTER="http://spandex.elasticsearch.aws-us-west-2-staging.socrata.net" ;;
  "rc" )
    CLUSTER="http://spandex.elasticsearch.aws-us-west-2-rc.socrata.net" ;;
  "eu-prod" )
    CLUSTER="http://spandex.elasticsearch.aws-eu-west-1-prod.socrata.net" ;;
  "fedramp-prod" )
    CLUSTER="http://spandex-6.elasticsearch.aws-us-east-1-fedramp-prod.socrata.net" ;;
  *)
    echo "Did not recognize stack name $STACK"
esac

if [ "$CLUSTER" == "" ] || [ "$DSID" == "" ] || [ "$COPY_NUMBER" == "" ]; then
  echo "Usage $0 <local|staging|rc|eu-prod|fedramp-prod> <dataset_id> <copy_number>"
  exit 1
fi


delete_query="{\"query\":{\"bool\":{\"must\":[{\"term\":{\"dataset_id\":\"$DCU.$DSID\"}},{\"term\":{\"copy_number\":$COPY_NUMBER}}]}}}"
echo $delete_query

copy_count=$(curl -u $SPANDEX_ES_USER:$SPANDEX_ES_PASSWORD -s $CLUSTER/spandex/dataset_copy/_search?size=0 -d $delete_query |jq '.hits.total')
column_count=$(curl -u $SPANDEX_ES_USER:$SPANDEX_ES_PASSWORD -s $CLUSTER/spandex/column_map/_search?size=0 -d $delete_query |jq '.hits.total')
value_count=$(curl -u $SPANDEX_ES_USER:$SPANDEX_ES_PASSWORD -s $CLUSTER/spandex/column_value/_search?size=0 -d $delete_query |jq '.hits.total')

echo "found $copy_count copies, $column_count columns, $value_count values; deleting $DSID"
curl -XDELETE $CLUSTER/spandex/_query -d $delete_query
echo
