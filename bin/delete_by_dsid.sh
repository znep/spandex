#!/bin/bash

STACK=$1
DCU=alpha # data coordinator universe or whatever
DSID=$2

case $STACK in
  "local" )
    DCU=primus
    CLUSTER="http://localhost:9200" ;;
  "staging" )
    CLUSTER="http://spandex.elasticsearch.aws-us-west-2-staging.socrata.net" ;;
  "rc" )
    CLUSTER="http://spandex.elasticsearch.aws-us-west-2-rc.socrata.net" ;;
  "prod" )
    CLUSTER="http://spandex-5.elasticsearch.aws-us-west-2-prod.socrata.net" ;;
  "eu-prod" )
    CLUSTER="http://spandex.elasticsearch.aws-eu-west-1-prod.socrata.net" ;;
  "fedramp-prod" )
    CLUSTER="http://spandex-6.elasticsearch.aws-us-east-1-fedramp-prod.socrata.net" ;;
  *)
    echo "Did not recognize stack name $STACK"
esac

if [ "$CLUSTER" == "" -o "$DSID" == "" ]; then
  echo "Usage $0 <local|staging|rc|prod|eu-prod|fedramp-prod> <dataset_id>"
  exit 1
fi


delete_query="{\"query\":{\"term\":{\"dataset_id\":{\"value\":\"$DCU.$DSID\"}}}}"
copy_count=$(curl -s $CLUSTER/spandex/dataset_copy/_search?size=0 -d $delete_query |jq '.hits.total')
column_count=$(curl -s $CLUSTER/spandex/column_map/_search?size=0 -d $delete_query |jq '.hits.total')
value_count=$(curl -s $CLUSTER/spandex/field_value/_search?size=0 -d $delete_query |jq '.hits.total')

echo "found $copy_count copies, $column_count columns, $value_count values; deleting $DSID"
curl -XDELETE $CLUSTER/spandex/_query -d $delete_query
echo
