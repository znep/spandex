#!/bin/bash

CLUSTER="http://spandex-3.elasticsearch.aws-us-west-2-prod.socrata.net"
delete_query="{\"query\":{\"term\":{\"dataset_id\":{\"value\":\"alpha.$1\"}}}}"

copy_count=$(curl -s $CLUSTER/spandex/dataset_copy/_search?size=0 -d $delete_query |jq '.hits.total')
column_count=$(curl -s $CLUSTER/spandex/column_map/_search?size=0 -d $delete_query |jq '.hits.total')
value_count=$(curl -s $CLUSTER/spandex/field_value/_search?size=0 -d $delete_query |jq '.hits.total')
echo "found $copy_count copies, $column_count columns, $value_count values; deleting alpha.$1"
curl -XDELETE $CLUSTER/spandex/_query -d $delete_query
echo
