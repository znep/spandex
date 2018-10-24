#!/bin/bash

STACK=$1

case $STACK in
  "local" )
    DCU=primus
    CLUSTER="http://localhost:9200" ;;
  "staging" )
    CLUSTER="http://spandex.elasticsearch.aws-us-west-2-staging.socrata.net" ;;
  "rc" )
    CLUSTER="http://spandex-5.elasticsearch.aws-us-west-2-prod.socrata.net" ;;
  "eu-prod" )
    CLUSTER="http://spandex.elasticsearch.aws-eu-west-1-prod.socrata.net" ;;
  "fedramp-prod" )
    CLUSTER="http://spandex-6.elasticsearch.aws-us-east-1-fedramp-prod.socrata.net" ;;
  *)
    echo "Did not recognize stack name $STACK"
esac

INDEX=$2

if [ "$CLUSTER" == "" -o "$INDEX" == "" ]; then
  echo "Usage: $0 <local|staging|rc|eu-prod|fedramp-prod> <index_name_or_alias>"
  exit 1
fi

count=`curl -s "$CLUSTER/$INDEX/dataset_copy/_search?size=0" |jq '.hits.total'`

size=100
page=1
lastpage=`echo $count / $size + 1 |bc`

t=`tempfile`
echo "writing output to $t"

while ( [ $page -le $lastpage ] ); do
  curl -u $SPANDEX_ES_USER:$SPANDEX_ES_PASSWORD -s "$CLUSTER/$INDEX/dataset_copy/_search?size=$size&page=$page" | jq '.hits.hits[]._source | "update secondary_manifest set latest_secondary_data_version = \(.version) where store_id like '\''spandex%fed'\'' and dataset_system_id = \(.dataset_id);"' |sed 's/"//g; s/alpha.//;' >> $t
  page=`echo "$page+1" |bc`
done
