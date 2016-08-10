#!/bin/bash

STACK=$1
case $STACK in
  "local" )
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

INDEX=$2
if [ "$CLUSTER" == "" -o "$INDEX" == "" ]; then
  echo "Usage $0 <local|staging|rc|rpod|eu-prod|fedramp-prod> <index_name_or_alias>"
  exit 1
fi

recovery=`curl -s $CLUSTER/$INDEX/_recovery`
x=`echo $recovery |jq '.[].shards[].index.size.recovered_in_bytes' |tr '\n' '+'; echo 0`
y=`echo $recovery |jq '.[].shards[].index.size.total_in_bytes' |tr '\n' '+'; echo 0`
r=`echo "($x) / 1073741824" |bc`
t=`echo "($y) / 1073741824" |bc`
p=`echo "scale=2; 100 * ($x) / ($y)" |bc`
echo "$p% complete ($r GiB recovered from $t GiB total)"
