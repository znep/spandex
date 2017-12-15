#!/bin/bash

STACK=$1

case $STACK in
  "local" )
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

if [ -z $CLUSTER ]; then
  echo "Usage $0 <local|staging|rc|prod|eu-prod|fedramp-prod>"
  exit 1
fi

for garbage in "*.html" "*.exe" perl spipe jsonrpc; do
    echo "curl -s -XDELETE $CLUSTER/$garbage"
    curl -s -XDELETE $CLUSTER/$garbage | jq .
done
