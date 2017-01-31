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

curl -s -XDELETE $CLUSTER/*.html
curl -s -XDELETE $CLUSTER/perl
curl -s -XDELETE $CLUSTER/spipe
