#!/bin/bash

STACK=$1
DCU=alpha # data coordinator universe or whatever
DSID=$2

case $STACK in
  "local" )
    DCU=primus
    DATACOORDINATOR="http://localhost:6001"
    CLUSTER="http://localhost:9200" ;;
  "staging" )
    DATACOORDINATOR="http://data-coordinator.app.aws-us-west-2-staging.socrata.net"
    CLUSTER="http://spandex.elasticsearch.aws-us-west-2-staging.socrata.net" ;;
  "rc" )
    DATACOORDINATOR="http://data-coordinator.app.aws-us-west-2-rc.socrata.net"
    CLUSTER="http://spandex.elasticsearch.aws-us-west-2-rc.socrata.net" ;;
  "eu-prod" )
    DATACOORDINATOR="http://data-coordinator.app.aws-us-west-2-rc.socrata.net"
    CLUSTER="http://spandex.elasticsearch.aws-eu-west-1-prod.socrata.net" ;;
  "fedramp-prod" )
    DATACOORDINATOR="http://data-coordinator.app.aws-us-east-1-fedramp-prod.socrata.net"
    CLUSTER="http://spandex-6.elasticsearch.aws-us-east-1-fedramp-prod.socrata.net" ;;
  *)
    echo "Did not recognize stack name $STACK"
esac

if [ "$CLUSTER" == "" ] || [ "$DSID" == "" ] || [ "$DATACOORDINATOR" == "" ]; then
  echo "Usage $0 <local|staging|rc|eu-prod|fedramp-prod> <dataset_id>"
  exit 1
fi

# delete from data-coordinator
curl -XDELETE $DATACOORDINATOR/secondary-manifest/spandex/$DCU.$DSID

# delete from spandex elasticsearch
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
$DIR/delete_by_dsid.sh $STACK $DSID
