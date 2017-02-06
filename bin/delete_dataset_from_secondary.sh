#!/bin/bash

STACK=$1
DCU=alpha # data coordinator universe or whatever
DSID=$2

case $STACK in
  "local" )
    DCU=primus
    DATACOORDINATOR="http://localhost:6020"
    SPANDEXHTTP="http://localhost:8042" ;;
  "staging" )
    DATACOORDINATOR="http://data-coordinator.app.aws-us-west-2-staging.socrata.net"
    SPANDEXHTTP="http://spandex-http.app.aws-us-west-2-staging.socrata.net" ;;
  "rc" )
    DATACOORDINATOR="http://data-coordinator.app.aws-us-west-2-rc.socrata.net"
    SPANDEXHTTP="http://spandex-http.app.aws-us-west-2-rc.socrata.net" ;;
  "eu-prod" )
    DATACOORDINATOR="http://data-coordinator.app.aws-us-west-2-rc.socrata.net"
    SPANDEXHTTP="http://spandex-http.app.aws-eu-west-1-prod.socrata.net" ;;
  "fedramp-prod" )
    DATACOORDINATOR="http://data-coordinator.app.aws-us-east-1-fedramp-prod.socrata.net"
    SPANDEXHTTP="http://spandex-http.app.aws-us-east-1-fedramp-prod.socrata.net" ;;
  *)
    echo "Did not recognize stack name $STACK"
esac

if [ "$SPANDEXHTTP" == "" ] || [ "$DSID" == "" ] || [ "$DATACOORDINATOR" == "" ]; then
  echo "Usage $0 <local|staging|rc|eu-prod|fedramp-prod> <dataset_id>"
  exit 1
fi

# delete from data-coordinator
echo "Deleting dataset $DCU.$DSID from data coordinator for spandex secondary"
curl -XDELETE $DATACOORDINATOR/secondary-manifest/spandex/$DCU.$DSID

# delete from spandex-http
result=$(curl -s -XDELETE $SPANDEXHTTP/suggest/$DCU.$DSID)
num_copies=$(echo $result | jq '.dataset_copy')
num_columns=$(echo $result | jq '.column_map')
num_field_values=$(echo $result | jq '.field_value')

echo "Deleted $num_copies dataset copies, $num_columns columns, and $num_field_values field values"
