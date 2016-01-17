#!/bin/bash

STACK=$1
DSID=$2
STORE_ID=$3

if [ "$STACK" == "" -o "$DSID" == "" ]; then
  echo "Usage $0 <local|staging|rc|prod|eu-prod|fedramp-prod> <dsid> <store_id>"
  exit 1
fi

echo "Connect to the truth psql database in $STACK and execute this statement"
echo "  DELETE FROM secondary_manifest"
echo "  WHERE store_id='$STORE_ID'"
echo "  AND   dataset_system_id=$DSID"

pause
echo "Hit <ENTER> when you're done there."
