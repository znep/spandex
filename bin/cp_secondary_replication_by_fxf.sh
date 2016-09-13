#!/bin/bash

STACK=$1
NBE_FXF=_$2
STORE_ID=$3

case $STACK in
  "local" )
    SODA="http://localhost:6010" ;;
  "staging" )
    SODA="http://soda-fountain.app.marathon.aws-us-west-2-staging.socrata.net" ;;
  "rc" )
    SODA="http://soda-fountain.app.marathon.aws-us-west-2-rc.socrata.net" ;;
  "prod" )
    SODA="http://soda-fountain.app.marathon.aws-us-west-2-prod.socrata.net" ;;
  "eu-prod" )
    SODA="http://soda-fountain.app.marathon.aws-eu-west-1-prod.socrata.net" ;;
  "fedramp-prod" )
    SODA="http://soda-fountain.app.marathon.aws-us-east-1-fedramp-prod.socrata.net" ;;
  *)
    echo "Did not recognize stack name $STACK" ;;
esac

if [ "$SODA" == "" -o "$NBE_FXF" == "" ]; then
  echo "Usage $0 <local|staging|rc|prod|eu-prod|fedramp-prod> <nbe_fxf> <store_id>"
  exit 1
fi

curl -XPOST $SODA/dataset-copy/$NBE_FXF/$STORE_ID
