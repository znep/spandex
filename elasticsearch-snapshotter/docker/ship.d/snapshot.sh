#!/bin/sh

set -evx

if [ -z "$SNAPSHOT_ID" ]; then
    SNAPSHOT_ID=$(date +%Y%m%d%H%M%S)
fi

curl -v -XPUT http://$ES_HOST:$ES_PORT/_snapshot/$SNAPSHOT_NAME \
     -d "{ \"type\": \"s3\", \"settings\": { \"region\": \"$AWS_REGION\", \"bucket\": \"$SNAPSHOT_NAME\" } }"
echo

curl -v -XPUT http://$ES_HOST:$ES_PORT/_snapshot/$SNAPSHOT_NAME/$SNAPSHOT_ID \
     -d "{ \"indices\": \"$INDICES\", \"ignore_unavailable\": $IGNORE_UNAVAILABLE, \"include_global_state\": $INCLUDE_GLOBAL_STATE }"
echo

curl -v -XGET http://$ES_HOST:$ES_PORT/_snapshot/$SNAPSHOT_NAME/$SNAPSHOT_ID/_status
echo
