#!/bin/sh

usage() {
    echo "ES_HOST=<host> [ES_PORT=<port>] AWS_REGION=<region> SNAPSHOT_NAME=<name> [SNAPSHOT_ID=<id>] $0"
    exit 1
}

set -vx

AGE_CLOSE=30
AGE_DROP=180
AGE_UNIT=days
INDEX_REGEX=.marvel*
INDEX_TIMESTRING=%Y.%m.%d

if [ -z "$ES_HOST" ]; then usage; fi
if [ -z "$ES_PORT" ]; then
    ES_PORT=80
fi
if [ -z "$AWS_REGION" ]; then usage; fi
if [ -z "$SNAPSHOT_NAME" ]; then usage; fi
if [ -z "$SNAPSHOT_ID" ]; then
    SNAPSHOT_ID=curator_$(date +%Y%m%d%H%M%S)
fi

# make sure snapshot repository is configured, just in case
curl -v -XPUT http://$ES_HOST:$ES_PORT/_snapshot/$SNAPSHOT_NAME \
     -d "{ \"type\": \"s3\", \"settings\": { \"region\": \"$AWS_REGION\", \"bucket\": \"$SNAPSHOT_NAME\" } }"
echo

# snapshot before closing indices
curator --host $ES_HOST --port $ES_PORT \
    snapshot --repository $SNAPSHOT_NAME --name $SNAPSHOT_ID --skip-repo-validation \
    indices --regex $INDEX_REGEX --timestring $INDEX_TIMESTRING --older-than $AGE_CLOSE --time-unit $AGE_UNIT

# it may be desirable to exit if any snapshots fail, but the following simple check doesn't cut it finely enough.
#snapshot_return_code=$?
#
#if [ $snapshot_return_code -ne 0 ]; then
#    echo "snapshot returned $snapshot_return_code, exiting before closing/deleting indices"
#    exit 1
#fi

# close indices that are kind of old
curator --host $ES_HOST --port $ES_PORT \
    close \
    indices --regex $INDEX_REGEX --timestring $INDEX_TIMESTRING --older-than $AGE_CLOSE --time-unit $AGE_UNIT

# delete indices that are really old
curator --host $ES_HOST --port $ES_PORT \
    delete \
    indices --regex $INDEX_REGEX --timestring $INDEX_TIMESTRING --older-than $AGE_DROP --time-unit $AGE_UNIT
