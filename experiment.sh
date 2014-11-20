#!/usr/bin/env bash
### initialize elasticsearch with mock data for autocomplete experimentation

NODE0="eel:9200"

curl -XPUT "$NODE0/chicago"

CRIMES=`cat ChicagoCrimes2014-PrimaryType.csv`

for crime in $CRIMES; do
  json=`printf '{ "name" : "%s" }' $crime`
  i=`shuf -i 1024-65534 -n 1`
  echo "inserting ($i,$crime)"
  for (( j=0; j <= i; j++ )); do
    curl -XPOST "${NODE0}/chicago/crime_type" -d "$json" 2>/dev/null 1>&2
  done
done

