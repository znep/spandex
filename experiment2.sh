#!/usr/bin/env bash
### initialize elasticsearch with mock data for autocomplete experimentation

NODE0="eel:9200"

curl -XPUT "$NODE0/chicago"

CRIMES="ChicagoCrimes2014-PrimaryType.csv"
tmpsert="/tmp/${$}_elasticsearch_upsert.json"
if [ -e $tmpsert ]; then
  rm $tmpsert
fi

while read crime; do
  i=`shuf -i 1024-65534 -n 1`
  json=`printf '{"name":"%s"}' "$crime"`
  echo "inserting ($i,$json)"
  for (( j=0; j <= i; j++ )); do
    echo '{"index" : {}}' >> $tmpsert
    echo $json >> $tmpsert
  done
done <$CRIMES

#echo "press any key to continue..." 
#read -n 1

tmpresult="/tmp/${$}_elasticsearch_upsert.log"
curl -XPOST "${NODE0}/chicago/crime_type/_bulk" --data-binary @$tmpsert 1>$tmpresult

