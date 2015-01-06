#!/usr/bin/env bash
### initialize elasticsearch with mock data for autocomplete experimentation

pushd "$( dirname "${BASH_SOURCE[0]}" )"

NODE0="54.186.54.239:9200"  #Guppy
NODE1="54.149.188.209:9200" #Haddock
upfour="qnmj-8ku6"

tmpsert="data/chicago-crimes-2014-es.json"
tmpresult0="/tmp/elasticsearch_${$}_upsert0.log"
tmpresult1="/tmp/elasticsearch_${$}_upsert1.log"
echo -n >$tmpresult0
echo -n >$tmpresult1

echo -n "delete index "
curl -XDELETE "$NODE0/$upfour"
echo
echo -n "create index "
curl -XPUT "$NODE0/$upfour" -d @index-settings.json
echo

for (( i=0; i <= 10; i++ )); do
  mapping=$(cat index-mapping.json |sed 's/"s"/"s'$i'"/')
  echo -n "add columns and analyzer "
  curl -XPUT "$NODE0/$upfour/s$i/_mapping" -d "$mapping"
  echo
  echo -n "transmitting bulk insert document "
  curl -XPOST "$NODE0/$upfour/s$i/_bulk" --data-binary @$tmpsert 1>>$tmpresult0
  echo
done

for (( i=0; i <= 10; i++ )); do
  echo -n "delete index "
  curl -XDELETE "$NODE1/s$i"
  echo
  echo -n "create index "
  curl -XPUT "$NODE1/s$i" -d @index-settings.json
  echo
  mapping=$(cat index-mapping.json |sed 's/"s"/"'$upfour'"/')
  echo -n "add columns and analyzer "
  curl -XPUT "$NODE1/s$i/$upfour/_mapping" -d "$mapping"
  echo
  echo -n "transmitting bulk insert document "
  curl -XPOST "$NODE1/s$i/$upfour/_bulk" --data-binary @$tmpsert 1>>$tmpresult1
  echo
done

