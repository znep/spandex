#!/usr/bin/env bash
### initialize elasticsearch with mock data for autocomplete experimentation

start=$1
if [ -z "$start" ]; then start=0; fi
count=$2
if [ -z "$count" ]; then count=1000; fi
$parallel=10

pushd "$( dirname "${BASH_SOURCE[0]}" )"

NODE0="54.186.54.239:9200"  #Guppy
NODE1="54.149.188.209:9200" #Haddock
upfour="qnmj-8ku6"

tmpsert="data/chicago-crimes-2014-es.json"
tmpresult0="/tmp/elasticsearch_${$}_upsert0.log"
tmpresult1="/tmp/elasticsearch_${$}_upsert1.log"
echo -n >$tmpresult0
echo -n >$tmpresult1

function prepare() {
  echo -n "a: delete index $upfour "
  curl -XDELETE "$NODE0/$upfour" 2>/dev/null
  echo
  echo -n "a: create index $upfour "
  curl -XPUT "$NODE0/$upfour" -d @index-settings.json 2>/dev/null
  echo
}

function testa() {
  i=$1
  mapping=$(cat index-mapping.json |sed 's/"s"/"s'$i'"/')
  echo -n "a: add columns and analyzer s$i "
  curl -XPUT "$NODE0/$upfour/s$i/_mapping" -d "$mapping" 2>/dev/null
  echo
  echo -n "a: transmitting bulk insert document s$i "
  curl -XPOST "$NODE0/$upfour/s$i/_bulk" --data-binary @$tmpsert 1>>$tmpresult0 2>/dev/null
  echo
}

function testb() {
  i=$1
  echo -n "b: delete index s$i "
  curl -XDELETE "$NODE1/s$i" 2>/dev/null
  echo
  echo -n "b: create index s$i "
  curl -XPUT "$NODE1/s$i" -d @index-settings.json 2>/dev/null
  echo
  mapping=$(cat index-mapping.json |sed 's/"s"/"'$upfour'"/')
  echo -n "b: add columns and analyzer s$i "
  curl -XPUT "$NODE1/s$i/$upfour/_mapping" -d "$mapping" 2>/dev/null
  echo
  echo -n "b: transmitting bulk insert document s$i "
  curl -XPOST "$NODE1/s$i/$upfour/_bulk" --data-binary @$tmpsert 1>>$tmpresult1 2>/dev/null
  echo
}

if [ "$start" -eq "0" ]; then time prepare; fi
for (( i=start; i< count; i++ )); do
  threads=0
  if [ "$threads" -ge "$parallel" ]; then wait; threads=0; fi
  threads=$(($threads+1))
  testa $i
  testb $i
done

