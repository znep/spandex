#!/usr/bin/env bash
### initialize elasticsearch with mock data for autocomplete experimentation

pushd "$( dirname "${BASH_SOURCE[0]}" )"

NODE0="eel:9200"

UPSERT="data/chicago-crimes-2014.bak"
#UPSERT="data/chicago-crimes-2014-sample.json"
CONFIG_CUID="154480034"
CONFIG_COLS="154480035 154480037 154480039 154480040 154480041"

upfour=`cat $UPSERT |jsawk 'return this.meta.view.id'`
upcols=`cat $UPSERT |jsawk 'return this.meta.view.columns' |jsawk 'this.cachedContents=null'`
updata=`cat $UPSERT |jsawk 'return this.data' |sed 's/^\[//; s/\]$//; s/\],\[/]\n[/g'`

upcols_ids=`echo $upcols |jsawk 'return this.id' |sed 's/[^0-9,\-]//g; s/,/ /g'`
upcols_ids_idposition=""
upcols_ids_selected=""
position=0
for id in $upcols_ids; do
  identity=`echo $CONFIG_CUID |grep -cw $id 2>/dev/null`
  if [ "$?" -eq "0" ]; then upcols_ids_idposition=$position; fi
  selected=`echo $CONFIG_COLS |grep -cw $id 2>/dev/null`
  if [ "$?" -ne "0" ]; then id="-1"; fi
  upcols_ids_selected="$upcols_ids_selected $id"
  let position++
done

ctor_index_acc='{"s": {"properties": {'
#ctor_index_inn='"%s": {"type": "string"}, '
ctor_index_inn='"%s": {"type":"completion", "index_analyzer": "simple", "search_analyzer": "simple", "payloads": false, "preserve_separators": false, "preserve_position_increments": false, "max_input_length": 50},'
ctor_index_suf='"b": {"type":"boolean"} }}}'
for id in $upcols_ids_selected; do
  if [ "$id" -gt "0" ]; then
    ctor_index_acc=`printf "$ctor_index_acc $ctor_index_inn" "$id"`
  fi
done
ctor_index_acc="$ctor_index_acc $ctor_index_suf"
#echo $ctor_index_acc

echo -n "delete index "
curl -XDELETE "$NODE0/$upfour"
echo
echo -n "create index "
curl -XPUT "$NODE0/$upfour" -d @index-settings.json
echo
echo -n "add columns and analyzer "
curl -XPUT "$NODE0/$upfour/s/_mapping" -d "$ctor_index_acc"
echo

echo "assembling bulk insert document "
tmpsert="/tmp/elasticsearch_${$}_upsert.json"
if [ -e $tmpsert ]; then
  rm $tmpsert
fi

while read -r row; do
  row=`printf '{"row": %s}' "$row"`
  id=`echo $row |jsawk "return this.row[$upcols_ids_idposition]"`
  printf '{"index": {"_field": "s", "_id": "%s"}}\n' "$id" >> $tmpsert
  
  json=""
  position=0
  for key in $upcols_ids_selected; do
    if [ $key -gt 0 ]; then
      val=`echo $row |jsawk "return this.row[$position]"`
      rowjson=`printf '"%s": "%s"' "$key" "$val"`
      json="$json, $rowjson"
    fi
    let position++
  done
  json=`echo $json |sed 's/^, //'`
  echo "{$json}" >> $tmpsert
done <<< "$updata"

#cat $tmpsert
echo -n "transmitting bulk insert document "
tmpresult="/tmp/elasticsearch_${$}_upsert.log"
curl -XPOST "$NODE0/$upfour/s/_bulk" --data-binary @$tmpsert 1>$tmpresult
echo

#cat $tmpresult

curl -XPOST "$NODE0/$upfour/_refresh"

