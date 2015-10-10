#!/bin/bash
ES="spandex-3.elasticsearch.aws-us-west-2-prod.socrata.net"
if [ "" != "$1" ]; then
  KEY="settings.$1"
else
  KEY="settings"
fi

nodes=$(curl -s $ES/_nodes/settings |jq ".nodes[]" -Sc)
node_addrs=$(echo "$nodes" |jq ".ip" -Sc)
confs=$(echo "$nodes" |jq "del(.settings.pidfile) |del(.settings.name) |del(.settings.node.name) |.$KEY" -Sc)

paste <(echo "$node_addrs") <(echo "$confs")

echo "**** count of unique $KEY ****"
echo "$confs" |sort |uniq -c
