#!/bin/sh
### install elasticsearch

ES_PACK='deb http://packages.elasticsearch.org/elasticsearch/1.4/debian stable main'
ES_CONF=/etc/elasticsearch/elasticsearch.yml
ES_INIT=/etc/init.d/elasticsearch
ES_HEAP='31g'

wget -qO - http://packages.elasticsearch.org/GPG-KEY-elasticsearch | sudo apt-key add -
echo $ES_PACK | sudo tee -a /etc/apt/sources.list >/dev/null
sudo add-apt-repository ppa:webupd8team/java
sudo apt-get update
sudo apt-get install elasticsearch oracle-java7-installer
java -version

echo 'bootstrap.mlockall: true' | sudo tee -a $ES_CONF >/dev/null
cat $ES_INIT | sed "s/^#ES_HEAP_SIZE=.*$/ES_HEAP_SIZE=$ES_HEAP/" | sudo tee $ES_INIT >/dev/null

### start directly, or add service startup
sudo /etc/init.d/elasticsearch start
# sudo update-rc.d elasticsearch defaults 95 10

