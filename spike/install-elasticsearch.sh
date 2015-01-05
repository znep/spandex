#!/bin/sh
### install elasticsearch

wget -qO - http://packages.elasticsearch.org/GPG-KEY-elasticsearch | sudo apt-key add -
echo 'deb http://packages.elasticsearch.org/elasticsearch/1.4/debian stable main' | sudo tee -a /etc/apt/sources.list
sudo add-apt-repository ppa:webupd8team/java
sudo apt-get update
sudo apt-get install elasticsearch oracle-java7-installer
java -version

### start directly, or add service startup
sudo /etc/init.d/elasticsearch start
# sudo update-rc.d elasticsearch defaults 95 10

