#!/usr/bin/env bash

BLUE='\033[0;34m'
NC='\033[0m' # No Color

function ph1() {
  echo -e "${BLUE}"
  echo -e "$@"
  echo -e "###############################################################################"
  echo -e "${NC}"
}

ph1 "Install Elasticsearch"
sudo add-apt-repository ppa:webupd8team/java -y
sudo apt-get update
echo "oracle-java8-installer shared/accepted-oracle-license-v1-1 select true" | sudo debconf-set-selections
sudo apt-get install -y oracle-java8-installer

sudo sysctl -w vm.max_map_count=262144

tar -xvzf elasticsearch-5.3.1.tgz
cd elasticsearch-5.3.1/
chmod 744 bin/elasticsearch

ph1 "Start Elasticsearch"
#./bin/elasticsearch -d
