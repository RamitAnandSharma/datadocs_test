#!/usr/bin/env bash

BLUE='\033[0;34m'
NC='\033[0m' # No Color

function ph1() {
  echo -e "${BLUE}"
  echo -e "$@"
  echo -e "###############################################################################"
  echo -e "${NC}"
}



ph1 "Install Java JDK 8"
sudo add-apt-repository ppa:webupd8team/java -y
sudo apt-get update
echo "oracle-java8-installer shared/accepted-oracle-license-v1-1 select true" | sudo debconf-set-selections
sudo apt-get install -y oracle-java8-installer
java -version

ph1 "Install Erlang"
sudo sh -c 'echo "deb https://packages.erlang-solutions.com/ubuntu $(lsb_release -sc) contrib" >> /etc/apt/sources.list.d/erlang.list'
wget https://packages.erlang-solutions.com/ubuntu/erlang_solutions.asc
sudo apt-key add erlang_solutions.asc
sudo apt update
sudo apt install -y erlang 
