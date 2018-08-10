#!/usr/bin/env bash

BLUE='\033[0;34m'
NC='\033[0m' # No Color

function ph1() {
  echo -e "${BLUE}"
  echo -e "$@"
  echo -e "###############################################################################"
  echo -e "${NC}"
}

ph1 "Install Mongo"
sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv EA312927
echo "deb http://repo.mongodb.org/apt/ubuntu xenial/mongodb-org/3.2 multiverse" | sudo tee /etc/apt/sources.list.d/mongodb-org-3.2.list
sudo apt-get update
sudo apt-get install -y mongodb-org
sudo systemctl start mongod
sudo systemctl status mongod
sudo cp config/mongod.conf /etc/mongod.conf

#mongo
#use dataparse
#exit

sudo systemctl restart mongod
