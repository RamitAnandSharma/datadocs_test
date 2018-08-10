#!/usr/bin/env bash

BLUE='\033[0;34m'
NC='\033[0m' # No Color

function ph1() {
  echo -e "${BLUE}"
  echo -e "$@"
  echo -e "###############################################################################"
  echo -e "${NC}"
}

ph1 "Install RabbitMQ"
sudo apt-get update
sudo sh -c 'echo "deb https://dl.bintray.com/rabbitmq/debian $(lsb_release -sc) main" >> /etc/apt/sources.list.d/rabbitmq.list'
wget -O- https://dl.bintray.com/rabbitmq/Keys/rabbitmq-release-signing-key.asc | sudo apt-key add -
wget -O- https://www.rabbitmq.com/rabbitmq-release-signing-key.asc | sudo apt-key add -
sudo apt update
sudo apt install rabbitmq-server -y

sudo rabbitmq-plugins enable rabbitmq_management
sudo rabbitmqctl delete_user guest
sudo rabbitmqctl add_user guest guest
sudo rabbitmqctl set_user_tags guest administrator
sudo rabbitmqctl set_permissions -p / guest ".*" ".*" ".*"
sudo cp config/rabbitmq.config /etc/rabbitmq/rabbitmq.config
sudo service rabbitmq-server restart
