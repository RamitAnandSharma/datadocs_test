#!/usr/bin/env bash

BLUE='\033[0;34m'
NC='\033[0m' # No Color

function ph1() {
  echo -e "${BLUE}"
  echo -e "$@"
  echo -e "###############################################################################"
  echo -e "${NC}"
}

ph1 "Install Postgres"
sudo cp config/pgdg.list /etc/apt/sources.list.d/pgdg.list
wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add -
sudo apt-get update
sudo apt-get install postgresql-client-9.4 postgresql-9.4 postgresql-contrib-9.4 libpq-dev postgresql-server-dev-9.4 -y
sudo cp config/postgresql.conf /etc/postgresql/9.4/main/postgresql.conf
sudo cp config/pg_hba.conf /etc/postgresql/9.4/main/pg_hba.conf
ph1 "start restart"
sudo invoke-rc.d postgresql restart
ph1 "done restart"

ph1 "Init Database And User"
#sudo -u postgres psql -f initdb.sql
