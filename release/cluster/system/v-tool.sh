#!/usr/bin/env bash

BLUE='\033[0;34m'
NC='\033[0m' # No Color

function ph1() {
  echo -e "${BLUE}"
  echo -e "$@"
  echo -e "###############################################################################"
  echo -e "${NC}"
}

COMMAND=$1
shift

if [ "$COMMAND" = "run" ] ; then
  ph1 "vagrant up --no-provision" 
  vagrant up --no-provision
elif [ "$COMMAND" = "up" ] ; then
  ph1 "vagrant up" 
  vagrant up
elif [ "$COMMAND" = "suspend" ] ; then
  ph1 "vagrant suspend" 
  vagrant suspend
elif [ "$COMMAND" = "resume" ] ; then
  ph1 "vagrant resume" 
  vagrant resume
elif [ "$COMMAND" = "halt" ] ; then
  ph1 "vagrant halt" 
  vagrant halt
elif [ "$COMMAND" = "destroy" ] ; then
  ph1 "vagrant destroy --force $@" 
  vagrant destroy --force $@
else
  echo 'usage.....'
fi
