#!/usr/bin/env bash


BLUE='\033[0;34m'
NC='\033[0m' # No Color

function info() {
  echo -e "${BLUE}"
  echo -e "$@"
  echo -e "###############################################################################${NC}"
  echo -e "${NC}"
}

function clean() {
  info "Clean the images" 
  sudo vagrant destroy --force
}

function run() {
  info "Run" 
}

function build() {
  info "Build" 
  sudo vagrant up 
}

function list() {
  info "List" 
  sudo docker ps -a --format "table {{.ID}}   {{.Labels}} {{.Command}}"
}

function running() {
  info "Running" 
  sudo docker ps --format "table {{.ID}}   {{.Labels}} {{.Command}}"
}

function status() {
  info "Status" 
  sudo vagrant status
}

COMMAND=$1
shift

if [ "$COMMAND" = "run" ] ; then
  clean && run
elif [ "$COMMAND" = "clean" ] ; then
  clean
elif [ "$COMMAND" = "build" ] ; then
  build
elif [ "$COMMAND" = "list" ] ; then
  list
elif [ "$COMMAND" = "running" ] ; then
  running
elif [ "$COMMAND" = "status" ] ; then
  status
else
  echo 'usage.....'
fi
