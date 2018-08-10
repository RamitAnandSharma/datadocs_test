#!/usr/bin/env bash

BLUE='\033[0;34m'
NC='\033[0m' # No Color

APP_HOST="192.168.5.200"
POSTGRES_HOST="192.168.5.201"
MONGODB_HOST="192.168.5.202"
RABBITMQ_HOST="192.168.5.203"
HOST_MEMBERS="$APP_HOST $POSTGRES_HOST $MONGODB_HOST $RABBITMQ_HOST"
USER="vagrant"
SSH_KEY="ssh/id_rsa"

function ph1() {
  echo -e "${BLUE}"
  echo -e "$@"
  echo -e "###############################################################################"
  echo -e "${NC}"
}

function ph2() {
  echo -e "\n$@"
  echo -e "-----------------------------------------------------------------------------"
}

function clusterExec() {
  for worker in $HOST_MEMBERS; do
    ph2 "Execute '$@' On  $worker by $USER"
    ssh -i $SSH_KEY $USER@$worker "$@"
  done
}

function report() {
  ph1 "Report"
}

function status() {
  ph1 "Status"
  clusterExec "echo 'Helloworld'"
}

function shutdown() {
  ph1 "Shutdown"
  clusterExec "sudo shutdown -h now"
}

COMMAND=$1
shift

if [ "$COMMAND" = "report" ] ; then
  report
elif [ "$COMMAND" = "status" ] ; then
  status
elif [ "$COMMAND" = "shutdown" ] ; then
  shutdown
elif [ "$COMMAND" = "exec" ] ; then
  clusterExec $@
else
  echo 'usage.....'
fi
