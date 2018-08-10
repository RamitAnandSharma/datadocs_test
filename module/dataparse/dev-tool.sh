#!/usr/bin/env bash

cygwin=false
case "`uname`" in
  CYGWIN*) cygwin=true;;
esac

function has_opt() {
  OPT_NAME=$1
  shift
  #Par the parameters
  for i in "$@"; do
    if [[ $i == $OPT_NAME ]] ; then
      echo "true"
      return
    fi
  done
  echo "false"
}

BLUE='\033[0;34m'
NC='\033[0m' # No Color

function ph1() {
  echo -e "${BLUE}"
  echo -e "STEP: $@"
  echo -e "###############################################################################${NC}"
  echo -e "${NC}"
}

PROJECT_DIR=`dirname "$0"`
PROJECT_DIR=`cd "$bin"; pwd`

cd $PROJECT_DIR

function testClass() {
  ph1 "Run mvn test" 
  mvn  test -Dtest=$@
}

COMMAND=$1
shift

if [ "$COMMAND" = "clean" ] ; then
  ph1 "Clean resource" 
  mvn clean
  rm -rf log & rm -rf lucene
elif [ "$COMMAND" = "build" ] ; then
  mvn -Dmaven.test.skip=true install
elif [ "$COMMAND" = "test" ] ; then
  ph1 "Run mvn test" 
  mvn clean test -Dtest=IntegrationTests
elif [ "$COMMAND" = "test-class" ] ; then
  ph1 "Run mvn test $@" 
  mvn  test -Dtest=$@
elif [ "$COMMAND" = "server-status" ] ; then
  ph1 "List Server Status" 
  sudo systemctl status --no-pager postgresql@9.4-main.service 
  sudo systemctl status mongod
  sudo systemctl status --no-pager rabbitmq-server
else
  sudo systemctl stmodification and rebuildecho 'Usage: '
  echo '  ./dev-tool.sh clean         To clean target and the other resources that the build'
  echo '                              or test can produce'
  echo '  ./dev-tool.sh build         To build the project jar file'
  echo '  ./dev-tool.sh test          To build and run the integration test'
  echo '  ./dev-tool.sh test-class    To build and run the specific test class'
  echo '  ./dev-tool.sh server-status To list the current status of postgressql, rabbitmq, mongodb'
  echo '                              server. This is available only on linux'
fi
