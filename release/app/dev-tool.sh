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
  echo -e "$@"
  echo -e "###############################################################################${NC}"
  echo -e "${NC}"
}

PROJECT_DIR=`dirname "$0"`
PROJECT_DIR=`cd "$bin"; pwd`

cd $PROJECT_DIR

COMMAND=$1
shift

if [ "$COMMAND" = "run" ] ; then
  mvn install -Dmaven.test.skip=true && \
    mvn assembly:directory -Dmaven.test.skip=true && \
    ./target/release.app-1.0-ourAssembly/bin/server.sh -console
elif [ "$COMMAND" = "clean" ] ; then
  ph1 "Clean resource" 
  mvn clean
elif [ "$COMMAND" = "build" ] ; then
  ph1 "Build the dependencies and jar file" 
  mvn install -Dmaven.test.skip=true 
elif [ "$COMMAND" = "package" ] ; then
  ph1 "Build and create the release app directory" 
  mvn install -Dmaven.test.skip=true && mvn assembly:directory -Dmaven.test.skip=true
else
  echo 'Usage: '
  echo '  ./dev-tool.sh clean         To clean target and the other resources that the build'
  echo '                              or test can produce'
  echo '  ./dev-tool.sh run           To build the dependencies, create the release app and run'
  echo '                              the application. Need to make sure that the postgresql, rabbitmq,'
  echo '                              mongodb and elasticsearch are running'
  echo '  ./dev-tool.sh build         To build the dependencies and jar files'
  echo '  ./dev-tool.sh package       To compile , build and create app release directory'
fi
