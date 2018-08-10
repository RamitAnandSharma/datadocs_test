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

COMMAND=$1
shift

if [ "$COMMAND" = "run-dev" ] ; then
  node --max-old-space-size=2048 /usr/local/bin/gulp watch
elif [ "$COMMAND" = "build" ] ; then
  rm -rf build & time node --max-old-space-size=2048 /usr/local/bin/gulp release
elif [ "$COMMAND" = "gulp" ] ; then
  node --max-old-space-size=2048 /usr/local/bin/gulp $@
elif [ "$COMMAND" = "clean" ] ; then
  ph1 "Clean resource" 
  rm -rf build
elif [ "$COMMAND" = "deploy" ] ; then
  rm -rf ../release/app/src/main/webapp & cp -r build ../release/app/src/main/webapp
else
  echo 'Usage: '
  echo '  ./dev-tool.sh clean         To clean the resources that the build or test can produce'
  echo '  ./dev-tool.sh build         To build and transform js, css, sass, html scripts'
  echo '  ./dev-tool.sh run-dev       To run a proxy webserver, watch for the js, css, html '
  echo '                              modification and rebuild'
fi
