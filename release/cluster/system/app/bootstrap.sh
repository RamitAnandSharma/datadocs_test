#!/usr/bin/env bash

BLUE='\033[0;34m'
NC='\033[0m' # No Color

function ph1() {
  echo -e "${BLUE}"
  echo -e "$@"
  echo -e "###############################################################################"
  echo -e "${NC}"
  echo -e "setsid ./tool.sh run >log.txt 1>&0 < /dev/null &"
}

ph1 "Change Mod deploy"
chmod 744 deploy/tool.sh
cd deploy/
