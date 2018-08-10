#!/usr/bin/env bash

BLUE='\033[0;34m'
NC='\033[0m' # No Color

function info() {
  echo -e "${BLUE}"
  echo -e "STEP: $@"
  echo -e "###############################################################################${NC}"
  echo -e "${NC}"
}

function clean() {
  info "Clean resource" 
  rm -rf log & rm -rf lucene
  rm -rf /tmp/postgresql-embed-*
  rm -rf /dataparse/tmp/rabbitmq_server-3.6.9
  rm -rf /dataparse/tmp/elasticsearch-5.3.0
}

function run() {
  JAVA_OPTS="-Dcom.zaxxer.hikari.aliveBypassWindowMs=30000 -XX:+UseCompressedOops -Dfile.encoding=UTF-8 -Dorg.eclipse.jetty.websocket.LEVEL=DEBUG"
  APP_OPTS="-DDB_RECREATE=true"
  # APP_OPTS="$APP_OPTS -DDB_HOST=192.168.5.201"
  APP_OPTS="$APP_OPTS -DELASTIC_URL=http://192.168.5.205:9200"
  APP_OPTS="$APP_OPTS -DMONGO_URI=mongodb://192.168.5.202:27017/dataparse"
  export MONGO_URI="mongodb://192.168.5.202:27017/dataparse"
  export AMQP_URL="amqp://guest:guest@192.168.5.203:5672"
  export DB_HOST="192.168.5.201"
  export DB_PORT="5432"
  export DB_NAME="dataparse"
  export DB_USERNAME="testuser"
  export DB_PASSWORD="testuser"
  $JAVA_HOME/bin/java  -cp target/dataparse.jar $JAVA_OPTS $APP_OPTS com.dataparse.server.RestServer
}

function test() {
  info "Run mvn test" 
  mvn clean test -Dtest=IntegrationTests
}

COMMAND=$1
shift

if [ "$COMMAND" = "run" ] ; then
  clean && run
elif [ "$COMMAND" = "clean" ] ; then
  clean
elif [ "$COMMAND" = "build" ] ; then
  mvn -Dmaven.test.skip=true install
elif [ "$COMMAND" = "build+run" ] ; then
  mvn -Dmaven.test.skip=true install -P release
  clean && run
elif [ "$COMMAND" = "test" ] ; then
  test
else
  echo 'usage.....'
fi
