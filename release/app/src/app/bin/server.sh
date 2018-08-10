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

function info() {
  echo -e "${BLUE}"
  echo -e "STEP: $@"
  echo -e "###############################################################################${NC}"
  echo -e "${NC}"
}

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

JAVACMD="$JAVA_HOME/bin/java"
APP_HOME=`cd $bin/..; pwd; cd $bin`
LOG_FILE="$APP_HOME/logs/server.stdout"
PID_FILE="$APP_HOME/logs/server.pid"

mkdir -p $APP_HOME/logs

LIB="$APP_HOME:$APP_HOME/lib/*"
CLASSPATH="$JAVA_HOME/lib/tools.jar"
CLASSPATH="${CLASSPATH}:$LIB"

if $cygwin; then
  JAVA_HOME=`cygpath --absolute --windows "$JAVA_HOME"`
  CLASSPATH=`cygpath --path --windows "$CLASSPATH"`
  APP_HOME=`cygpath --path --windows "$APP_HOME"`
fi

JAVA_OPTS="-server -XX:+UseParallelGC -Xshare:auto -Xms128m -Xmx512m"
CLASS="com.hkt.server.ServerApp"
ARGS="--app.home=$APP_HOME --spring.config.location=file:$APP_HOME/config/application.properties"

CONSOLE_OPT=$(has_opt "-console" $@ )
DAEMON_OPT=$(has_opt "-daemon" $@ )
STOP_OPT=$(has_opt "-stop" $@ )
CLEAN_OPT=$(has_opt "-clean" $@ )

echo "JAVA_HOME: $JAVA_HOME"
echo "APP_HOME:  $APP_HOME"
echo "JAVA_OPTS: $JAVA_OPTS"

cd $APP_HOME

if [ "$CLEAN_OPT" = "true" ] ; then
  info "Clean resources" 
  rm -rf log lucene storage
fi

if [ "$CONSOLE_OPT" = "true" ] ; then
  export DB_HOST=localhost 
  export DB_PORT=5432 
  export DB_NAME=dataparse_test
  export DB_USERNAME=testuser
  export DB_PASSWORD=testuser
  export DB_RECREATE=true
  export MONGO_URI="mongodb://localhost:27017/dataparse"

  APP_OPTS="-Dcom.zaxxer.hikari.aliveBypassWindowMs=30000 -Dfile.encoding=UTF-8 -Dorg.eclipse.jetty.websocket.LEVEL=DEBUG"
  APP_OPTS="$APP_OPTS -Dapp.home=$APP_HOME"
  APP_OPTS="$APP_OPTS -DELASTIC_URL=http://localhost:9200"
  APP_OPTS="$APP_OPTS -DMONGO_URI=mongodb://localhost:27017/dataparse"
	echo "RUN WITH: $JAVA_HOME/bin/java $JAVA_OPTS -cp "$CLASSPATH" $APP_OPTS com.dataparse.server.RestServer"
	$JAVA_HOME/bin/java $JAVA_OPTS -cp "$CLASSPATH" $APP_OPTS com.dataparse.server.RestServer

elif [ "$DAEMON_OPT" = "true" ] ; then
  #nohup "$JAVACMD" $JAVA_OPTS -cp "$CLASSPATH" $CLASS $ARGS > $LOG_FILE 2>&1 < /dev/null &
  #printf '%d' $! > $PID_FILE
  echo "todo........................................."
elif [ "$STOP_OPT" = "true" ] ; then
  PID=`cat $PID_FILE`
  kill -9 $PID
  echo "Stopped processs $PID"

else
  echo "Usage: "
  echo "  To run the server as daemon"
  echo "    ./server.sh -daemon "
  echo "  To stop the daemon server"
  echo "    ./server.sh -stop "
  echo "  To run the server as console"
  echo "    ./server.sh"
  echo "  Optional parameters for the console mode:"
  echo "    --app.db.load=[test,none] to load the sample test data or an empty database"
  echo "    --server.port=7080 to override the default web server port"
  echo "    --h2.server.tcp-port=8043 to override the default h2 db server port"
fi
