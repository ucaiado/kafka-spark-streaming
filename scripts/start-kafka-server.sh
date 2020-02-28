echo "starting kafka server..."
mkdir -p startup/
/usr/bin/kafka-server-start ../config/server.properties # > startup/startup.log 2>&1
