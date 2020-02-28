# start zookeeper and kafka server
echo "starting zookeeper server..."
mkdir -p startup/
/usr/bin/zookeeper-server-start ../config/zookeeper.properties
