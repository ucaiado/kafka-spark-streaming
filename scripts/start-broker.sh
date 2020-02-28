cp conf.py conf &&. ./conf && rm conf
echo 'run in another console:'
echo "/usr/bin/kafka-console-consumer --bootstrap-server $BROKER_URL --topic $TOPIC_NAME --from-beginning --max-messages 10"
echo

# starting kafka server
echo "starting broker server..."
python ../kafka_server.py
