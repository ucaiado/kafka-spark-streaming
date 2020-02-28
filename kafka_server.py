import producer_server
from conf import (TOPIC_NAME, INPUT_FILE, BROKER_URL, CLIENT_ID)


def run_kafka_server():
    # TODO get the json file path
    input_file = INPUT_FILE

    # TODO fill in blanks
    producer = producer_server.ProducerServer(
        input_file=input_file,
        topic=TOPIC_NAME,
        bootstrap_servers=BROKER_URL
    )

    return producer


def feed():
    producer = run_kafka_server()
    producer.generate_data()


if __name__ == "__main__":
    feed()
