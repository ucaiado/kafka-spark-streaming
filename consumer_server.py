from confluent_kafka import Consumer
from conf import BROKER_URL, TOPIC_NAME


def consume(topic_name):
    """Consumes data from the Kafka Topic"""
    c = Consumer(
        {"bootstrap.servers": BROKER_URL,
         "group.id": "0"}
    )
    c.subscribe([topic_name])

    while True:
        messages = c.consume(5, timeout=1.0)
        print(f"consumed {len(messages)} messages")
        ii = 0
        for ii, message in enumerate(messages):
            print(f"consume message {message.key()}: {message.value()}")
        if ii > 0:
            print('\n')


if __name__ == "__main__":
    consume(TOPIC_NAME)