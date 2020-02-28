from kafka import KafkaProducer
import json
import time


class ProducerServer(KafkaProducer):

    def __init__(self, input_file, topic, **kwargs):
        super().__init__(**kwargs)
        self.input_file = input_file
        self.topic = topic
        self._count = 0

    #TODO we're generating a dummy data
    def generate_data(self):
        with open(self.input_file, 'r') as f:
            for ii, line in enumerate(json.load(f)):
                message = self.dict_to_binary(line)
                # TODO send the correct data
                self.send(self.topic, message)
                time.sleep(1)
                if ii % 10 == 0:
                    print(f'{ii} messages already generated')

    # TODO fill this in to return the json dictionary to binary
    def dict_to_binary(self, json_dict):
        return json.dumps(json_dict).encode('utf-8')