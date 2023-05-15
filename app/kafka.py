from confluent_kafka import Producer, KafkaError, KafkaException


def error_callback(err):
    print(f'Client error {err}')
    if err.code() == KafkaError.SASL_AUTHENTICATION_FAILED:
        raise KafkaException(err)


from datetime import datetime


class Writer:

    def __init__(self, config):
        self.p = None
        self.config = config

    def configure(self):
        self.p = Producer({
            'bootstrap.servers': self.config["BOOTSTRAP_SERVERS"],
            'sasl.mechanism': 'PLAIN',
            'security.protocol': 'SASL_SSL',
            'sasl.username': self.config["API_KEY"],
            'sasl.password': self.config["API_SECRET"],
            'error_cb': error_callback,
            'acks': -1,
            'linger.ms': 100,
            'batch.size': 2000000,
            'compression.type': 'snappy'
        })

    def produce(self, topic, message, message_callback):
        #print(f'producing topic={topic} message={message}')
        self.p.produce(topic=topic, value=message, callback=message_callback)
        self.p.poll(timeout=10)

    def close(self):
        self.p.close()


class VoidWriter:

    def __init__(self, config):
        self.p = None
        self.config = config

    def configure(self):
        pass

    def produce(self, topic, message, message_callback):
        print(f'producing topic={topic} message={message}')
        # message_callback(None, message)

    def close(self):
        pass
