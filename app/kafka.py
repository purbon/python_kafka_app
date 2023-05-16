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
        self.json_stats = None

    def configure(self):
        self.p = Producer({
            'bootstrap.servers': self.config["BOOTSTRAP_SERVERS"],
            'sasl.mechanism': 'PLAIN',
            'security.protocol': 'SASL_SSL',
            'sasl.username': self.config["API_KEY"],
            'sasl.password': self.config["API_SECRET"],
            'error_cb': error_callback,
            'stats_cb': self.stats_callback,
            'acks': -1,
            'linger.ms': 1000,
            'batch.size': 5000,
            'statistics.interval.ms': 500,
            'compression.type': 'gzip'
        })

    def produce(self, topic, message, message_callback):
        print(f'producing topic={topic}')
        self.p.produce(topic=topic, value=message, on_delivery=message_callback)
        self.p.poll(timeout=50)

    def stats_callback(self, json_str):
        self.json_stats = json_str

    def close(self):
        self.p.close()


class FlushingWriter(Writer):

    def produce(self, topic, message, message_callback):
        # print(f'producing topic={topic} message={message}')
        self.p.produce(topic=topic, value=message, callback=message_callback)
        self.p.flush()


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
