import json

import pika
import lorem


class MQClient:

    def __init__(self):
        self.credentials = None
        self.connection = None
        self.channel = None

    def configure(self, username, password):
        self.credentials = pika.PlainCredentials(username=username, password=password)

    def connect(self, host="localhost"):
        conn = pika.ConnectionParameters(host=host, credentials=self.credentials)
        self.connection = pika.BlockingConnection(conn)
        self.channel = self.connection.channel()
        self.channel.exchange_declare('test', durable=True, exchange_type='topic')
        self.channel.queue_declare(queue='A')
        self.channel.queue_bind(exchange='test', queue='A', routing_key='A')

    def send(self, message):
        self.channel.basic_publish(exchange='test', routing_key='A', body=message)

    def close(self):
        self.channel.close()


from json import dumps
from faker import Faker


def fake_person_generator(n, fake):
    for x in range(n):
        yield {'last_name': fake.last_name(),
               'first_name': fake.first_name(),
               'street_address': fake.street_address(),
               'email': fake.email(),
               'index': x}


if __name__ == "__main__":
    client = MQClient()
    client.configure("rabbitmq", "rabbitmq")
    client.connect()

    fake = Faker()
    for message in fake_person_generator(10_000, fake):
        client.send(message=json.dumps(message))

    client.close()
