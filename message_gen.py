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


if __name__ == "__main__":
    client = MQClient()
    client.configure("rabbitmq", "rabbitmq")
    client.connect()

    for i in range(1_000):
        #message = " ".join(lorem.paragraph() for _ in range(100))
        message = f'Message#{i}'
        client.send(message=message)

    client.close()

