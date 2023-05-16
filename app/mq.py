import pika


class MQClient:

    def __init__(self):
        self.credentials = None
        self.connection = None
        self.channel = None
        self.stop_requested = False

    def configure(self, username, password):
        self.credentials = pika.PlainCredentials(username=username, password=password)

    def connect(self, queue, host="localhost"):
        params = pika.ConnectionParameters(host=host, credentials=self.credentials)
        self.connection = pika.BlockingConnection(params)
        self.connection.call_later(delay=5, callback=self.check_for_stop_callback)
        self.channel = self.connection.channel()
        self.channel.exchange_declare('test',
                                      durable=True,
                                      passive=False,
                                      auto_delete=False,
                                      exchange_type='topic')
        self.channel.queue_declare(queue=queue)
        self.channel.queue_bind(exchange='test', queue=queue, routing_key=queue)
        self.channel.basic_qos(prefetch_count=100)

    def send(self, message, queue):
        self.channel.basic_publish(exchange='test', routing_key=queue, body=message)

    def receive(self, queue, callback):
        self.channel.basic_consume(consumer_tag="app", queue=queue, on_message_callback=callback)

    def start_consuming(self):
        self.stop_requested = False
        self.channel.start_consuming()

    def stop_consuming(self):
        self.channel.stop_consuming(consumer_tag="app")
        self.stop_requested = True

    def ack_message(self, channel, delivery_tag):
        channel.basic_ack(delivery_tag, multiple=False)

    def check_for_stop_callback(self):
        print(f'check_for_stop_callback: {self.stop_requested}')
        if self.stop_requested:
            self.channel.stop_consuming(consumer_tag="app")

    def request_stop(self):
        self.stop_requested = True

    def close(self):
        self.channel.close()
        self.connection.close()
