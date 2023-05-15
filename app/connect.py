from app.mq import MQClient


class ConnectTask:

    def __init__(self, mq_client, kafka_client):
        self.stop = False
        self.mq_client = mq_client
        self.kafka_client = kafka_client

    def run(self):
        self.mq_client.receive(queue="A", callback=self.mq_read_callback)
        try:
            self.mq_client.start_consuming()
        except:
            self.mq_client.stop_consuming()
        self.mq_client.close()
        self.kafka_client.close()

    def mq_read_callback(self, channel, basic_delivery, properties, body):
        def message_callback(err, msg):
            if err is None:
                ## ack the message in rabbitmq
                #print(f'Ack messages to the MQ client {str(body)} {basic_delivery.delivery_tag}')
                self.mq_client.ack_message(channel=channel, delivery_tag=basic_delivery.delivery_tag)
            else:
                print(f'Error happened during delivery {err}')

        ## produce into kafka
        self.kafka_client.produce(topic="test", message=body, message_callback=message_callback)

    def request_stop(self):
        self.mq_client.request_stop()
