import concurrent
from threading import Event

from flask import Flask

from app.connect import ConnectTask
from app.kafka import Writer, VoidWriter
from app.mq import MQClient

app = Flask(__name__)
app.config.from_envvar('APP_SETTINGS')

futures = []
tasks = []

executor = concurrent.futures.ThreadPoolExecutor()
event = Event()

kafka_writer = Writer(config=app.config)
kafka_writer.configure()


def task(ct):
    ct.run()


@app.route('/')
def index():
    return 'index'


@app.route('/start')
def start():
    print("Starting a Connect Consume Task")
    mq_client = MQClient()
    mq_client.configure(username="rabbitmq", password="rabbitmq")
    mq_client.connect(queue="A")

    ct = ConnectTask(mq_client=mq_client, kafka_client=kafka_writer)
    tasks.append(ct)

    print(f'Starting a CT {ct} {len(futures)}')
    futures.append(executor.submit(task, ct))
    print(f'Starting a CT {ct} {len(futures)}')

    return f'{len(futures)} tasks running in the background'


@app.route('/status')
def status():
    tasks = []
    for i, feature in enumerate(futures):
        tasks.append(f'Feature {i} running?={feature.running()}')
    return "\n".join(tasks)


@app.route('/stop')
def stop():
    for _task in tasks:
        _task.request_stop()

    for i, future in enumerate(futures):
        future.cancel()

    concurrent.futures.wait(futures)
    futures.clear()
    tasks.clear()
    return 'All futures are stopped'
