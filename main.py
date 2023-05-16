import concurrent
from threading import Event

from flask import Flask

from app.connect import ConnectTask
from app.kafka import Writer
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

    print(f'Starting tasks: {len(futures)}')
    for _ in range(1):
        ct = ConnectTask(mq_client=mq_client, kafka_client=kafka_writer, config=app.config)
        tasks.append(ct)
        futures.append(executor.submit(task, ct))
    print(f'Starting tasks: {len(futures)}')

    return f'{len(futures)} tasks running in the background'


@app.route('/status')
def status():
    tasks = []
    for i, feature in enumerate(futures):
        tasks.append(f'Feature {i} running?={feature.running()}')
    return "\n".join(tasks)


@app.route('/stats.json')
def stats():
    json_str = kafka_writer.json_stats
    response_str = "{}" if json_str is None else json_str
    return app.response_class(
        response=response_str,
        status=200,
        mimetype="application/json"
    )


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
