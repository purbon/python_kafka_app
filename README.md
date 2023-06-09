# Sample Python Kafka App

### Functionality

* Read message from a local rabbitmq server (available localy via docker-compose)
* Write this message into a Kafka topic

### Configuration

You should setup all required connection metrics in a file ```app.cfg```

### start

Run the script ```run.sh```

### API

* ```/start``` start the in memory workers
* ```/stats``` pull processing stats (librdkafka)

Load sample messages using the ```message_gen.py``` script into the local rabbitmq.

#### Write case

* Pulling from a legacy queue system, MQ style
* Need to ack messages once acknowledged / received in Kafka
* Leverage as much as possible compaction
* Get metrics of the producing side
