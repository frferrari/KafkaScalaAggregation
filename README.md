## Starting zookeeper and kafka

You could start zookeeper and kafka in 2 different terminal so that you can look at the log messages and check if anything fails.
The below commands have to be started from the kafka directory as the paths to the config files are relative.

```
zookeeper-server-start.sh config/zookeeper.properties

kafka-server-start.sh config/server.properties
```

## Creating the kafka topics

```
kafka-topics.sh --zookeeper 127.0.0.1:2181 --topic last-fm-listenings --create --partitions 4 --replication-factor 1

kafka-topics.sh --zookeeper 127.0.0.1:2181 --topic last-fm-metrics --create --partitions 4 --replication-factor 1
```

## Starting a consumer for the unique users metrics

```
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic last-fm-metrics --property print.key=true --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer --property value.deserializer=org.apache.kafka.common.serialization.StringDeserializer
```