# Lastfm dataset analysis

We are given a dataset containing the listening habits of people listening the Lastfm radio.  
The dataset is described here [Lastfm dataset page](http://ocelma.net/MusicRecommendationDataset/lastfm-1K.html) and is quite big.

We define a user **session** to be comprised of one or more songs played by that user, where each song is started within 20 minutes of the previous song's start time.  

The goal of this exercise if to extract a list of the top 10 songs played in the top 10 longest sessions by track counts.

## Analysis phase

Reading the Lastfm dataset page is useful to understand the structure of the dataset. It is a text file whose fields are separated by the tab character.
Looking at the file we can see that the newest records appear first, the older being at the end of the file. 
This is a problem as we will inject the file in a kafka-producer-client using the cat command, and we expect the oldest events to be injected first.

Creating **sessions** will be accomplished using the **Session Windows** mechanism provided by Kafka, with a duration of 20 minutes.

## Sorting the dataset

After downloading the dataset we can sort it using the following command, so that the oldest events appear first and the latest appear last in the file.

You should replace the \t character by the tab character, for this type CTRL-V then TAB on your keyboard instead of entering \t

```
sort -k2 -t"\t" userid-timestamp-artid-artname-traid-traname.tsv -o userid-timestamp-artid-artname-traid-traname.tsv_sorted
```

## Creating a sample dataset

To experiment while building our project we will need an extract of the original dataset, the command below allows to do this :

```
head -5 userid-timestamp-artid-artname-traid-traname.tsv_sorted > sample.txt
```

# Running the project

To run our project we would like to have more than one consumer so that we can benefit from the distribution mechanism that Kafka offers.
This means that we will create a topic that will have multiple partitions, the dataset will be injected in this topic with a key corresponding to the userid, as the first requirement is to create sessions per userid.

In terms of topics we will have :
* an input topic from which our tracks consumer will read the original lastfm dataset. 
* a session topic that will be feeded by the tracks consumer with the sessions as defined previously, each session will contain the list of tracks for this sessions
* a top session topic that will contain the top 50 sessions as defined previously (not implemented yet)

To run the project we need to follow the steps below

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

kafka-topics.sh --zookeeper 127.0.0.1:2181 --topic last-fm-sessions --create --partitions 1 --replication-factor 1
```

## Starting a top sessions consumer

We can start the sessions consumer to display the top &0 songs in the top 50 sessions produced by our tracks consumer job

```
kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 \
    --topic last-fm-top-songs \
    --property print.key=true \
    --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
    --property value.deserializer=org.apache.kafka.common.serialization.StringDeserializer
```

## Starting the tracks consumer

We can run the tracks consumer from Intellij or by producing a jar file and run it

## Injecting sample data

The command below allows to inject the sample in the input topic with a key being the userid (first field of the dataset)

```
cat sample.txt | kafka-console-producer.sh \
--broker-list localhost:9092 \
--topic last-fm-listenings \
--property "parse.key=true" \
--property "key.separator=	"
```

## Injecting the full dataset

The command below allows to inject the full datast in the input topic with a key being the userid (first field of the dataset)

```
cat lastfm-dataset-1K/userid-timestamp-artid-artname-traid-traname.tsv_sorted | kafka-console-producer.sh \
--broker-list localhost:9092 \
--topic last-fm-listenings \
--property "parse.key=true" \
--property "key.separator=	"
```
