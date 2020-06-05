# Lastfm dataset analysis

We are given a dataset containing the listening habits of people listening the Lastfm radio.  
The dataset is described here [Lastfm dataset page](http://ocelma.net/MusicRecommendationDataset/lastfm-1K.html) and is quite big.

We define a user **session** to be comprised of one or more songs played by that user, where each song is started within 20 minutes of the previous song's start time.  

The goal of this exercise if to extract a list of the top 10 songs played in the top 50 longest sessions by track counts.

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

## Output produced

Below is an extract of the data produced by the tracks consumer, the format is :

```
sessionId\tuserId\tsessionDurationSeconds\ttrackRank\ttrackId\ttrackName\ttrackPlayCount
```

```
73dab5b3-1cfb-456c-8575-4a0f5748e66a	user_000285	0	1	46909ba9-46c7-461e-a2ef-280eacd550e4	Jenny Was A Friend Of Mine	1	
91283cb0-26d6-4dd2-9682-3de1de6b937b	user_000792	0	1	787fa50f-1a43-4043-9f9a-ec350b31292a	Heroin	1	
829e0241-362d-44a6-bff4-62e22720b732	user_000142	321	1	14025355-94c2-4e9b-b63f-c16cab9e8086	Revelations	1	
829e0241-362d-44a6-bff4-62e22720b732	user_000142	321	2	bc862198-8883-4253-b1b6-51f2d3048e85	Flight Of Icarus	1	
d95b806d-0a0e-4cfb-b543-f64da21248e4	user_000966	0	1	7c47da83-b277-41cd-943c-fa5cfb0e5fe5	Billie Jean	1	
a944861a-bd96-469d-aa73-2e120a885291	user_000525	912	1	137d6675-2cb0-4ba0-926e-b6cee5d33906	Butterflies And Hurricanes	1	
a944861a-bd96-469d-aa73-2e120a885291	user_000525	912	2	4107e45b-48fe-45b1-9e92-6b6701c3484b	The Small Print	1	
a944861a-bd96-469d-aa73-2e120a885291	user_000525	912	3	4e578cf4-3b5a-4200-9786-10038df5d26a	Thoughts Of A Dying Atheist	1	
a944861a-bd96-469d-aa73-2e120a885291	user_000525	912	4	a818e115-26d3-40ae-b7ee-f7f3f91ede63	Endlessly	1	
a944861a-bd96-469d-aa73-2e120a885291	user_000525	912	5	b60ff03a-0afc-498b-8f4d-087a5b0a921c	Ruled By Secrecy	1	
02c0228e-328b-4621-8d3d-00850d8934c7	user_000304	800	1	87a72a5a-0d2f-43a8-9b1a-bf8154d6fdfa	Ladyflash	1	
02c0228e-328b-4621-8d3d-00850d8934c7	user_000304	800	2	5797d522-ac2d-4321-9029-d472ab8093e6	Rich	1	
02c0228e-328b-4621-8d3d-00850d8934c7	user_000304	800	3	aaf977de-bd18-43f3-a7e7-41de2434ebe4	Yeah! New York	1	
02c0228e-328b-4621-8d3d-00850d8934c7	user_000304	800	4	9fdf945c-9888-401a-9321-eabde98093a9	Bang	1	
02c0228e-328b-4621-8d3d-00850d8934c7	user_000304	800	5	93720e70-95f0-4373-9df1-27938c95086a	Feelgood By Numbers	1	
47d44dfa-e149-45ee-9a55-826ba44acafe	user_000174	1512	1	0c43f8b9-3c5b-491f-9230-1dacecc31568	Primrose Hill	2	
47d44dfa-e149-45ee-9a55-826ba44acafe	user_000174	1512	2	597a3a79-3d95-4e2c-b22d-cf193dc0b9ed	Fascination	2	
47d44dfa-e149-45ee-9a55-826ba44acafe	user_000174	1512	3	44df019e-8c09-414d-9690-d09d27d49f4b	Only Love Can Break Your Heart	1	
47d44dfa-e149-45ee-9a55-826ba44acafe	user_000174	1512	4	c08da71c-e876-43d2-8756-3d3674fae876	Finisterre	1	
47d44dfa-e149-45ee-9a55-826ba44acafe	user_000174	1512	5		Farm To Follow (Ft Kelster)	1	
47d44dfa-e149-45ee-9a55-826ba44acafe	user_000174	1512	6	9906282b-c96d-4324-8f60-204c4fc34604	Action	1	
6c763250-aac6-42ce-8b47-87018b23ccca	user_000709	4441	1		Tell Him (Feat. Celine Dion)	1	
6c763250-aac6-42ce-8b47-87018b23ccca	user_000709	4441	2	a99c39dd-3bb1-4021-be2e-ecc6c6db8351	Bridge Over Troubled Water	1	
6c763250-aac6-42ce-8b47-87018b23ccca	user_000709	4441	3	60572fab-c835-4733-aa5a-2864c318c9c8	Your Song	1	
6c763250-aac6-42ce-8b47-87018b23ccca	user_000709	4441	4	3ce14282-0188-470f-bd11-1f242f2dfa7f	China Roses	1	
6c763250-aac6-42ce-8b47-87018b23ccca	user_000709	4441	5	59c17446-d391-4dd2-b275-54a5a50e68a8	Brightest	1	
6c763250-aac6-42ce-8b47-87018b23ccca	user_000709	4441	6	c9adb837-71e8-4f13-bd2d-dd24165a027d	Biggest Part Of Me	1	
6c763250-aac6-42ce-8b47-87018b23ccca	user_000709	4441	7	2751bb41-418b-47a5-ac0c-6f3a49a7e1ec	Fever	1	
6c763250-aac6-42ce-8b47-87018b23ccca	user_000709	4441	8	de4f1a92-f7a4-4ca5-9a96-7dfb9c891087	Relativity	1	
6c763250-aac6-42ce-8b47-87018b23ccca	user_000709	4441	9	bf5e5557-a56f-44d2-b55f-f61fdb0270b8	You'Re A Superstar	1	
6c763250-aac6-42ce-8b47-87018b23ccca	user_000709	4441	10		Singabahambayo	1	
```
