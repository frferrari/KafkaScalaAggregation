package com.viooh.challenge

import java.nio.file.Files
import java.time.Duration
import java.util.Properties

import com.typesafe.config.{Config, ConfigFactory}
import com.viooh.challenge.model.{Session, Track}
import com.viooh.challenge.serializer.{SessionDeserializer, SessionSerializer, SessionStoreDeserializer, SessionStoreSerializer, TrackStoreDeserializer, TrackStoreSerializer}
import com.viooh.challenge.utils.TrackTimestampExtractor
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.kstream.{SessionWindows, Windowed}
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream.{KStream, Materialized, Produced}
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.slf4j.LoggerFactory

object TrackConsumer {

  type TrackId = String
  type TrackName = String

  val MAX_SESSIONS = 50

  def main(args: Array[String]): Unit = {
    import Serdes._

    val logger = LoggerFactory.getLogger(TrackConsumer.getClass)
    val config: Config = ConfigFactory.load().getConfig("dev")

    val inputTopic: String = config.getString("inputTopic")
    val outputTopic: String = config.getString("outputTopic")

    val props: Properties = new Properties()
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "LastFmListeningsApp")
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, config.getString("bootstrap.server"))
    props.put(StreamsConfig.CLIENT_ID_CONFIG, "LastFmListeningsConsumer")
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String.getClass)
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String.getClass)
    props.put(StreamsConfig.STATE_DIR_CONFIG, Files.createTempDirectory("last-fm-listenings-state").toAbsolutePath.toString)
    props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, classOf[TrackTimestampExtractor])

    import org.apache.kafka.common.serialization.{Serdes => JSerdes}

    // val lastFmListeningSerdes: Serde[LastFmListening] = JSerdes.serdeFrom(new LastFmListeningSerializer, new LastFmListeningDeserializer)
    val trackStoreSerdes: Serde[Map[TrackId, Track]] = JSerdes.serdeFrom(new TrackStoreSerializer, new TrackStoreDeserializer)
    val sessionSerdes: Serde[Session] = JSerdes.serdeFrom(new SessionSerializer, new SessionDeserializer)
    val sessionStoreSerdes: Serde[List[Session]] = JSerdes.serdeFrom(new SessionStoreSerializer, new SessionStoreDeserializer)

    // https://kafka.apache.org/25/documentation/streams/developer-guide/write-streams
    // https://jaceklaskowski.gitbooks.io/mastering-kafka-streams/kafka-streams-scala.html
    val builder: StreamsBuilder = new StreamsBuilder
    val lastFmListenings: KStream[String, String] = builder.stream[String, String](inputTopic)
    val sessionWindowDuration: Duration = java.time.Duration.ofMinutes(20)
    val sessionWindow: SessionWindows = SessionWindows.`with`(sessionWindowDuration)

    implicit val trackStoreMaterializer: Materialized[String, Map[TrackId, Track], ByteArraySessionStore] =
      Materialized.`with`[String, Map[TrackId, Track], ByteArraySessionStore](Serdes.String, trackStoreSerdes)

    implicit val sessionMaterializer: Materialized[Long, List[Session], ByteArraySessionStore] =
      Materialized.`with`[Long, List[Session], ByteArraySessionStore](Serdes.Long, sessionStoreSerdes)

    implicit val sessionStoreMaterializer: Materialized[Long, List[Session], ByteArrayKeyValueStore] =
      Materialized.`with`[Long, List[Session], ByteArrayKeyValueStore](Serdes.Long, sessionStoreSerdes)

    val sessions: KStream[String, Session] = lastFmListenings
      .peek((userId, v) => println(s"userId=$userId v=$v"))
      .groupByKey
      .windowedBy(sessionWindow)
      .aggregate(Map.empty[TrackId, Track])(trackAggregator, trackMerger)
      .toStream
      .map(toSession)
      .peek((userId, v) => println(s"userId=$userId v=$v"))

    val top50Sessions: KStream[Long, List[Session]] = sessions
      .selectKey((userId, session) => session.sessionDurationSeconds)
      .groupByKey(kstream.Grouped.`with`(Serdes.Long, sessionSerdes))
      .aggregate(List.empty[Session])(sessionAggregator)
      .toStream
      .peek((sessionDurationSeconds, sessions) => println(s"sessionDuration $sessionDurationSeconds sessions $sessions"))
    // .to(outputTopic)(Produced.`with`(Serdes.String, sessionSerdes))

    val streams = new KafkaStreams(builder.build(), props)
    streams.start()

    sys.ShutdownHookThread {
      streams.close(Duration.ofSeconds(10))
    }
  }

  def toSession(windowedUserId: Windowed[String], tracks: Map[TrackId, Track]): (String, Session) = {
    val sessionSeconds: Long = windowedUserId.window().endTime().getEpochSecond - windowedUserId.window().startTime().getEpochSecond

    (windowedUserId.key(), Session(windowedUserId.key(), sessionSeconds, tracks))
  }

  def trackAggregator(userId: String, recordValue: String, trackStore: Map[TrackId, Track]): Map[TrackId, Track] = {
    val trackInfo: Array[String] = recordValue.split("\t")

    // Increment the count of track per trackId
    if (trackInfo.length == 5) {
      val trackId: TrackId = trackInfo(3)
      val trackName: TrackName = trackInfo(4)

      trackStore.get(trackId) match {
        case Some(track) => {
          trackStore + (trackId -> track.copy(playCount = track.playCount + 1))
        }
        case None => trackStore + (trackId -> Track(trackId, trackName, 1))
      }
    } else trackStore
  }

  def trackMerger(userId: String, trackStore1: Map[TrackId, Track], trackStore2: Map[TrackId, Track]): Map[TrackId, Track] = {
    trackStore1 ++ trackStore2 // TODO add values
  }

  def sessionAggregator(sessionDurationSeconds: Long, session: Session, sessionStore: List[Session]): List[Session] = {
    if (sessionStore.isEmpty) {
      // If the sessionSore is empty then we can add the given session
      sessionStore :+ session
    } else {
      if (sessionStore.length < MAX_SESSIONS)
      // we can add sessions in the store without contrainst until the session store contains the max authorized sessions
      sessionStore :+ session
        else {
        val minSessionDuration: Session = sessionStore.minBy(_.sessionDurationSeconds)

        if (sessionDurationSeconds > minSessionDuration.sessionDurationSeconds) {
          (sessionStore :+ session).sortWith(_.sessionDurationSeconds < _.sessionDurationSeconds).take(MAX_SESSIONS)
        } else {
          sessionStore
        }
      }
    }
  }
}