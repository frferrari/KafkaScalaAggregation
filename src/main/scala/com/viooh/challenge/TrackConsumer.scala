package com.viooh.challenge

import java.nio.file.Files
import java.time.Duration
import java.util.Properties

import com.typesafe.config.{Config, ConfigFactory}
import com.viooh.challenge.model.{Session, Track}
import com.viooh.challenge.utils.TrackTimestampExtractor
import org.apache.kafka.streams.kstream.{SessionWindows, Windowed}
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream.KStream
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.slf4j.{Logger, LoggerFactory}

object TrackConsumer {

  type TrackId = String
  type TrackName = String

  val MAX_SESSIONS = 50
  val MAX_TRACKS = 10

  def main(args: Array[String]): Unit = {
    import Serdes._
    import com.viooh.challenge.aggregation.SessionAggregator._
    import com.viooh.challenge.aggregation.TrackAggregator._
    import com.viooh.challenge.serdes.SessionSerdes._
    import com.viooh.challenge.serdes.TrackSerdes._

    val logger: Logger = LoggerFactory.getLogger(TrackConsumer.getClass)
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

    val builder: StreamsBuilder = new StreamsBuilder
    val lastFmListenings: KStream[String, String] = builder.stream[String, String](inputTopic)
    val sessionWindowDuration: Duration = java.time.Duration.ofMinutes(20)
    val gracePeriod: Duration = java.time.Duration.ofSeconds(10)
    val sessionWindow: SessionWindows = SessionWindows.`with`(sessionWindowDuration).grace(gracePeriod)

    val sessions: KStream[String, Session] =
      lastFmListenings
        .peek((userId, v) => println(s"userId=$userId v=$v"))
        .groupByKey
        .windowedBy(sessionWindow)
        .aggregate(Map.empty[TrackId, Track])(trackAggregator, trackMerger)
        .toStream
        .map(toSession)
        .filter((k, session) => session.sessionDurationSeconds > 0) // TODO Check why we have empty sessions
        .peek((userId, v) => println(s"userId=$userId v=$v"))

    val top50Sessions: KStream[Long, List[Session]] =
      sessions
        .selectKey((userId, session) => session.sessionDurationSeconds)
        .groupByKey(kstream.Grouped.`with`(Serdes.Long, sessionSerdes))
        .aggregate(List.empty[Session])(sessionAggregator)
        .toStream
        .peek((sessionDurationSeconds, sessions) => println(s"sessionDuration $sessionDurationSeconds sessions $sessions"))

    top50Sessions
      .flatMapValues((sessionDuration, sessions) => sessions.flatMap(_.tracks.values))
      .selectKey { case (sessionDuration, track) => track.playCount }
      .groupByKey(kstream.Grouped.`with`(Serdes.Integer, trackSerdes))
      .aggregate(List.empty[Track])(mostPlayedTrackAggregator)
      .toStream
      .flatMapValues((playCount, tracks) => tracks)
      .peek((playCount, track) => println(s"playCount=$playCount track=$track"))
    // .to(outputTopic)(Produced.`with`(Serdes.String, sessionSerdes))

    val streams = new KafkaStreams(builder.build(), props)
    streams.start()

    sys.ShutdownHookThread {
      streams.close(Duration.ofSeconds(10))
    }
  }

  /**
   * Converts a collection of tracks to a Session object
   *
   * @param windowedUserId The window to which the tracks belong to
   * @param tracks         The collection of tracks to move to a session
   * @return
   */
  def toSession(windowedUserId: Windowed[String], tracks: Map[TrackId, Track]): (String, Session) = {
    val sessionSeconds: Long = windowedUserId.window().endTime().getEpochSecond - windowedUserId.window().startTime().getEpochSecond

    (windowedUserId.key(), Session(windowedUserId.key(), sessionSeconds, tracks))
  }
}
