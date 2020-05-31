package com.viooh.challenge

import java.nio.file.Files
import java.time.Duration
import java.util.Properties
import java.util.UUID.randomUUID

import com.typesafe.config.{Config, ConfigFactory}
import com.viooh.challenge.model.{PlayedTrack, Session, Track}
import com.viooh.challenge.serdes.SessionSerdes
import com.viooh.challenge.utils.TrackTimestampExtractor
import org.apache.kafka.streams.kstream.{SessionWindows, Windowed}
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream.{KStream, Produced}
import org.apache.kafka.streams.state.{KeyValueBytesStoreSupplier, KeyValueStore, StoreBuilder, Stores}
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.slf4j.{Logger, LoggerFactory}

object TrackConsumer {

  type TrackId = String
  type TrackName = String
  type UserId = String
  type SessionId = String
  type SessionDuration = Long
  type PlayCount = Int
  type TrackRank = Int

  val MAX_SESSIONS = 10
  val MAX_TRACKS = 10

  def main(args: Array[String]): Unit = {
    import Serdes._
    import com.viooh.challenge.aggregation.TrackAggregator._
    import com.viooh.challenge.serdes.PlayedTrackSerdes._
    import com.viooh.challenge.serdes.SessionSerdes._
    import com.viooh.challenge.serdes.TrackSerdes._

    val logger: Logger = LoggerFactory.getLogger(TrackConsumer.getClass)
    val config: Config = ConfigFactory.load().getConfig("dev")

    val inputTopic: String = config.getString("inputTopic")
    val sessionTopic: String = config.getString("sessionTopic")
    val topSongsTopic: String = config.getString("topSongsTopic")

    val props: Properties = new Properties()
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "LastFmListeningsApp")
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, config.getString("bootstrap.server"))
    props.put(StreamsConfig.CLIENT_ID_CONFIG, "LastFmListeningsConsumer")
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String.getClass)
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String.getClass)
    props.put(StreamsConfig.STATE_DIR_CONFIG, Files.createTempDirectory("last-fm-listenings-state").toAbsolutePath.toString)
    props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, classOf[TrackTimestampExtractor])

    // Build a session store
    val sessionStoreName: String = "session-store"
    val sessionStoreSupplier: KeyValueBytesStoreSupplier = Stores.persistentKeyValueStore(sessionStoreName)
    val sessionStoreBuilder: StoreBuilder[KeyValueStore[String, Session]] =
      Stores.keyValueStoreBuilder(sessionStoreSupplier, Serdes.String, SessionSerdes.sessionSerdes)

    // Stream builder
    val builder: StreamsBuilder = new StreamsBuilder
    builder.addStateStore(sessionStoreBuilder)
    val lastFmListenings: KStream[UserId, String] = builder.stream[UserId, String](inputTopic)

    // Windows
    val sessionWindowDuration: Duration = java.time.Duration.ofMinutes(20)
    val gracePeriod: Duration = java.time.Duration.ofSeconds(10)
    val sessionWindow: SessionWindows = SessionWindows.`with`(sessionWindowDuration).grace(gracePeriod)

    lastFmListenings
      .flatMapValues((userId, record) => PlayedTrack(userId, record))
      .groupByKey(kstream.Grouped.`with`(Serdes.String, playedTrackSerdes))
      .windowedBy(sessionWindow)
      .aggregate(Map.empty[TrackName, Track])(trackAggregator, trackMerger)
      .toStream
      .filter(isValidEvent)
      .map(toSession(MAX_TRACKS))
      .to(sessionTopic)(Produced.`with`(Serdes.String, sessionSerdes))

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
  def toSession(maxTracks: Int)(windowedUserId: Windowed[UserId], tracks: Map[TrackName, Track]): (SessionId, Session) = {
    val sessionSeconds: Long =
      windowedUserId
        .window()
        .endTime()
        .getEpochSecond - windowedUserId.window().startTime().getEpochSecond

    val topTracksPerPlayCount: List[(Track, TrackRank)] =
      tracks
        .values
        .toList
        .sortWith(_.playCount > _.playCount)
        .take(maxTracks)
        .zipWithIndex

    val sessionId: TrackName = randomUUID().toString

    (sessionId, Session(sessionId, windowedUserId.key(), sessionSeconds, topTracksPerPlayCount))
  }

  /**
   * Checks if an event is valid, it allows to get rid of intermediary events produced by Kafka
   *
   * @param windowedUserId
   * @param tracks
   * @return
   */
  def isValidEvent(windowedUserId: Windowed[UserId], tracks: Map[TrackName, Track]): Boolean = {
    tracks != null &&
      tracks.nonEmpty
  }
}
