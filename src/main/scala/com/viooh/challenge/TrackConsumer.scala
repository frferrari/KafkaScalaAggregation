package com.viooh.challenge

import java.nio.file.Files
import java.time.Duration
import java.util.Properties
import java.util.UUID.randomUUID

import com.typesafe.config.{Config, ConfigFactory}
import com.viooh.challenge.aggregation.store.TopSessions
import com.viooh.challenge.model.{PlayedTrack, Session, Track}
import com.viooh.challenge.serdes.{SessionSerdes, TopSessionsSerdes}
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

  val MAX_SESSIONS = 50
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
    val sessionWindow: SessionWindows = SessionWindows.`with`(sessionWindowDuration)

    val adder: (SessionId, Session, TopSessions) => TopSessions =
      (sessionId: SessionId, session: Session, agg: TopSessions) => {
        agg.add(session)
        agg
      }

    val subtractor: (SessionId, Session, TopSessions) => TopSessions =
      (sessionId: SessionId, session: Session, agg: TopSessions) => {
        agg.remove(session)
        agg
      }

    lastFmListenings
      .flatMapValues((userId, record) => PlayedTrack(userId, record))
      .groupByKey(kstream.Grouped.`with`(Serdes.String, playedTrackSerdes))
      .windowedBy(sessionWindow)
      .aggregate(Map.empty[TrackName, Track])(trackAggregator, trackMerger)
      //.suppress(Suppressed.untilTimeLimit(java.time.Duration.ofSeconds(20), Suppressed.BufferConfig.unbounded()))
      // .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
      .mapValues(toSession(MAX_TRACKS)(_, _))
      .groupBy((w, session) => session)(kstream.Grouped.`with`(Serdes.String, sessionSerdes))
      .aggregate(new TopSessions(MAX_SESSIONS))(adder, subtractor)(TopSessionsSerdes.topSessionsMaterializer)
      .toStream
      .flatMapValues(_.toStream)
      .flatMap(mkString)
      .peek((sessionId, session) => println(s"sessionId=$sessionId session=$session"))
      .to(topSongsTopic)(Produced.`with`(Serdes.String, Serdes.String))

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
   * Produces a sequence of strings containing the details for each track and associated session
   *
   * @param sessionId
   * @param session
   * @return
   */
  def mkString(sessionId: SessionId, session: Session): List[(SessionId, String)] = {
    session
      .tracks
      .map(track => (sessionId, toTsv(sessionId, session)(track)))
  }

  /**
   * Produces a string containing all the track details and its associated session
   * Fields are separated by the TRACK_RECORD_FIELD_SEPARATOR
   *
   * @param sessionId
   * @param session
   * @param track
   * @return
   */
  def toTsv(sessionId: SessionId, session: Session)(track: (Track, TrackRank)): String = {
    s"""${session.userId}${TRACK_RECORD_FIELD_SEPARATOR}
       |${session.sessionDurationSeconds}${TRACK_RECORD_FIELD_SEPARATOR}
       |${track._2 + 1}${TRACK_RECORD_FIELD_SEPARATOR}
       |${track._1.trackId}${TRACK_RECORD_FIELD_SEPARATOR}
       |${track._1.trackName}${TRACK_RECORD_FIELD_SEPARATOR}
       |${track._1.playCount}${TRACK_RECORD_FIELD_SEPARATOR}""".stripMargin.replaceAll("\n", "")
  }
}
