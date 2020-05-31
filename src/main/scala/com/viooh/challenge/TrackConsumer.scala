package com.viooh.challenge

import java.nio.file.Files
import java.time.Duration
import java.util.Properties

import com.typesafe.config.{Config, ConfigFactory}
import com.viooh.challenge.aggregation.TopSessionTransformer
import com.viooh.challenge.model.{PlayedTrack, Session, Track}
import com.viooh.challenge.serdes.SessionSerdes
import com.viooh.challenge.utils.TrackTimestampExtractor
import org.apache.kafka.streams.kstream.{SessionWindows, Transformer, TransformerSupplier, ValueTransformerSupplier, Window, Windowed}
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream.{KGroupedStream, KStream}
import org.apache.kafka.streams.state.{KeyValueBytesStoreSupplier, KeyValueStore, StoreBuilder, Stores}
import org.apache.kafka.streams.{KafkaStreams, KeyValue, StreamsConfig}
import org.slf4j.{Logger, LoggerFactory}
import java.util.UUID.randomUUID

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
    import com.viooh.challenge.aggregation.SessionAggregator._
    import com.viooh.challenge.aggregation.TrackAggregator._
    import com.viooh.challenge.serdes.SessionSerdes._
    import com.viooh.challenge.serdes.TrackSerdes._
    import com.viooh.challenge.serdes.PlayedTrackSerdes._

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

    val sessions: KStream[SessionId, Session] =
      lastFmListenings
        .flatMapValues((userId, record) => PlayedTrack(userId, record))
        // .peek((userId, v) => println(s"userId=$userId v=$v"))
        .groupByKey(kstream.Grouped.`with`(Serdes.String, playedTrackSerdes))
        .windowedBy(sessionWindow)
        .aggregate(Map.empty[TrackName, Track])(trackAggregator, trackMerger)
        .toStream
        .filter(isValidEvent)
        // .peek((wk, m) => println(s"wk=${wk} m=$m"))
        .map(toSession(MAX_TRACKS))
    // .flatMap(mkString)
    // .to(outputTopic)

    /*
    val TransformerSupplier: TransformerSupplier[SessionId, Session, KeyValue[SessionId, Session]] =
      () => new TopSessionTransformer(sessionStoreName, MAX_SESSIONS)

     */

    val valueTransformerSupplier: ValueTransformerSupplier[Session, Session] =
      () => new TopSessionTransformer(sessionStoreName, MAX_SESSIONS)

    sessions
      .transformValues(valueTransformerSupplier, sessionStoreName)
      .filter((sessionId, session) => session != null)
      .peek((sessionId, session) => println(s"sessionId=$sessionId session=$session"))

    sessions
      .groupBy((sessionId, session) => session.tracks.size)(kstream.Grouped.`with`(Serdes.Integer, sessionSerdes))


    //  .transform(TransformerSupplier, sessionStoreName)

    /*
    val TransformerSupplier: TransformerSupplier[SessionId, Session, KeyValue[SessionId, Session]] =
      () => new TopSessionTransformer(sessionStoreName, MAX_SESSIONS)

      sessions
      .transform(TransformerSupplier, sessionStoreName)
      .peek((sessionId, session) => println(s"sessionId=$sessionId session=$session"))
     */

    /*
    // The top sessions in terms of duration
    val topSessions: KStream[SessionDuration, List[Session]] =
      sessions
        .selectKey((userId, session) => session.sessionDurationSeconds)
        .groupByKey(kstream.Grouped.`with`(Serdes.Long, sessionSerdes))
        .aggregate(List.empty[Session])(sessionAggregator(MAX_SESSIONS))
        .toStream
        .map((sessionDuration, sessions) =>)
        .peek((sessionDurationSeconds, sessions) => println(s"sessionDuration $sessionDurationSeconds sessions $sessions"))

    topSessions
      .flatMapValues((sessionDuration, sessions) => sessions.flatMap(_.tracks.values))
      .selectKey { case (sessionDuration, track) => track.playCount }
      .groupByKey(kstream.Grouped.`with`(Serdes.Integer, trackSerdes))
      .aggregate(List.empty[Track])(mostPlayedTrackAggregator(MAX_TRACKS))
      .toStream
      .flatMapValues((playCount, tracks) => tracks)
      .peek((playCount, track) => println(s"playCount=$playCount track=$track"))
    // .to(outputTopic)(Produced.`with`(Serdes.String, sessionSerdes))
     */

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
   * Computes the duration of a window
   *
   * @param window The window to compute the duration for
   * @return The duration of the window in seconds
   */
  def windowDuration(window: Window): Long = window.end() - window.start()

  /**
   * Checks wether a window is empty or not (meaning its duration equals 0)
   *
   * @param window The window to check for 0 length
   * @return A boolean indicating if the window length equals 0 or not
   */
  def isWindowEmpty(window: Window): Boolean = windowDuration(window) == 0

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
