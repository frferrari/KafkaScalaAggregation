package com.viooh.challenge.serdes

import com.viooh.challenge.TrackConsumer.TrackId
import com.viooh.challenge.model.{PlayedTrack, Track}
import com.viooh.challenge.serdes.TrackSerdes.trackStoreSerdes
import com.viooh.challenge.serializer._
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.scala.{ByteArrayKeyValueStore, ByteArraySessionStore, Serdes}
import org.apache.kafka.streams.scala.kstream.Materialized

object PlayedTrackSerdes {
  import org.apache.kafka.common.serialization.{Serdes => JSerdes}

  val playedTrackSerdes: Serde[PlayedTrack] = JSerdes.serdeFrom(new PlayedTrackSerializer, new PlayedTrackDeserializer)

  implicit val playedTrackMaterializer: Materialized[String, PlayedTrack, ByteArrayKeyValueStore] =
    Materialized.`with`[String, PlayedTrack, ByteArrayKeyValueStore](Serdes.String, playedTrackSerdes)
}
