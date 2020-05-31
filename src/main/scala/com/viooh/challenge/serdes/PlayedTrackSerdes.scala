package com.viooh.challenge.serdes

import com.viooh.challenge.model.PlayedTrack
import com.viooh.challenge.serializer._
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.scala.kstream.Materialized
import org.apache.kafka.streams.scala.{ByteArrayKeyValueStore, Serdes}

object PlayedTrackSerdes {
  import org.apache.kafka.common.serialization.{Serdes => JSerdes}

  val playedTrackSerdes: Serde[PlayedTrack] = JSerdes.serdeFrom(new PlayedTrackSerializer, new PlayedTrackDeserializer)

  implicit val playedTrackMaterializer: Materialized[String, PlayedTrack, ByteArrayKeyValueStore] =
    Materialized.`with`[String, PlayedTrack, ByteArrayKeyValueStore](Serdes.String, playedTrackSerdes)
}
