package com.viooh.challenge.serdes

import com.viooh.challenge.TrackConsumer.TrackId
import com.viooh.challenge.model.Track
import com.viooh.challenge.serializer._
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.scala.kstream.Materialized
import org.apache.kafka.streams.scala.{ByteArrayKeyValueStore, ByteArraySessionStore, Serdes}

object TrackSerdes {
  import org.apache.kafka.common.serialization.{Serdes => JSerdes}

  val trackSerdes: Serde[Track] = JSerdes.serdeFrom(new TrackSerializer, new TrackDeserializer)
  val trackStoreSerdes: Serde[Map[TrackId, Track]] = JSerdes.serdeFrom(new TrackStoreSerializer, new TrackStoreDeserializer)
  val mostPlayedTrackStoreSerdes: Serde[List[Track]] = JSerdes.serdeFrom(new MostPlayedTrackStoreSerializer, new MostPlayedTrackStoreDeserializer)

  implicit val trackStoreMaterializer: Materialized[String, Map[TrackId, Track], ByteArraySessionStore] =
    Materialized.`with`[String, Map[TrackId, Track], ByteArraySessionStore](Serdes.String, trackStoreSerdes)

  implicit val mostPlayedTrackStoreMaterializer: Materialized[Int, List[Track], ByteArrayKeyValueStore] =
    Materialized.`with`[Int, List[Track], ByteArrayKeyValueStore](Serdes.Integer, mostPlayedTrackStoreSerdes)
}
