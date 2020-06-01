package com.viooh.challenge.serdes

import com.viooh.challenge.TrackConsumer.SessionId
import com.viooh.challenge.aggregation.store.TopSessions
import com.viooh.challenge.serializer._
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.scala.kstream.Materialized
import org.apache.kafka.streams.scala.{ByteArrayKeyValueStore, Serdes}

object TopSessionsSerdes {
  import org.apache.kafka.common.serialization.{Serdes => JSerdes}

  val topSessionsSerdes: Serde[TopSessions] = JSerdes.serdeFrom(new TopSessionsSerializer, new TopSessionsDeserializer)

  implicit val topSessionsMaterializer: Materialized[SessionId, TopSessions, ByteArrayKeyValueStore] =
    Materialized.`with`[SessionId, TopSessions, ByteArrayKeyValueStore](Serdes.String, topSessionsSerdes)
}
