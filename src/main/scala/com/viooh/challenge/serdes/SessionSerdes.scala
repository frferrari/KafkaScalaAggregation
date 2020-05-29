package com.viooh.challenge.serdes

import com.viooh.challenge.model.Session
import com.viooh.challenge.serializer.{SessionDeserializer, SessionSerializer, SessionStoreDeserializer, SessionStoreSerializer}
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.scala.kstream.Materialized
import org.apache.kafka.streams.scala.{ByteArrayKeyValueStore, ByteArraySessionStore, Serdes}

object SessionSerdes {
  import org.apache.kafka.common.serialization.{Serdes => JSerdes}

  val sessionSerdes: Serde[Session] = JSerdes.serdeFrom(new SessionSerializer, new SessionDeserializer)
  val sessionStoreSerdes: Serde[List[Session]] = JSerdes.serdeFrom(new SessionStoreSerializer, new SessionStoreDeserializer)

  implicit val sessionMaterializer: Materialized[Long, List[Session], ByteArraySessionStore] =
    Materialized.`with`[Long, List[Session], ByteArraySessionStore](Serdes.Long, sessionStoreSerdes)

  implicit val sessionStoreMaterializer: Materialized[Long, List[Session], ByteArrayKeyValueStore] =
    Materialized.`with`[Long, List[Session], ByteArrayKeyValueStore](Serdes.Long, sessionStoreSerdes)
}
