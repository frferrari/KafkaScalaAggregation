package com.viooh.challenge.serializer

import java.util

import argonaut.Argonaut._
import com.viooh.challenge.aggregation.store.TopSessions
import org.apache.kafka.common.serialization.Deserializer

class TopSessionsDeserializer extends Deserializer[TopSessions] {
  override def configure(map: util.Map[String, _], b: Boolean): Unit = {
  }

  override def deserialize(s: String, bytes: Array[Byte]): TopSessions = {
    new String(bytes).decodeOption[TopSessions].get // TODO FIX
  }

  override def close(): Unit = {
  }
}
