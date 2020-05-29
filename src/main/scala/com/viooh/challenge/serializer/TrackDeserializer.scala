package com.viooh.challenge.serializer

import java.util

import argonaut.Argonaut._
import com.viooh.challenge.model.Track
import org.apache.kafka.common.serialization.Deserializer

class TrackDeserializer extends Deserializer[Track] {
  override def configure(map: util.Map[String, _], b: Boolean): Unit = {
  }

  override def deserialize(s: String, bytes: Array[Byte]): Track = {
    new String(bytes).decodeOption[Track].get // TODO FIX
  }

  override def close(): Unit = {
  }
}
