package com.viooh.challenge.serializer

import java.util

import argonaut.Argonaut._
import com.viooh.challenge.TrackConsumer.TrackId
import com.viooh.challenge.model.Track
import org.apache.kafka.common.serialization.Deserializer

class TrackStoreDeserializer extends Deserializer[Map[TrackId, Track]] {
  override def configure(map: util.Map[String, _], b: Boolean): Unit = {
  }

  override def deserialize(s: String, bytes: Array[Byte]): Map[TrackId, Track] = {
    if (bytes != null ) {
      new String(bytes).decodeOption[Map[TrackId, Track]].getOrElse(Map.empty[TrackId, Track])
    } else Map.empty[TrackId, Track]
  }

  override def close(): Unit = {
  }
}
