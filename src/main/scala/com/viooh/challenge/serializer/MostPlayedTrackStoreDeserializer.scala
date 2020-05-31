package com.viooh.challenge.serializer

import java.util

import argonaut.Argonaut._
import com.viooh.challenge.model.Track
import org.apache.kafka.common.serialization.Deserializer

class MostPlayedTrackStoreDeserializer extends Deserializer[List[Track]] {
  override def configure(map: util.Map[String, _], b: Boolean): Unit = {
  }

  override def deserialize(s: String, bytes: Array[Byte]): List[Track] = {
    if (bytes != null ) {
      new String(bytes).decodeOption[List[Track]].getOrElse(List.empty[Track])
    } else List.empty[Track]
  }

  override def close(): Unit = {
  }
}
