package com.viooh.challenge.serializer

import java.util

import argonaut.Argonaut._
import com.viooh.challenge.model.{PlayedTrack, Track}
import org.apache.kafka.common.serialization.Deserializer

class PlayedTrackDeserializer extends Deserializer[PlayedTrack] {
  override def configure(map: util.Map[String, _], b: Boolean): Unit = {
  }

  override def deserialize(s: String, bytes: Array[Byte]): PlayedTrack = {
    new String(bytes).decodeOption[PlayedTrack].get // TODO FIX
  }

  override def close(): Unit = {
  }
}
