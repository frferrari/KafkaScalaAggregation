package com.viooh.challenge.serializer

import java.util

import argonaut.Argonaut._
import com.viooh.challenge.model.Track
import org.apache.kafka.common.serialization.Serializer

class TrackSerializer extends Serializer[Track] {
  override def configure(map: util.Map[String, _], b: Boolean): Unit = ()

  override def serialize(s: String, t: Track): Array[Byte] = {
    if (t == null)
      null
    else {
      t.asJson.toString().getBytes
    }
  }

  override def close(): Unit = {
  }
}