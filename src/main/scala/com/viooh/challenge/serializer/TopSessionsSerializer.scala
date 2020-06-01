package com.viooh.challenge.serializer

import java.util

import argonaut.Argonaut._
import com.viooh.challenge.aggregation.TopSessions
import com.viooh.challenge.model.PlayedTrack
import org.apache.kafka.common.serialization.Serializer


class TopSessionsSerializer extends Serializer[TopSessions] {
  override def configure(map: util.Map[String, _], b: Boolean): Unit = ()

  override def serialize(s: String, t: TopSessions): Array[Byte] = {
    if (t == null)
      null
    else {
      t.asJson.toString().getBytes
    }
  }

  override def close(): Unit = {
  }
}
