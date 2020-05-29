package com.viooh.challenge.serializer

import java.util

import argonaut.Argonaut._
import com.viooh.challenge.TrackConsumer.TrackId
import com.viooh.challenge.model.{Session, Track}
import org.apache.kafka.common.serialization.Serializer

class SessionSerializer extends Serializer[Session] {
  override def configure(map: util.Map[String, _], b: Boolean): Unit = ()

  override def serialize(s: String, t: Session): Array[Byte] = {
    if (t == null)
      null
    else {
      val json = t.asJson.toString()
      println(s"serializer $t ($json)")
      json.getBytes
    }
  }

  override def close(): Unit = {
  }
}