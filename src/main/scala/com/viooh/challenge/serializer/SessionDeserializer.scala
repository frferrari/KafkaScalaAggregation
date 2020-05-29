package com.viooh.challenge.serializer

import java.util

import argonaut.Argonaut._
import com.viooh.challenge.TrackConsumer.TrackId
import com.viooh.challenge.model.{Session, Track}
import org.apache.kafka.common.serialization.Deserializer

class SessionDeserializer extends Deserializer[Session] {
  override def configure(map: util.Map[String, _], b: Boolean): Unit = {
  }

  override def deserialize(s: String, bytes: Array[Byte]): Session = {
    val s = new String(bytes)
    println(s"===========> $s")
    val t = new String(bytes).decodeOption[Session].get // TODO FIX
    println(s"deserialized $t")
    t
  }

  override def close(): Unit = {
  }
}
