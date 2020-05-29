package com.viooh.challenge.serializer

import java.util

import argonaut.Argonaut._
import com.viooh.challenge.model.Session
import org.apache.kafka.common.serialization.Deserializer

class SessionDeserializer extends Deserializer[Session] {
  override def configure(map: util.Map[String, _], b: Boolean): Unit = {
  }

  override def deserialize(s: String, bytes: Array[Byte]): Session = {
    new String(bytes).decodeOption[Session].get // TODO FIX
  }

  override def close(): Unit = {
  }
}
