package com.viooh.challenge.serializer

import java.util

import argonaut.Argonaut._
import com.viooh.challenge.model.Session
import org.apache.kafka.common.serialization.Serializer

class SessionStoreSerializer extends Serializer[List[Session]] {
  override def configure(map: util.Map[String, _], b: Boolean): Unit = ()

  override def serialize(s: String, sessions: List[Session]): Array[Byte] = {
    if (sessions == null)
      null
    else {
      sessions.asJson.toString().getBytes
    }
  }

  override def close(): Unit = {
  }
}