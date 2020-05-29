package com.viooh.challenge.serializer

import java.util

import argonaut.Argonaut._
import com.viooh.challenge.model.Session
import org.apache.kafka.common.serialization.Deserializer

class SessionStoreDeserializer extends Deserializer[List[Session]] {
  override def configure(map: util.Map[String, _], b: Boolean): Unit = {
  }

  override def deserialize(s: String, bytes: Array[Byte]): List[Session] = {
    if (bytes != null ) {
      new String(bytes).decodeOption[List[Session]].getOrElse(List.empty[Session])
    } else List.empty[Session]
  }

  override def close(): Unit = {
  }
}
