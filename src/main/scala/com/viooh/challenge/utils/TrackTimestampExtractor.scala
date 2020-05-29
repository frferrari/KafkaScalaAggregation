package com.viooh.challenge.utils

import java.sql.Timestamp

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.streams.processor.TimestampExtractor

import scala.util.{Failure, Success, Try}

class TrackTimestampExtractor extends TimestampExtractor {
  override def extract(record: ConsumerRecord[AnyRef, AnyRef], previousTimestamp: Long): Long = {
    Try(Timestamp.valueOf(record.value().toString.split("\t")(0))) match {
      case Success(ts) => ts.toInstant.getEpochSecond * 1000 // milliseconds
      case Failure(_) => 0L
    }
  }
}
