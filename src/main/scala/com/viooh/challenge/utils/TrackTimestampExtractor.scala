package com.viooh.challenge.utils

import java.sql.Timestamp
import java.text.SimpleDateFormat

import com.viooh.challenge.TRACK_RECORD_TIMESTAMP_FORMAT
import com.viooh.challenge.TRACK_RECORD_FIELD_SEPARATOR
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.streams.processor.TimestampExtractor

import scala.util.Try

class TrackTimestampExtractor extends TimestampExtractor {
  override def extract(record: ConsumerRecord[AnyRef, AnyRef], previousTimestamp: Long): Long = {
    TrackTimestampExtractor.toTimestamp(record.value().toString.split(TRACK_RECORD_FIELD_SEPARATOR)(0)) match {
      case Some(ts) =>
        ts.toInstant.getEpochSecond * 1000 // milliseconds
      case None =>
        throw new RuntimeException("TrackTimestampExtractor could not extract the timestamp")
    }
  }
}

object TrackTimestampExtractor {
  def toTimestamp(ts: String, tsFormat: String = TRACK_RECORD_TIMESTAMP_FORMAT): Option[Timestamp] =
    Try {
      val dateFormat: SimpleDateFormat = new SimpleDateFormat(tsFormat)
      Timestamp.from(dateFormat.parse(ts).toInstant)
    }.toOption
}
