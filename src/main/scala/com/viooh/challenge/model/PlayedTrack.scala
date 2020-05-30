package com.viooh.challenge.model

import java.sql.Timestamp
import java.text.SimpleDateFormat

import argonaut.Argonaut._
import argonaut.CodecJson
import com.viooh.challenge.TrackConsumer.{TrackId, TrackName}

import scala.util.Try

case class PlayedTrack(userId: String,
                       eventTime: Timestamp,
                       artistId: String,
                       artistName: String,
                       trackId: TrackId,
                       trackName: TrackName)

object PlayedTrack {
  def apply(userId: String, record: String, sep: Char = '\t', timestampFormat: String = "yyyy-MM-dd'T'hh:mm:ssX"): Option[PlayedTrack] = {
    record.split(sep) match {
      case Array(eventTime, artistId, artistName, trackId, trackName) =>
        Try {
          val dateFormat: SimpleDateFormat = new SimpleDateFormat(timestampFormat)
          val timestamp: Timestamp = Timestamp.from(dateFormat.parse(eventTime).toInstant)

          new PlayedTrack(userId, timestamp, artistId, artistName, trackId, trackName)
        }.toOption
      case _ =>
        None
    }
  }

  implicit def EventTimeCodecJson: CodecJson[Timestamp] =
    CodecJson(
      (t: Timestamp) =>
        ("eventTime" := t.getTime) ->:
          jEmptyObject,
      l => for {
        timestamp <- (l --\ "eventTime").as[Long]
      } yield new Timestamp(timestamp)
    )

  implicit def TrackCodecJson: CodecJson[PlayedTrack] =
    casecodec6(PlayedTrack.apply, PlayedTrack.unapply)("userId", "eventTime", "artistId", "artistName", "trackId", "trackName")
}
