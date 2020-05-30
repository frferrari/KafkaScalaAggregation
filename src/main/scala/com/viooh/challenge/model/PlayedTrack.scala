package com.viooh.challenge.model

import java.sql.Timestamp

import argonaut.Argonaut._
import argonaut.CodecJson
import com.viooh.challenge.{TRACK_RECORD_FIELD_SEPARATOR, TRACK_RECORD_TIMESTAMP_FORMAT}
import com.viooh.challenge.TrackConsumer.{TrackId, TrackName}
import com.viooh.challenge.utils.TrackTimestampExtractor

case class PlayedTrack(userId: String,
                       eventTime: Timestamp,
                       artistId: String,
                       artistName: String,
                       trackId: TrackId,
                       trackName: TrackName)

object PlayedTrack {
  /**
   * Converts a string containing a record from the lastfm dataset to a PlayedTrack object
   * The expected format for the record is the following list of fields separated by the sep character
   * eventTime as a timestamp (ex: 2020-05-30T14:20:00Z)
   * artistId as a string
   * artistName as a string
   * trackId as a string
   * trackName as a string
   *
   * @param userId          The userId who played the track
   * @param record          INput record containing all the required fields separated by the sep character
   * @param sep             The field separator for the input record
   * @param timestampFormat The format of the eventTime timestamp for decoding
   * @return An optional PlayedTrack object, it is not defined if some fields are missing or if the eventTime could not be decoded
   */
  def apply(userId: String, record: String, sep: Char = TRACK_RECORD_FIELD_SEPARATOR, timestampFormat: String = TRACK_RECORD_TIMESTAMP_FORMAT): Option[PlayedTrack] = {
    record.split(sep) match {
      case Array(eventTime, artistId, artistName, trackId, trackName) =>
        TrackTimestampExtractor
          .toTimestamp(eventTime, timestampFormat)
          .map(new PlayedTrack(userId, _, artistId, artistName, trackId, trackName))
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
