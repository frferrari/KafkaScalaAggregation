package com.viooh.challenge.model

import argonaut.Argonaut._
import argonaut.{CodecJson, _}
import com.viooh.challenge.TrackConsumer.PlayCount

case class Track(trackId: String,
                 trackName: String,
                 playCount: PlayCount)

object Track {
  implicit def TrackCodecJson: CodecJson[Track] =
    casecodec3(Track.apply, Track.unapply)("trackId", "trackName", "playCount")
}
