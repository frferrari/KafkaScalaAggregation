package com.viooh.challenge.model

import argonaut.Argonaut.casecodec3
import argonaut.CodecJson
import com.viooh.challenge.TrackConsumer.TrackId

case class Session(userId: String,
                   sessionDurationSeconds: Long,
                   tracks: Map[TrackId, Track])
object Session {
  implicit def SessionCodecJson: CodecJson[Session] =
    casecodec3(Session.apply, Session.unapply)("userId", "sessionDurationSeconds", "tracks")
}