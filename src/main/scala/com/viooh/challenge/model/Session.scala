package com.viooh.challenge.model

import argonaut.Argonaut.casecodec4
import argonaut.CodecJson
import com.viooh.challenge.TrackConsumer.{SessionDuration, TrackName, TrackRank}

case class Session(sessionId: String,
                   userId: String,
                   sessionDurationSeconds: SessionDuration,
                   tracks: List[(Track, TrackRank)])

object Session {
  implicit def SessionCodecJson: CodecJson[Session] =
    casecodec4(Session.apply, Session.unapply)("sessionId", "userId", "sessionDurationSeconds", "tracks")
}