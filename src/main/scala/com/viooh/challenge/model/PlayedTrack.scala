package com.viooh.challenge.model

import java.sql.Timestamp

import com.viooh.challenge.TrackConsumer.{TrackId, TrackName}

case class PlayedTrack(userId: String,
                       timestamp: Timestamp,
                       artistId: String,
                       artistName: String,
                       trackId: TrackId,
                       trackName: TrackName)
