package com.viooh.challenge.aggregation

import com.viooh.challenge.TrackConsumer.{MAX_TRACKS, TrackName}
import com.viooh.challenge.model.{PlayedTrack, Track}

object TrackAggregator {
  /**
   * This aggregator receives each track coming from the last fm dataset, this dataset is partitioned by userId.
   * It creates a collection of tracks containing all the tracks received.
   *
   * @param userId      The userId the tracks belong to
   * @param playedTrack The track informations as a string whose fields are separated by a tab character
   * @param trackStore  The collection of tracks that will be filled and returned by the aggregator
   * @return The collection of tracks
   */
  def trackAggregator(userId: String, playedTrack: PlayedTrack, trackStore: Map[TrackName, Track]): Map[TrackName, Track] = {
    // Increment the count of track per trackName
    trackStore.get(playedTrack.trackName) match {
      case Some(track) => trackStore + (playedTrack.trackName -> track.copy(playCount = track.playCount + 1))
      case None => trackStore + (playedTrack.trackName -> Track(playedTrack.trackId, playedTrack.trackName, 1))
    }
  }

  /**
   * Allows to merge 2 different trackStore by summing the playCount values for identical tracks (trackId)
   *
   * @param userId      The userId the tracks belong to
   * @param trackStore1 The first track store
   * @param trackStore2 The second track store
   * @return A new trackSTore containing the added values of playCount
   */
  def trackMerger(userId: String, trackStore1: Map[TrackName, Track], trackStore2: Map[TrackName, Track]): Map[TrackName, Track] = {
    trackStore2.foldLeft(trackStore1) { case (acc, (trackName2, track2)) =>
      acc.get(trackName2) match {
        case Some(track1) => acc + (trackName2 -> track1.copy(playCount = track1.playCount + track2.playCount))
        case None => acc + (trackName2 -> track2)
      }
    }
  }

  /**
   * This aggregator processes each track whose play count is the same, and produces a collection of this tracks
   * by keeping only the top maxTracks tracks per play count
   *
   * @param playCount  How many times a track has been played
   * @param track      The track details
   * @param trackStore A store of 1 to many tracks, from which we want to extract the top maxTracks tracks
   * @return A list of maxTracks tracks
   */
  def mostPlayedTrackAggregator(maxTracks: Int)(playCount: Int, track: Track, trackStore: List[Track]): List[Track] = {
    if (trackStore.isEmpty) {
      // If the sessionSore is empty then we can add the given session
      trackStore :+ track
    } else {
      if (trackStore.length < maxTracks)
      // we can add tracks in the store without contrainst until the track store contains the max authorized tracks
      trackStore :+ track
        else {
        val minPlayCount: Track = trackStore.minBy(_.playCount)

        if (playCount > minPlayCount.playCount) {
          (trackStore :+ track).sortWith(_.playCount < _.playCount).take(MAX_TRACKS)
        } else {
          trackStore
        }
      }
    }
  }
}
