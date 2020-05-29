package com.viooh.challenge.aggregation

import com.viooh.challenge.TrackConsumer.{MAX_TRACKS, TrackId, TrackName}
import com.viooh.challenge.model.Track

object TrackAggregator {
  /**
   * This aggregator receives each record coming from the last fm dataset, this dataset is partitioned by userId
   * Each record corresponds to a track (fields are separated by a tab character), this aggregator creates a collection
   * of tracks containing all the tracks received as an input in the recordValue field.
   *
   * @param userId      The userId the tracks belong to
   * @param recordValue The track informations as a string whose fields are separated by a tab character
   * @param trackStore  The collection of tracks that will be filled and returned by the aggregator
   * @return The collection of tracks
   */
  def trackAggregator(userId: String, recordValue: String, trackStore: Map[TrackId, Track]): Map[TrackId, Track] = {
    val trackInfo: Array[String] = recordValue.split("\t")

    // Increment the count of track per trackId
    if (trackInfo.length == 5) {
      val trackId: TrackId = trackInfo(3)
      val trackName: TrackName = trackInfo(4)

      trackStore.get(trackId) match {
        case Some(track) => {
          trackStore + (trackId -> track.copy(playCount = track.playCount + 1))
        }
        case None => trackStore + (trackId -> Track(trackId, trackName, 1))
      }
    } else trackStore
  }

  /**
   * Allows to merge 2 different trackStore by summing the playCount values for identical tracks (trackId)
   *
   * @param userId      The userId the tracks belong to
   * @param trackStore1 The first track store
   * @param trackStore2 The second track store
   * @return A new trackSTore containing the added values of playCount
   */
  def trackMerger(userId: String, trackStore1: Map[TrackId, Track], trackStore2: Map[TrackId, Track]): Map[TrackId, Track] = {
    trackStore2.foldLeft(trackStore1) { case (acc, (trackId, track2)) =>
      acc.get(trackId) match {
        case Some(track1) => acc + (trackId -> track1.copy(playCount = track1.playCount + track2.playCount))
        case None => acc + (trackId -> track2)
      }
    }
  }

  /**
   * This aggregator processes each session whose session duration is the same, and produces a collection of this sessions
   * by keeping only the top 50 sessions per session duration.
   *
   * @param playCount
   * @param track
   * @param trackStore
   * @return
   */
  def mostPlayedTrackAggregator(playCount: Int, track: Track, trackStore: List[Track]): List[Track] = {
    if (trackStore.isEmpty) {
      // If the sessionSore is empty then we can add the given session
      trackStore :+ track
    } else {
      if (trackStore.length < MAX_TRACKS)
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
