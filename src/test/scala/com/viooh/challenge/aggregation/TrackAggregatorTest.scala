package com.viooh.challenge.aggregation

import java.sql.Timestamp
import java.time.Instant

import com.viooh.challenge.TrackConsumer.TrackName
import com.viooh.challenge.aggregation.TrackAggregator._
import com.viooh.challenge.model.{PlayedTrack, Track}
import org.scalatest.{Matchers, WordSpec}

class TrackAggregatorTest extends WordSpec with Matchers {
  val (artistId1, artistName1) = ("A1", "AN1")

  val (trackId1, trackName1) = ("T1", "TN1")
  val (trackId2, trackName2) = ("T2", "TN2")
  val (trackId3, trackName3) = ("T3", "TN3")
  val (trackId4, trackName4) = ("T4", "TN4")

  val track1: Track = Track(trackId1, trackName1, 2)
  val track2: Track = Track(trackId2, trackName2, 3)
  val track3: Track = Track(trackId3, trackName3, 4)
  val track4: Track = Track(trackId4, trackName4, 5)

  val nonEmptyTrackStore: Map[TrackName, Track] = Map(trackName1 -> Track(trackId1, trackName1, 1))
  val emptyTrackStore: Map[TrackName, Track] = Map.empty[TrackName, Track]

  "trackAggregator" when {
    val anEmptyTrackStore: Map[TrackName, Track] = Map.empty[TrackName, Track]
    val playedTrack1 = PlayedTrack(trackId1, Timestamp.from(Instant.now()), artistId1, artistName1, trackId1, trackName1)

    "given a playedTrack and an empty trackStore" should {
      "return a trackStore containing a track matching the playedTrack with a playCount = 1" in {
        trackAggregator("", playedTrack1, anEmptyTrackStore) shouldEqual Map(trackName1 -> Track(trackId1, trackName1, 1))
      }
    }

    "given a playedTrack that is not part of the given trackStore containing another track" should {
      "return a trackStore containing the original track and a new track matching the playedTrack with a playCount = 1" in {
        trackAggregator("", playedTrack1, Map(track2.trackName -> track2)) should
          contain theSameElementsAs Map(trackName1 -> Track(trackId1, trackName1, 1), trackName2 -> track2)
      }
    }

    "given a playedTrack that is already part of the given trackStore containing only this track" should {
      "return a trackStore containing the original track with a playCount incremented by 1" in {
        val track: Track = track1
        val trackStore: Map[String, Track] = Map(track.trackName -> track)

        trackAggregator("", playedTrack1, trackStore) should
          contain theSameElementsAs Map(track.trackName -> Track(track.trackId, track.trackName, track.playCount + 1))
      }
    }

    "given a playedTrack that is already part of the given trackStore containing also another track" should {
      "return a trackStore containing the original track with a playCount incremented by 1 and the other track" in {
        val trackA: Track = track1
        val trackB: Track = track2
        val playedTrackA: PlayedTrack = playedTrack1
        val trackStore: Map[String, Track] = Map(trackA.trackName -> trackA, trackB.trackName -> trackB)

        trackAggregator("", playedTrackA, trackStore) should
          contain theSameElementsAs Map(
          trackA.trackName -> Track(trackA.trackId, trackA.trackName, trackA.playCount + 1),
          trackB.trackName -> Track(trackB.trackId, trackB.trackName, trackB.playCount)
        )
      }
    }
  }

  "trackMerger" when {
    "given 2 empty trackStores" should {
      "return an empty trackStore" in {
        trackMerger("", emptyTrackStore, emptyTrackStore) shouldEqual emptyTrackStore
      }
    }

    "given a non empty trackStore AND an empty trackStore" should {
      "return the provided non empty trackStore" in {
        trackMerger("", nonEmptyTrackStore, emptyTrackStore) shouldEqual nonEmptyTrackStore
      }
    }

    "given an empty trackStore and a non empty trackStore" should {
      "return the provided non empty trackStore" in {
        trackMerger("", emptyTrackStore, nonEmptyTrackStore) shouldEqual nonEmptyTrackStore
      }
    }

    "given 2 non empty trackStores with non overlapping trackNames" should {
      "return a trackStore having the same playCount for each original tracks" in {
        val trackStore1: Map[TrackName, Track] = Map(trackName1 -> Track(trackId1, trackName1, 1))
        val trackStore2: Map[TrackName, Track] = Map(trackName2 -> Track(trackId2, trackName2, 1))
        val expectedTrackStore: Map[TrackName, Track] = Map(trackName1 -> Track(trackId1, trackName1, 1), trackName2 -> Track(trackId2, trackName2, 1))
        trackMerger("", trackStore1, trackStore2) should contain theSameElementsAs expectedTrackStore
      }
    }

    "given non empty trackStores with overlapping trackNames AND non overlapping trackNames" should {
      "return a trackStore having the proper playCount for each track" in {
        val trackStore1: Map[TrackName, Track] =
          Map(
            trackName1 -> Track(trackId1, trackName1, 1),
            trackName2 -> Track(trackId2, trackName2, 1)
          )
        val trackStore2: Map[TrackName, Track] =
          Map(
            trackName2 -> Track(trackId2, trackName2, 5),
            trackName3 -> Track(trackId3, trackName3, 1)
          )
        val expectedTrackStore: Map[TrackName, Track] =
          Map(
            trackName1 -> Track(trackId1, trackName1, 1),
            trackName2 -> Track(trackId2, trackName2, 6),
            trackName3 -> Track(trackId3, trackName3, 1)
          )
        trackMerger("", trackStore1, trackStore2) should contain theSameElementsAs expectedTrackStore
      }
    }
  }

  "mostPlayedTrackAggregator" when {
    val anEmptyTrackStore: List[Track] = List.empty[Track]
    val track1: Track = Track(trackId1, trackName1, 2)
    val track2: Track = Track(trackId2, trackName2, 3)
    val track3: Track = Track(trackId3, trackName3, 4)
    val track4: Track = Track(trackId4, trackName4, 5)

    "given a track and an empty trackStore and a maxTracks = 10" should {
      "return a trackStore containing the given track" in {
        mostPlayedTrackAggregator(10)(track1.playCount, track1, anEmptyTrackStore) shouldEqual List(track1)
      }
    }

    "given a track and a trackStore containing 1 track and a maxTracks = 10" should {
      "return a trackStore containing the original trackStore plus the given track" in {
        mostPlayedTrackAggregator(10)(track1.playCount, track1, List(track2)) should contain theSameElementsAs List(track1, track2)
      }
    }

    "given a track and a trackStore containing 3 tracks and a maxTracks = 4" should {
      "return a trackStore containing the original trackStore plus the given track" in {
        val trackStore: List[Track] = List(track1, track2, track3)
        val expectedTrackStore: List[Track] = List(track1, track2, track3, track4)
        mostPlayedTrackAggregator(4)(track4.playCount, track4, trackStore) should contain theSameElementsAs expectedTrackStore
      }
    }

    "given a track whose playCount is higher than the track who has the lowest playCount from the trackStore tracks AND a trackStore containing 3 tracks AND a maxTracks = 3" should {
      "return a trackStore containing the 3 tracks having the highest playCount" in {
        val trackStore: List[Track] = List(track1, track2, track3)
        val expectedTrackStore: List[Track] = List(track2, track3, track4)
        mostPlayedTrackAggregator(3)(track4.playCount, track4, trackStore) should contain theSameElementsAs expectedTrackStore
      }
    }

    "given a track whose playCount is lower than the track who has the lowest playCount from the trackStore tracks AND a trackStore containing 3 tracks AND a maxTracks = 3" should {
      "return a trackStore containing the 3 tracks having the highest playCount" in {
        val trackStore: List[Track] = List(track2, track3, track4)
        val expectedTrackStore: List[Track] = List(track2, track3, track4)
        mostPlayedTrackAggregator(3)(track1.playCount, track1, trackStore) should contain theSameElementsAs expectedTrackStore
      }
    }
  }
}
