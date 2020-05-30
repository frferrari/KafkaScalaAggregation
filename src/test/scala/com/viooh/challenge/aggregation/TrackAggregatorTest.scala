package com.viooh.challenge.aggregation

import com.viooh.challenge.TrackConsumer.TrackName
import com.viooh.challenge.model.Track
import org.scalatest.{Matchers, WordSpec}
import com.viooh.challenge.aggregation.TrackAggregator._

class TrackAggregatorTest extends WordSpec with Matchers {
  val (trackId1, trackName1) = ("T1", "TN1")
  val (trackId2, trackName2) = ("T2", "TN2")
  val (trackId3, trackName3) = ("T3", "TN3")
  val (trackId4, trackName4) = ("T4", "TN4")

  val nonEmptyTrackStore: Map[TrackName, Track] = Map(trackName1 -> Track(trackId1, trackName1, 1))
  val emptyTrackStore: Map[TrackName, Track] = Map.empty[TrackName, Track]

  "trackMerger" when {
    "given 2 empty trackStores" should {
      "return an empty trackStore" in {
        trackMerger("", emptyTrackStore, emptyTrackStore) shouldBe(emptyTrackStore)
      }
    }

    "given a non empty trackStore AND an empty trackStore" should {
      "return the provided non empty trackStore" in {
        trackMerger("", nonEmptyTrackStore, emptyTrackStore) shouldBe(nonEmptyTrackStore)
      }
    }

    "given an empty trackStore and a non empty trackStore" should {
      "return the provided non empty trackStore" in {
        trackMerger("", emptyTrackStore, nonEmptyTrackStore) shouldBe(nonEmptyTrackStore)
      }
    }

    "given 2 non empty trackStores with non overlapping trackNames" should {
      "return a trackStore having the same playCount for each original tracks" in {
        val trackStore1: Map[TrackName, Track] = Map(trackName1 -> Track(trackId1, trackName1, 1))
        val trackStore2: Map[TrackName, Track] = Map(trackName2 -> Track(trackId2, trackName2, 1))
        val expectedTrackStore: Map[TrackName, Track] = Map(trackName1 -> Track(trackId1, trackName1, 1), trackName2 -> Track(trackId2, trackName2, 1))
        trackMerger("", trackStore1, trackStore2) shouldBe(expectedTrackStore)
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
        trackMerger("", trackStore1, trackStore2) shouldBe(expectedTrackStore)
      }
    }
  }
}
