package com.viooh.challenge.aggregation

import com.viooh.challenge.TrackConsumer.TrackName
import com.viooh.challenge.aggregation.SessionAggregator._
import com.viooh.challenge.model.{Session, Track}
import org.scalatest.{Matchers, WordSpec}

class SessionAggregatorTest extends WordSpec with Matchers {
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

  "sessionAggregator" when {
    val anEmptySessionStore: List[Session] = List.empty[Session]

    val track1: Track = Track(trackId1, trackName1, 2)
    val track2: Track = Track(trackId2, trackName2, 3)
    val track3: Track = Track(trackId3, trackName3, 4)
    val track4: Track = Track(trackId4, trackName4, 5)

    val session1: Session = Session("session1", "user1", 60, List((track1, 1)))
    val session2: Session = Session("session2", "user2", 120, List((track1, 1), (track2, 1)))
    val session3: Session = Session("session3", "user3", 180, List((track1, 1), (track2, 1), (track3, 1)))
    val session4: Session = Session("session4", "user4", 240, List((track1, 1), (track2, 1), (track3, 1), (track4, 1)))

    "given a session and an empty sessionStore and a maxSessions = 10" should {
      "return a sessionStore containing the given session" in {
        sessionAggregator(10)(session1.tracks.size, session1, anEmptySessionStore) should contain theSameElementsAs List(session1)
      }
    }

    "given a session and a sessionStore containing 1 session and a maxSessions = 10" should {
      "return a trackStore containing the original sessionStore plus the given session" in {
        sessionAggregator(10)(session2.tracks.size, session2, List(session1)) should contain theSameElementsAs List(session1, session2)
      }
    }

    "given a session whose count of tracks is higher than the session who has the lowest count of tracks from the sessionStore AND a sessionStore containing 3 sessions AND a maxSessions = 3" should {
      "return a sessionStore containing the 3 sessions having the highest duration" in {
        val sessionStore: List[Session] = List(session1, session2, session3)
        val expectedSessionStore: List[Session] = List(session2, session3, session4)
        sessionAggregator(3)(session4.tracks.size, session4, sessionStore) should
          contain theSameElementsAs expectedSessionStore
      }
    }

    "given a session whose count of tracks is lower than the session who has the lowest count of tracks from the sessionStore AND a sessionStore containing 3 sessions AND a maxSessions = 3" should {
      "return a sessionStore containing the 3 sessions having the highest duration" in {
        val sessionStore: List[Session] = List(session2, session3, session4)
        val expectedSessionStore: List[Session] = List(session2, session3, session4)
        sessionAggregator(3)(session1.tracks.size, session1, sessionStore) should
          contain theSameElementsAs expectedSessionStore
      }
    }
  }
}
