package com.viooh.challenge.aggregation

import com.viooh.challenge.TrackConsumer.TrackName
import com.viooh.challenge.aggregation.SessionAggregator._
import com.viooh.challenge.aggregation.TrackAggregator.mostPlayedTrackAggregator
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

    val session1: Session = Session("user1", 60, Map(track1.trackName -> track1))
    val session2: Session = Session("user2", 120, Map(track2.trackName -> track2))
    val session3: Session = Session("user3", 180, Map(track3.trackName -> track3))
    val session4: Session = Session("user4", 240, Map(track4.trackName -> track4))

    "given a session and an empty sessionStore and a maxSessions = 10" should {
      "return a sessionStore containing the given session" in {
        sessionAggregator(10)(session1.sessionDurationSeconds, session1, anEmptySessionStore) should contain theSameElementsAs List(session1)
      }
    }

    "given a session and a sessionStore containing 1 session and a maxSessions = 10" should {
      "return a trackStore containing the original sessionStore plus the given session" in {
        sessionAggregator(10)(session2.sessionDurationSeconds, session2, List(session1)) should contain theSameElementsAs List(session1, session2)
      }
    }

    "given a session whose duration is higher than the session who has the lowest duration from the sessionStore sessions AND a sessionStore containing 3 sessions AND a maxSessions = 3" should {
      "return a sessionStore containing the 3 sessions having the highest duration" in {
        val sessionStore: List[Session] = List(session1, session2, session3)
        val expectedSessionStore: List[Session] = List(session2, session3, session4)
        sessionAggregator(3)(session4.sessionDurationSeconds, session4, sessionStore) should
          contain theSameElementsAs expectedSessionStore
      }
    }

    "given a session whose duration is lower than the session who has the lowest duration from the sessionStore sessions AND a sessionStore containing 3 sessions AND a maxSessions = 3" should {
      "return a sessionStore containing the 3 sessions having the highest duration" in {
        val sessionStore: List[Session] = List(session2, session3, session4)
        val expectedSessionStore: List[Session] = List(session2, session3, session4)
        sessionAggregator(3)(session1.sessionDurationSeconds, session1, sessionStore) should
          contain theSameElementsAs expectedSessionStore
      }
    }
  }
}
