package com.viooh.challenge.aggregation.store

import com.viooh.challenge.TrackConsumer.SessionId
import com.viooh.challenge.model.{Session, Track}
import org.scalatest.{Matchers, WordSpec}

import scala.collection.mutable

class TopSessionsTest extends WordSpec with Matchers {
  val sessionId1 = "sessionId1"
  val sessionId2 = "sessionId2"
  val sessionId3 = "sessionId3"
  val sessionId4 = "sessionId4"
  val user1 = "user1"
  val user2 = "user2"
  val user3 = "user3"
  val user4 = "user4"

  val (trackIdA, trackNameA) = ("trackIdA", "trackNameA")
  val (trackIdB, trackNameB) = ("trackIdB", "trackNameB")
  val (trackIdC, trackNameC) = ("trackIdC", "trackNameC")
  val (trackIdD, trackNameD) = ("trackIdD", "trackNameD")
  val (trackIdE, trackNameE) = ("trackIdE", "trackNameE")

  val trackA: Track = Track(trackIdA, trackNameA, 1)
  val trackB: Track = Track(trackIdB, trackNameB, 4)
  val trackC: Track = Track(trackIdC, trackNameC, 8)
  val trackD: Track = Track(trackIdD, trackNameD, 12)
  val trackE: Track = Track(trackIdE, trackNameE, 18)

  "On an empty TopSessions defined with a maxSessions of 2" when {
    val topSessions = new TopSessions(2)
    import topSessions.DescendingSessionTrackCount

    val sessionA: Session = Session(sessionId1, user1, 1800, List((trackD, 1), (trackC, 2), (trackB, 3), (trackA, 1)))
    val sessionB: Session = Session(sessionId2, user2, 720, List((trackD, 1), (trackC, 2), (trackB, 3)))
    val sessionC: Session = Session(sessionId3, user3, 180, List((trackD, 1), (trackC, 2)))
    val sessionD: Session = Session(sessionId4, user4, 3600, List((trackE, 1), (trackD, 1), (trackC, 2), (trackB, 3), (trackA, 1)))

    "calling add with a session having a track count of 4" should {
      "add the session" in {
        topSessions.add(sessionA)
        topSessions.topSessions should contain theSameElementsAs mutable.TreeSet(sessionA)
        topSessions.currentSessions should contain theSameElementsAs mutable.Map(sessionA.sessionId -> sessionA)
      }
    }

    "calling the add method with a session having track count of 2" should {
      "add the session" in {
        topSessions.add(sessionC)
        topSessions.topSessions should contain theSameElementsAs mutable.TreeSet(sessionA, sessionC)
        topSessions.currentSessions should contain theSameElementsAs mutable.Map(sessionA.sessionId -> sessionA, sessionC.sessionId -> sessionC)
      }
    }

    "calling the add method with a session having track count of 3" should {
      "add the session and remove the session previously added with a track cound of 2" in {
        topSessions.add(sessionB)
        topSessions.topSessions should contain theSameElementsAs mutable.TreeSet(sessionA, sessionB)
        topSessions.currentSessions should contain theSameElementsAs mutable.Map(sessionA.sessionId -> sessionA, sessionB.sessionId -> sessionB)
      }
    }

    "calling the add method with a session having track count of 5" should {
      "add the session and remove the session having a track cound of 3" in {
        topSessions.add(sessionD)
        topSessions.topSessions should contain theSameElementsAs mutable.TreeSet(sessionA, sessionD)
        topSessions.currentSessions should contain theSameElementsAs mutable.Map(sessionA.sessionId -> sessionA, sessionD.sessionId -> sessionD)
      }
    }

    "calling the remove method with a session that doesn't exist in the topSessions" should {
      "not change the existing topSessions" in {
        topSessions.remove(sessionC)
        topSessions.topSessions should contain theSameElementsAs mutable.TreeSet(sessionA, sessionD)
        topSessions.currentSessions should contain theSameElementsAs mutable.Map(sessionA.sessionId -> sessionA, sessionD.sessionId -> sessionD)
      }
    }

    "calling the remove method with a session that exists in the topSessions" should {
      "remove the session from the topSessions" in {
        topSessions.remove(sessionA)
        topSessions.topSessions should contain theSameElementsAs mutable.TreeSet(sessionD)
        topSessions.currentSessions should contain theSameElementsAs mutable.Map(sessionD.sessionId -> sessionD)
      }
    }

    "calling the remove method with the latest session that exists in the topSessions" should {
      "remove this session and leave an empty topSessions" in {
        topSessions.remove(sessionD)
        topSessions.topSessions should contain theSameElementsAs mutable.TreeSet.empty[Session]
        topSessions.currentSessions should contain theSameElementsAs mutable.Map.empty[SessionId, Session]
      }
    }
  }
}
