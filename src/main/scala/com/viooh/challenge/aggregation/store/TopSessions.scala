package com.viooh.challenge.aggregation.store

import argonaut.Argonaut._
import argonaut.Json.jArray
import argonaut.{DecodeJson, EncodeJson}
import com.viooh.challenge.TrackConsumer.SessionId
import com.viooh.challenge.model.Session
import com.viooh.challenge.model.Session._

import scala.collection.mutable

class TopSessions(maxSessions: Int) extends Iterable[Session] {
  var currentSessions: mutable.Map[SessionId, Session] = mutable.Map.empty[SessionId, Session]

  object DescendingSessionTrackCount extends Ordering[Session] {
    def compare(left: Session, right: Session): Int = {
      val cmp = left.tracks.size.compareTo(right.tracks.size)
      if (cmp != 0)
        cmp
      else
        left.sessionId.compareTo(right.sessionId)
    }
  }

  var topSessions: mutable.TreeSet[Session] = mutable.TreeSet.empty[Session](DescendingSessionTrackCount)

  def add(session: Session): Unit = {
    if (currentSessions.contains(session.sessionId)) {
      topSessions.remove(currentSessions.remove(session.sessionId).get) // TODO FIX .get
    }
    topSessions.add(session)
    currentSessions.put(session.sessionId, session)
    if (topSessions.size > maxSessions) {
      val lastSession: Session = topSessions.last
      currentSessions.remove(lastSession.sessionId)
      topSessions.remove(lastSession)
    }
    ()
  }

  def remove(session: Session): Unit = {
    topSessions.remove(session)
    currentSessions.remove(session.sessionId)
    ()
  }

  override def iterator: Iterator[Session] = topSessions.iterator
}

object TopSessions {
  implicit def TopSessionsEncodeJson: EncodeJson[TopSessions] =
    EncodeJson((topSessions: TopSessions) => jArray(topSessions.toList.map(_.asJson)))

  implicit def TopSessionsDecodeJson: DecodeJson[TopSessions] =
    DecodeJson(c => {
      val topSessions = new TopSessions(50) // TODO FIX as it should be a parameter
      for {
        session <- (c \\).as[Session]
        _ = topSessions.add(session)
      } yield topSessions
    }
    )
}
