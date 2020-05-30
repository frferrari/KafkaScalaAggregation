package com.viooh.challenge.aggregation

import com.viooh.challenge.TrackConsumer.SessionDuration
import com.viooh.challenge.model.Session

object SessionAggregator {
  /**
   * This aggregator processes each session whose session duration is the same, and produces a collection of this sessions
   * by keeping only the top 50 sessions per session duration.
   *
   * @param sessionDurationSeconds
   * @param session
   * @param sessionStore
   * @return
   */
  def sessionAggregator(maxSessions: Int)(sessionDurationSeconds: SessionDuration, session: Session, sessionStore: List[Session]): List[Session] = {
    if (sessionStore.isEmpty) {
      // If the sessionSore is empty then we can add the given session
      sessionStore :+ session
    } else {
      if (sessionStore.length < maxSessions)
      // we can add sessions in the store without contrainst until the session store contains the max authorized sessions
      sessionStore :+ session
        else {
        val minSessionDuration: Session = sessionStore.minBy(_.sessionDurationSeconds)

        if (sessionDurationSeconds > minSessionDuration.sessionDurationSeconds) {
          (sessionStore :+ session).sortWith(_.sessionDurationSeconds > _.sessionDurationSeconds).take(maxSessions)
        } else {
          sessionStore
        }
      }
    }
  }
}
