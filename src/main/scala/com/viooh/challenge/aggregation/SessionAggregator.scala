package com.viooh.challenge.aggregation

import com.viooh.challenge.model.Session

object SessionAggregator {
  /**
   * This aggregator processes each session whose count of tracks is the same, and produces a collection of this sessions
   * by keeping only the top 50 sessions per count of tracks
   *
   * @param trackCount The length of the session in seconds
   * @param session                The session containing different tracks played in this session
   * @param sessionStore           A collection to store our sessions
   * @return A collection containing the top maxSessions sessions per sessionDurationInSeconds
   */
  def sessionAggregator(maxSessions: Int)(trackCount: Int,
                                          session: Session,
                                          sessionStore: List[Session]): List[Session] = {
    if (sessionStore.isEmpty) {
      // If the sessionSore is empty then we can add the given session
      sessionStore :+ session
    } else {
      if (sessionStore.length < maxSessions)
      // we can add sessions in the store without contrainst until the session store contains the max authorized sessions
      sessionStore :+ session
        else {
        val minByTrackCount: Session = sessionStore.minBy(_.tracks.size)

        if (session.tracks.size > minByTrackCount.tracks.size) {
          (sessionStore :+ session)
            .sortWith(_.tracks.size > _.tracks.size)
            .take(maxSessions)
        } else {
          sessionStore
        }
      }
    }
  }
}
