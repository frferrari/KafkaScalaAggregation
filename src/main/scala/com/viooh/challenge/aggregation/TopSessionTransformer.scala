package com.viooh.challenge.aggregation

import com.viooh.challenge.TrackConsumer.{SessionId, UserId}
import com.viooh.challenge.model.Session
import org.apache.kafka.streams.kstream.ValueTransformer
import org.apache.kafka.streams.processor.ProcessorContext
import org.apache.kafka.streams.state.KeyValueStore

import scala.collection.mutable

class TopSessionTransformer(sessionStoreName: String, maxSessions: Int) extends ValueTransformer[Session, Session] {
  // Transformer[SessionId, Session, KeyValue[SessionId, Session]] {

  var sessionStore: KeyValueStore[SessionId, Session] = _
  var processorContext: ProcessorContext = _

  val topSessionStore: mutable.Map[SessionId, Session] = mutable.Map.empty[SessionId, Session]

  override def init(context: ProcessorContext): Unit = {
    processorContext = context
    sessionStore = context.getStateStore(sessionStoreName).asInstanceOf[KeyValueStore[UserId, Session]]
  }

  // override def transform(readOnlyKey: SessionId, value: Session): KeyValue[SessionId, Session] = {
  override def transform(session: Session): Session = {
    println(s"TopSessionTransformer::transform in ${topSessionStore.size} $topSessionStore")

    if (topSessionStore.size < maxSessions) {
      topSessionStore(session.sessionId) = session
      println(s"TopSessionTransformer::transform out (low count) ${topSessionStore.size} $topSessionStore")
      session
    } else {
      val shortestSession: Session = topSessionStore.minBy(_._2.sessionDurationSeconds)._2

      if (session.sessionDurationSeconds < shortestSession.sessionDurationSeconds) {
        println(s"TopSessionTransformer::transform out (reject) $topSessionStore")
        null
      } else {
        topSessionStore.remove(shortestSession.sessionId)
        topSessionStore(session.sessionId) = session
        println(s"TopSessionTransformer::transform out (add) ${topSessionStore.size} $topSessionStore")
        session
      }
    }

    /*
    val storeIterator: KeyValueIterator[SessionId, Session] = sessionStore.all()
    val sessions: mutable.ListBuffer[KeyValue[SessionId, Session]] = mutable.ListBuffer.empty[KeyValue[SessionId, Session]]

    while (storeIterator.hasNext) {
      sessions += storeIterator.next()
    }
    storeIterator.close()

    val session: KeyValue[SessionId, Session] = new KeyValue(readOnlyKey, value)
    sessions += session

    val newSessions: ListBuffer[KeyValue[SessionId, Session]] =
      sessions
        .sortWith(_.value.sessionDurationSeconds > _.value.sessionDurationSeconds)
        .take(maxSessions)

    if (newSessions.contains(session)) {
      sessionStore.put(readOnlyKey, value)
      session
    } else {
      null
    }
     */
  }

  override def close(): Unit = ()
}
