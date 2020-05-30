package com.viooh.challenge.utils

import java.sql.Timestamp
import java.time.Instant

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.scalatest.{Matchers, WordSpec}

class TrackTimestampExtractorTest extends WordSpec with Matchers {
  val ts: String = "2020-05-30T14:00:00Z"
  val tsEpochSecond: Long = 1590847200L
  val tsEpochMillisecond: Long = tsEpochSecond * 1000L

  "TimestampExtractor.toTimestamp" when {
    "given 2020-05-30T14:00:00Z" should {
      "return the proper timestamp" in {
        TrackTimestampExtractor.toTimestamp(ts) shouldEqual Option(Timestamp.from(Instant.ofEpochSecond(tsEpochSecond)))
      }
    }

    "given an invalid timestamp" should {
      "return a None" in {
        TrackTimestampExtractor.toTimestamp(":!:") shouldEqual None
      }
    }
  }

  "TimestampExtractor.extract" when {
    "given a valid track record" should {
      "return the proper timestamp" in {
        val trackRecord = s"${ts}\tf1b1cf71-bd35-4e99-8624-24a6e15f133a\tDeep Dish\t\tFuck Me Im Famous (Pacha Ibiza)-09-28-2007"
        new TrackTimestampExtractor()
          .extract(new ConsumerRecord("topic", 0, 0, "userId1", trackRecord), 0) shouldEqual(tsEpochMillisecond)
      }
    }

    "given an invalid track record" should {
      "throw an exception" in {
        val trackRecord = s":!:\tf1b1cf71-bd35-4e99-8624-24a6e15f133a\tDeep Dish\t\tFuck Me Im Famous (Pacha Ibiza)-09-28-2007"

        assertThrows[RuntimeException] {
          new TrackTimestampExtractor().extract(new ConsumerRecord("topic", 0, 0, "userId1", trackRecord), 0)
        }
      }
    }
  }
}
