package com.adevinta.bq.gcswriter

import com.adevinta.bq.gcswriter.ZstreamOps._
import zio._
import zio.stream._
import zio.test._

object ZstreamOpsSpec extends ZIOSpecDefault {

  override def spec: Spec[TestEnvironment, Any] =
    suite("ZstreamOpsSpec")(
      test("timeInterleaved for an empty stream") {
        for {
          result <- ZStream.empty
            .timeInterleaved(10.millis, -1)
            .runCollect
        } yield assertTrue(result.isEmpty)
      },
      test("timeInterleaved for a single chunk") {
        for {
          result <- ZStream.fromIterable(1 to 10)
            .timeInterleaved(10.millis, -1)
            .runCollect
        } yield assertTrue(result == Chunk.fromIterable(1 to 10) ++ Chunk.single(-1))
      },
      test("timeInterleaved for time separated chunks") {
        for {
          fib <- ZStream.fromSchedule(Schedule.fixed(5.millis) <* Schedule.recurs(7))
            .timeInterleaved(7.millis, -1)
            .mapZIO(e => Clock.instant.map(i => (i.toEpochMilli, e)))
            .runCollect
            .fork
          _ <- TestClock.adjust(1.millis).repeatN(5 * 7)
          result <- fib.join
        } yield {
          val startTime = result.head._1
          val relativeResult = result.map(t => (t._1 - startTime, t._2))
          // format: off
          val expectedResult = Chunk[(Long, Long)](
            (0, 0),     // 0ms => 0
            (5, 1),     // 5ms => 1
            (7, -1),    // 0ms + 7ms => -1
            (10, 2),    // 10ms => 2
            (15, 3),    // 15ms => 3
            (17, -1),   // 10 + 7ms => -1
            (20, 4),    // 20ms => 4
            (25, 5),    // 25ms => 5
            (27, -1),   // 20ms + 7ms => -1
            (30, 6),    // 30ms => 6
            (30, -1),   // stream end => -1
          )
          // format: on
          assertTrue(relativeResult == expectedResult)
        }
      },
      test("timeInterleaved retains chunking structure") {
        for {
          result <- ZStream.fromIterator(Iterator.from(0).take(1000), maxChunkSize = 10)
            .timeInterleaved(10.millis, -1)
            .chunks
            .map(_.size)
            .runCollect
        } yield {
          assertTrue(result.head == 10)
        }
      }
    )

}
