package com.adevinta.zc.metrics

import io.prometheus.client.Counter
import io.prometheus.client.{Histogram => PHistogram}
import zio._

object ZioInstrumented {

  val DefaultTimerBuckets: Array[Double] =
    Array(0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1.0, 2.5, 5.0, 7.5, 10.0)

  val DefaultBatchSizeBuckets: Array[Double] =
    Array(10.0, 100.0, 1000.0, 5000.0, 10000.0, 25000.0, 50000.0)

  /** Allows the following syntax:
    *
    * {{{
    *   val counter = Counter.build(...).register(...)
    *   val task: ZIO[R, E, A] = ???
    *
    *   // Count success only:
    *   val task1: ZIO[R, E, A] = task <* counter.incIO
    *   // Count success and failure:
    *   val task2: ZIO[R, E, A] = task ensuring counter.incIO
    * }}}
    */
  implicit class ZioCounter(counter: Counter) {
    def incIO: UIO[Unit] = ZIO.succeed(counter.inc())

    /** @throws IllegalArgumentException when 'increase' is negative */
    def incIO(increase: Double): UIO[Unit] = ZIO.succeed(counter.inc(increase))
  }

  /** Allows the following syntax:
    *
    * {{{
    *   val timer = Histogram.build(...).register(...)
    *   val task: ZIO[R, E, A] = ???
    *
    *   // A new task that also times `task`:
    *   val timedTask: ZIO[R, E, A] = task.time(timer)
    * }}}
    */
  implicit class ZioTime[R, E, A](task: ZIO[R, E, A]) {
    def time(timer: PHistogram): ZIO[R, E, A] = {
      task
        .timed
        .map { case (duration, result) =>
          val durationInSeconds =
            BigDecimal(duration.getSeconds) + BigDecimal(duration.getNano.toLong, 9)
          timer.observe(durationInSeconds.toDouble)
          result
        }
    }
  }

  /** Allows the following syntax:
    *
    * {{{
    *   val histogram = Histogram.build(...).register(...)
    *
    *   val observerTask: ZIO[Any, Throwable, Unit] = histogram.observeZIO(1.0)
    * }}}
    */
  implicit class HistogramObserveZIO(val histogram: PHistogram) extends AnyVal {
    def observeZIO(amt: Double): ZIO[Any, Throwable, Unit] = ZIO.attempt(histogram.observe(amt))

  }
}
