package com.adevinta.bq.gcswriter

import zio._
import zio.stream._
import zio.stream.ZStream.HaltStrategy

object ZstreamOps {

  implicit class Ops[R, E, A](input: ZStream[R, E, A]) {

    /** Creates a new stream that contains all elements of the original stream interleaved with
      * `marker`s that indicate that elements have been streaming for `duration`.
      *
      * The first marker is emitted `duration` after seeing the first element. The second marker is
      * emitted `duration` after seeing an element after the first marker, and so on.
      *
      * When the input stream ends and a marker is still pending, the last marker is emitted
      * immediately.
      */
    def timeInterleaved(duration: Duration, marker: A): ZStream[R, E, A] =
      ZstreamOps.timeInterleaved(duration, marker)(input)

    def timeInterleaved(duration: Duration): ZStream[R, E, Option[A]] =
      ZstreamOps.timeInterleaved[R, E, Option[A]](duration, None)(
        input.mapChunks(c => c.map(Some(_)))
      )
  }

  private def timeInterleaved[R, E, A](duration: Duration, marker: A)(
      input: ZStream[R, E, A]
  ): ZStream[R, E, A] = {
    ZStream.unwrapScoped[R] {
      for {
        scope <- ZIO.scope
        windowActive <- Ref.make[Boolean](false)
        markerQueue <- Queue.unbounded[A]
      } yield {
        val lastMarkerStream = ZStream.fromIterableZIO {
          windowActive.getAndSet(false).map(active =>
            if (active) Chunk.single(marker) else Chunk.empty
          )
        }
        (input ++ lastMarkerStream)
          .merge(ZStream.fromQueueWithShutdown(markerQueue), HaltStrategy.Left)
          .chunksWith {
            _.tap { chunk =>
              for {
                startTimer <-
                  if (chunk == Chunk.single(marker)) ZIO.succeed(false)
                  else windowActive.getAndSet(true).negate
                _ <- ZIO.when(startTimer) {
                  ZIO.whenZIO(windowActive.getAndSet(false))(markerQueue.offer(marker))
                    .delay(duration)
                    .forkIn(scope)
                }
              } yield ()
            }
          }
      }
    }
  }

}
