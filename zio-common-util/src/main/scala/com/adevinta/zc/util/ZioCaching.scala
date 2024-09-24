package com.adevinta.zc.util

import zio._

object ZioCaching {

  implicit class ZioCachedBy[R, E, A](zio: ZIO[R, E, A]) {
    def cachedBy[B](cacheRef: Ref[Map[B, Promise[E, A]]], key: B): ZIO[R, E, A] = {
      for {
        newPromise <- Promise.make[E, A]
        actualPromise <- cacheRef.modify { cache =>
          cache.get(key) match {
            case Some(existingPromise) => (existingPromise, cache)
            case None                  => (newPromise, cache + (key -> newPromise))
          }
        }
        _ <- ZIO.when(actualPromise eq newPromise) {
          zio.intoPromise(newPromise)
        }
        value <- actualPromise.await
      } yield value
    }
  }

}
